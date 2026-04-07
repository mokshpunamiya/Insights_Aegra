"""
Cognitive Map Vector Store
==========================
Embeds the cognitive map JSON directly into Qdrant — no LangChain Document
wrapper needed.  Each JSON entry is flattened into:

    text_to_embed  – plain text built from the JSON fields  (what gets vectorised)
    payload        – the original JSON fields stored as-is  (what comes back)

Five entry types upserted:
    type="table"         – one point per table
    type="column"        – one point per column  (granular field matching)
    type="metric"        – one point per business metric
    type="gotcha"        – one point per common gotcha
    type="business_rule" – one point per business rule string

Dependencies
------------
    pip install qdrant-client langchain requests

Environment variables  (all read from .env via python-dotenv)
-------------------------------------------------------------
    QDRANT_URL            – Qdrant endpoint  (default: http://localhost:6333)
    QDRANT_API_KEY        – Qdrant Cloud API key
    QDRANT_COLLECTION     – Collection name   (default: cognitive_map_knowledge)

    COHERE_EMBED_URL      – Azure Cohere inference endpoint
    COHERE_EMBED_MODEL    – Model name        (default: embed-v-4-0)
    COHERE_API_KEY        – Azure Cohere API key
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

from langchain.tools import tool
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import PointStruct

# Shared Azure Cohere embeddings — reads COHERE_EMBED_URL / COHERE_API_KEY / COHERE_EMBED_MODEL
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from eerly_studio.insights.utils.embeddings import AzureCohereEmbeddings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Internal keys stamped by build_cognitive_map() that should never be embedded
_SKIP_KEYS = {"_table_names", "_skipped_chunks", "_error", "_build_meta"}

# Collection name — fetched dynamically so load_dotenv() order doesn't break it
def _get_default_collection() -> str:
    return os.getenv("QDRANT_COLLECTION", "cognitive_map_knowledge")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid(type_: str, *parts: str) -> str:
    """Deterministic UUID — same input always produces the same point ID."""
    key = f"{type_}::" + "::".join(p for p in parts if p)
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


# ---------------------------------------------------------------------------
# JSON → (text, payload) pairs
# Each function returns a list of (text_to_embed, payload_dict) tuples.
# The payload is stored verbatim in Qdrant — no information is lost.
# ---------------------------------------------------------------------------

def _table_points(table_knowledge: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """One point per table — overview level."""
    points = []
    for table_key, info in table_knowledge.items():
        table_name  = info.get("table_name") or table_key
        schema      = info.get("schema", "")
        schema_desc = info.get("schema_description", "")

        # Build the text that will be embedded.
        # More context → better semantic matching.
        lines = [f"Table: {table_key}"]
        if schema:
            lines.append(f"Schema: {schema}")
        if schema_desc:
            lines.append(f"Schema Context: {schema_desc}")
        if desc := info.get("table_description"):
            lines.append(f"Description: {desc}")
        if use_cases := info.get("use_cases"):
            lines.append(f"Use Cases: {', '.join(use_cases)}")
        if notes := info.get("data_quality_notes"):
            lines.append(f"Data Quality Notes: {' | '.join(notes)}")
        if cols := info.get("table_columns", []):
            col_details = [f"{c.get('name')} ({c.get('type')})" for c in cols]
            lines.append(f"Columns: {', '.join(col_details)}")

        text = "\n".join(lines)

        # Payload = full JSON entry + search-friendly top-level fields.
        # Storing the full entry means a retrieval hit gives you everything
        # without a second lookup.
        payload = {
            "type":               "table",
            "table_key":          table_key,
            "table_name":         table_name,
            "schema":             schema,
            "schema_description": schema_desc,
            "table_description":  info.get("table_description", ""),
            "use_cases":          info.get("use_cases", []),
            "data_quality_notes": info.get("data_quality_notes", []),
            "table_columns":      info.get("table_columns", []),  # full column list
            "id":                 _uid("table", table_key),
        }
        points.append((text, payload))
    return points


def _column_points(table_knowledge: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """One point per column — granular field-level matching."""
    points = []
    for table_key, info in table_knowledge.items():
        table_name  = info.get("table_name") or table_key
        schema      = info.get("schema", "")
        schema_desc = info.get("schema_description", "")
        table_desc  = info.get("table_description", "")

        for col in info.get("table_columns", []):
            col_name = col.get("name", "unknown")
            col_type = col.get("type", "unknown")
            col_desc = col.get("description", "")

            lines = [
                f"Column: {col_name}",
                f"Table: {table_key}",
                f"Data Type: {col_type}",
            ]
            if schema:
                lines.append(f"Schema: {schema}")
            if col_desc:
                lines.append(f"Description: {col_desc}")
            if schema_desc:
                lines.append(f"Schema Context: {schema_desc[:100]}")
            if table_desc:
                lines.append(f"Parent Table Context: {table_desc[:120]}")

            text = "\n".join(lines)

            payload = {
                "type":                "column",
                "table_key":           table_key,
                "table_name":          table_name,
                "schema":              schema,
                "column_name":         col_name,
                "data_type":           col_type,
                "column_description":  col_desc,
                "table_description":   table_desc,
                "id":                  _uid("column", table_key, col_name),
            }
            points.append((text, payload))
    return points


def _metric_points(business_knowledge: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """One point per metric — {name, definition, table, calculation}."""
    points = []
    for metric in business_knowledge.get("metrics", []):
        name = metric.get("name", "unknown_metric")
        lines = [f"Metric: {name}"]
        if defn := metric.get("definition"):
            lines.append(f"Definition: {defn}")
        if table := metric.get("table"):
            lines.append(f"Primary Table: {table}")
        if calc := metric.get("calculation"):
            lines.append(f"Calculation: {calc}")

        payload = {
            "type":        "metric",
            "metric_name": name,
            **{k: v for k, v in metric.items()},   # full metric dict preserved
            "id":          _uid("metric", name),
        }
        points.append(("\n".join(lines), payload))
    return points


def _gotcha_points(business_knowledge: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """One point per gotcha — {issue, tables_affected, solution}."""
    points = []
    for i, gotcha in enumerate(business_knowledge.get("common_gotchas", [])):
        issue = gotcha.get("issue", f"gotcha_{i}")
        lines = []
        if issue:
            lines.append(f"Issue: {issue}")
        if tables := gotcha.get("tables_affected"):
            lines.append(f"Tables Affected: {', '.join(tables)}")
        if solution := gotcha.get("solution"):
            lines.append(f"Solution: {solution}")

        payload = {
            "type":            "gotcha",
            "issue":           issue,
            "tables_affected": gotcha.get("tables_affected", []),
            "solution":        gotcha.get("solution", ""),
            "id":              _uid("gotcha", issue),
        }
        points.append(("\n".join(lines), payload))
    return points


def _rule_points(business_knowledge: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """One point per business rule string."""
    points = []
    for i, rule in enumerate(business_knowledge.get("business_rules", [])):
        if rule in _SKIP_KEYS:
            continue
        payload = {
            "type": "business_rule",
            "rule": rule,
            "id":   _uid("business_rule", str(i), rule[:60]),
        }
        points.append((f"Business Rule: {rule}", payload))
    return points


def _cog_map_to_points(cog_map: Dict[str, Any]) -> List[Tuple[str, Dict]]:
    """
    Master function: walk the cognitive map JSON and produce every
    (text_to_embed, payload) pair.  Skips all internal meta keys.
    """
    table_knowledge    = cog_map.get("table_knowledge", {})
    business_knowledge = {
        k: v for k, v in cog_map.get("business_knowledge", {}).items()
        if k not in _SKIP_KEYS
    }

    all_points: List[Tuple[str, Dict]] = []
    all_points.extend(_table_points(table_knowledge))
    all_points.extend(_column_points(table_knowledge))
    all_points.extend(_metric_points(business_knowledge))
    all_points.extend(_gotcha_points(business_knowledge))
    all_points.extend(_rule_points(business_knowledge))

    logger.info(f"Prepared {len(all_points)} points for embedding")
    return all_points


# ---------------------------------------------------------------------------
# QdrantKnowledgeStore
# ---------------------------------------------------------------------------

class QdrantKnowledgeStore:
    """
    Thin wrapper around QdrantClient that handles:
      - collection creation / dimension detection
      - batch embedding via OpenAI
      - upsert with deterministic IDs (idempotent sync)
      - filtered similarity search
    """

    def __init__(self, collection_name: Optional[str] = None):
        self.collection_name = collection_name or _get_default_collection()
        self.client          = QdrantClient(
            url=os.getenv("QDRANT_URL", "http://localhost:6333"),
            api_key=os.getenv("QDRANT_API_KEY"),
            timeout=120.0,
        )
        self.embeddings      = AzureCohereEmbeddings()   # reads COHERE_* from env
        self._vector_size: Optional[int] = None
        self._ensure_collection()   # ensures collection + payload indexes exist

    def _get_vector_size(self) -> int:
        """Returns the fixed dimension for Azure Cohere (embed-v-4-0 is 1536)."""
        if self._vector_size is None:
            # Statically fallback to 1536 to avoid the 'probe' API call
            self._vector_size = int(os.getenv("COHERE_EMBED_DIMENSION", "1536"))
        return self._vector_size

    def _ensure_collection(self) -> None:
        existing = [c.name for c in self.client.get_collections().collections]
        if self.collection_name not in existing:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self._get_vector_size(),
                    distance=models.Distance.COSINE,
                ),
            )
            logger.info(f"Created collection '{self.collection_name}'")

        # Always ensure payload indexes exist for filtered search
        # (safe to call repeatedly — no-ops if index already exists)
        for field_name in ("type", "table_name", "schema"):
            try:
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field_name,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
            except Exception:
                pass  # index already exists

    # ------------------------------------------------------------------
    # Sync  (JSON → embed → upsert)
    # ------------------------------------------------------------------

    def sync_knowledge(
        self,
        cog_map: Dict[str, Any],
        batch_size: int = 10,
    ) -> str:
        """
        Embed the cognitive map and upsert every point into Qdrant.

        Flow:
            1. Walk cog_map JSON → list of (text, payload) pairs
            2. Batch-embed all texts in one go
            3. Upsert PointStructs with deterministic IDs

        Safe to call repeatedly — same ID = upsert, not duplicate.
        """
        self._ensure_collection()
        all_points = _cog_map_to_points(cog_map)

        if not all_points:
            return "Nothing to sync — cognitive map appears empty."

        texts    = [text for text, _ in all_points]
        payloads = [payload for _, payload in all_points]

        total   = len(texts)
        upserted = 0

        for i in range(0, total, batch_size):
            batch_texts    = texts[i : i + batch_size]
            batch_payloads = payloads[i : i + batch_size]

            try:
                # Embed the batch
                vectors = self.embeddings.embed_documents(batch_texts)

                # Build Qdrant PointStructs directly
                qdrant_points = [
                    PointStruct(
                        id=payload["id"],
                        vector=vector,
                        payload=payload,          # full JSON payload stored as-is
                    )
                    for vector, payload in zip(vectors, batch_payloads)
                ]

                self.client.upsert(
                    collection_name=self.collection_name,
                    points=qdrant_points,
                )
                upserted += len(qdrant_points)
                logger.info(
                    f"  Upserted batch {i // batch_size + 1} "
                    f"({len(qdrant_points)} points, {upserted}/{total} total)"
                )
            except Exception as e:
                logger.error(f"  Failed to upsert batch {i // batch_size + 1}: {e}")
                continue

        msg = f"Synced {upserted} points to '{self.collection_name}'"
        logger.info(msg)
        return msg

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def similarity_search(
        self,
        query: str,
        k: int = 5,
        filter_type: Optional[str] = None,
        table_name: Optional[str] = None,
        db_schema: Optional[str] = None,
    ) -> List[Dict]:
        """
        Embed query and search Qdrant with optional payload filters.

        All filter keys reference top-level payload fields stored during upsert.
        There is no "metadata." prefix — Qdrant stores payload flat.

        Args:
            query:       Natural-language question.
            k:           Max results.
            filter_type: "table" | "column" | "metric" | "gotcha" | "business_rule"
            table_name:  Filter by base table name.
            db_schema:   Filter by schema name (e.g. "sales").
        """
        query_vector = self.embeddings.embed_query(query)

        conditions: List[models.FieldCondition] = []
        if filter_type:
            conditions.append(
                models.FieldCondition(key="type", match=models.MatchValue(value=filter_type))
            )
        if table_name:
            conditions.append(
                models.FieldCondition(key="table_name", match=models.MatchValue(value=table_name))
            )
        if db_schema:
            conditions.append(
                models.FieldCondition(key="schema", match=models.MatchValue(value=db_schema))
            )

        qdrant_filter = models.Filter(must=conditions) if conditions else None

        response = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=k,
            query_filter=qdrant_filter,
            with_payload=True,
        )

        return [
            {
                "payload":         hit.payload,
                "relevance_score": round(hit.score, 4),
            }
            for hit in response.points
        ]

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------

    def search_tables(self, query: str, schema: Optional[str] = None, k: int = 5) -> List[Dict]:
        return self.similarity_search(query, k=k, filter_type="table", db_schema=schema)

    def search_columns(
        self,
        query: str,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        k: int = 5,
    ) -> List[Dict]:
        return self.similarity_search(
            query, k=k, filter_type="column", table_name=table_name, db_schema=schema
        )

    def search_metrics(self, query: str, k: int = 5) -> List[Dict]:
        return self.similarity_search(query, k=k, filter_type="metric")

    def search_gotchas(self, query: str, k: int = 5) -> List[Dict]:
        return self.similarity_search(query, k=k, filter_type="gotcha")

    def drill_down(self, column_name: str, table_name: str) -> List[Dict]:
        """Precise single-column lookup."""
        return self.similarity_search(
            query=f"column {column_name} in table {table_name}",
            k=1,
            filter_type="column",
            table_name=table_name,
        )


# ---------------------------------------------------------------------------
# LangChain @tool  (used by Data Engineer Node)
# ---------------------------------------------------------------------------

@tool
def cognitive_map_vector_search(
    query: str,
    k: int = 5,
    filter_type: Optional[str] = None,
    table_name: Optional[str] = None,
    db_schema: Optional[str] = None,
) -> str:
    """
    Search the cognitive map knowledge base stored in Qdrant.

    Use this to find:
    - Database tables and their purpose  (filter_type="table")
    - Specific columns and their types   (filter_type="column")
    - Business metric definitions        (filter_type="metric")
    - Common SQL gotchas / pitfalls      (filter_type="gotcha")
    - Business rules                     (filter_type="business_rule")

    Args:
        query:       Natural-language question,
                     e.g. "which column stores position as TEXT"
        k:           Number of results (default 5)
        filter_type: One of "table" | "column" | "metric" | "gotcha" |
                     "business_rule".  Leave None to search all types.
        table_name:  Restrict to a specific table (base name).
        db_schema:   Restrict to a specific schema (e.g. "sales").

    Returns:
        Formatted string of ranked results with full payload.
    """
    try:
        store   = QdrantKnowledgeStore()
        results = store.similarity_search(
            query=query, k=k, filter_type=filter_type,
            table_name=table_name, db_schema=db_schema,
        )

        if not results:
            return f"No results found for: '{query}'"

        type_emoji = {
            "table":         "📊",
            "column":        "🔢",
            "metric":        "📈",
            "gotcha":        "⚠️",
            "business_rule": "📋",
        }

        lines = [f"Search results for: '{query}'\n{'─' * 50}"]
        for i, r in enumerate(results, 1):
            p     = r["payload"]
            t     = p.get("type", "")
            emoji = type_emoji.get(t, "📄")

            lines.append(f"\n{emoji} Result {i}  [{t}]  score={r['relevance_score']}")

            if s := p.get("schema"):
                lines.append(f"   Schema : {s}")
            if tkey := p.get("table_key"):
                lines.append(f"   Table  : {tkey}")
            if col := p.get("column_name"):
                lines.append(f"   Column : {col}  ({p.get('data_type', '')})")
            if metric := p.get("metric_name"):
                lines.append(f"   Metric : {metric}")
            if issue := p.get("issue"):
                lines.append(f"   Issue  : {issue}")

            # Show the most informative field per type
            if t == "table":
                lines.append(p.get("table_description", ""))
            elif t == "column":
                lines.append(p.get("column_description", ""))
            elif t == "metric":
                lines.append(p.get("definition", ""))
            elif t == "gotcha":
                lines.append(p.get("solution", ""))
            elif t == "business_rule":
                lines.append(p.get("rule", ""))

        return "\n".join(lines)

    except Exception as exc:
        return f"Search failed: {exc}"


