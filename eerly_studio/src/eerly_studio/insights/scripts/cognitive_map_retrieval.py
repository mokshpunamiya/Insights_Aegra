"""
Cognitive Map Retrieval  –  Large-Scale Edition
===============================================
Builds two knowledge directories for the Data Engineer Node:

  1. TABLE KNOWLEDGE DIRECTORY
     Per-table metadata enriched by an LLM:
       - table_description, use_cases, data_quality_notes
       - per-column descriptions

  2. BUSINESS KNOWLEDGE DIRECTORY
     Cross-table intelligence enriched by an LLM via Map-Reduce:
       - metrics definitions + calculations
       - business_rules
       - common_gotchas (type mismatches, nulls, naming inconsistencies, …)

Architecture improvements for large-scale databases (1000+ tables)
------------------------------------------------------------------
  ■ Map-Reduce business knowledge  – schema split into ~60k-token chunks
  ■ Column chunking                – wide tables (>50 cols) split per batch;
                                     sample_rows attached to EVERY chunk
  ■ Parallel table processing      – ThreadPoolExecutor (10 workers)
  ■ Per-table disk caching         – keyed by schema hash; incremental updates
  ■ LLM call timeouts              – 30-second hard cap per call
  ■ Retry with exponential back-off – handles OpenAI 429 / transient errors
  ■ Human-editable JSON layer       – export to / import from cognitive_map.json;
                                     JSON file takes priority over pkl cache
  ■ Smart business invalidation     – only rebuild if table LIST changes

Dependencies
------------
    pip install langchain langchain-openai sqlalchemy tenacity

Environment variables
---------------------
    OPENAI_API_KEY             – for LLM calls
    ADVENTURE_DATABASE_URL     – SQLAlchemy connection string
    COGNITIVE_MAP_CACHE_DIR    – (optional) directory for pickle cache files;
                                 defaults to  "./cognitive_map_cache/"
    COGNITIVE_MAP_JSON_PATH    – (optional) human-editable JSON override;
                                 defaults to  "./cognitive_map.json"
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import logging
import os
import pickle
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

# Load .env automatically so the module works even when called directly
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)   # don't override values already in the environment
except ImportError:
    pass  # python-dotenv not installed — env vars must be set externally

import sqlalchemy
from langchain.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from sqlalchemy import create_engine, inspect, text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

# Prompts loaded from prompts/ directory (Jinja2 templates)
try:
    from eerly_studio.insights.prompts import load_prompt as _load_prompt
except ImportError:
    # Fallback for when run directly as a script (sys.path may differ)
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent.parent))
    from eerly_studio.insights.prompts import load_prompt as _load_prompt

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

SAMPLE_ROW_LIMIT      = 3       # rows sampled per table
COLUMN_CHUNK_SIZE     = 50      # max columns per LLM call for wide tables
BUSINESS_CHUNK_TOKENS = 60_000  # target tokens per business-knowledge chunk
AVG_CHARS_PER_TOKEN   = 4       # rough estimate for token counting
MAX_WORKERS           = 10      # ThreadPoolExecutor parallelism
LLM_TIMEOUT_SECONDS   = 30      # per-call timeout

import pathlib
_INSIGHTS_DIR = pathlib.Path(__file__).parent.parent
CACHE_DIR        = os.getenv("COGNITIVE_MAP_CACHE_DIR",  str(_INSIGHTS_DIR / "cognitive_map_output" / "cognitive_map_cache"))
JSON_EXPORT_PATH = os.getenv("COGNITIVE_MAP_JSON_PATH",  str(_INSIGHTS_DIR / "cognitive_map_output" / "cognitive_map.json"))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry decorator for LLM calls  (handles 429 / transient errors)
# ---------------------------------------------------------------------------

_llm_retry = retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)

# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------

_llm: Optional[Any] = None       # AzureChatOpenAI or ChatOpenAI
_engine: Optional[sqlalchemy.engine.Engine] = None


def _get_llm() -> Any:
    """
    Return the LLM singleton.

    Priority:
      1. Azure Mistral  – if MISTRAL_OPENAI_API_KEY is set in the environment
         (set via .env or shell).  Uses AzureChatOpenAI with the deployment
         named by MISTRAL_OPENAI_LLM_DEPLOYMENT.
      2. Standard OpenAI  – falls back to OPENAI_API_KEY / gpt-4o.

    This avoids conflicts with the application-level OPENAI_API_KEY that the
    rest of the system (agents, orchestrators) may be using.
    """
    global _llm
    if _llm is None:
        mistral_key  = os.getenv("MISTRAL_OPENAI_API_KEY")
        mistral_ep   = os.getenv("MISTRAL_OPENAI_ENDPOINT")
        mistral_ver  = os.getenv("MISTRAL_OPENAI_API_VERSION", "2024-05-01-preview")
        mistral_dep  = os.getenv("MISTRAL_OPENAI_LLM_DEPLOYMENT", "mistral-medium-2505")

        if mistral_key and mistral_ep:
            # AzureChatOpenAI stores the deployment as .model in newer versions;
            # we log from local vars to avoid attribute errors.
            print(
                f"[CognitiveMap] Using Azure Mistral LLM "
                f"(endpoint={mistral_ep}, deployment={mistral_dep})"
            )
            _llm = AzureChatOpenAI(
                azure_endpoint=mistral_ep,
                azure_deployment=mistral_dep,
                openai_api_version=mistral_ver,
                openai_api_key=mistral_key,
                temperature=0,
            )
        else:
            print("[CognitiveMap] Falling back to OPENAI_API_KEY / gpt-4o")
            _llm = ChatOpenAI(model="gpt-4o", temperature=0)
    return _llm


def reset_llm_singleton() -> None:
    """Force the next _get_llm() call to re-read env vars and create a fresh LLM.

    Useful in tests that swap credentials between test cases.
    """
    global _llm
    _llm = None


def _get_engine() -> sqlalchemy.engine.Engine:
    global _engine
    if _engine is None:
        db_url = os.environ["ADVENTURE_DATABASE_URL"]
        # Schema reflection uses synchronous calls; avoid asyncpg error.
        if "+asyncpg" in db_url:
            db_url = db_url.replace("+asyncpg", "")
        _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine


# ---------------------------------------------------------------------------
# NEW Helper utilities
# ---------------------------------------------------------------------------

def _strip_fences(text: str) -> str:
    """Remove accidental markdown code fences from LLM output."""
    text = re.sub(r"^```[a-z]*\n?", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\n?```$", "", text)
    return text.strip()


def _schema_hash(raw_meta: Dict[str, Any]) -> str:
    """
    Deterministic SHA-256 hash of a table's structural metadata.
    Used as the cache key so only changed tables are re-processed.
    """
    canonical = json.dumps(raw_meta, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: len(chars) / 4."""
    return len(text) // AVG_CHARS_PER_TOKEN


def _calculate_optimal_chunk_size(raw_schema: Dict[str, Any]) -> int:
    """
    Dynamically compute how many tables fit in one business-knowledge chunk
    while staying under BUSINESS_CHUNK_TOKENS.

    Returns an integer >= 1 (always at least one table per chunk).
    """
    if not raw_schema:
        return 1

    total_chars = sum(
        len(json.dumps(meta, default=str))
        for meta in raw_schema.values()
    )
    avg_chars_per_table = total_chars / len(raw_schema)
    tables_per_chunk = max(
        1,
        int((BUSINESS_CHUNK_TOKENS * AVG_CHARS_PER_TOKEN) / avg_chars_per_table),
    )
    return tables_per_chunk


# ---------------------------------------------------------------------------
# Caching system  (CognitiveMapBuilder)
# ---------------------------------------------------------------------------

class CognitiveMapBuilder:
    """
    Manages per-table caching on disk.

    Cache layout:
        <CACHE_DIR>/
            table_<hash>.pkl      – enriched table dict
            business_<hash>.pkl   – a merged business-knowledge dict shard
            meta.json             – index: table_name → hash
    """

    def __init__(self) -> None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._meta_path = os.path.join(CACHE_DIR, "meta.json")
        self._meta: Dict[str, str] = self._load_meta()

    # ── meta index ────────────────────────────────────────────────────────

    def _load_meta(self) -> Dict[str, str]:
        if os.path.exists(self._meta_path):
            with open(self._meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_meta(self) -> None:
        with open(self._meta_path, "w", encoding="utf-8") as f:
            json.dump(self._meta, f, indent=2)

    # ── per-table cache ───────────────────────────────────────────────────

    def _table_cache_path(self, schema_hash: str) -> str:
        return os.path.join(CACHE_DIR, f"table_{schema_hash}.pkl")

    def is_table_cached(self, table_name: str, schema_hash: str) -> bool:
        """True iff cached hash matches current hash."""
        return (
            self._meta.get(table_name) == schema_hash
            and os.path.exists(self._table_cache_path(schema_hash))
        )

    def load_table_cache(self, table_name: str, schema_hash: str) -> Optional[Dict[str, Any]]:
        path = self._table_cache_path(schema_hash)
        if os.path.exists(path):
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    def save_table_cache(
        self, table_name: str, schema_hash: str, enriched: Dict[str, Any]
    ) -> None:
        path = self._table_cache_path(schema_hash)
        with open(path, "wb") as f:
            pickle.dump(enriched, f)
        self._meta[table_name] = schema_hash
        self._save_meta()

    # ── business knowledge cache ──────────────────────────────────────────

    def _biz_cache_path(self) -> str:
        return os.path.join(CACHE_DIR, "business_knowledge.pkl")

    def load_business_cache(self) -> Optional[Dict[str, Any]]:
        path = self._biz_cache_path()
        if os.path.exists(path):
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    def save_business_cache(self, business_knowledge: Dict[str, Any]) -> None:
        path = self._biz_cache_path()
        with open(path, "wb") as f:
            pickle.dump(business_knowledge, f)

    def invalidate_business_cache(self) -> None:
        """Called whenever any table changes — forces business knowledge rebuild."""
        path = self._biz_cache_path()
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# Human-editable JSON export / import
# ---------------------------------------------------------------------------

def export_cognitive_map_to_json(
    cognitive_map: Dict[str, Any],
    path: str = JSON_EXPORT_PATH,
) -> str:
    """
    Dump the full cognitive map to a human-readable, formatted JSON file.

    Use this after a successful build so analysts can hand-edit table
    descriptions, use_cases, or business rules.  The file is then picked up
    automatically on the next call to build_cognitive_map() (JSON takes
    priority over the pkl cache).

    Args:
        cognitive_map: Dict returned by build_cognitive_map().
        path:          Destination JSON file path.
                       Defaults to JSON_EXPORT_PATH env var or ./cognitive_map.json.

    Returns:
        Absolute path to the written file.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cognitive_map, f, indent=2, ensure_ascii=False, default=str)
    logger.info("[CognitiveMap] Exported to %s", path)
    print(f"[CognitiveMap] Cognitive map exported → {path}")
    return os.path.abspath(path)


def load_cognitive_map_from_json(path: str = JSON_EXPORT_PATH) -> Dict[str, Any]:
    """
    Load a previously exported (or hand-edited) cognitive map JSON file.

    This is called automatically by build_cognitive_map() when the file exists
    and force_refresh=False.  You can also call it directly to inspect or
    diff cognitive maps.

    Args:
        path: Source JSON file.  Defaults to JSON_EXPORT_PATH.

    Returns:
        The cognitive map dict  (same shape as build_cognitive_map() output).

    Raises:
        FileNotFoundError: if the path does not exist.
        json.JSONDecodeError: if the file is not valid JSON.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Cognitive map JSON not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("[CognitiveMap] Loaded from %s", path)
    return data


# ---------------------------------------------------------------------------
# Step 1 – Raw structural extraction
# ---------------------------------------------------------------------------

def _extract_raw_schema() -> Dict[str, Any]:
    """
    Inspects every table across all non-system schemas and returns raw metadata dict.

    Shape per table:
        {
          "columns": [{"name", "type", "nullable", "default"}, ...],
          "primary_keys": [...],
          "foreign_keys": [{"constrained_columns", "referred_table", ...}],
          "unique_constraints": [...],
          "sample_rows": [[val, ...], ...]
        }
    """
    engine = _get_engine()
    inspector = inspect(engine)
    raw: Dict[str, Any] = {}

    # Common system schemas to exclude
    EXCLUDED_SCHEMAS = ("information_schema", "pg_catalog", "pg_toast")

    for schema in inspector.get_schema_names():
        if schema.lower() in EXCLUDED_SCHEMAS:
            continue
            
        for table_name in inspector.get_table_names(schema=schema):
            # Use qualified name as key if not in public schema
            full_table_name = f"{schema}.{table_name}" if schema != "public" else table_name
            
            # columns
            columns = []
            for col in inspector.get_columns(table_name, schema=schema):
                columns.append({
                    "name":     col["name"],
                    "type":     str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default":  str(col["default"]) if col.get("default") is not None else None,
                })

            # primary keys
            pk_info      = inspector.get_pk_constraint(table_name, schema=schema)
            primary_keys = pk_info.get("constrained_columns", [])

            # foreign keys
            foreign_keys = []
            for fk in inspector.get_foreign_keys(table_name, schema=schema):
                foreign_keys.append({
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table":      fk["referred_table"],
                    "referred_columns":    fk["referred_columns"],
                })

            # unique constraints
            unique_constraints = []
            try:
                unique_constraints = inspector.get_unique_constraints(table_name, schema=schema)
            except NotImplementedError:
                pass

            # sample rows (best-effort)
            sample_rows: List[List] = []
            try:
                col_names   = [c["name"] for c in columns]
                quoted_cols = ", ".join(f'"{c}"' for c in col_names)
                with engine.connect() as conn:
                    # Use qualified name in SQL
                    from_clause = f'"{schema}"."{table_name}"'
                    result = conn.execute(
                        text(f'SELECT {quoted_cols} FROM {from_clause} LIMIT {SAMPLE_ROW_LIMIT}')
                    )
                    for row in result:
                        sample_rows.append([str(v) if v is not None else None for v in row])
            except Exception:
                pass

            raw[full_table_name] = {
                "schema_name":        schema,
                "table_name":         table_name,
                "columns":            columns,
                "primary_keys":       primary_keys,
                "foreign_keys":       foreign_keys,
                "unique_constraints": unique_constraints,
                "sample_rows":        sample_rows,
            }

    return raw


# ---------------------------------------------------------------------------
# Step 2a – Table enrichment (normal tables ≤ COLUMN_CHUNK_SIZE columns)
# ---------------------------------------------------------------------------

# Table enrichment prompt loaded from prompts/cognitive_map_table_system.j2
_TABLE_SYSTEM_PROMPT = _load_prompt("cognitive_map_table_system")


@_llm_retry
def _enrich_table(table_name: str, raw_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send one table's raw metadata to the LLM (<= COLUMN_CHUNK_SIZE columns).
    Decorated with retry (3 attempts, exp back-off) so rate-limit 429s are
    handled automatically during parallel execution.
    Raises on persistent LLM or parse failure.
    """
    llm = _get_llm()

    schema     = raw_meta.get("schema_name", "unknown")
    table_base = raw_meta.get("table_name", table_name) # table_name is the qualified one

    # Inject a hint into raw_meta so the LLM sees the field name
    raw_meta["schema_description"] = f"Derive purpose from schema name: '{schema}'"

    user_content = (
        f"Schema: {schema}\n"
        f"Table name: {table_base}\n\n"
        f"Raw metadata:\n{json.dumps(raw_meta, indent=2)}\n\n"
        "Produce the enriched table knowledge JSON."
    )

    response = llm.invoke(
        [SystemMessage(content=_TABLE_SYSTEM_PROMPT), HumanMessage(content=user_content)],
        config={"timeout": LLM_TIMEOUT_SECONDS},
    )
    return json.loads(_strip_fences(response.content))


# ---------------------------------------------------------------------------
# Step 2a-wide – Column-chunking for wide tables (> COLUMN_CHUNK_SIZE cols)
# ---------------------------------------------------------------------------

# Column chunk prompt loaded from prompts/cognitive_map_column_chunk_system.j2
_COLUMN_CHUNK_SYSTEM_PROMPT = _load_prompt("cognitive_map_column_chunk_system")

# Column merge prompt loaded from prompts/cognitive_map_column_merge_system.j2
_COLUMN_MERGE_SYSTEM_PROMPT = _load_prompt("cognitive_map_column_merge_system")


@_llm_retry
def _enrich_table_chunk(
    table_name: str,
    column_subset: List[Dict[str, Any]],
    table_meta_minimal: Dict[str, Any],
    chunk_index: int,
) -> Dict[str, Any]:
    """
    Process a single column batch for a wide table.

    sample_rows is attached to EVERY chunk so the LLM has the same row-level
    context regardless of which column batch it is analysing.  Without this
    the model lacks values to ground descriptions of later columns.
    """
    schema         = table_meta_minimal.get("schema_name", "unknown")
    table_base     = table_meta_minimal.get("table_name", table_name)
    n_total_chunks = table_meta_minimal.get("_total_chunks", "N")


    partial_meta = {
        "columns":      column_subset,
        "schema_name":  schema,
        "table_name":   table_base,
        "primary_keys": table_meta_minimal.get("primary_keys", []),
        "foreign_keys": table_meta_minimal.get("foreign_keys", []),
        # ← sample_rows always included; gives every chunk row-level context
        "sample_rows":  table_meta_minimal.get("sample_rows", []),
    }

    user_content = (
        f"Schema: {schema}\n"
        f"Table name: {table_base}\n"
        f"Column batch {chunk_index + 1} of {n_total_chunks}\n\n"
        f"Raw metadata for this column subset (sample rows included for context):\n"
        f"{json.dumps(partial_meta, indent=2)}\n\n"
        "Produce the partial column documentation JSON."
    )

    response = llm.invoke(
        [SystemMessage(content=_COLUMN_CHUNK_SYSTEM_PROMPT), HumanMessage(content=user_content)],
        config={"timeout": LLM_TIMEOUT_SECONDS},
    )
    enriched = json.loads(_strip_fences(response.content))

    # Post-processing: ensure schema_description exists
    if "schema_description" not in enriched:
        enriched["schema_description"] = f"Data related to the {schema} domain."

    return enriched


def _merge_column_chunks(
    table_name: str,
    partial_results: List[Dict[str, Any]],
    raw_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Ask the LLM to merge all column-batch results into one enriched table dict."""
    schema     = raw_meta.get("schema_name", "unknown")
    table_base = raw_meta.get("table_name", table_name)

    # Inject a hint into raw_meta so the LLM sees the field name
    raw_meta["schema_description"] = f"Derive purpose from schema name: '{schema}'"

    user_content = (
        f"Schema: {schema}\n"
        f"Table name: {table_base}\n\n"
        f"Partial column analyses (one per batch):\n"
        f"{json.dumps(partial_results, indent=2)}\n\n"
        f"Table-level context (PKs, FKs, sample rows):\n"
        f"Schema       : {schema}\n"
        f"Primary keys : {raw_meta.get('primary_keys', [])}\n"
        f"Foreign keys : {raw_meta.get('foreign_keys', [])}\n"
        f"Sample rows  : {raw_meta.get('sample_rows', [])}\n\n"
        "Merge all partial results into the single final table knowledge JSON."
    )

    response = llm.invoke(
        [SystemMessage(content=_COLUMN_MERGE_SYSTEM_PROMPT), HumanMessage(content=user_content)],
        config={"timeout": LLM_TIMEOUT_SECONDS},
    )
    enriched = json.loads(_strip_fences(response.content))

    # Post-processing: ensure schema_description exists
    if "schema_description" not in enriched:
        enriched["schema_description"] = f"Data related to the {schema} domain."

    return enriched


def _enrich_table_with_chunking(table_name: str, raw_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point for table enrichment — auto-selects chunked vs direct path.

      ≤ COLUMN_CHUNK_SIZE columns → _enrich_table()
      >  COLUMN_CHUNK_SIZE columns → column-chunked map-reduce
    """
    schema     = raw_meta.get("schema_name", "unknown")
    table_base = raw_meta.get("table_name", table_name)
    columns    = raw_meta.get("columns", [])
    if len(columns) <= COLUMN_CHUNK_SIZE:
        return _enrich_table(table_name, raw_meta)

    # Wide table: split columns into chunks, process in parallel, merge
    logger.info(
        "[CognitiveMap] Wide table '%s' has %d columns — chunking into batches of %d.",
        table_name, len(columns), COLUMN_CHUNK_SIZE,
    )

    chunks = [
        columns[i : i + COLUMN_CHUNK_SIZE]
        for i in range(0, len(columns), COLUMN_CHUNK_SIZE)
    ]

    # Minimal meta shared across ALL chunks.
    # sample_rows is intentionally kept here so that _enrich_table_chunk
    # attaches it to every batch — the LLM needs row-level context to describe
    # columns in the 2nd/3rd/… chunk just as well as the first.
    minimal_meta = {
        "schema_name":        raw_meta.get("schema_name"),
        "table_name":         raw_meta.get("table_name"),
        "primary_keys":       raw_meta.get("primary_keys", []),
        "foreign_keys":       raw_meta.get("foreign_keys", []),
        "unique_constraints": raw_meta.get("unique_constraints", []),
        "sample_rows":        raw_meta.get("sample_rows", []),   # ← every chunk gets these
        "_total_chunks":      len(chunks),                        # informational for prompt
    }

    partial_results: List[Dict[str, Any]] = [None] * len(chunks)  # type: ignore[list-item]

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(chunks))) as pool:
        future_map = {
            pool.submit(_enrich_table_chunk, table_name, chunk, minimal_meta, idx): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                partial_results[idx] = future.result()
            except Exception as exc:
                logger.warning(
                    "[CognitiveMap] Column chunk %d for '%s' failed: %s", idx, table_name, exc
                )
                # Fallback: store raw column names so something is preserved
                partial_results[idx] = {
                    "schema": schema,
                    "schema_description": "unavailable",
                    "table_name": table_base,
                    "partial_description": f"Chunk {idx} enrichment failed.",
                    "table_columns": [
                        {"name": c["name"], "type": c["type"], "description": "unavailable"}
                        for c in chunks[idx]
                    ],
                }

    return _merge_column_chunks(table_name, partial_results, raw_meta)


# ---------------------------------------------------------------------------
# Step 2b – Business knowledge  (Map-Reduce)
# ---------------------------------------------------------------------------

# Business chunk prompt loaded from prompts/cognitive_map_business_chunk_system.j2
_BUSINESS_CHUNK_SYSTEM_PROMPT = _load_prompt("cognitive_map_business_chunk_system")

# Business merge prompt loaded from prompts/cognitive_map_business_merge_system.j2
_BUSINESS_MERGE_SYSTEM_PROMPT = _load_prompt("cognitive_map_business_merge_system")


@_llm_retry
def _process_business_chunk(
    chunk_index: int,
    chunk_tables: Dict[str, Any],
) -> Dict[str, Any]:
    """Process one schema chunk to extract partial business knowledge."""
    llm = _get_llm()

    schema_summary = {
        tbl: {col["name"]: col["type"] for col in meta["columns"]}
        for tbl, meta in chunk_tables.items()
    }

    user_content = (
        f"Schema chunk {chunk_index + 1} — tables in this chunk: "
        f"{list(chunk_tables.keys())}\n\n"
        f"Schema summary (table → column:type):\n"
        f"{json.dumps(schema_summary, indent=2)}\n\n"
        f"Full raw schema with samples:\n"
        f"{json.dumps(chunk_tables, indent=2)}\n\n"
        "Produce the PARTIAL business knowledge directory JSON for these tables."
    )

    response = llm.invoke(
        [SystemMessage(content=_BUSINESS_CHUNK_SYSTEM_PROMPT), HumanMessage(content=user_content)],
        config={"timeout": LLM_TIMEOUT_SECONDS},
    )
    return json.loads(_strip_fences(response.content))


def _merge_business_chunks(partial_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Ask the LLM to merge all chunk results into one final business knowledge dict.

    For very large numbers of chunks a recursive merge could be added, but a
    single merge pass is fine for ≤ 100 chunks (each summary is compact JSON).
    """
    if len(partial_results) == 1:
        return partial_results[0]

    llm = _get_llm()

    user_content = (
        f"Partial business knowledge documents ({len(partial_results)} chunks):\n\n"
        + "\n\n---\n\n".join(json.dumps(p, indent=2) for p in partial_results)
        + "\n\nMerge all partial results into the single final business knowledge JSON."
    )

    response = llm.invoke(
        [SystemMessage(content=_BUSINESS_MERGE_SYSTEM_PROMPT), HumanMessage(content=user_content)],
        config={"timeout": LLM_TIMEOUT_SECONDS},
    )
    return json.loads(_strip_fences(response.content))


def _enrich_business_knowledge_chunked(raw_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map-Reduce pipeline for business knowledge:

      1. Calculate optimal chunk size based on average table size
      2. Split schema into chunks of ~60k tokens each
      3. Process chunks in parallel (ThreadPoolExecutor)
      4. Merge all chunk results with one final LLM call

    Falls back to an empty knowledge dict if all chunks fail.
    """
    if not raw_schema:
        return {"metrics": [], "business_rules": [], "common_gotchas": []}

    tables     = list(raw_schema.items())
    chunk_size = _calculate_optimal_chunk_size(raw_schema)

    chunks: List[Dict[str, Any]] = []
    for i in range(0, len(tables), chunk_size):
        chunks.append(dict(tables[i : i + chunk_size]))

    logger.info(
        "[CognitiveMap] Business knowledge: %d tables → %d chunks of ~%d tables each.",
        len(tables), len(chunks), chunk_size,
    )

    partial_results: List[Dict[str, Any]] = []
    failed_chunks:   List[int]            = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(chunks))) as pool:
        future_map = {
            pool.submit(_process_business_chunk, idx, chunk): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                partial_results.append(future.result())
            except Exception as exc:
                logger.warning(
                    "[CognitiveMap] Business chunk %d failed: %s", idx, exc
                )
                failed_chunks.append(idx)

    if not partial_results:
        return {
            "metrics": [], "business_rules": [], "common_gotchas": [],
            "_error": f"All {len(chunks)} business knowledge chunks failed.",
        }

    if failed_chunks:
        logger.warning(
            "[CognitiveMap] %d/%d business chunks failed and were skipped: %s",
            len(failed_chunks), len(chunks), failed_chunks,
        )

    merged = _merge_business_chunks(partial_results)
    if failed_chunks:
        merged["_skipped_chunks"] = failed_chunks

    return merged


# Keep the original name as an alias for backward compatibility
def _enrich_business_knowledge(raw_schema: Dict[str, Any]) -> Dict[str, Any]:
    return _enrich_business_knowledge_chunked(raw_schema)


# ---------------------------------------------------------------------------
# Step 3 – Parallel table enrichment worker
# ---------------------------------------------------------------------------

def _enrich_table_worker(
    args: Tuple[str, Dict[str, Any], CognitiveMapBuilder],
) -> Tuple[str, Dict[str, Any]]:
    """
    Worker function executed inside ThreadPoolExecutor.

    Checks cache first; calls LLM only if the schema has changed.
    Returns (table_name, enriched_dict).
    """
    table_name, raw_meta, cache = args
    h = _schema_hash(raw_meta)

    # Cache hit
    if cache.is_table_cached(table_name, h):
        cached = cache.load_table_cache(table_name, h)
        if cached is not None:
            return table_name, cached

    # Cache miss — call LLM
    enriched = _enrich_table_with_chunking(table_name, raw_meta)
    cache.save_table_cache(table_name, h, enriched)
    return table_name, enriched


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_cognitive_map(force_refresh: bool = False) -> Dict[str, Any]:
    """
    Full pipeline (large-scale edition):

      1. Extract raw schema via SQLAlchemy
      2. For each table:
           a. Compute schema hash
           b. Load from per-table cache if unchanged → skip LLM
           c. Otherwise send to LLM (column-chunked if wide)
              All tables processed in parallel (ThreadPoolExecutor)
      3. Build business knowledge via Map-Reduce chunking
           (skipped if no table schemas changed AND biz cache exists)
      4. Return structured result with build metadata

    Returns:
        {
          "table_knowledge":   { "<table>": { enriched dict }, ... },
          "business_knowledge": { metrics, business_rules, common_gotchas },
          "_build_meta":       { strategy, tables_total, tables_cached,
                                 tables_processed, elapsed_seconds }
        }
    """
    t_start = time.time()

    # ── Priority 1: human-editable JSON override ──────────────────────────
    # If cognitive_map.json exists (and we're not forcing a refresh), load it
    # immediately and return.  This lets users hand-edit the JSON to fix
    # descriptions and have those edits survive subsequent runs.
    if not force_refresh and os.path.exists(JSON_EXPORT_PATH):
        print(f"[CognitiveMap] Loading human-editable JSON from {JSON_EXPORT_PATH} …")
        try:
            loaded = load_cognitive_map_from_json(JSON_EXPORT_PATH)
            print("[CognitiveMap] JSON override loaded — skipping LLM pipeline.")
            return loaded
        except Exception as exc:
            logger.warning(
                "[CognitiveMap] Could not load JSON override (%s) — falling back to pkl pipeline.",
                exc,
            )

    cache   = CognitiveMapBuilder()

    print("[CognitiveMap] Extracting raw schema from database…")
    raw_schema = _extract_raw_schema()
    n_total    = len(raw_schema)
    print(f"[CognitiveMap] Found {n_total} tables.")

    # ── Check which tables actually changed ───────────────────────────────
    changed_tables = [
        name
        for name, meta in raw_schema.items()
        if force_refresh or not cache.is_table_cached(name, _schema_hash(meta))
    ]
    cached_tables = [t for t in raw_schema if t not in changed_tables]

    print(
        f"[CognitiveMap] {len(cached_tables)} tables cached, "
        f"{len(changed_tables)} need LLM enrichment."
    )

    # ── Parallel table enrichment ─────────────────────────────────────────
    table_knowledge: Dict[str, Any] = {}

    # Load cached tables immediately (no I/O bottleneck)
    for name in cached_tables:
        h      = _schema_hash(raw_schema[name])
        cached = cache.load_table_cache(name, h)
        table_knowledge[name] = cached if cached is not None else {
            "schema": raw_schema[name].get("schema_name"),
            "schema_description": "unavailable",
            "table_name": raw_schema[name].get("table_name", name),
            "table_description": "Cache read error — raw metadata preserved.",
            "use_cases": [], "data_quality_notes": [],
            "table_columns": raw_schema[name]["columns"],
        }

    # Enrich changed tables in parallel
    if changed_tables:
        worker_args = [
            (name, raw_schema[name], cache)
            for name in changed_tables
        ]
        n_done = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_map = {
                pool.submit(_enrich_table_worker, arg): arg[0]
                for arg in worker_args
            }
            for future in as_completed(future_map):
                table_name = future_map[future]
                try:
                    _, enriched = future.result()
                    table_knowledge[table_name] = enriched
                except Exception as exc:
                    tb = traceback.format_exc()
                    logger.error("[CognitiveMap] ✗ %s: %s\n%s", table_name, exc, tb)
                    table_knowledge[table_name] = {
                        "schema": raw_schema[table_name].get("schema_name"),
                        "schema_description": "unavailable",
                        "table_name": raw_schema[table_name].get("table_name", table_name),
                        "table_description": "Enrichment failed — raw metadata preserved.",
                        "use_cases": [], "data_quality_notes": [],
                        "table_columns": raw_schema[table_name]["columns"],
                        "_error": str(exc),
                    }
                n_done += 1
                print(
                    f"\r[CognitiveMap] Tables enriched: {n_done}/{len(changed_tables)}",
                    end="", flush=True,
                )
        print()  # newline after progress

    # ── Business knowledge (Map-Reduce) ───────────────────────────────────
    # Smart invalidation: only rebuild if the SET of table names changed, or
    # force_refresh was requested.  A column description update alone (same
    # table names, different schema content) does NOT trigger a rebuild.
    current_table_names = frozenset(raw_schema.keys())
    biz_cached          = cache.load_business_cache() if not force_refresh else None

    if biz_cached:
        cached_table_names = frozenset(biz_cached.get("_table_names", []))
        table_list_changed = cached_table_names != current_table_names
    else:
        table_list_changed = True   # no cache at all → must build

    need_biz_rebuild = force_refresh or table_list_changed

    if biz_cached and not need_biz_rebuild:
        print("[CognitiveMap] Business knowledge loaded from cache (table list unchanged).")
        business_knowledge = biz_cached
    else:
        if table_list_changed and not force_refresh:
            print(
                "[CognitiveMap] Table list changed "
                f"({len(current_table_names - frozenset(biz_cached.get('_table_names', [])))} added / "
                f"{len(frozenset(biz_cached.get('_table_names', [])) - current_table_names)} removed) "
                "— rebuilding business knowledge."
            ) if biz_cached else None
        print("[CognitiveMap] Building business knowledge via Map-Reduce…")
        if force_refresh:
            cache.invalidate_business_cache()
        try:
            business_knowledge = _enrich_business_knowledge_chunked(raw_schema)
            # Stamp the table-name set so we can detect changes on next run
            business_knowledge["_table_names"] = sorted(current_table_names)
            cache.save_business_cache(business_knowledge)
        except Exception as exc:
            tb = traceback.format_exc()
            logger.error("[CognitiveMap] Business enrichment failed: %s\n%s", exc, tb)
            business_knowledge = {
                "metrics": [], "business_rules": [], "common_gotchas": [],
                "_error": str(exc),
            }

    # ── Final schema_description enforcement ──────────────────────────────
    for tbl, info in table_knowledge.items():
        if not info.get("schema_description") or info.get("schema_description") == "unavailable":
            s = info.get("schema") or (tbl.split('.')[0] if "." in tbl else "unknown")
            info["schema_description"] = f"Metadata and business context for the {s} schema."

    elapsed = round(time.time() - t_start, 1)

    result = {
        "table_knowledge":    table_knowledge,
        "business_knowledge": business_knowledge,
        "_build_meta": {
            "strategy":          "parallel+chunked",
            "tables_total":      n_total,
            "tables_cached":     len(cached_tables),
            "tables_processed":  len(changed_tables),
            "elapsed_seconds":   elapsed,
            "cache_dir":         CACHE_DIR,
        },
    }

    print(
        f"[CognitiveMap] Done in {elapsed}s — "
        f"{len(cached_tables)} cached, {len(changed_tables)} re-enriched."
    )
    return result


# ---------------------------------------------------------------------------
# LangChain Tools  (public API — signatures unchanged)
# ---------------------------------------------------------------------------

@tool
def cognitive_map_retriever_tool(
    query: str,
    section: str = "both",
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """
    Retrieves the cognitive map knowledge directories and filters them
    to the information most relevant to the user's query.

    Args:
        query:         Natural-language question or topic to focus on.
                       Used to filter which tables / metrics to surface.
                       Pass "all" to return the full map unfiltered.
        section:       Which directory to return —
                         "table"    → table knowledge directory only
                         "business" → business knowledge directory only
                         "both"     → (default) both directories
        force_refresh: If True, re-runs the full extraction + LLM enrichment
                       pipeline, ignoring all caches.

    Returns:
        {
          "table_knowledge":    { ... }   # present when section in ("table","both")
          "business_knowledge": { ... }   # present when section in ("business","both")
          "query_used":         str
          "tables_returned":    [str]
        }
    """
    try:
        cog_map = build_cognitive_map(force_refresh=force_refresh)

        table_knowledge    = cog_map.get("table_knowledge", {})
        business_knowledge = cog_map.get("business_knowledge", {})

        # Filter table knowledge to query-relevant tables
        if query.strip().lower() != "all":
            q_lower = query.lower()
            filtered_tables = {
                tbl: info
                for tbl, info in table_knowledge.items()
                if (
                    q_lower in tbl.lower()
                    or any(q_lower in uc.lower() for uc in info.get("use_cases", []))
                    or q_lower in info.get("table_description", "").lower()
                    or any(
                        q_lower in col.get("description", "").lower()
                        for col in info.get("table_columns", [])
                    )
                )
            }
            # Broad fallback: if nothing matched, return everything
            if not filtered_tables:
                filtered_tables = table_knowledge
        else:
            filtered_tables = table_knowledge

        result: Dict[str, Any] = {
            "query_used":      query,
            "tables_returned": list(filtered_tables.keys()),
        }

        if section in ("table", "both"):
            result["table_knowledge"] = filtered_tables
        if section in ("business", "both"):
            result["business_knowledge"] = business_knowledge

        return result

    except Exception as exc:
        return {
            "query_used":         query,
            "tables_returned":    [],
            "table_knowledge":    {},
            "business_knowledge": {},
            "error":              str(exc),
            "traceback":          traceback.format_exc(),
        }


@tool
def cognitive_map_table_tool(table_name: str) -> Dict[str, Any]:
    """
    Returns the full enriched knowledge entry for a single table.

    Args:
        table_name: Exact table name as it appears in the database.

    Returns:
        The enriched table dict (table_description, use_cases,
        data_quality_notes, table_columns) or an error dict.
    """
    try:
        cog_map         = build_cognitive_map()
        table_knowledge = cog_map.get("table_knowledge", {})

        if table_name in table_knowledge:
            return table_knowledge[table_name]

        # Case-insensitive fallback
        for tbl, info in table_knowledge.items():
            if tbl.lower() == table_name.lower():
                return info

        return {
            "error":            f"Table '{table_name}' not found in cognitive map.",
            "available_tables": list(table_knowledge.keys()),
        }

    except Exception as exc:
        return {"error": str(exc), "traceback": traceback.format_exc()}


@tool
def cognitive_map_business_tool(topic: str = "all") -> Dict[str, Any]:
    """
    Returns the business knowledge directory (metrics, business rules,
    common gotchas), optionally filtered by topic keyword.

    Args:
        topic: Keyword to filter metrics/gotchas by (e.g. "position",
               "championship", "dnf"). Pass "all" for the full directory.

    Returns:
        Filtered (or full) business knowledge directory.
    """
    try:
        cog_map = build_cognitive_map()
        bk      = cog_map.get("business_knowledge", {})

        if topic.strip().lower() == "all":
            return bk

        t = topic.lower()

        filtered_metrics = [
            m for m in bk.get("metrics", [])
            if t in m.get("name",        "").lower()
            or t in m.get("definition",  "").lower()
            or t in m.get("table",       "").lower()
            or t in m.get("calculation", "").lower()
        ]
        filtered_rules = [r for r in bk.get("business_rules", []) if t in r.lower()]
        filtered_gotchas = [
            g for g in bk.get("common_gotchas", [])
            if t in g.get("issue",    "").lower()
            or t in g.get("solution", "").lower()
            or any(t in tbl.lower() for tbl in g.get("tables_affected", []))
        ]

        return {
            "topic_filter":   topic,
            "metrics":        filtered_metrics,
            "business_rules": filtered_rules,
            "common_gotchas": filtered_gotchas,
        }

    except Exception as exc:
        return {"error": str(exc), "traceback": traceback.format_exc()}


# ---------------------------------------------------------------------------
# Convenience export
# ---------------------------------------------------------------------------

COGNITIVE_MAP_TOOLS = [
    cognitive_map_retriever_tool,
    cognitive_map_table_tool,
    cognitive_map_business_tool,
]


# ---------------------------------------------------------------------------
# CLI entry point  –  python cognitive_map_retrieval.py [--refresh]
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    force = "--refresh" in sys.argv

    print("=" * 60)
    print("Building Cognitive Map (large-scale edition)…")
    print("=" * 60)

    cog_map   = build_cognitive_map(force_refresh=force)
    build_meta = cog_map.get("_build_meta", {})

    print(f"\n  Total tables  : {build_meta.get('tables_total')}")
    print(f"  Cached        : {build_meta.get('tables_cached')}")
    print(f"  Re-enriched   : {build_meta.get('tables_processed')}")
    print(f"  Elapsed       : {build_meta.get('elapsed_seconds')}s")

    print("\n=== TABLE KNOWLEDGE DIRECTORY ===")
    for tbl, info in cog_map["table_knowledge"].items():
        print(f"\n[{tbl}]")
        print(f"  Description : {info.get('table_description', '')[:120]}…")
        print(f"  Columns     : {[c['name'] for c in info.get('table_columns', [])]}")

    print("\n=== BUSINESS KNOWLEDGE DIRECTORY ===")
    bk = cog_map["business_knowledge"]
    print(f"  Metrics  : {[m['name'] for m in bk.get('metrics', [])]}")
    print(f"  Rules    : {len(bk.get('business_rules', []))} rules")
    print(f"  Gotchas  : {[g['issue'] for g in bk.get('common_gotchas', [])]}")

    print("\n=== Tool smoke-test ===")
    result = cognitive_map_retriever_tool.invoke({"query": "all", "section": "both"})
    print(f"  Tables returned : {result.get('tables_returned', [])[:5]} …")

    # Auto-export to human-editable JSON after a successful build
    json_path = export_cognitive_map_to_json(cog_map)
    print(f"\nHuman-editable JSON : {json_path}")
    print(f"Cache directory     : {CACHE_DIR}")
    print(
        "\nTip: Edit cognitive_map.json to override descriptions, then re-run "
        "without --refresh to load your edits instead of calling the LLM."
    )
