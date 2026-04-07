from __future__ import annotations

import traceback
from typing import Any, Dict, List, Optional

from langchain.tools import tool

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from eerly_studio.insights.storage.cognitive_map_vector_store import QdrantKnowledgeStore


_store: Optional[QdrantKnowledgeStore] = None


def _get_store() -> QdrantKnowledgeStore:
    global _store
    if _store is None:
        _store = QdrantKnowledgeStore()
    return _store


def _fetch(query: str, filter_type: str, k: int, db_schema: Optional[str] = None) -> List[Dict]:
    return _get_store().similarity_search(
        query=query, k=k, filter_type=filter_type, db_schema=db_schema
    )


def _build_context(query: str, k: int, db_schema: Optional[str]) -> Dict[str, Any]:
    """
    Runs five parallel searches against the vector store and assembles
    them into a single structured context dict ready for the SQL generator.

    Returns:
    {
      "tables":         [ full table payload, ... ],
      "columns":        [ full column payload, ... ],
      "metrics":        [ full metric payload, ... ],
      "gotchas":        [ full gotcha payload, ... ],
      "business_rules": [ rule string, ... ],
      "visualization":  []   # reserved for chart widget configs
    }
    """
    tables   = [r["payload"] for r in _fetch(query, "table",         k,     db_schema)]
    columns  = [r["payload"] for r in _fetch(query, "column",        k * 2, db_schema)]
    metrics  = [r["payload"] for r in _fetch(query, "metric",        k,     db_schema)]
    gotchas  = [r["payload"] for r in _fetch(query, "gotcha",        k,     db_schema)]
    rules    = [r["payload"].get("rule", "") for r in _fetch(query, "business_rule", k, db_schema)]

    # De-duplicate columns by (table_key, column_name) — multiple searches
    # for the same query can return the same column at different scores.
    seen_cols: set = set()
    unique_columns = []
    for col in columns:
        key = (col.get("table_key"), col.get("column_name"))
        if key not in seen_cols:
            seen_cols.add(key)
            unique_columns.append(col)

    return {
        "tables":         tables,
        "columns":        unique_columns,
        "metrics":        metrics,
        "gotchas":        gotchas,
        "business_rules": [r for r in rules if r],
        "visualization":  [],
    }


@tool
def knowledge_retriever_tool(
    query: str,
    k: int = 5,
    db_schema: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetches schema and business knowledge from the cognitive map vector store
    relevant to the user's query.

    Searches all five knowledge types (tables, columns, metrics, gotchas,
    business rules) in a single call and returns structured context ready
    to be passed directly into sql_generator_tool.

    Args:
        query:  The user's natural-language question, e.g.
                "total race wins per driver in 2023"
        k:      Number of results per knowledge type (default 5).
                Columns retrieve k*2 to maximise field coverage.
        db_schema: Optional schema name to restrict search
                (e.g. "sales", "humanresources").

    Returns:
        {
          "tables":         [ {table_key, table_name, schema,
                               table_description, use_cases,
                               data_quality_notes, table_columns}, ... ],
          "columns":        [ {table_key, column_name, data_type,
                               column_description, ...}, ... ],
          "metrics":        [ {metric_name, definition, table,
                               calculation}, ... ],
          "gotchas":        [ {issue, tables_affected, solution}, ... ],
          "business_rules": [ str, ... ],
          "visualization":  []
        }
    """
    try:
        return _build_context(query=query, k=k, db_schema=db_schema)
    except Exception as exc:
        return {
            "tables":         [],
            "columns":        [],
            "metrics":        [],
            "gotchas":        [],
            "business_rules": [],
            "visualization":  [],
            "error":          str(exc),
            "traceback":      traceback.format_exc(),
        }
