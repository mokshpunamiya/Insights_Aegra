"""
agents/data_engineer_node.py
────────────────────────────
Node 2 of the Multi-Agent Data Analysis System (Subgraph architecture).

Subgraph nodes operate on DataEngineerState (isolated, no messages).
Wrapper node bridges DataEngineerState → AgentState at the boundary.

Owned fields at AgentState level (wrapper only):
  - query_result, generated_sql, sql_status, exec_error,
    data_file_path, metadata, chart_config, knowledge_ctx,
    current_node, status, errors, trace

NOTE: Wrapper does NOT emit AIMessage — only Synthesizer produces user-facing messages.

Audit improvements (2026-03-02):
  - SQL AST validation via sqlglot before execution (C6 fix)
  - Session-scoped parquet path via UUID (C3 fix)
  - de_execute_sql preserves attempt_count on skip (C1 fix)
  - Result size guard at serialization boundary (500 KB)
  - retry_exhausted as distinct sql_status
  - Duration tracking in trace entries
  - knowledge_ctx deep-copied at boundary (C7 fix)
"""
from __future__ import annotations

import os
import json
import copy
import time
import uuid
import logging
from typing import Literal, Optional

from langgraph.types import Command
from langgraph.graph import StateGraph, END

from eerly_studio.insights.agents.state import (
    AgentState,
    DataEngineerState,
    extract_user_query,
    create_agent_error,
)
from eerly_studio.insights.agents.artifact_reuse import (
    get_most_similar_previous,
    build_artifact_record,
)
from eerly_studio.insights.tools.knowledge_retriever_tool import _build_context
from eerly_studio.insights.tools.data_engineer_tools import (
    _format_context_for_llm,
    sql_generator_tool,
    sql_executor_tool,
    validate_sql_ast,
)
from eerly_studio.insights.utils.highcharts_generator import generate_highcharts_config

logger = logging.getLogger(__name__)

MAX_SQL_RETRIES = 2
_MAX_RESULT_BYTES = 500_000  # 500 KB checkpoint safety limit


# ────────────────────────────────────────────────────────────────────────
# Subgraph Nodes (operate on DataEngineerState — never touch messages)
# ────────────────────────────────────────────────────────────────────────

def de_knowledge_retrieval(state: DataEngineerState) -> dict:
    """Step 1: Retrieve schema context from Qdrant cognitive map."""
    user_query = state.get("user_query", "")
    logger.info("[DE Sub] Step 1: knowledge retrieval for '%s'", user_query)

    knowledge_ctx = _build_context(query=user_query, k=5, db_schema=None)
    logger.info(
        "[DE Sub] Retrieved %d tables, %d columns, %d metrics",
        len(knowledge_ctx.get("tables", [])),
        len(knowledge_ctx.get("columns", [])),
        len(knowledge_ctx.get("metrics", [])),
    )
    return {"knowledge_ctx": knowledge_ctx}


def de_generate_sql(state: DataEngineerState) -> dict:
    """Step 2: Generate SQL from user query + schema context."""
    user_query = state.get("user_query", "")
    knowledge_ctx = state.get("knowledge_ctx", {})

    schema_context_str = _format_context_for_llm(knowledge_ctx)
    if state.get("exec_error") and state.get("attempt_count", 0) > 0:
        schema_context_str += (
            f"\n\n⚠ PREVIOUS ERROR — FIX THIS:\n"
            f"{state['exec_error']}\n"
            f"Rewrite the query to avoid this exact error."
        )

    attempt = state.get("attempt_count", 0)
    logger.info("[DE Sub] Step 2: SQL generation (Attempt %d)", attempt)

    gen = sql_generator_tool.invoke({
        "user_query":     user_query,
        "schema_context": schema_context_str,
        "dialect":        "postgres",
    })

    errors = list(state.get("errors", []))

    if gen.get("status") != "ok":
        reason = gen.get("reason", "Unknown")
        logger.warning("[DE Sub] SQL generation failed: %s", reason)
        errors.append(create_agent_error(
            "data_engineer", f"SQL generation failed: {reason}", "critical"
        ))
        return {
            "generated_sql": "",
            "exec_error":    f"SQL generation failed: {reason}",
            "attempt_count": attempt + 1,
            "errors":        errors,
        }

    sql = gen.get("sql", "")

    # ── AST safety validation (consolidated in tools layer) ──
    safe, rejection_reason, _ = validate_sql_ast(sql)
    if not safe:
        logger.error("[DE Sub] SQL rejected by validator: %s", rejection_reason)
        errors.append(create_agent_error(
            "data_engineer", f"SQL validation failed: {rejection_reason}", "critical"
        ))
        return {
            "generated_sql": sql,
            "exec_error":    f"SQL validation rejected: {rejection_reason}",
            "attempt_count": attempt + 1,
            "errors":        errors,
        }

    logger.info("[DE Sub] SQL generated:\n%s", sql)
    return {
        "generated_sql": sql,
        "exec_error":    None,
        "attempt_count": attempt + 1,
    }


def de_execute_sql(state: DataEngineerState) -> dict:
    """Step 3: Execute the generated SQL against PostgreSQL."""
    sql = state.get("generated_sql", "")

    if state.get("exec_error") and not sql:
        # C1 fix: preserve attempt_count so route guard can terminate
        return {"attempt_count": state.get("attempt_count", 0)}

    logger.info("[DE Sub] Step 3: SQL execution")
    exec_result = sql_executor_tool.invoke({
        "sql":     sql,
        "dialect": "postgres",
    })

    errors = list(state.get("errors", []))

    if exec_result.get("status") != "ok":
        error_msg = exec_result.get("error", "Unknown SQL execution error")
        logger.error("[DE Sub] Execution error: %s", error_msg)
        errors.append(create_agent_error(
            "data_engineer", f"SQL execution error: {error_msg}", "critical"
        ))
        return {"exec_error": error_msg, "errors": errors}

    # Success — store as List[Dict] (subgraph-internal, not JSON string)
    rows = exec_result.get("rows", [])

    logger.info(
        "[DE Sub] Execution OK — %d rows%s",
        exec_result.get("row_count", 0),
        " (truncated)" if exec_result.get("truncated") else "",
    )

    return {
        "exec_error":   None,
        "query_result": rows,
    }


def de_route_after_execute(state: DataEngineerState) -> str:
    """Retry SQL generation if execution failed and retries remain."""
    if state.get("exec_error") and state.get("attempt_count", 0) < MAX_SQL_RETRIES:
        return "de_generate_sql"
    return END


# ────────────────────────────────────────────────────────────────────────
# Subgraph Compilation
# ────────────────────────────────────────────────────────────────────────

de_builder = StateGraph(DataEngineerState)
de_builder.add_node("de_knowledge_retrieval", de_knowledge_retrieval)
de_builder.add_node("de_generate_sql", de_generate_sql)
de_builder.add_node("de_execute_sql", de_execute_sql)

de_builder.set_entry_point("de_knowledge_retrieval")
de_builder.add_edge("de_knowledge_retrieval", "de_generate_sql")
de_builder.add_edge("de_generate_sql", "de_execute_sql")
de_builder.add_conditional_edges("de_execute_sql", de_route_after_execute)

de_subgraph = de_builder.compile()


# ────────────────────────────────────────────────────────────────────────
# Wrapper Node for Main Graph (bridges subgraph → AgentState)
# ────────────────────────────────────────────────────────────────────────

def data_engineer_wrapper(state: AgentState) -> Command[Literal["synthesizer", "data_scientist"]]:
    """
    Wrapper injecting AgentState into DataEngineerState subgraph.
    Serializes subgraph outputs back into checkpoint-safe AgentState fields.

    Hybrid Reuse:
      Before invoking the expensive subgraph, check whether the current
      standalone_query is semantically similar to the previous SQL artifact
      (state["previous_artifacts"]["last_sql"]).  If similarity ≥ threshold,
      short-circuit and return cached results; skip subgraph execution.

    Returns ONLY owned fields — never overwrites user_query or unrelated keys.
    Does NOT emit AIMessage — only Synthesizer produces user-facing messages.
    """
    rc        = state.get("relevant_context", {})
    in_depth  = rc.get("in_depth_analysis", False)
    only_sql  = rc.get("only_sql_analysis", False)
    needs_viz = rc.get("needs_visualization", False)

    # ── Resolve standalone_query ──────────────────────────────────────────
    # Prefer the top-level state field (set by AgentState update from orchestrator).
    # Fall back to relevant_context.standalone_query, then raw user message.
    raw_query = extract_user_query(state)
    standalone_query = (
        state.get("standalone_query")           # <<< NEW: top-level field first
        or rc.get("standalone_query")
        or raw_query
    )

    parent_errors = copy.deepcopy(state.get("errors", []))
    parent_trace  = copy.deepcopy(state.get("trace", []))

    force_fresh = state.get("force_fresh", True)

    # ── [REUSE] Check previous SQL artifact ─────────────────────────────
    # get_most_similar_previous uses embedding cosine similarity (real embeddings).
    # This is ONLY attempted when there was a prior SQL turn in this session.
    reused_artifact = None
    if not force_fresh:
        reused_artifact = get_most_similar_previous(
            state, standalone_query, path_type="sql"
        )
    else:
        logger.info(
            "[DE Wrapper] 🔄 force_fresh=True: Skipping cache and forcing fresh SQL execution for: '%s'", 
            standalone_query[:80]
        )

    if reused_artifact:
        # ── Fast path: return cached artifact without touching the subgraph ──
        logger.info(
            "[DE Wrapper] ✅ Cache hit — reusing previous SQL artifact for: '%s'",
            standalone_query[:80],
        )
        chart_config = reused_artifact.get("chart_config") if needs_viz else None

        parent_trace.append({
            "node":    "data_engineer",
            "status":  "reused",
            "source":  "previous_artifacts.last_sql",
        })

        # Propagate previous_artifacts unchanged (do NOT update — artifact is still fresh)
        return Command(
            update={
                "standalone_query":   standalone_query,
                "query_result":       reused_artifact.get("query_result") or "",
                "generated_sql":      reused_artifact.get("generated_sql") or "",
                "sql_status":         "ok",
                "exec_error":         None,
                "data_file_path":     state.get("data_file_path", ""),  # keep existing
                "metadata":           state.get("metadata", "{}"),       # keep existing
                "chart_config":       chart_config,
                "current_node":       "synthesizer",
                "status":             "running",
                "trace":              parent_trace,
                "errors":             parent_errors,
            },
            goto="synthesizer",
        )

    # ── [FRESH RUN] Invoke the subgraph normally ──────────────────────────
    t0 = time.monotonic()

    sub_state = de_subgraph.invoke({
        "user_query":     standalone_query,
        "knowledge_ctx":  {},
        "generated_sql":  "",
        "exec_error":     None,
        "attempt_count":  0,
        "query_result":   [],
        "data_file_path": "",
        "metadata":       {},
        "chart_config":   None,
        "errors":         [],
    })

    duration_ms = int((time.monotonic() - t0) * 1000)
    exec_failed = bool(sub_state.get("exec_error"))
    query_rows = sub_state.get("query_result", [])
    attempts = sub_state.get("attempt_count", 1)

    # ── Serialize query_result with size guard ───────────────────────────
    query_result_str = ""
    if query_rows:
        query_result_str = json.dumps(query_rows, default=str)
        if len(query_result_str) > _MAX_RESULT_BYTES:
            truncate_at = max(1, len(query_rows) // 2)
            while (truncate_at > 1 and
                   len(json.dumps(query_rows[:truncate_at], default=str))
                   > _MAX_RESULT_BYTES):
                truncate_at = max(1, truncate_at // 2)
            query_result_str = json.dumps(query_rows[:truncate_at], default=str)
            parent_errors.append(create_agent_error(
                "data_engineer",
                f"Result truncated for checkpoint safety: {len(query_rows)} → {truncate_at} rows",
                "warning",
            ))

    # ── Parquet Preparation — session-scoped path (C3 fix) ───────────────
    data_file_path = ""
    metadata_str   = json.dumps({
        "tables": [t.get("table_key") for t in sub_state.get("knowledge_ctx", {}).get("tables", [])]
    })

    if in_depth and not exec_failed and query_rows:
        try:
            import pandas as pd
            import tempfile
            session_id = uuid.uuid4().hex[:8]
            temp_dir = tempfile.gettempdir()
            parquet_path = os.path.join(temp_dir, f"eerly_data_{session_id}.parquet")
            df = pd.DataFrame(query_rows)
            df.to_parquet(parquet_path, index=False)
            data_file_path = parquet_path
            metadata_str = json.dumps({
                "tables":    [t.get("table_key") for t in sub_state.get("knowledge_ctx", {}).get("tables", [])],
                "columns":   list(df.columns),
                "dtypes":    {c: str(df[c].dtype) for c in df.columns},
                "row_count": len(df),
                "sample":    df.head(3).to_dict(orient="records"),
            }, default=str)
        except Exception as e:
            logger.warning("[DE] Parquet save failed: %s", e)
            parent_errors.append(create_agent_error(
                "data_engineer", f"Parquet save failed: {e}", "warning"
            ))

    # ── Visualization Config (SQL-only hint) ─────────────────────────────
    chart_config: Optional[dict] = None
    if only_sql and needs_viz and not exec_failed and query_result_str:
        try:
            chart_config = generate_highcharts_config(
                data_source=query_result_str,
                user_query=standalone_query,
                preferred_type=rc.get("chart_suggestion")
            )
        except Exception as ce:
            logger.warning("[DE] Chart config failed: %s", ce)
            parent_errors.append(create_agent_error(
                "data_engineer", f"Chart config generation failed: {ce}", "warning"
            ))

    # ── Collect trace and errors ─────────────────────────────────────────
    # Distinguish retry_exhausted from single-attempt failure
    if exec_failed and attempts >= MAX_SQL_RETRIES:
        de_status = "retry_exhausted"
        sql_status = "retry_exhausted"
    elif exec_failed:
        de_status = "error"
        sql_status = "error"
    else:
        de_status = "ok"
        sql_status = "ok"

    parent_trace.append({
        "node":        "data_engineer",
        "status":      de_status,
        "attempts":    attempts,
        "duration_ms": duration_ms,
    })

    parent_errors.extend(sub_state.get("errors", []))

    # ── [SAVE] Persist artifact for potential reuse next turn ─────────────
    # Store a 2 KB preview of query_result (avoid checkpoint bloat).
    # chart_config is stored in full (usually small JSON).
    previous_artifacts = copy.deepcopy(state.get("previous_artifacts") or {})
    if not exec_failed:
        previous_artifacts["last_sql"] = build_artifact_record(
            standalone_query=standalone_query,
            generated_sql=sub_state.get("generated_sql", ""),
            query_result=query_result_str,   # truncated internally by build_artifact_record
            chart_config=chart_config,
        )
        logger.debug("[DE Wrapper] Saved last_sql artifact for future reuse.")

    # ── Determine routing ────────────────────────────────────────────────
    if exec_failed:
        goto = "synthesizer"
    elif in_depth and data_file_path:
        goto = "data_scientist"
    else:
        goto = "synthesizer"

    # Return ONLY owned fields (no messages — only Synthesizer emits AIMessage)
    return Command(
        update={
            "standalone_query":   standalone_query,       # <<< NEW
            "previous_artifacts": previous_artifacts,     # <<< NEW
            "query_result":   query_result_str,
            "generated_sql":  sub_state.get("generated_sql", ""),
            "sql_status":     sql_status,
            "exec_error":     sub_state.get("exec_error"),
            "data_file_path": data_file_path,
            "metadata":       metadata_str,
            "chart_config":   chart_config,
            "current_node":   goto,
            "status":         "failed" if exec_failed else "running",
            "trace":          parent_trace,
            "errors":         parent_errors,
        },
        goto=goto,
    )
