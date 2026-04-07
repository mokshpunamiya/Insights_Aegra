"""
Data Engineer Node – Tools (Production-Grade)
===============================================
Three LangChain-compatible tools for the Data Engineer Node:

  1. knowledge_retriever_tool  – rich multi-type semantic search via cognitive map in Qdrant
                                  Returns structured context: tables, columns, metrics, gotchas, rules.
  2. sql_generator_tool        – generates SQL from user query + full structured context
  3. sql_executor_tool         – executes the SQL safely against a live PostgreSQL database

Safety layers (executed in order before query runs):
  ① AST validation       – sqlglot parse → reject DML/DDL inside CTEs
  ② Identifier quoting   – normalize mixed-case identifiers for PG
  ③ AST-based LIMIT      – inject LIMIT via AST, not regex
  ④ Live schema check    – validate referenced tables exist in information_schema
  ⑤ EXPLAIN cost check   – reject queries with estimated cost above threshold
  ⑥ Execution timeout    – SET statement_timeout before every execution

Pipeline orchestrator:
  run_data_engineer_pipeline() — calls all three tools in sequence.

Dependencies:
    langchain langchain-openai sqlalchemy psycopg2-binary sqlglot qdrant-client requests tenacity

Environment variables (all read from .env):
    QDRANT_URL, QDRANT_API_KEY, QDRANT_COLLECTION
    COHERE_EMBED_URL, COHERE_API_KEY, COHERE_EMBED_MODEL
    ADVENTURE_DATABASE_URL
    MISTRAL_OPENAI_API_KEY, MISTRAL_OPENAI_ENDPOINT
    MISTRAL_OPENAI_LLM_DEPLOYMENT, MISTRAL_OPENAI_API_VERSION
    SQL_EXECUTOR_MAX_ROWS         – default 1000
    SQL_EXECUTOR_TIMEOUT_SEC      – default 30
    SQL_EXECUTOR_COST_THRESHOLD   – default 100000
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import pathlib
import traceback
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy
import sqlglot
from sqlglot import exp
from langchain.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Path setup — allow importing from project root
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# Import the rich structured retriever (uses QdrantKnowledgeStore internally)
from eerly_studio.insights.tools.knowledge_retriever_tool import knowledge_retriever_tool, _build_context
from eerly_studio.insights.prompts import load_prompt

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

_MAX_ROWS          = int(os.getenv("SQL_EXECUTOR_MAX_ROWS", "1000"))
_TIMEOUT_SEC       = int(os.getenv("SQL_EXECUTOR_TIMEOUT_SEC", "30"))
_COST_THRESHOLD    = float(os.getenv("SQL_EXECUTOR_COST_THRESHOLD", "100000"))


# ═══════════════════════════════════════════════════════════════════════════
# Structured Error Classification
# ═══════════════════════════════════════════════════════════════════════════

class SQLErrorType(str, Enum):
    """Structured error categories for the SQL execution pipeline."""
    EMPTY_SQL           = "empty_sql"
    AST_PARSE_FAILED    = "ast_parse_failed"
    DML_DDL_REJECTED    = "dml_ddl_rejected"
    SCHEMA_MISMATCH     = "schema_mismatch"
    COST_TOO_HIGH       = "cost_too_high"
    EXECUTION_TIMEOUT   = "execution_timeout"
    EXECUTION_FAILED    = "execution_failed"
    GENERATION_FAILED   = "generation_failed"
    CANNOT_GENERATE     = "cannot_generate"
    PIPELINE_ERROR      = "pipeline_error"


def _error_result(
    error_type: SQLErrorType,
    message: str,
    sql: str = "",
    **extra: Any,
) -> Dict[str, Any]:
    """Build a structured error response for sql_executor_tool."""
    return {
        "columns":    [],
        "rows":       [],
        "row_count":  0,
        "status":     "error",
        "error":      message,
        "error_type": error_type.value,
        "truncated":  False,
        "sql":        sql[:500] if sql else "",
        **extra,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Shared Singletons (lazy init)
# ═══════════════════════════════════════════════════════════════════════════

_llm:    Optional[Any]                        = None
_engine: Optional[sqlalchemy.engine.Engine]   = None

from eerly_studio.insights.utils.llm import get_llm as _get_llm


def _get_engine() -> sqlalchemy.engine.Engine:
    global _engine
    if _engine is None:
        db_url = os.environ["ADVENTURE_DATABASE_URL"]
        if "postgresql+asyncpg" in db_url:
            db_url = db_url.replace("postgresql+asyncpg", "postgresql+psycopg2")
        _engine = sqlalchemy.create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
    return _engine


# ═══════════════════════════════════════════════════════════════════════════
# Safety Layer 1: AST Validation (reject DML/DDL)
# ═══════════════════════════════════════════════════════════════════════════

_DANGEROUS_NODES = (
    exp.Delete, exp.Update, exp.Insert,
    exp.Drop, exp.Create, exp.Alter,
    exp.Command,  # GRANT, REVOKE, TRUNCATE, etc.
)


def validate_sql_ast(sql: str) -> Tuple[bool, str, Optional[list]]:
    """
    Parse SQL via sqlglot AST and reject any non-SELECT operations.

    A CTE starting with WITH can contain DELETE ... RETURNING * which passes
    a naive first-token check. This walks the full AST.

    Returns:
        (is_safe, rejection_reason, parsed_statements)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL", None

    try:
        parsed = sqlglot.parse(sql, dialect="postgres")
    except sqlglot.errors.ParseError as e:
        return False, f"SQL parse failed: {e}", None

    for stmt in parsed:
        if stmt is None:
            continue
        for node in stmt.walk():
            if isinstance(node, _DANGEROUS_NODES):
                return False, f"DML/DDL rejected: {type(node).__name__}", parsed

    return True, "", parsed


# ═══════════════════════════════════════════════════════════════════════════
# Safety Layer 2: Identifier Normalization / Quoting
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_identifiers(sql: str) -> str:
    """
    Re-generate SQL through sqlglot with proper PostgreSQL identifier quoting.

    PostgreSQL folds unquoted identifiers to lowercase. If the cognitive map
    contains mixed-case column names (firstName), the LLM might generate
    SELECT firstName which PG interprets as firstname.

    This function parses → walks identifiers → quotes those containing
    uppercase or special characters → regenerates SQL.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect="postgres")

        for node in parsed.walk():
            if isinstance(node, (exp.Column, exp.Table)):
                # Quote identifier parts that have uppercase or special chars
                for part_name in ("this", "db", "catalog"):
                    part = getattr(node, "args", {}).get(part_name)
                    if part and isinstance(part, exp.Identifier):
                        name = part.name
                        # Quote if: has uppercase, has spaces, is a PG reserved word
                        if (name != name.lower()
                                or " " in name
                                or name.upper() in _PG_RESERVED):
                            part.set("quoted", True)

        return parsed.sql(dialect="postgres")
    except Exception as e:
        logger.debug("[IdentifierNorm] Fallback to raw SQL: %s", e)
        return sql


# Minimal set of PG reserved words that cause ambiguity when unquoted
_PG_RESERVED = {
    "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "BETWEEN", "BY", "CASE",
    "CHECK", "COLUMN", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
    "CURRENT_TIME", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE",
    "END", "EXISTS", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "FULL",
    "GRANT", "GROUP", "HAVING", "IN", "INDEX", "INNER", "INSERT", "INTO",
    "IS", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "ON",
    "OR", "ORDER", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT",
    "SET", "TABLE", "THEN", "TO", "TRUE", "UNION", "UNIQUE", "UPDATE",
    "USING", "VALUES", "WHEN", "WHERE", "WITH", "USER", "TYPE", "OFFSET",
}


# ═══════════════════════════════════════════════════════════════════════════
# Safety Layer 3: AST-based LIMIT Injection
# ═══════════════════════════════════════════════════════════════════════════

def _apply_limit_ast(sql: str, limit: int) -> str:
    """
    Inject LIMIT via sqlglot AST instead of fragile regex.

    If the query already has a LIMIT clause, leave it alone.
    Handles CTEs (WITH ... SELECT ...) correctly by modifying only the
    outermost SELECT.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect="postgres")

        # Check if LIMIT already exists anywhere in the outermost select
        if parsed.find(exp.Limit):
            return sql

        # Inject LIMIT into the outermost SELECT
        limited = parsed.limit(limit)
        return limited.sql(dialect="postgres")
    except Exception as e:
        # Fallback: regex-based injection if AST fails
        logger.warning("[LIMIT] AST injection failed, using fallback: %s", e)
        return _apply_limit_regex_fallback(sql, limit)


def _apply_limit_regex_fallback(sql: str, limit: int) -> str:
    """Regex-based LIMIT injection — used only when AST fails."""
    if re.search(r"\bLIMIT\b|\bTOP\b|\bFETCH\b|\bROWNUM\b", sql, re.IGNORECASE):
        return sql
    stripped = sql.rstrip().rstrip(";").rstrip()
    return f"{stripped}\nLIMIT {limit};"


# ═══════════════════════════════════════════════════════════════════════════
# Safety Layer 4: Live Schema Validation
# ═══════════════════════════════════════════════════════════════════════════

def _validate_schema_live(sql: str, engine: sqlalchemy.engine.Engine) -> Tuple[bool, str]:
    """
    Extract all table references from SQL AST and verify each exists
    in information_schema.tables.

    Returns (is_valid, rejection_reason).
    Fails open on introspection errors (don't block execution if
    information_schema itself is inaccessible).
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        tables: set = set()

        # Collect all CTE aliases defined in the query
        cte_names = set()
        for with_node in parsed.find_all(exp.With):
            for expressions in with_node.expressions:
                if expressions.alias:
                    cte_names.add(expressions.alias.lower())

        for table_node in parsed.find_all(exp.Table):
            table_name = table_node.name
            if not table_name:
                continue
            
            # Skip tables that are actually CTEs defined in this query
            if table_name.lower() in cte_names:
                continue
                
            # Extract schema — sqlglot uses 'db' for the schema in PG
            schema_name = table_node.db or "public"
            tables.add((schema_name.lower(), table_name.lower()))

        if not tables:
            return True, ""

        with engine.connect() as conn:
            for schema, tbl in tables:
                result = conn.execute(text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE LOWER(table_schema) = :schema "
                    "AND LOWER(table_name) = :table_name "
                    "LIMIT 1"
                ), {"schema": schema, "table_name": tbl})
                if not result.fetchone():
                    return False, (
                        f"Table '{schema}.{tbl}' does not exist. "
                        f"Check table name and schema spelling."
                    )

        return True, ""
    except sqlglot.errors.ParseError:
        return True, ""  # fail-open
    except Exception as e:
        logger.warning("[SchemaCheck] Introspection failed (fail-open): %s", e)
        return True, ""  # fail-open: don't block on introspection errors


# ═══════════════════════════════════════════════════════════════════════════
# Safety Layer 5: EXPLAIN Cost Threshold
# ═══════════════════════════════════════════════════════════════════════════

def _check_explain_cost(
    sql: str,
    engine: sqlalchemy.engine.Engine,
    threshold: float = _COST_THRESHOLD,
) -> Tuple[bool, str, Optional[Dict]]:
    """
    Run EXPLAIN (FORMAT JSON) and check the estimated total cost.

    Returns (is_acceptable, reason, plan_summary).
    Fails open on EXPLAIN errors.
    """
    try:
        explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{_TIMEOUT_SEC}s'"))
            result = conn.execute(text(explain_sql))
            row = result.fetchone()

        if not row or not row[0]:
            return True, "", None

        plan = row[0]
        # plan is a list with one element containing the Plan
        if isinstance(plan, list) and plan:
            top_plan = plan[0].get("Plan", {})
        elif isinstance(plan, dict):
            top_plan = plan.get("Plan", {})
        else:
            return True, "", None

        total_cost = top_plan.get("Total Cost", 0)
        est_rows   = top_plan.get("Plan Rows", 0)

        plan_summary = {
            "total_cost":     round(total_cost, 2),
            "estimated_rows": est_rows,
            "node_type":      top_plan.get("Node Type", ""),
        }

        if total_cost > threshold:
            return False, (
                f"Query cost ({total_cost:.0f}) exceeds threshold ({threshold:.0f}). "
                f"Estimated rows: {est_rows}. Consider adding filters or limits."
            ), plan_summary

        return True, "", plan_summary
    except Exception as e:
        logger.warning("[EXPLAIN] Cost check failed (fail-open): %s", e)
        return True, "", None


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build schema context string from structured retriever output
# ═══════════════════════════════════════════════════════════════════════════

def _format_context_for_llm(ctx: Dict[str, Any]) -> str:
    """
    Converts the structured dict from knowledge_retriever_tool into a compact,
    human-readable string for the SQL generator LLM prompt.

    Sections:
      - TABLES      – table_key, description, column list with types
      - COLUMNS     – granular column entries (table.column | type | description)
      - METRICS     – name, definition, SQL hint
      - GOTCHAS     – issue + solution
      - RULES       – plain-text business rules
    """
    lines: List[str] = []

    # ── TABLES ──────────────────────────────────────────────────────────────
    tables = ctx.get("tables", [])
    if tables:
        lines.append("=== TABLES ===")
        for t in tables:
            lines.append(f"\n[Table] {t.get('table_key', '')}  (schema: {t.get('schema', '')})")
            if desc := t.get("table_description"):
                lines.append(f"  Description : {desc}")
            cols = t.get("table_columns", [])
            if cols:
                col_str = ", ".join(
                    f"{c.get('name')} ({c.get('type', '?')})" for c in cols
                )
                lines.append(f"  Columns     : {col_str}")
            for uc in t.get("use_cases", [])[:2]:
                lines.append(f"  Use Case    : {uc}")
            for dq in t.get("data_quality_notes", [])[:2]:
                lines.append(f"  Data Note   : {dq}")

    # ── COLUMNS ─────────────────────────────────────────────────────────────
    columns = ctx.get("columns", [])
    if columns:
        lines.append("\n=== KEY COLUMNS ===")
        for c in columns:
            entry = (
                f"  {c.get('table_key', '?')}.{c.get('column_name', '?')}"
                f"  [{c.get('data_type', '?')}]"
            )
            if desc := c.get("column_description"):
                entry += f"  — {desc}"
            lines.append(entry)

    # ── METRICS ─────────────────────────────────────────────────────────────
    metrics = ctx.get("metrics", [])
    if metrics:
        lines.append("\n=== BUSINESS METRICS ===")
        for m in metrics:
            lines.append(f"  [{m.get('metric_name', m.get('name', ''))}]")
            if d := m.get("definition"):
                lines.append(f"    Definition : {d}")
            if calc := m.get("calculation"):
                lines.append(f"    SQL hint   : {calc}")
            if tbl := m.get("table"):
                lines.append(f"    Table      : {tbl}")

    # ── GOTCHAS ─────────────────────────────────────────────────────────────
    gotchas = ctx.get("gotchas", [])
    if gotchas:
        lines.append("\n=== COMMON GOTCHAS ===")
        for g in gotchas:
            lines.append(f"  ⚠  {g.get('issue', '')}")
            if sol := g.get("solution"):
                lines.append(f"     Solution: {sol}")

    # ── BUSINESS RULES ───────────────────────────────────────────────────────
    rules = ctx.get("business_rules", [])
    if rules:
        lines.append("\n=== BUSINESS RULES ===")
        for r in rules:
            lines.append(f"  • {r}")

    return "\n".join(lines) if lines else "No schema context retrieved."


# ═══════════════════════════════════════════════════════════════════════════
# Tool 1 – knowledge_retriever_tool (re-exported)
# ═══════════════════════════════════════════════════════════════════════════
# The actual implementation lives in tools/knowledge_retriever_tool.py.
# We re-export it here so DATA_ENGINEER_TOOLS stays a single import point.


# ═══════════════════════════════════════════════════════════════════════════
# Tool 2 – sql_generator_tool
# ═══════════════════════════════════════════════════════════════════════════

# SQL generator system prompt loaded from prompts/sql_generator_system.j2
_SQL_SYSTEM_PROMPT = load_prompt("sql_generator_system")


@tool
def sql_generator_tool(
    user_query: str,
    schema_context: str,
    dialect: str = "postgres",
) -> Dict[str, Any]:
    """
    Generates a SQL query from the user's natural-language question and
    the structured schema context retrieved from the cognitive map vector store.

    Args:
        user_query:      The original user question.
        schema_context:  Formatted context string from knowledge_retriever_tool
                         (tables, columns, metrics, gotchas, rules).
        dialect:         SQL dialect hint — "postgresql" | "mysql" | "sqlite" | etc.

    Returns:
        {
          "sql"    : str   – the generated SQL (empty on failure),
          "status" : str   – "ok" | "cannot_generate" | "error",
          "reason" : str   – explanation when status != "ok"
        }
    """
    try:
        llm = _get_llm()

        user_message = (
            f"Dialect: {dialect}\n\n"
            f"Schema & Business Rules:\n{schema_context}\n\n"
            f"Question: {user_query}\n\n"
            "Write the SQL query:"
        )

        response = llm.invoke(
            [
                SystemMessage(content=_SQL_SYSTEM_PROMPT),
                HumanMessage(content=user_message),
            ]
        )

        raw: str = response.content.strip()

        # Strip accidental markdown fences
        raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\n?```$", "", raw)
        raw = raw.strip()

        if raw.upper().startswith("CANNOT_GENERATE"):
            reason = raw.split(":", 1)[-1].strip() if ":" in raw else raw
            return {"sql": "", "status": "cannot_generate", "reason": reason}

        return {"sql": raw, "status": "ok", "reason": ""}

    except Exception as exc:
        return {
            "sql":       "",
            "status":    "error",
            "reason":    str(exc),
            "traceback": traceback.format_exc(),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Tool 3 – sql_executor_tool (production-grade)
# ═══════════════════════════════════════════════════════════════════════════

@tool
def sql_executor_tool(
    sql: str,
    max_rows: int = _MAX_ROWS,
    dialect: str = "postgres",
) -> Dict[str, Any]:
    """
    Executes a SQL SELECT query against the configured PostgreSQL database.

    Safety pipeline (executed in order):
      1. AST validation   – reject DML/DDL
      2. Identifier norm  – quote mixed-case identifiers
      3. LIMIT injection  – via AST, not regex
      4. Schema check     – verify tables exist in information_schema
      5. EXPLAIN check    – reject queries with estimated cost > threshold
      6. Execute          – with statement_timeout and lock_timeout

    Args:
        sql:      A valid SQL SELECT statement.
        max_rows: Safety cap on returned rows (default 1000).
        dialect:  SQL dialect for parser-aware processing.

    Returns:
        {
          "columns"    : [str, ...]      – column names,
          "rows"       : [{col: val}]    – row dicts,
          "row_count"  : int,
          "status"     : "ok" | "error",
          "error"      : str | None,
          "error_type" : str | None      – structured error category,
          "truncated"  : bool,
          "plan"       : dict | None     – EXPLAIN plan summary (if available)
        }
    """
    # ── Guard: empty SQL ─────────────────────────────────────────────────
    if not sql or not sql.strip():
        return _error_result(SQLErrorType.EMPTY_SQL, "Empty SQL string received.")

    # ── Layer 1: AST validation ──────────────────────────────────────────
    is_safe, rejection, _ = validate_sql_ast(sql)
    if not is_safe:
        return _error_result(
            SQLErrorType.DML_DDL_REJECTED if "DML" in rejection or "DDL" in rejection
            else SQLErrorType.AST_PARSE_FAILED,
            rejection,
            sql=sql,
        )

    # ── Layer 2: Identifier normalization ────────────────────────────────
    normalized_sql = _normalize_identifiers(sql)

    # ── Layer 3: AST-based LIMIT injection ───────────────────────────────
    limited_sql = _apply_limit_ast(normalized_sql, max_rows + 1)

    # ── Acquire engine ───────────────────────────────────────────────────
    try:
        engine = _get_engine()
    except Exception as exc:
        return _error_result(
            SQLErrorType.EXECUTION_FAILED,
            f"Database connection failed: {exc}",
            sql=sql,
        )

    # ── Layer 4: Live schema validation ──────────────────────────────────
    schema_ok, schema_reason = _validate_schema_live(normalized_sql, engine)
    if not schema_ok:
        return _error_result(
            SQLErrorType.SCHEMA_MISMATCH,
            schema_reason,
            sql=sql,
        )

    # ── Layer 5: EXPLAIN cost check ──────────────────────────────────────
    cost_ok, cost_reason, plan_summary = _check_explain_cost(
        normalized_sql, engine, threshold=_COST_THRESHOLD
    )
    if not cost_ok:
        return _error_result(
            SQLErrorType.COST_TOO_HIGH,
            cost_reason,
            sql=sql,
            plan=plan_summary,
        )

    # ── Layer 6: Execute with timeout ────────────────────────────────────
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{_TIMEOUT_SEC}s'"))
            conn.execute(text("SET lock_timeout = '5s'"))
            result   = conn.execute(text(limited_sql))
            columns: List[str] = list(result.keys())
            all_rows = result.fetchall()

        truncated = len(all_rows) > max_rows
        rows = [dict(zip(columns, row)) for row in all_rows[:max_rows]]
        rows = _coerce_rows(rows)

        return {
            "columns":    columns,
            "rows":       rows,
            "row_count":  len(rows),
            "status":     "ok",
            "error":      None,
            "error_type": None,
            "truncated":  truncated,
            "plan":       plan_summary,
        }

    except Exception as exc:
        error_str = str(exc)
        # Classify timeout errors specifically
        if "statement timeout" in error_str.lower() or "canceling statement" in error_str.lower():
            return _error_result(
                SQLErrorType.EXECUTION_TIMEOUT,
                f"Query exceeded {_TIMEOUT_SEC}s timeout: {error_str}",
                sql=sql,
                plan=plan_summary,
            )
        return _error_result(
            SQLErrorType.EXECUTION_FAILED,
            error_str,
            sql=sql,
            plan=plan_summary,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _coerce_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert non-JSON-serialisable values (dates, Decimals, bytes) to strings."""
    import decimal
    import datetime

    coerced = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
                new_row[k] = v.isoformat()
            elif isinstance(v, decimal.Decimal):
                new_row[k] = float(v)
            elif isinstance(v, (bytes, bytearray)):
                new_row[k] = v.hex()
            else:
                new_row[k] = v
        coerced.append(new_row)
    return coerced


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def run_data_engineer_pipeline(
    user_query: str,
    k: int = 5,
    db_schema: Optional[str] = None,
    dialect: str = "postgres",
) -> Dict[str, Any]:
    """
    Runs the full Data Engineer pipeline in three steps:

      Step 1 – Retrieve structured schema context from Qdrant (cognitive map)
      Step 2 – Generate SQL from the user query + retrieved context
      Step 3 – Execute the SQL against the database

    Returns a dict that maps directly onto AgentState fields:
      {
        "retrieved_context"  : str   – human-readable schema context used for SQL gen,
        "knowledge_ctx"      : dict  – full structured retriever output (tables/cols/…),
        "generated_sql"      : str   – the generated SQL query,
        "sql_status"         : str   – "ok" | "cannot_generate" | "error",
        "sql_reason"         : str   – reason string when sql_status != "ok",
        "query_result"       : str   – JSON-serialised rows or error message,
        "query_columns"      : list  – list of column names,
        "query_row_count"    : int   – number of rows returned,
        "exec_status"        : str   – "ok" | "error",
        "exec_error"         : str   – execution error message (if any),
        "truncated"          : bool  – True if result was capped at max_rows,
        "pipeline_error"     : str   – set only if top-level exception occurred,
      }
    """
    result: Dict[str, Any] = {
        "retrieved_context": "",
        "knowledge_ctx":     {},
        "generated_sql":     "",
        "sql_status":        "error",
        "sql_reason":        "",
        "query_result":      "",
        "query_columns":     [],
        "query_row_count":   0,
        "exec_status":       "error",
        "exec_error":        None,
        "truncated":         False,
        "pipeline_error":    None,
    }

    try:
        # ── Step 1: Retrieve ────────────────────────────────────────────────
        logger.info("[DataEngineer] Step 1: knowledge retrieval for '%s'", user_query)
        knowledge_ctx = _build_context(query=user_query, k=k, db_schema=db_schema)
        schema_context_str = _format_context_for_llm(knowledge_ctx)

        result["knowledge_ctx"]     = knowledge_ctx
        result["retrieved_context"] = schema_context_str

        logger.info(
            "[DataEngineer] Retrieved %d tables, %d columns, %d metrics",
            len(knowledge_ctx.get("tables", [])),
            len(knowledge_ctx.get("columns", [])),
            len(knowledge_ctx.get("metrics", [])),
        )

        # ── Step 2: Generate SQL ─────────────────────────────────────────────
        logger.info("[DataEngineer] Step 2: SQL generation")
        gen = sql_generator_tool.invoke({
            "user_query":     user_query,
            "schema_context": schema_context_str,
            "dialect":        dialect,
        })

        result["generated_sql"] = gen.get("sql", "")
        result["sql_status"]    = gen.get("status", "error")
        result["sql_reason"]    = gen.get("reason", "")

        if gen["status"] != "ok":
            logger.warning("[DataEngineer] SQL generation failed: %s", gen.get("reason"))
            result["query_result"] = f"SQL generation failed: {gen.get('reason', '')}"
            return result

        logger.info("[DataEngineer] SQL generated:\n%s", gen["sql"])

        # ── Step 3: Execute SQL ─────────────────────────────────────────────
        logger.info("[DataEngineer] Step 3: SQL execution")
        exec_result = sql_executor_tool.invoke({
            "sql":     gen["sql"],
            "dialect": dialect,
        })

        result["query_columns"]  = exec_result.get("columns", [])
        result["query_row_count"] = exec_result.get("row_count", 0)
        result["exec_status"]    = exec_result.get("status", "error")
        result["exec_error"]     = exec_result.get("error")
        result["truncated"]      = exec_result.get("truncated", False)

        if exec_result["status"] == "ok":
            result["query_result"] = json.dumps(
                exec_result.get("rows", []), default=str
            )
            logger.info(
                "[DataEngineer] Execution OK — %d rows returned%s",
                exec_result["row_count"],
                " (truncated)" if exec_result.get("truncated") else "",
            )
        else:
            result["query_result"] = f"SQL execution error: {exec_result.get('error', '')}"
            logger.error("[DataEngineer] Execution error: %s", exec_result.get("error"))

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("[DataEngineer] Pipeline exception: %s\n%s", exc, tb)
        result["pipeline_error"] = str(exc)
        result["query_result"]   = f"Pipeline error: {exc}"

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Convenience Export
# ═══════════════════════════════════════════════════════════════════════════

DATA_ENGINEER_TOOLS = [
    knowledge_retriever_tool,
    sql_generator_tool,
    sql_executor_tool,
]
