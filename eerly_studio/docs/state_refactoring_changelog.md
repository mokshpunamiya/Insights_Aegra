# State Layer & Architecture Refactoring — Changelog

**Date:** 2026-03-02  
**Scope:** Full multi-agent architecture refactoring for production reliability.

---

## Summary

Refactored the state layer and all node files to enforce:
- Strict node ownership (each node only returns fields it owns)
- Message accumulation via LangGraph's `add_messages` reducer (no manual mutation)
- Isolated subgraph states (DataEngineerState/DataScientistState have no `messages` or `chat_history`)
- Structured error handling via `AgentError` Pydantic model
- Execution tracing via `trace` list on AgentState
- DRY helpers (`extract_user_query`, `create_agent_error`)

## Files Changed

### `agents/state.py`
- Added `RelevantContext(BaseModel)` with `ConfigDict(extra="forbid")` and `Literal` chart suggestions
- Added `AgentError(BaseModel)` with `node`, `message`, `severity` fields
- Redesigned `AgentState(TypedDict)` with new execution metadata: `status`, `current_node`, `errors`, `trace`
- Redesigned `DataEngineerState(TypedDict)` — no messages, `query_result` as `list`, `metadata` as `dict`
- Redesigned `DataScientistState(TypedDict)` — no messages, `chart_paths` as `list`, `metadata` as `dict`
- Added `extract_user_query(state)` helper (replaces 14 duplicated lines across 3 nodes)
- Added `create_agent_error(node, message, severity)` factory

### `agents/orchestrator_node.py`
- Uses `extract_user_query()` instead of inline extraction
- Validates routing via `RelevantContext(**fields).model_dump()`
- Reports LLM failures via `create_agent_error()`
- Appends execution trace entries
- Strict ownership: only returns `relevant_context`, `final_answer`, `messages`, `current_node`, `status`, `trace`, `errors`

### `agents/data_engineer_node.py`
- Subgraph nodes never touch `messages`
- `query_result` is `List[Dict]` inside subgraph, serialized to JSON string at wrapper boundary
- Structured errors via `create_agent_error()`
- Fresh isolated subgraph initialization per invocation
- Wrapper strict ownership enforced

### `agents/data_scientist_node.py`
- Subgraph nodes never touch `messages`
- `chart_paths` is `List[str]` inside subgraph, comma-joined at wrapper boundary
- `metadata` parsed from JSON string to dict at wrapper entry
- Structured errors via `create_agent_error()`
- Fresh isolated subgraph initialization per invocation

### `agents/synthesizer_node.py`
- Reads structured `errors` list and includes error summaries in synthesis prompt
- Returns exactly ONE `AIMessage`
- Resets all transient fields to prevent cross-turn leakage
- Sets `status: "completed"`, appends trace entry

### `run_graph.py`
- `build_initial_state()` updated with new fields: `status`, `current_node`, `errors`, `trace`
- Removed obsolete fields: `retrieved_context`, `retry_count`, `sql_reason`, `query_columns`, `query_row_count`, `exec_status`, `truncated`, `python_code`, `error_traceback`

## Node Ownership Matrix

| Field | Orchestrator | DE Wrapper | DS Wrapper | Synthesizer |
|---|---|---|---|---|
| messages | ✅ (1 msg) | ✅ (1 msg) | ✅ (1 msg) | ✅ (1 msg) |
| relevant_context | ✅ | ❌ | ❌ | ❌ |
| final_answer | ✅ | ❌ | ❌ | ❌ |
| query_result | ❌ | ✅ | ❌ | ✅ (reset) |
| generated_sql | ❌ | ✅ | ❌ | ✅ (reset) |
| execution_result | ❌ | ❌ | ✅ | ✅ (reset) |
| chart_paths | ❌ | ❌ | ✅ | ✅ (reset) |
| errors | ✅ (append) | ✅ (append) | ✅ (append) | ✅ (reset) |
| trace | ✅ (append) | ✅ (append) | ✅ (append) | ✅ (append) |

---

## Phase 2: Audit Refactoring (2026-03-02)

### `agents/data_engineer_node.py`
- **C1 fix:** `de_execute_sql` returns `{"attempt_count": ...}` instead of `{}` to prevent infinite retry loops
- **C3 fix:** Parquet path uses UUID session ID (`data/{uuid}_data.parquet`) to prevent race conditions
- **C6 fix:** Added `_validate_sql_safety()` — walks `sqlglot` AST to reject any DML/DDL operations (DELETE, UPDATE, INSERT, DROP, CREATE, ALTER)
- **C7 fix:** `knowledge_ctx` deep-copied at wrapper boundary to prevent mutable reference leaks
- **New:** Result size guard — truncates `query_result` JSON if > 500 KB for checkpoint safety
- **New:** `retry_exhausted` as distinct `sql_status` when retries are exhausted
- **New:** Duration tracking (`duration_ms`) in trace entries via `time.monotonic()`

### `tools/data_engineer_tools.py`
- **C4 fix:** Connection pool limits: `pool_size=5`, `max_overflow=10`, `pool_timeout=30`
- **C5 fix:** Statement timeout `SET statement_timeout = '30s'` and lock timeout `SET lock_timeout = '5s'` before every SQL execution

---

## Phase 3: Tools Layer Production Refactoring (2026-03-02)

### `tools/data_engineer_tools.py` — Full rewrite
- **Structured error classification:** `SQLErrorType` enum with 10 error categories (`empty_sql`, `ast_parse_failed`, `dml_ddl_rejected`, `schema_mismatch`, `cost_too_high`, `execution_timeout`, `execution_failed`, `generation_failed`, `cannot_generate`, `pipeline_error`)
- **Safety Layer 1:** AST validation via `validate_sql_ast()` — walks full sqlglot AST including CTEs, rejects DML/DDL including `Command` (GRANT/TRUNCATE)
- **Safety Layer 2:** Identifier normalization via `_normalize_identifiers()` — auto-quotes mixed-case and reserved-word identifiers for PostgreSQL
- **Safety Layer 3:** AST-based LIMIT injection via `_apply_limit_ast()` — replaces fragile regex, handles CTEs correctly, with regex fallback
- **Safety Layer 4:** Live schema validation via `_validate_schema_live()` — checks all referenced tables against `information_schema.tables`, fail-open on introspection errors
- **Safety Layer 5:** EXPLAIN cost threshold via `_check_explain_cost()` — runs `EXPLAIN (FORMAT JSON)`, rejects queries with `Total Cost > 100000` (configurable via `SQL_EXECUTOR_COST_THRESHOLD`)
- **Safety Layer 6:** Execution with `SET statement_timeout` and `SET lock_timeout`
- **Config:** New env vars: `SQL_EXECUTOR_TIMEOUT_SEC` (default 30), `SQL_EXECUTOR_COST_THRESHOLD` (default 100000)

### `agents/data_engineer_node.py` — Consolidation
- **Removed** duplicate `_validate_sql_safety()` function
- **Imports** consolidated `validate_sql_ast()` from tools layer
