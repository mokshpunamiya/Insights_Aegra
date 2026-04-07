# Changelog

## [Unreleased] — 2026-03-06 (force_fresh Bypass Cache updates)

### Fixed — Passive Cache Reuse on Distinct or Repeated Queries

- **`agents/state.py`**: Introduced `force_fresh: bool` to the global `AgentState` schema.
- **`agents/orchestrator_node.py`**: Updated ReAct tool schemas (`request_sql_analysis`, `request_python_analysis`) to bind explicitly to the `force_fresh` logic.
  - Rewrote routing definitions to respect `force_fresh` overrides, completely purging `state["previous_artifacts"]` downstream on `True`.
- **`prompts/orchestrator_system.j2`**: Hardened caching semantics.
  - LLM now defaults to `force_fresh = True` unless recognizing distinct contextual continuations.
  - Forced appending of `"Refresh / recompute:"` keys to identical repeated questions.
- **`agents/data_engineer_node.py` & `agents/data_scientist_node.py`**: Updated graph wrappers to inspect the `force_fresh` guardrail at runtime.
  - If `True`, subgraph strictly skips similarity matrix caches (`get_most_similar_previous`), guaranteeing an expensive new execution path, logging skips distinctly with `🔄 force_fresh=True: Skipping cache`.

---

## [Unreleased] — 2026-03-06 (Data Hallucination Guard)

### Fixed — LLM Hallucinations on Empty SQL Results

- **`agents/synthesizer_node.py`**: Intercepted empty SQL results directly in Python to bypass the LLM synthesis call entirely, eliminating data-hallucination issues.
  - Implemented an `is_empty_sql` detection check for instances combining `[]`, `[{}]`, `null`, `None`, `"rows": []`, or `"row_count": 0`.
  - Defined an early-exit path that returns a predefined "No Matching Data" markdown template summarizing the execution failure, rather than passing essentially empty context to the LLM.
- **`prompts/synthesizer_system.j2`**: Hardened anti-hallucination directives.
  - Placed "CRITICAL RULES AGAINST HALLUCINATION" prominently above layout structures.
  - Explicitly forbade table construction or placeholder data invention when context is devoid of matched rows.

---

## [Unreleased] — 2026-03-05 (fix 2)

### Fixed — Azure 400 "Assistant message must have content or tool_calls"

Root causes identified and fixed:

**1. `agents/orchestrator_node.py` — `_sanitize_messages()` guard**
- Azure/Mistral sets `content=None` on tool-call assistant messages.
  If that message ever reaches `state["messages"]` (checkpoint race, API
  layer bug, etc.), the next turn's orchestrator replays it verbatim →
  `400 Bad Request`.
- Added `_sanitize_messages(messages)` helper that strips any message with
  `content=None` **and** no `tool_calls` before `messages_to_send.extend()`.
  Messages that are valid tool-call responses (`tool_calls` present) are kept.

**2. `agents/synthesizer_node.py` — `response.content or ""` guard**
- Azure can return `response.content = None` even without tool binding.
  `AIMessage(content=None)` stored in state triggers the 400 on the next
  orchestrator call.
- Replaced bare `answer = response.content` with:
  `answer = response.content or ""`
  With a fallback that assembles a raw-data answer from `execution_result`
  / `query_result` so the user still gets a useful response even if the
  LLM returns no text.

---

## [Unreleased] — 2026-03-05

### Fixed — Restored accidentally reverted hybrid-reuse code

- **`agents/data_scientist_node.py`**: Re-added all hybrid-reuse logic that was
  accidentally deleted:
  - Import of `get_most_similar_previous` and `build_artifact_record` from
    `artifact_reuse`.
  - Three-level `standalone_query` priority chain:
    `state["standalone_query"] > rc["standalone_query"] > extract_user_query(state)`.
  - `get_most_similar_previous(..., path_type="python")` cache-hit check with
    early-return `Command` (skips E2B execution when similarity ≥ 0.85).
  - `build_artifact_record` persistence on successful fresh runs.
  - `"standalone_query"` and `"previous_artifacts"` returned in `Command.update`
    so downstream nodes and the next turn's orchestrator see the updated values.

- **`run_graph.py`**: File was accidentally cleared to a single blank line.
  Fully restored with:
  - `build_initial_state()` including `standalone_query=None` and
    `previous_artifacts={}` initial defaults.
  - Streaming `run_scenario()` runner with Langfuse callback support.
  - Three default scenarios (greeting, SQL, SQL+chart).
  - UTF-8 stdout/stderr wrapper for Windows compatibility.

---

## [Unreleased] — 2026-03-03

### Added — Hybrid History + Reuse Pattern

- **`agents/artifact_reuse.py`** (new file): Pure-utility module implementing the
  hybrid history + reuse helpers:
  - `compute_embedding_similarity(text1, text2) → float`: real cosine similarity
    using `AzureCohereEmbeddings`; batches both texts in a single API call.
  - `should_reuse(artifact, current_standalone, threshold=0.87) → bool`: decides
    whether the current turn can short-circuit re-execution.
  - `get_most_similar_previous(state, current_standalone, path_type) → dict | None`:
    looks up `state["previous_artifacts"]["last_sql" | "last_python"]` and returns
    it if similarity clears the threshold.
  - `build_artifact_record(...)`: uniform constructor for the artifact dict stored
    in state.
  - `truncate_for_storage(value, max_chars=2000)`: trims large payloads to ≤2 KB
    before persisting in the checkpoint.

- **`AgentState`** — two new top-level fields (state.py):
  - `standalone_query: Optional[str]` — the de-contextualised query produced by the
    orchestrator each turn; consumed directly by both wrappers without digging into
    `relevant_context`.
  - `previous_artifacts: Dict[str, Any]` — keyed by `"last_sql"` / `"last_python"`;
    stores the last SQL or Python execution artifact (query, code, result preview,
    chart_config, timestamp) for semantic reuse.

### Changed

- **`orchestrator_node.py`**: Both routing branches (`text_brief → __end__` and
  `analyze → data_engineer`) now write `standalone_query` and `previous_artifacts`
  as top-level `AgentState` fields in `Command.update`, replacing the previous
  approach where `standalone_query` was only reachable via `relevant_context`.

- **`data_engineer_wrapper`**: Before invoking the DE subgraph, checks
  `get_most_similar_previous(..., path_type="sql")`. On cache hit, returns the
  previous result immediately (fast path, `sql_status="ok"`, `"reused"` trace entry).
  On cache miss, runs the full subgraph and saves the new artifact as `"last_sql"` in
  `previous_artifacts`. Both `standalone_query` and `previous_artifacts` are now
  included in the returned `Command.update`.

- **`data_scientist_wrapper`**: Same pattern as `data_engineer_wrapper` for
  `path_type="python"` / `"last_python"`. Reuse short-circuits E2B sandbox
  execution entirely. `chart_config` is served from the cached artifact when
  `needs_visualization=True`.

- **`run_graph.py` (`build_initial_state`)**: Added `"standalone_query": None` and
  `"previous_artifacts": {}` to the default initial state so the fields are always
  present from turn 1, preventing `KeyError` / LangGraph missing-key warnings.

## [0.1.0]

### Fixed
- **Windows Event Loop Incompatibility**: Injected `asyncio.WindowsSelectorEventLoopPolicy()` at the top of `src/eerly_studio/main.py`. Because `eerly_studio` components are dynamically instantiated within the overarching `aegra_api` ecosystem, this effectively patches `psycopg`'s Proactor loop incompatibilities on Windows environments natively without altering external library files (`libs/`).
- **Database Alignment Fixes**: Unified the auth service's database generation. Modified `src/eerly_studio/core/config.py` to dynamically construct the local Postgres URLs utilizing the centralized `.env` configuration keys (like `POSTGRES_USER` and `POSTGRES_PASSWORD`), resolving authentication denial errors (`asyncpg.exceptions.InvalidPasswordError`).
- **Alembic Mapping Conflicts**: Remedied crashes in `alembic/env.py` and `alembic_app/env.py` when running programmatic migrations. Updated target references to use the synthesized `settings.database_url` python `@property` correctly.
- **SQL Execution Schema Validation**: Fixed a bug where CTEs (Common Table Expressions) were incorrectly parsed as physical tables by `sqlglot` and validated against `information_schema.tables`, causing execution errors (`Execution error: Table 'public.vacation_sick_leave' does not exist`) in `eerly_studio/insights/tools/data_engineer_tools.py`.
- **Parquet Save Permission Error**: Modified the data serialization step in `data_engineer_node.py` to utilize the system's temporary directory (`tempfile.gettempdir()`) for caching `.parquet` result files instead of saving them to a local `data/` folder. This fixes `[Errno 13] Permission denied: 'data'` errors during live Docker executions.
- **Query Extraction Overwrite Fix**: Fixed an issue in `eerly_studio/insights/agents/state.py` where traversing the graph repeatedly passed the initial user query over new ones. `extract_user_query(state)` now correctly prioritizes and reads the latest `HumanMessage` from the LangGraph `messages` array before defaulting to the old static `user_query` string saved in state.
- **Cognitive Map Partial-Sync Fix**: Resolved an issue where only 23 vector entries (affecting solely `humanresources.employee` tables) were indexed and accessible by the agents in Qdrant out of the entire database. Updated the `sync_to_qdrant.py` script to forcefully wipe the stale collection and ingest the entire `cognitive_map.json` into the vector store. Agents now dynamically retrieve accurate context for `purchasing`, `production`, and `sales` queries.
- **Agent Refactoring**: Converted the `orchestrator_node.py` from an explicitly parsed LLM JSON array mode to a modern `ReAct Agent` style. Implemented native `@tool` bound nodes (e.g., `request_sql_analysis`, `reply_to_user`, `request_python_analysis`), increasing classification reliability over manual arbitrary Markdown JSON parses.
- **Strict Node State Isolation**: Removed `knowledge_ctx` from the global `AgentState` schema in `state.py` and excluded it from the `data_engineer_node.py` return wrapper. The `knowledge_ctx` dict is now strictly scoped only inside the `DataEngineerState` subgraph, preventing previous-turn Qdrant extraction data from persisting and polluting the Orchestrator on subsequent thread queries.

