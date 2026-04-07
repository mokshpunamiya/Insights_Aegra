"""
run_graph.py
────────────
Runner for the Multi-Agent Data Analysis System.
Streams state updates and control flow node by node.

All nodes now use real tools:
  - Orchestrator   → LLM tool-calling intent classification + routing
  - Data Engineer  → Qdrant retrieval + SQL generation + execution
  - Data Scientist → LLM code gen + E2B sandbox
  - Synthesizer    → LLM-powered final report

Hybrid Reuse Pattern:
  - standalone_query   → populated by orchestrator each turn (de-contextualised)
  - previous_artifacts → keyed by "last_sql" | "last_python"; updated by wrappers
    after a fresh run and checked at the start of the next turn for cache hits.

Usage:
  uv run python run_graph.py                          # default scenarios
  uv run python run_graph.py "your custom question"  # single query
"""

from __future__ import annotations

import sys
import io
import os
import uuid
import logging

# Force UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from langchain_core.messages import HumanMessage, AIMessage

from eerly_studio.insights.graph import graph


# ────────────────────────────────────────────────
# Display Helpers
# ────────────────────────────────────────────────

COLORS = {
    "header":  "\033[1;96m",   # bold cyan
    "node":    "\033[1;93m",   # bold yellow
    "key":     "\033[0;36m",   # cyan
    "value":   "\033[0;37m",   # light grey
    "success": "\033[0;92m",   # green
    "divider": "\033[0;90m",   # dark grey
    "reset":   "\033[0m",
}

def c(color: str, text: str) -> str:
    return f"{COLORS[color]}{text}{COLORS['reset']}"

def print_banner(title: str):
    print(c("header", f"\n{'═'*64}"))
    print(c("header", f"  {title}"))
    print(c("header", f"{'═'*64}"))

def print_divider():
    print(c("divider", "─" * 64))

def format_value(v) -> str:
    if isinstance(v, list) and all(hasattr(m, "content") for m in v):
        return f"[{len(v)} message(s)] last: \"{v[-1].content[:80]}...\"" if v else "[]"
    if isinstance(v, str) and len(v) > 120:
        return v[:120] + "..."
    if isinstance(v, dict):
        return str({k: str(vv)[:60] for k, vv in v.items()})
    return str(v)

def print_node_update(node_name: str, update: dict):
    if not isinstance(update, dict):
        return
    print(c("node", f"\n  ▶  NODE: {node_name.upper()}"))
    print_divider()
    SKIP_KEYS = {"messages", "chat_history", "knowledge_ctx", "retrieved_context"}
    for k, v in update.items():
        if k in SKIP_KEYS:
            continue
        print(f"    {c('key', k)}: {c('value', format_value(v))}")
    if "messages" in update:
        msgs = update["messages"]
        if msgs:
            last = msgs[-1]
            label = "AI" if isinstance(last, AIMessage) else "Human"
            print(f"    {c('key', 'message')} [{label}]: {c('value', last.content[:200])}")
    print()


# ────────────────────────────────────────────────
# Initial State Builder
# ────────────────────────────────────────────────

def build_initial_state(user_query: str) -> dict:
    """
    Build a clean initial AgentState for a new conversation thread.

    Hybrid Reuse fields:
      - standalone_query   → None (orchestrator will populate on first turn)
      - previous_artifacts → {}   (no artifacts yet; wrappers will populate)
    """
    return {
        # ── Conversation ──────────────────────────────────────────────────
        # user_query is NOT an AgentState field — use extract_user_query(state) instead.
        # chat_history removed — was never read/written by any node.
        "messages":     [HumanMessage(content=user_query)],

        # ── Routing (Orchestrator-owned) ──────────────────────────────────
        "relevant_context": {
            "in_depth_analysis":   False,
            "only_sql_analysis":   False,
            "text_brief":          False,
            "needs_visualization": False,
            "chart_suggestion":    None,
            "standalone_query":    None,
        },
        "final_answer": None,

        # ── Hybrid Reuse ──────────────────────────────────────────────────
        "standalone_query":   None,   # populated by orchestrator each turn
        "previous_artifacts": {},     # keyed by "last_sql" | "last_python"

        # ── SQL Path ─────────────────────────────────────────────────────
        "query_result":   "",
        "generated_sql":  "",
        "sql_status":     "",
        "exec_error":     None,
        # knowledge_ctx removed — internal to DataEngineerState, not AgentState
        "data_file_path": "",
        "metadata":       "",

        # ── Python Path ───────────────────────────────────────────────────
        "execution_result": "",
        "chart_paths":      "",
        "chart_config":     None,

        # ── Execution Metadata ────────────────────────────────────────────
        "status":       "running",
        "current_node": None,
        "errors":       [],
        "trace":        [],
    }


# ────────────────────────────────────────────────
# Graph Runner
# ────────────────────────────────────────────────

def run_scenario(label: str, user_query: str):
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    print_banner(f"SCENARIO: {label}")
    print(f"  {c('key', 'Query')}: \"{user_query}\"")
    print(f"  {c('key', 'Thread')}: {thread_id}\n")

    initial_state = build_initial_state(user_query)

    # Optional Langfuse tracing
    try:
        from langfuse.langchain import CallbackHandler
        langfuse_handler = CallbackHandler()
        config["callbacks"] = [langfuse_handler]
    except ImportError:
        langfuse_handler = None

    for chunk in graph.stream(initial_state, config=config, stream_mode="updates"):
        for node_name, update in chunk.items():
            print_node_update(node_name, update)

    # Final state summary
    final = graph.get_state(config)
    msgs  = final.values.get("messages", [])
    last_ai_msg = next(
        (m.content for m in reversed(msgs) if isinstance(m, AIMessage)), "—"
    )

    print(c("success", "\n  ✅  FINAL ANSWER"))
    print_divider()
    print(f"  {c('value', last_ai_msg[:800])}")
    print(f"  {c('key', 'next nodes')} : {c('value', str(final.next))}")
    print()

    if langfuse_handler:
        print(c("success", "  🔗  LANGFUSE TRACING"))
        print_divider()
        print(f"  {c('value', 'Visit https://cloud.langfuse.com to review the full trace.')}")
        print()


# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    print_banner("Multi-Agent Data Analysis System — Live Runner")
    print(c("divider", "  All nodes use REAL tools (Qdrant + Azure Mistral + PostgreSQL + E2B)"))
    print()

    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None

    if query:
        run_scenario(label="Custom Query", user_query=query)
    else:
        # Scenario A — greeting → direct reply, no analysis
        run_scenario(
            label="Greeting (text_brief → END)",
            user_query="Hello! What can you help me with?",
        )

        # Scenario B — SQL-only query
        run_scenario(
            label="SQL Query (data_engineer → synthesizer)",
            user_query="What are the top 5 employees by vacation hours?",
        )

        # Scenario C — SQL + visualization
        run_scenario(
            label="SQL + Chart hint (data_engineer → synthesizer)",
            user_query="Show me a bar chart of the top 10 products by sales revenue.",
        )
