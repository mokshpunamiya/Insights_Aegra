"""
agents/synthesizer_node.py
──────────────────────────
Node 4 (final) of the Multi-Agent Data Analysis System.
Generates the final polished markdown report.

Owned fields:
  - messages (single AIMessage — the final report)
  - status ("completed")
  - current_node
  - trace (append)
  - Resets transient fields to prevent cross-turn leakage:
    query_result, execution_result, chart_paths, chart_config,
    generated_sql, sql_status, exec_error, errors
"""

from __future__ import annotations
import copy
import json
import logging
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from eerly_studio.insights.prompts import load_prompt
from eerly_studio.insights.agents.state import AgentState, extract_user_query
from eerly_studio.insights.utils.llm import get_llm

logger = logging.getLogger(__name__)

# System prompt loaded from prompts/synthesizer_system.j2
_SYNTH_SYSTEM_PROMPT = load_prompt("synthesizer_system")

def synthesizer_node(state: AgentState) -> dict:
    """
    Final synthesis node.
    
    GUARD: If `final_answer` is set (e.g. by Orchestrator for greetings), 
    just echo it and exit to avoid redundant LLM calls.

    Returns exactly ONE AIMessage — never duplicates.
    Resets all transient data fields to prevent cross-turn state leakage.
    """
    final_answer = state.get("final_answer")
    if final_answer:
        # text_brief path — orchestrator already set this, just pass through
        return {"messages": [AIMessage(content=final_answer)]}

    user_query       = extract_user_query(state)
    query_result     = state.get("query_result", "")
    execution_result = state.get("execution_result", "")
    chart_paths      = state.get("chart_paths", "")
    chart_config     = state.get("chart_config")
    sql              = state.get("generated_sql", "")
    sql_status       = state.get("sql_status", "")
    errors           = state.get("errors", [])

    # ── Fast path: CANNOT_GENERATE ──────────────────────────────────────────
    # When the SQL LLM declined to generate (too vague, impossible join, etc.),
    # surface its reason as a friendly clarification prompt — no LLM call needed.
    if sql_status == "cannot_generate":
        # Extract the reason from query_result or errors
        reason = ""
        for err in errors:
            if isinstance(err, dict) and "SQL generation failed" in err.get("message", ""):
                reason = err["message"].replace("SQL generation failed: ", "").strip()
                break
        if not reason and query_result:
            reason = query_result.replace("SQL generation failed: ", "").strip()

        clarification = (
            f"I wasn't able to generate a specific query for: **\"{user_query}\"**\n\n"
            f"{reason}\n\n"
            "**Try asking something more specific, for example:**\n"
            "- *\"Show me total sales by region for 2023\"*\n"
            "- *\"What are the top 10 products by revenue?\"*\n"
            "- *\"How many employees are in each department?\"*\n"
            "- *\"List all orders placed in January 2024\"*"
        )
        trace = copy.deepcopy(state.get("trace", []))
        trace.append({"node": "synthesizer", "status": "cannot_generate"})
        return {
            "messages":         [AIMessage(content=clarification)],
            "status":           "completed",
            "current_node":     None,
            "trace":            trace,
            "query_result":     "",
            "execution_result": "",
            "chart_paths":      "",
            "chart_config":     None,
            "generated_sql":    "",
            "sql_status":       "",
            "exec_error":       None,
            "errors":           [],
        }

    # ── Fast path: EMPTY SQL RESULT ─────────────────────────────────────────
    # If the SQL result is truly empty and no other data (Python/charts) exists,
    # skip the LLM call entirely to prevent hallucination of random data.
    _qr_strip = query_result.strip() if isinstance(query_result, str) else str(query_result).strip()
    is_empty_sql = (
        _qr_strip in ("[]", "[{}]", "null", "None", "") or
        '"rows": []' in _qr_strip or
        '"row_count": 0' in _qr_strip
    )

    if is_empty_sql and not execution_result and not chart_paths and not chart_config:
        sql_display = f"\n```sql\n{sql}\n```" if sql else ""
        no_data_msg = (
            f"## No Matching Data\n\n"
            f"Your question: **{user_query}**\n\n"
            f"The database query returned **0 matching rows**. No records satisfy the current criteria.\n\n"
            f"**Possible reasons:**\n"
            f"- The filters (dates, regions, stages, etc.) are too narrow\n"
            f"- No data exists yet for this time period or combination\n"
            f"- Typo in names/terms — try alternative spellings\n\n"
            f"**Suggestions:**\n"
            f"- Broaden time range (e.g. remove quarter filter)\n"
            f"- Remove some conditions\n"
            f"- Ask for a different metric or aggregation\n\n"
            f"SQL attempted:{sql_display}"
        )

        trace = copy.deepcopy(state.get("trace", []))
        trace.append({"node": "synthesizer", "status": "ok"})
        
        return {
            "messages":         [AIMessage(content=no_data_msg)],
            "status":           "completed",
            "current_node":     None,
            "trace":            trace,
            "query_result":     "",
            "execution_result": "",
            "chart_paths":      "",
            "chart_config":     None,
            "generated_sql":    "",
            "sql_status":       "",
            "exec_error":       None,
            "errors":           [],
        }

    # ── Build synthesis prompt ───────────────────────────────────────────
    parts = [f"User question: {user_query}"]


    if sql and sql_status in ("ok", "retry_exhausted"):
        parts.append(f"\nSQL Query:\n```sql\n{sql}\n```")
        if sql_status == "retry_exhausted":
            parts.append("\n⚠ Note: SQL execution failed after all retry attempts.")

    if query_result and "error" not in query_result.lower():
        # Anti-hallucination guard: If result is an empty array, explicitly state it
        # rather than feeding '[]' which some LLMs ignore and replace with made-up tables.
        _clean_res = query_result.replace(" ", "").replace("\n", "")
        if _clean_res in ("[]", "[{}]"):
            parts.append("\nSQL Results:\n[0 rows returned. Tell the user there is no data matching their request without generating a generic table.]")
        else:
            preview = query_result[:3000]
            parts.append(f"\nSQL Results (JSON):\n{preview}")
            if len(query_result) > 3000:
                parts.append("... (truncated)")

    if execution_result:
        parts.append(f"\nPython Analysis Output:\n{execution_result}")

    if chart_paths:
        parts.append(f"\nCharts saved at: {chart_paths}")

    if chart_config:
        parts.append(f"\nChart widget suggested: {json.dumps(chart_config)}")

    # Include structured errors in the synthesis context
    if errors:
        error_summaries = []
        for err in errors:
            if isinstance(err, dict):
                error_summaries.append(
                    f"[{err.get('severity', 'warning').upper()}] {err.get('node', '?')}: {err.get('message', '')}"
                )
        if error_summaries:
            parts.append(f"\nSystem Warnings/Errors:\n" + "\n".join(error_summaries))

    # ── Generate report via LLM ──────────────────────────────────────────
    try:
        llm = get_llm()
        response = llm.invoke([
            SystemMessage(content=_SYNTH_SYSTEM_PROMPT),
            HumanMessage(content="\n".join(parts)),
        ])
        # Guard: Azure can return content=None even without tool binding.
        # Storing AIMessage(content=None) would cause a 400 on the next
        # orchestrator call ("Assistant message must have content or tool_calls").
        answer = response.content or ""
        if not answer:
            logger.warning("[Synthesizer] LLM returned empty content — using fallback.")
            answer = f"## Analysis Report\n**Query:** {user_query}\n"
            if execution_result:
                answer += f"\n### Python Analysis\n{execution_result}"
            if query_result and "error" not in query_result.lower():
                answer += f"\n### SQL Results\n{query_result[:1000]}"
    except Exception as e:
        logger.error("[Synthesizer] LLM call failed: %s", e)
        answer = f"## Analysis Report\n**Query:** {user_query}\n"
        if execution_result:
            answer += f"\n### Python Analysis\n{execution_result}"
        if query_result and "error" not in query_result.lower():
            answer += f"\n### SQL Results\n{query_result[:1000]}"

    # ── Build trace entry ────────────────────────────────────────────────
    trace = copy.deepcopy(state.get("trace", []))
    trace.append({"node": "synthesizer", "status": "ok"})

    # Reset all transient data fields to prevent cross-turn leakage
    # when using persistent memory checkpointers in LangGraph.
    return {
        "messages":         [AIMessage(content=answer)],
        "status":           "completed",
        "current_node":     None,
        "trace":            trace,
        # ── Reset transient fields ──
        "query_result":     "",
        "execution_result": "",
        "chart_paths":      "",
        "chart_config":     None,
        "generated_sql":    "",
        "sql_status":       "",
        "exec_error":       None,
        "errors":           [],
    }
