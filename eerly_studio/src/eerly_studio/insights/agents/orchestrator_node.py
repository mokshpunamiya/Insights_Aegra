"""
agents/orchestrator_node.py
───────────────────────────
Node 1 of the Multi-Agent Data Analysis System.

Responsibilities:
  - Classify the user's intent using LangChain Tool Calling (ReAct style)
  - Determine routing: reply_to_user → END | analyze → data_engineer
  - Populate RelevantContext with visualization intent and chart suggestion
  - Set `final_answer` on the state for direct replies so the Synthesizer
    can pass through without generating an analysis report.

Owned fields (only these may be returned):
  - relevant_context, final_answer, messages, current_node, status, trace, errors
"""

from __future__ import annotations

import copy
import logging
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.types import Command

from eerly_studio.insights.prompts import load_prompt
from eerly_studio.insights.agents.state import (
    AgentState,
    RelevantContext,
    extract_user_query,
    create_agent_error,
)
from eerly_studio.insights.utils.llm import get_llm

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────
# Tools for Intent Routing
# ────────────────────────────────────────────────

@tool
def request_sql_analysis(standalone_query: str, needs_chart: bool, chart_type: str, force_fresh: bool) -> str:
    """Use this for simple data lookups where a plain SQL query and tabular result is sufficient."""
    return "sql"


@tool
def request_python_analysis(standalone_query: str, needs_chart: bool, chart_type: str, force_fresh: bool) -> str:
    """Use this for deep analysis, statistics, ML, correlations, or code-generated charts."""
    return "python"


# ────────────────────────────────────────────────
# LLM classification prompt
# ────────────────────────────────────────────────

# System prompt loaded from prompts/orchestrator_system.j2
_SYSTEM_PROMPT = load_prompt("orchestrator_system")


# ────────────────────────────────────────────────
# Keyword fallback (if LLM fails)
# ────────────────────────────────────────────────

def _keyword_fallback(user_query: str) -> dict:
    q = user_query.lower()
    text_brief = any(w in q for w in ["hi", "hello", "hey", "what is", "who are", "explain", "tell me about"])
    in_depth   = any(w in q for w in ["deep", "python", "correlation", "model", "stats", "distribution", "analysis", "ml"])
    needs_viz  = any(w in q for w in ["chart", "graph", "plot", "visual", "show", "trend"])

    chart_suggestion = None
    if needs_viz:
        if   any(w in q for w in ["bar", "column", "compare"]):        chart_suggestion = "bar"
        elif any(w in q for w in ["line", "trend", "over time"]):       chart_suggestion = "line"
        elif any(w in q for w in ["scatter", "correlation"]):           chart_suggestion = "scatter"
        elif any(w in q for w in ["pie", "share", "proportion"]):       chart_suggestion = "pie"
        elif any(w in q for w in ["histogram", "distribution"]):        chart_suggestion = "histogram"
        else:                                                            chart_suggestion = "bar"

    only_sql = not text_brief and not in_depth
    return {
        "text_brief":          text_brief,
        "in_depth_analysis":   in_depth,
        "only_sql_analysis":   only_sql,
        "needs_visualization": needs_viz,
        "chart_suggestion":    chart_suggestion,
        "force_fresh":         True,  # Default to true for fallback
        # Informative fallback — used only when the LLM call fails entirely.
        # The LLM path uses response.content directly, so this only fires on exceptions.
        "direct_reply": (
            "I'm Eerly Insights — a multi-agent data analysis assistant. "
            "I work with structured data in your PostgreSQL database. "
            "You can ask me to query data (SQL), run statistical analysis or ML (Python + E2B sandbox), "
            "or generate interactive charts. What would you like to explore?"
        ) if text_brief else None,
    }


# ────────────────────────────────────────────────
# Message Sanitizer
# ────────────────────────────────────────────────

def _sanitize_messages(messages: list) -> list:
    """
    Drop any message with content=None and no tool_calls before sending to the API.

    Root cause: Azure/Mistral returns content=None when the model fires a tool call.
    If this raw response ever reaches state["messages"] (checkpoint race, API layer bug,
    etc.), replaying it on the next turn triggers:
      400 Bad Request — "Assistant message must have either content or tool_calls"

    Valid cases kept:
      • Any message whose content is not None          (normal)
      • AIMessage with content=None but has tool_calls (valid tool-call response)

    This is a defensive guard — it should rarely trigger in normal flow.
    """
    safe: list = []
    for msg in messages:
        content    = getattr(msg, "content",    None)
        tool_calls = getattr(msg, "tool_calls", [])
        
        # Guard against OpenAI 400 Validation Error.
        # It rejects messages where content evaluates empty AND no tool_calls.
        if (content is None or content == "") and not tool_calls:
            logger.warning(
                "[Orchestrator] Dropping malformed message (content empty, no tool_calls): %s",
                type(msg).__name__,
            )
            continue
            
        # Filter out internal orchestration routing messages so the LLM doesn't learn
        # to output them as plain text instead of firing a tool call.
        if isinstance(content, str) and content.startswith("Orchestrator: "):
            continue

        safe.append(msg)
    return safe


# ────────────────────────────────────────────────
# Orchestrator Node
# ────────────────────────────────────────────────

def orchestrator_node(state: AgentState) -> Command[Literal["data_engineer", "__end__"]]:
    """
    Node 1: Orchestrator — LLM tool-calling intent classification and routing.

    Reads:  state["user_query"], state["messages"]
    Writes: relevant_context, final_answer, messages, current_node, status, trace, errors
    """
    user_query = extract_user_query(state)
    errors = copy.deepcopy(state.get("errors", []))
    trace = copy.deepcopy(state.get("trace", []))

    # ── Step 1: Classify via LLM Tool Binding ───────────────────────────
    classification = None
    classification_method = "llm_tools"
    
    try:
        llm = get_llm()
        tools = [request_sql_analysis, request_python_analysis]
        llm_with_tools = llm.bind_tools(tools)
        
        # Build prompt: System message followed by all conversation messages.
        # _sanitize_messages guards against content=None AIMessages that cause
        # Azure 400 errors when replayed in multi-turn conversation history.
        messages_to_send = [SystemMessage(content=_SYSTEM_PROMPT)]
        messages = state.get("messages", [])
        if messages:
            messages_to_send.extend(_sanitize_messages(messages))
        else:
            messages_to_send.append(HumanMessage(content=user_query))
            
        # Strongly reinforce tool usage right at the end of the prompt to avoid hallucination
        # caused by seeing past plain-text reports in the conversation history.
        messages_to_send.append(SystemMessage(content=(
            "CRITICAL REMINDER: If the user asks for ANY report, analysis, or data, "
            "you MUST call `request_sql_analysis` or `request_python_analysis`. "
            "DO NOT answer data questions directly."
        )))
        
        response = llm_with_tools.invoke(messages_to_send)
        
        # Parse Tool Calls
        if response.tool_calls:
            tool_call = response.tool_calls[0]
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            
            logger.info(f"[Orchestrator] LLM selected tool: {tool_name} with args: {tool_args}")
            
            if tool_name == "request_python_analysis":
                classification = {
                    "text_brief": False,
                    "in_depth_analysis": True,
                    "only_sql_analysis": False,
                    "needs_visualization": tool_args.get("needs_chart", False),
                    "chart_suggestion": tool_args.get("chart_type", None),
                    "standalone_query": tool_args.get("standalone_query", user_query),
                    "force_fresh": tool_args.get("force_fresh", True),
                    "direct_reply": None,
                }
            elif tool_name == "request_sql_analysis":
                classification = {
                    "text_brief": False,
                    "in_depth_analysis": False,
                    "only_sql_analysis": True,
                    "needs_visualization": tool_args.get("needs_chart", False),
                    "chart_suggestion": tool_args.get("chart_type", None),
                    "standalone_query": tool_args.get("standalone_query", user_query),
                    "force_fresh": tool_args.get("force_fresh", True),
                    "direct_reply": None,
                }
        else:
            # If no tool calls exist, treat response.content as final_answer (text_brief)
            logger.info(f"[Orchestrator] LLM responded with plain text. Content: {response.content}")
            classification = {
                "text_brief": True,
                "in_depth_analysis": False,
                "only_sql_analysis": False,
                "needs_visualization": False,
                "chart_suggestion": None,
                "force_fresh": False,
                "direct_reply": response.content,
            }
            
    except Exception as exc:
        logger.warning(f"[Orchestrator] LLM classification failed ({exc}) — using keyword fallback")
        errors.append(create_agent_error(
            "orchestrator",
            f"LLM tool classification failed: {exc}",
            "warning",
        ))
        classification_method = "keyword_fallback"

    if classification is None:
        classification = _keyword_fallback(user_query)

    # ── Step 2: Normalise flags ─────────────────────────────────────────
    text_brief       = bool(classification.get("text_brief",          False))
    in_depth         = bool(classification.get("in_depth_analysis",   False))
    only_sql         = bool(classification.get("only_sql_analysis",   False))
    needs_viz        = bool(classification.get("needs_visualization", False))
    force_fresh      = bool(classification.get("force_fresh",         True))
    chart_suggestion = classification.get("chart_suggestion")           # str | None
    direct_reply     = classification.get("direct_reply") or "Hello! How can I help you today?"

    # Guard: at least one routing flag must be true
    if not any([text_brief, in_depth, only_sql]):
        only_sql = True

    # in_depth always wins over only_sql if both accidentally set by LLM
    if in_depth:
        only_sql = False

    # Validate via Pydantic (will raise if chart_suggestion is invalid)
    try:
        relevant_context = RelevantContext(
            in_depth_analysis  = in_depth,
            only_sql_analysis  = only_sql,
            text_brief         = text_brief,
            needs_visualization= needs_viz,
            chart_suggestion   = chart_suggestion,
            standalone_query   = classification.get("standalone_query"),
        ).model_dump()
    except Exception:
        # Fallback: drop invalid chart_suggestion
        relevant_context = RelevantContext(
            in_depth_analysis  = in_depth,
            only_sql_analysis  = only_sql,
            text_brief         = text_brief,
            needs_visualization= needs_viz,
            chart_suggestion   = None,
            standalone_query   = classification.get("standalone_query"),
        ).model_dump()

    # ── Step 3: Route ────────────────────────────────────────────────────
    if text_brief:
        logger.info("[Orchestrator] text_brief → END (direct reply, no data nodes)")
        trace.append({"node": "orchestrator", "status": "ok", "method": classification_method, "route": "end"})
        return Command(
            update={
                "relevant_context":   relevant_context,
                "final_answer":       direct_reply,
                # <<< NEW: propagate standalone_query at top level for consistency
                "standalone_query":   classification.get("standalone_query") or user_query,
                "previous_artifacts": state.get("previous_artifacts") or {},  # carry forward
                "messages":           [AIMessage(content=direct_reply)],
                "current_node":       "orchestrator",
                "status":             "completed",
                "trace":              trace,
                "errors":             errors,
            },
            goto="__end__",
        )

    route_label = "in-depth Python analysis" if in_depth else "SQL query"
    viz_label   = f" | chart hint: {chart_suggestion}" if needs_viz else ""
    logger.info("[Orchestrator] %s detected%s → data_engineer", route_label, viz_label)

    trace.append({"node": "orchestrator", "status": "ok", "method": classification_method, "route": "data_engineer"})

    return Command(
        update={
            "relevant_context":   relevant_context,
            "final_answer":       None,
            # <<< NEW: write standalone_query at top level so wrappers consume directly
            "standalone_query":   classification.get("standalone_query") or user_query,
            "previous_artifacts": state.get("previous_artifacts") if not force_fresh else {},  # Clear on fresh
            "force_fresh":        force_fresh,
            "messages":           [AIMessage(content=(
                f"Orchestrator: {route_label} detected{viz_label} — routing to Data Engineer."
            ))],
            "current_node":       "data_engineer",
            "status":             "running",
            "trace":              trace,
            "errors":             errors,
        },
        goto="data_engineer",
    )
