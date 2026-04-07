"""
agents/state.py
───────────────
Production-grade state definitions for the Multi-Agent Data Analysis System.

Architecture:
  - Pydantic BaseModel for validation sub-models (RelevantContext, AgentError)
  - TypedDict for LangGraph-compatible state schemas (AgentState, DataEngineerState, DataScientistState)
  - DRY helpers for user query extraction and structured error creation

Node Ownership Rules:
  - Each node returns ONLY fields it owns (see ownership matrix in implementation_plan.md)
  - Nodes NEVER manually mutate `messages` — use add_messages reducer automatically
  - `user_query` lives ONLY inside subgraph states (DataEngineerState, DataScientistState),
    NOT in AgentState. Use extract_user_query(state) at the AgentState level.
  - Subgraph states never contain `messages`
"""

from __future__ import annotations

import logging
from typing import TypedDict, List, Annotated, Any, Dict, Optional, Literal

from pydantic import BaseModel, ConfigDict, Field
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Pydantic Validation Models
# ═══════════════════════════════════════════════════════════════════════════

class RelevantContext(BaseModel):
    """
    Routing decision produced by the Orchestrator node.
    Immutable after creation — validated at the orchestrator boundary.

    Lifecycle: Created once per user turn by orchestrator_node.
    """
    model_config = ConfigDict(extra="forbid")

    in_depth_analysis: bool = False
    only_sql_analysis: bool = False
    text_brief: bool = False
    needs_visualization: bool = False
    chart_suggestion: Optional[
        Literal["bar", "line", "scatter", "pie", "histogram", "heatmap"]
    ] = None
    standalone_query: Optional[str] = None


class AgentError(BaseModel):
    """
    Structured error — replaces loose error strings scattered across nodes.
    Appended to state['errors'] by any node encountering an error.

    Lifecycle: Created by create_agent_error(), consumed by synthesizer_node.
    """
    model_config = ConfigDict(extra="forbid")

    node: str
    message: str
    severity: Literal["warning", "critical"]


# ═══════════════════════════════════════════════════════════════════════════
# LangGraph State Schemas (TypedDict — required by StateGraph)
# ═══════════════════════════════════════════════════════════════════════════

class AgentState(TypedDict):
    """
    Global orchestrator state for the main LangGraph graph.

    Contains:
      - Conversation context (messages) — auto-accumulated via add_messages reducer
      - Routing and control fields — owned by orchestrator
      - Final outputs from subgraph wrappers — owned by DE/DS wrappers
      - Execution metadata (status, current_node, trace, errors)
      - Hybrid reuse fields (standalone_query, previous_artifacts)

    Does NOT contain:
      - Internal retry counters (live inside subgraph states)
      - Python internal errors (live inside DataScientistState)

    Hybrid Reuse Pattern:
      - standalone_query: clean, self-contained rewrite produced by the orchestrator
        for routing / Qdrant retrieval / main LLM understanding. Avoids feeding the
        full raw message history to every downstream node.
      - previous_artifacts: dict keyed by path type ("last_sql", "last_python").
        Each value stores the standalone_query + expensive result + chart_config from
        the previous turn so that semantically similar follow-up queries can skip
        re-execution (see artifact_reuse.py helpers).
        Schema per key:
          {
            "standalone_query": str,
            "generated_sql"   : str,          # SQL path only
            "python_code"     : str,          # Python path only
            "query_result"    : str | None,   # preview / hash if large
            "execution_result": str | None,   # Python path only
            "chart_config"    : dict | None,
            "timestamp"       : float,        # unix epoch
          }
    """
    # ── Conversation (LangGraph auto-merges via add_messages) ─────────
    messages:      Annotated[List[BaseMessage], add_messages]
    # NOTE: chat_history removed — was never written or read by any node.
    # NOTE: user_query removed at AgentState level — never written by any node.
    #       Use extract_user_query(state) which reads from messages directly.

    # ── Inputs & Routing (Orchestrator-owned) ─────────────────────────
    relevant_context:  dict                    # Validated via RelevantContext at orchestrator
    final_answer:      Optional[str]

    # ── Multi-Turn Reuse (Orchestrator sets standalone_query; wrappers update previous_artifacts)
    standalone_query:    Optional[str]         # De-contextualised query (avoids token bloat)
    previous_artifacts:  Dict[str, Any]        # Keyed by "last_sql" | "last_python"
    force_fresh:         bool                  # If True, bypass cache and force recomputation

    # ── SQL Path Outputs (DataEngineer wrapper-owned) ─────────────────
    query_result:      str                     # JSON-serialized rows (checkpoint-safe)
    generated_sql:     str
    sql_status:        str                     # "ok" | "error" | "cannot_generate"
    exec_error:        Optional[str]
    data_file_path:    str
    metadata:          str                     # JSON-serialized metadata

    # ── Python Path Outputs (DataScientist wrapper-owned) ─────────────
    execution_result:  str
    chart_paths:       str                     # Comma-separated paths (checkpoint-safe)
    chart_config:      Optional[dict]

    # ── Execution Metadata (all nodes may update) ─────────────────────
    status:            str                     # "running" | "completed" | "failed"
    current_node:      Optional[str]
    errors:            list                    # List of AgentError.model_dump() dicts
    trace:             list                    # List of {"node": ..., "status": ...} dicts


class DataEngineerState(TypedDict):
    """
    Internal state for the Data Engineer subgraph.
    Completely isolated from conversation — no messages or chat_history.

    Lifecycle: Created fresh by data_engineer_wrapper per invocation.
    Retry counter lives here, NOT in AgentState.
    """
    user_query:     str
    knowledge_ctx:  dict
    generated_sql:  str
    exec_error:      Optional[str]
    attempt_count:   int
    query_result:   list                       # List[Dict] internally (not JSON string)
    data_file_path: str
    metadata:       dict
    chart_config:   Optional[dict]
    errors:         list                       # List of AgentError dicts


class DataScientistState(TypedDict):
    """
    Internal state for the Data Scientist subgraph.
    Completely isolated from conversation — no messages or chat_history.

    Lifecycle: Created fresh by data_scientist_wrapper per invocation.
    Retry counter lives here, NOT in AgentState.
    """
    user_query:       str
    data_file_path:   str
    metadata:         dict
    python_code:      str
    error_traceback:  Optional[str]
    attempt_count:    int
    relevant_context: dict
    execution_result: str
    chart_paths:      list                     # List[str] internally
    chart_config:     Optional[dict]
    errors:           list                     # List of AgentError dicts


# ═══════════════════════════════════════════════════════════════════════════
# DRY Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def extract_user_query(state: dict) -> str:
    """
    Extract the user's query from state, prioritizing the latest message.

    Priority:
      1. Latest HumanMessage in state["messages"]
      2. state["user_query"] (fallback for API / tests)
      3. "Hello" (safe default)

    Handles multimodal messages (list of dicts with "text" keys).
    """
    user_query = ""
    messages = state.get("messages", [])

    if messages:
        # Traverse list in reverse to find the latest human message
        for msg in reversed(messages):
            # Checking getattr type for LangChain's BaseMessage classes
            msg_type = getattr(msg, "type", "")
            if msg_type == "human" or getattr(msg, "__class__", None).__name__ == "HumanMessage":
                if hasattr(msg, "content"):
                    content = msg.content
                    if isinstance(content, str):
                        user_query = content.strip()
                    elif isinstance(content, list):
                        user_query = " ".join(
                            item.get("text", "") if isinstance(item, dict) else str(item)
                            for item in content
                        ).strip()
                break
        
        # If no human message was found but messages exist, arbitrarily use the very last one
        if not user_query:
            last_message = messages[-1]
            if hasattr(last_message, "content"):
                content = last_message.content
                if isinstance(content, str):
                    user_query = content.strip()
                elif isinstance(content, list):
                    user_query = " ".join(
                        item.get("text", "") if isinstance(item, dict) else str(item)
                        for item in content
                    ).strip()

    # Fallback to state["user_query"] if messages didn't supply anything
    if not user_query:
        user_query = (state.get("user_query") or "").strip()

    return user_query or "Hello"


def create_agent_error(
    node: str,
    message: str,
    severity: Literal["warning", "critical"] = "warning",
) -> dict:
    """
    Create a validated AgentError dict for appending to state['errors'].

    Usage:
        errors = state.get("errors", [])
        errors.append(create_agent_error("data_engineer", "SQL failed", "critical"))
        return {"errors": errors, ...}
    """
    error = AgentError(node=node, message=message, severity=severity)
    return error.model_dump()
