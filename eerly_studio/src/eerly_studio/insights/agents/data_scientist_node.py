"""
agents/data_scientist_node.py
──────────────────────────────
Node 3 of the Multi-Agent Data Analysis System (Subgraph architecture).

Subgraph nodes operate on DataScientistState (isolated, no messages).
Wrapper node bridges DataScientistState → AgentState at the boundary.

Owned fields at AgentState level (wrapper only):
  - execution_result, chart_paths, chart_config,
    current_node, status, errors, trace

NOTE: Wrapper does NOT emit AIMessage — only Synthesizer produces user-facing messages.
"""
from __future__ import annotations

import os
import re
import json
import copy
import time
import base64
import logging
from typing import Literal, List

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.types import Command
from langgraph.graph import StateGraph, END

from eerly_studio.insights.tools.e2b_executor_tool import e2b_executor_tool
from eerly_studio.insights.agents.state import (
    AgentState,
    DataScientistState,
    extract_user_query,
    create_agent_error,
)
from eerly_studio.insights.agents.artifact_reuse import (
    get_most_similar_previous,
    build_artifact_record,
)
from eerly_studio.insights.utils.llm import get_llm
from eerly_studio.insights.utils.highcharts_generator import generate_highcharts_config

from eerly_studio.insights.prompts import load_prompt

logger = logging.getLogger(__name__)

MAX_RETRIES = 5

# System prompt loaded from prompts/data_scientist_code_system.j2
_CODE_SYSTEM_PROMPT = load_prompt("data_scientist_code_system")

def _encode_image(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def _build_prompt(
    user_query: str,
    metadata: dict | str,
    data_file_path: str,
    error_traceback: str | None,
    previous_code: str | None,
    chart_paths: List[str] | None = None,
) -> list:
    meta_str = metadata if not isinstance(metadata, dict) else json.dumps(metadata, indent=2)

    user_parts = [
        f"User question: {user_query}",
        f"Data file path (use exactly this): {data_file_path}",
        f"Dataset metadata (JSON):\n{meta_str}",
    ]

    if error_traceback and previous_code:
        user_parts += [
            f"\nThe following code raised an error:",
            f"{previous_code}",
            f"\nError traceback:",
            f"{error_traceback}",
            "\nFix the error and rewrite the complete corrected script.",
        ]
    elif chart_paths and previous_code:
        user_parts += [
            f"\nThe following code successfully generated the attached charts:",
            f"{previous_code}",
            "\nPlease review the visual charts generated. If they look perfect, return the exact same Python code to end the execution. If they need visual improvements, rewrite the code to fix them.",
        ]
    else:
        user_parts.append("\nWrite the analysis script.")

    content_list = [{"type": "text", "text": "\n".join(user_parts)}]
    
    if chart_paths:
        for p in chart_paths:
            if os.path.exists(p):
                b64 = _encode_image(p)
                content_list.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{b64}"}
                })

    return [
        SystemMessage(content=_CODE_SYSTEM_PROMPT),
        HumanMessage(content=content_list),
    ]

def _strip_fences(text: str) -> str:
    text = re.sub(r"^```[a-z]*\n?", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\n?```$", "", text)
    return text.strip()


# ────────────────────────────────────────────────────────────────────────
# Subgraph Nodes (operate on DataScientistState — never touch messages)
# ────────────────────────────────────────────────────────────────────────

def ds_generate_code(state: DataScientistState) -> dict:
    """Generate Python analysis code via LLM."""
    llm = get_llm()
    
    # chart_paths is List[str] internally
    chart_paths_list = state.get("chart_paths", [])
    chart_paths = chart_paths_list if chart_paths_list and not state.get("error_traceback") else None

    messages = _build_prompt(
        user_query=state.get("user_query", ""),
        metadata=state.get("metadata", {}),
        data_file_path=state.get("data_file_path") or "data/data.parquet",
        error_traceback=state.get("error_traceback"),
        previous_code=state.get("python_code") or None,
        chart_paths=chart_paths
    )

    response = llm.invoke(messages)
    new_code = _strip_fences(response.content)

    return {"python_code": new_code}


def ds_execute_code(state: DataScientistState) -> dict:
    """Execute Python code in E2B sandbox."""
    code = state.get("python_code", "")
    
    result = e2b_executor_tool(
        python_code=code,
        data_file_path=state.get("data_file_path") or "data/data.parquet",
    )

    attempt = state.get("attempt_count", 0) + 1
    errors = list(state.get("errors", []))

    error_tb = result.get("error_traceback")
    if error_tb:
        errors.append(create_agent_error(
            "data_scientist", f"Code execution error: {error_tb[:200]}", "warning"
        ))

    # chart_paths from e2b is a string — parse to List[str]
    raw_paths = result.get("chart_paths", "")
    chart_paths_list = [p.strip() for p in raw_paths.split(",") if p.strip()] if raw_paths else []

    return {
        "execution_result": result.get("execution_result", ""),
        "error_traceback":  error_tb,
        "chart_paths":      chart_paths_list,
        "attempt_count":    attempt,
        "errors":           errors,
    }


def ds_route_after_execute(state: DataScientistState) -> str:
    """Retry code generation if execution failed and retries remain."""
    if state.get("error_traceback") and state.get("attempt_count", 0) < MAX_RETRIES:
        return "ds_generate_code"
    return END


# ────────────────────────────────────────────────────────────────────────
# Subgraph Compilation
# ────────────────────────────────────────────────────────────────────────

ds_builder = StateGraph(DataScientistState)
ds_builder.add_node("ds_generate_code", ds_generate_code)
ds_builder.add_node("ds_execute_code", ds_execute_code)

ds_builder.set_entry_point("ds_generate_code")
ds_builder.add_edge("ds_generate_code", "ds_execute_code")
ds_builder.add_conditional_edges("ds_execute_code", ds_route_after_execute)

ds_subgraph = ds_builder.compile()


# ────────────────────────────────────────────────────────────────────────
# Wrapper Node for Main Graph (bridges subgraph → AgentState)
# ────────────────────────────────────────────────────────────────────────

def data_scientist_wrapper(state: AgentState) -> Command[Literal["synthesizer"]]:
    """
    Wrapper injecting AgentState into DataScientistState subgraph.
    Serializes subgraph outputs back into checkpoint-safe AgentState fields.

    Hybrid Reuse:
      Before invoking the expensive subgraph, check whether the current
      standalone_query is semantically similar to the previous Python artifact
      (state["previous_artifacts"]["last_python"]).  If similarity ≥ threshold,
      short-circuit and return cached results; skip subgraph + E2B execution.

    Returns ONLY owned fields — never overwrites user_query or unrelated keys.
    """
    rc = state.get("relevant_context", {})

    # ── Resolve standalone_query ──────────────────────────────────────────
    # Prefer the top-level state field (set by orchestrator), then rc, then raw message.
    raw_query = extract_user_query(state)
    standalone_query = (
        state.get("standalone_query")       # <<< top-level field first (set by orchestrator)
        or rc.get("standalone_query")
        or raw_query
    )

    needs_viz = rc.get("needs_visualization", False)

    parent_errors = copy.deepcopy(state.get("errors", []))
    parent_trace  = copy.deepcopy(state.get("trace", []))

    force_fresh = state.get("force_fresh", True)

    # ── [REUSE] Check previous Python artifact ────────────────────────────
    # Uses embedding cosine similarity — only attempted when a prior Python
    # artifact exists in this session (state["previous_artifacts"]["last_python"]).
    reused_artifact = None
    if not force_fresh:
        reused_artifact = get_most_similar_previous(
            state, standalone_query, path_type="python"
        )
    else:
        logger.info(
            "[DS Wrapper] 🔄 force_fresh=True: Skipping cache and forcing fresh Python execution for: '%s'", 
            standalone_query[:80]
        )

    if reused_artifact:
        logger.info(
            "[DS Wrapper] ✅ Cache hit — reusing previous Python artifact for: '%s'",
            standalone_query[:80],
        )
        chart_config = reused_artifact.get("chart_config") if needs_viz else None

        parent_trace.append({
            "node":   "data_scientist",
            "status": "reused",
            "source": "previous_artifacts.last_python",
        })

        return Command(
            update={
                "standalone_query":   standalone_query,
                "previous_artifacts": state.get("previous_artifacts") or {},  # carry forward unchanged
                "execution_result":   reused_artifact.get("execution_result") or "",
                "chart_paths":        state.get("chart_paths", ""),  # keep existing
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

    # Parse metadata from JSON string to dict for subgraph
    metadata_raw = state.get("metadata", "{}")
    try:
        metadata_dict = json.loads(metadata_raw) if isinstance(metadata_raw, str) else metadata_raw
    except (json.JSONDecodeError, TypeError):
        metadata_dict = {}

    # Invoke subgraph with contextualized query
    sub_state = ds_subgraph.invoke({
        "user_query":       standalone_query,
        "data_file_path":   state.get("data_file_path") or "data/data.parquet",
        "metadata":         metadata_dict,
        "python_code":      "",
        "error_traceback":  None,
        "attempt_count":    0,
        "relevant_context": state.get("relevant_context", {}),
        "execution_result": "",
        "chart_paths":      [],
        "chart_config":     None,
        "errors":           [],
    })

    duration_ms = int((time.monotonic() - t0) * 1000)

    rc = state.get("relevant_context", {})
    needs_viz = rc.get("needs_visualization", False)
    chart_config = None

    if needs_viz and not sub_state.get("error_traceback") and sub_state.get("execution_result"):
        try:
            import pandas as pd
            data_path = sub_state.get("data_file_path", "")
            if data_path and os.path.exists(data_path):
                df = pd.read_parquet(data_path, engine="fastparquet")
                chart_config = generate_highcharts_config(
                    data_source=df,
                    user_query=standalone_query,
                    preferred_type=rc.get("chart_suggestion")
                )
        except Exception as e:
            logger.warning("[DataScientist Wrapper] Could not build Highcharts config: %s", e)
            parent_errors.append(create_agent_error(
                "data_scientist", f"Chart config generation failed: {e}", "warning"
            ))

    # Serialize chart_paths from List[str] to comma-separated string for AgentState
    chart_paths_list = sub_state.get("chart_paths", [])
    chart_paths_str = ",".join(chart_paths_list) if chart_paths_list else ""

    # Collect trace and errors
    attempt = sub_state.get("attempt_count", 1)
    error_traceback = sub_state.get("error_traceback")

    # Distinguish retry_exhausted from single-attempt failure
    if error_traceback and attempt >= MAX_RETRIES:
        ds_status = "retry_exhausted"
    elif error_traceback:
        ds_status = "error"
    else:
        ds_status = "ok"

    parent_trace.append({
        "node":        "data_scientist",
        "status":      ds_status,
        "attempts":    attempt,
        "duration_ms": duration_ms,
    })

    parent_errors.extend(sub_state.get("errors", []))

    # ── [SAVE] Persist artifact for potential reuse next turn ─────────────
    # Store a 2 KB preview of execution_result to avoid checkpoint bloat.
    # chart_config is stored in full (usually small JSON).
    previous_artifacts = copy.deepcopy(state.get("previous_artifacts") or {})
    if not error_traceback:
        previous_artifacts["last_python"] = build_artifact_record(
            standalone_query=standalone_query,
            python_code=sub_state.get("python_code", ""),
            execution_result=sub_state.get("execution_result", ""),  # trimmed internally
            chart_config=chart_config,
        )
        logger.debug("[DS Wrapper] Saved last_python artifact for future reuse.")

    # Return ONLY owned fields (no messages — only Synthesizer emits AIMessage)
    return Command(
        update={
            "standalone_query":   standalone_query,       # <<< carry forward for synthesizer context
            "previous_artifacts": previous_artifacts,     # <<< updated with this turn's artifact
            "execution_result": sub_state.get("execution_result", ""),
            "chart_paths":      chart_paths_str,
            "chart_config":     chart_config,
            "current_node":     "synthesizer",
            "status":           "failed" if error_traceback else "running",
            "trace":            parent_trace,
            "errors":           parent_errors,
        },
        goto="synthesizer",
    )

