# 🤖 Custom Agent Integration

This guide explains how to add a new specialist agent (node) to the Insights Aegra graph — for example, a dedicated **Financial Analyst** node or a **Report Formatter** node.

---

## Core Concepts

Every agent in Insights Aegra is a **LangGraph node** — a plain Python `async` function that:
1. Receives the current `AgentState` dict
2. Performs work (LLM call, tool call, DB query, etc.)
3. Returns a `dict` containing **only the fields it owns** (partial state update)

LangGraph automatically merges the returned dict into the shared state.

---

## Step 1 — Define Your Node

Create a new file in `eerly_studio/src/eerly_studio/insights/agents/`:

```python
# agents/financial_analyst_node.py

from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def financial_analyst_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Financial Analyst node — runs specialized financial calculations.

    Owned fields returned:
      - final_answer (str): Appended financial interpretation
      - status (str): "completed" or "failed"
    """
    user_query: str = state.get("standalone_query") or ""
    query_result: str = state.get("query_result", "")

    if not query_result:
        return {
            "status": "failed",
            "errors": state.get("errors", []) + [{
                "node": "financial_analyst",
                "message": "No query result to analyze.",
                "severity": "warning",
            }],
        }

    # --- Your LLM / calculation logic here ---
    financial_insight = f"Financial analysis for '{user_query}': ..."

    logger.info("Financial analyst node completed successfully.")
    return {
        "final_answer": financial_insight,
        "status": "completed",
    }
```

> **Rule**: Only return state fields your node **owns**. Never mutate `messages` directly — append to it via LangGraph's `add_messages` reducer automatically.

---

## Step 2 — Register in the Graph

Open `eerly_studio/src/eerly_studio/insights/graph.py` and add your node:

```python
from eerly_studio.insights.agents.financial_analyst_node import financial_analyst_node

def get_graph():
    workflow = StateGraph(AgentState)

    # Existing nodes
    workflow.add_node("orchestrator",       orchestrator_node)
    workflow.add_node("data_engineer",      data_engineer_wrapper)
    workflow.add_node("data_scientist",     data_scientist_wrapper)
    workflow.add_node("synthesizer",        synthesizer_node)

    # ✅ Your new node
    workflow.add_node("financial_analyst",  financial_analyst_node)

    workflow.set_entry_point("orchestrator")
    workflow.add_edge("synthesizer", END)
    workflow.add_edge("financial_analyst", "synthesizer")  # chain into synthesizer

    ...
```

---

## Step 3 — Add Routing in the Orchestrator

To route to your new node, update the Orchestrator's `RelevantContext` model and routing logic:

**In `agents/state.py`**, add a new flag:
```python
class RelevantContext(BaseModel):
    in_depth_analysis: bool = False
    only_sql_analysis: bool = False
    text_brief: bool = False
    needs_visualization: bool = False
    # ✅ Add your custom flag
    needs_financial_analysis: bool = False
    ...
```

**In `agents/orchestrator_node.py`**, handle the new flag and emit a `Command(goto="financial_analyst")` when applicable.

---

## Step 4 — Add to Prompts (Optional but Recommended)

Create a Jinja2 prompt template in `eerly_studio/src/eerly_studio/insights/prompts/`:

```
prompts/financial_analyst_system.j2
```

Load it using the existing `load_prompt()` utility from `prompts/__init__.py`.

---

## Node Design Checklist

- [ ] Function is `async` and fully type-annotated
- [ ] Only returns fields the node owns
- [ ] Uses `state.get(...)` defensively — keys may not always be present
- [ ] Appends to `errors` list (never overwrites it) on failure
- [ ] Logs decisions at `INFO` level, errors at `WARNING`/`ERROR`
- [ ] Does **not** silently swallow exceptions

---

## Testing Your Node

Add tests in `eerly_studio/tests/`:

```python
# tests/test_financial_analyst_node.py
import pytest
from eerly_studio.insights.agents.financial_analyst_node import financial_analyst_node


@pytest.mark.asyncio
async def test_returns_failed_when_no_query_result() -> None:
    state = {"standalone_query": "revenue analysis", "query_result": ""}
    result = await financial_analyst_node(state)
    assert result["status"] == "failed"
    assert len(result["errors"]) > 0


@pytest.mark.asyncio
async def test_returns_completed_with_insight() -> None:
    state = {
        "standalone_query": "revenue analysis",
        "query_result": '[{"revenue": 5000000}]',
    }
    result = await financial_analyst_node(state)
    assert result["status"] == "completed"
    assert result["final_answer"] is not None
```
