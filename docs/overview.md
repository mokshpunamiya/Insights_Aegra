# 🏛️ Architecture Overview

This document provides a comprehensive deep-dive into the Insights Aegra system architecture — how agents are orchestrated, how state flows between nodes, and how the system scales.

---

## System Layers

The platform is composed of three distinct layers:

| Layer | Technology | Role |
| :--- | :--- | :--- |
| **Protocol Layer** | Aegra (FastAPI) | Exposes Agent Protocol-compliant HTTP endpoints |
| **Orchestration Layer** | LangGraph | Manages agent state machine and routing |
| **Intelligence Layer** | Multi-Agent Graph | Executes business logic — SQL, Python, Synthesis |

---

## Multi-Agent Graph

```
User Query
    │
    ▼
┌─────────────────────────────────────────┐
│             🧠 Orchestrator              │
│  • De-contextualizes query               │
│  • Classifies intent via LLM tool-call   │
│  • Sets routing flags in AgentState      │
└──────────────────┬──────────────────────┘
                   │
       ┌───────────┴────────────┐
       ▼                        ▼
┌──────────────┐      ┌──────────────────┐
│ 👷 Data Eng  │      │ 🔬 Data Scientist │
│ (SQL Path)   │      │ (Python Path)     │
│              │      │                   │
│ Qdrant RAG   │      │ LLM Code Gen      │
│ SQL Gen      │      │ E2B Sandbox       │
│ SQL Execute  │      │ Chart Rendering   │
└──────┬───────┘      └────────┬──────────┘
       │                       │
       └───────────┬───────────┘
                   ▼
        ┌─────────────────────┐
        │   🖋️ Synthesizer    │
        │  Merges all outputs  │
        │  Generates report   │
        └─────────────────────┘
                   │
                   ▼
          Final Insight to User
```

---

## Agent State (`AgentState`)

The `AgentState` TypedDict is the shared memory of the graph. Key fields:

| Field | Owner | Purpose |
| :--- | :--- | :--- |
| `messages` | LangGraph Reducer | Full conversation history |
| `relevant_context` | Orchestrator | Routing flags (`only_sql`, `in_depth_analysis`, etc.) |
| `standalone_query` | Orchestrator | De-contextualized query for RAG retrieval |
| `generated_sql` | Data Engineer | SQL query generated and executed |
| `query_result` | Data Engineer | JSON-serialized DB result rows |
| `python_code` | Data Scientist | Generated analysis code |
| `chart_paths` | Data Scientist | Paths to rendered chart images |
| `final_answer` | Synthesizer | The final user-facing response |
| `previous_artifacts` | Wrappers | Cache of previous SQL/Python for reuse |

---

## Hybrid Reuse Pattern

To avoid redundant expensive DB calls or code executions, the system implements an **artifact cache**:

1. After each successful run, the wrapper nodes persist the result under `previous_artifacts["last_sql"]` or `previous_artifacts["last_python"]`.
2. On the next turn, the Orchestrator checks semantic similarity of the new `standalone_query` against the cached query.
3. If similarity is high enough (and `force_fresh` is false), the cached result is reused directly, bypassing re-execution.

---

## PostgreSQL Checkpointing

Multi-turn conversation memory is handled by `langgraph-checkpoint-postgres`. Each conversation is identified by a `thread_id` passed by the Agent Chat UI. This means:

- Full conversation history persists across server restarts.
- Multiple users maintain independent conversation threads.
- State can be inspected and replayed at any checkpoint.

---

## Frontend → Backend Flow

```
Insights Chat UI (Next.js @ :3000)
    │  @langchain/langgraph-sdk
    │  POST /threads/{thread_id}/runs/stream
    ▼
Aegra API Server (FastAPI @ :2024)
    │  Agent Protocol
    ▼
LangGraph Runtime
    │  Executes insights graph
    ▼
Multi-Agent Graph (Postgres checkpointed)
```

The `useStream()` hook in the frontend automatically handles:
- Creating new threads
- Resuming existing threads via `thread_id` URL param
- Streaming SSE responses node-by-node
