"""
export_graph.py
───────────────
Exports the Multi-Agent Data Analysis System LangGraph orchestration
as a PNG image (langgraph_orchestration.png).

Graph Topology:
  START
    └─► orchestrator
            ├─ text_brief=True ──────────────────────────────► END
            └─ DATA_QUERY ──► [PAUSE: human approval] ──► data_engineer
                                                                │
                                    ┌───────────────────────────┤
                                    │                           │               │
                               learning_node           synthesizer ◄──  data_scientist
                                    │                           ▲        (E2B tool runs
                                    └──────► data_engineer      │         internally)
                                                                └────────────────┘
                                                               synthesizer ──► END

Nodes:
  1. orchestrator    – Intent classification & routing
  2. data_engineer   – SQL generation & execution (knowledge_retriever, sql_generator, sql_executor tools)
  3. data_scientist  – Python code generation + E2B execution (internal self-healing loop)
  4. learning_node   – SQL error introspection & Vector Store update
  5. synthesizer     – Final report generation (SSE streamed to client)

Usage:
  uv run python export_graph.py
"""

import sys
from main import graph


OUTPUT_FILE = "langgraph_orchestration.png"


def main():
    try:
        print("Generating PNG from LangGraph...")
        png_data = graph.get_graph(xray=True).draw_mermaid_png()
        with open(OUTPUT_FILE, "wb") as f:
            f.write(png_data)
        print(f"Successfully exported → {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error exporting graph: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
