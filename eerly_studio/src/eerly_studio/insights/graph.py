import os
import logging
from dotenv import load_dotenv

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
# Using AsyncPostgresSaver for production-grade persistence
try:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

from eerly_studio.insights.agents.state import AgentState
from eerly_studio.insights.agents.orchestrator_node import orchestrator_node
from eerly_studio.insights.agents.data_engineer_node import data_engineer_wrapper
from eerly_studio.insights.agents.data_scientist_node import data_scientist_wrapper
from eerly_studio.insights.agents.synthesizer_node import synthesizer_node

load_dotenv()
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# Build the Graph
# ────────────────────────────────────────────────

def get_graph():
    """Compiles and returns the Multi-Agent Data Analysis Graph."""
    
    workflow = StateGraph(AgentState)

    # Add Nodes
    workflow.add_node("orchestrator",   orchestrator_node)
    workflow.add_node("data_engineer",  data_engineer_wrapper)
    workflow.add_node("data_scientist", data_scientist_wrapper)
    workflow.add_node("synthesizer",    synthesizer_node)

    # Entry Point
    workflow.set_entry_point("orchestrator")

    # routing is mostly handled by Command(goto=...) inside the nodes.
    workflow.add_edge("synthesizer",   END)

    # Checkpointer for state persistence
    db_url = os.getenv("DATABASE_URL")
    
    # In Aegra, if we are running within the server, we typically want the platform 
    # to manage persistence. However, for explicit Postgres usage:
    if HAS_POSTGRES and db_url:
        # LangGraph requires postgresql:// (psycopg) vs postgresql+asyncpg:// (sqlalchemy)
        pg_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
        # Optimization: We return the graph. Aegra or the runner will manage the saver lifetime.
        # For now, we provide the saver class/config if possible, or a live one if we can.
        
        # Note: AsyncPostgresSaver.from_conn_string is an async CM.
        # Since Aegra imports this module, we'll use a MemorySaver as default 
        # but allow the runtime to override or use Postgres if initialized.
        # LEGACY: conn = sqlite3.connect("/tmp/memory.db", check_same_thread=False)
        # memory = SqliteSaver(conn)
        
        # For professional modularity, we use MemorySaver here and expect 
        # the Aegra platform to provide the persistent Postgres checkpointer 
        # which it does by default when DATABASE_URL is set in the environment.
        memory = MemorySaver()
        logger.info("Graph compiled with MemorySaver (Aegra will override with Postgres in production)")
    else:
        memory = MemorySaver()
        logger.warning("Postgres not available or DATABASE_URL missing. Using MemorySaver.")
    
    return workflow.compile(checkpointer=memory)

# Export the compiled graph instance
graph = get_graph()

if __name__ == "__main__":
    print("Graph compiled successfully. Run 'runner.py' to execute scenarios.")