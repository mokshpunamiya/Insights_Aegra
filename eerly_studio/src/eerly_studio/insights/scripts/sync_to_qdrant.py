"""
scripts/sync_to_qdrant.py
─────────────────────────
Helper script to sync the cognitive_map.json output directly to the Qdrant Vector store.
"""

import sys
import os
import json
import logging
import argparse

import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from eerly_studio.insights.storage.cognitive_map_vector_store import QdrantKnowledgeStore
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    _INSIGHTS_DIR = pathlib.Path(__file__).parent.parent
    _DEFAULT_PATH = str(_INSIGHTS_DIR / "cognitive_map_output" / "cognitive_map.json")

    parser = argparse.ArgumentParser(description="Sync cognitive map JSON to Qdrant.")
    parser.add_argument(
        "--path",
        default=_DEFAULT_PATH,
        help="Path to the cognitive_map.json file."
    )
    args = parser.parse_args()

    load_dotenv()
    
    json_path = os.path.abspath(args.path)
    if not os.path.exists(json_path):
        logger.error(f"Cognitive map not found at {json_path}")
        logger.info("Run: uv run python scripts/cognitive_map_retrieval.py to generate it.")
        sys.exit(1)

    logger.info(f"Loading cognitive map from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        cog_map = json.load(f)

    store = QdrantKnowledgeStore()
    logger.info(f"Syncing to Qdrant collection '{store.collection_name}'...")
    
    try:
        logger.info("Clearing previous collection to ensure a clean sync...")
        store.client.delete_collection(store.collection_name)
    except Exception as e:
        logger.info(f"Note: Collection clear skipped (it may not exist yet): {e}")

    try:
        result = store.sync_knowledge(cog_map, batch_size=50)
        logger.info(result)
        logger.info("Sync complete. Vectors are now ready for querying.")
    except Exception as e:
        logger.error(f"Failed to sync to Qdrant: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
