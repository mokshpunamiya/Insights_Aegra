"""
prompts/__init__.py
───────────────────
Central prompt loader for the Eerly Insights system.

All LLM prompts are stored as Jinja2 (.j2) templates in this directory
so they can be edited, versioned, and A/B tested independently of Python code.

Usage
─────
    from eerly_studio.insights.prompts import load_prompt

    # Static prompt (no variables)
    system_text = load_prompt("orchestrator_system")

    # Prompt with template variables
    user_text = load_prompt("sql_generator_user", question=user_query, schema_ctx=schema_str)

File naming convention
──────────────────────
    <node_or_tool>_<role>.j2

    Role is one of:
      system  – the SystemMessage content
      user    – a HumanMessage template with {{ variables }}

Available prompts
─────────────────
    orchestrator_system.j2                  → orchestrator_node.py
    synthesizer_system.j2                   → synthesizer_node.py
    sql_generator_system.j2                 → data_engineer_tools.py  (sql_generator_tool)
    data_scientist_code_system.j2           → data_scientist_node.py  (ds_generate_code)
    cognitive_map_table_system.j2           → scripts/cognitive_map_retrieval.py (_enrich_table)
    cognitive_map_column_chunk_system.j2    → scripts/cognitive_map_retrieval.py (_enrich_table_chunk)
    cognitive_map_column_merge_system.j2    → scripts/cognitive_map_retrieval.py (_merge_column_chunks)
    cognitive_map_business_chunk_system.j2  → scripts/cognitive_map_retrieval.py (_process_business_chunk)
    cognitive_map_business_merge_system.j2  → scripts/cognitive_map_retrieval.py (_merge_business_chunks)
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Directory that contains all .j2 template files
_PROMPTS_DIR = Path(__file__).parent


@lru_cache(maxsize=64)
def _read_template(name: str) -> str:
    """
    Load raw .j2 template text from disk, cached after the first read.

    Args:
        name: Filename without extension, e.g. "orchestrator_system".

    Returns:
        Raw template string.

    Raises:
        FileNotFoundError: if the .j2 file does not exist.
    """
    path = _PROMPTS_DIR / f"{name}.j2"
    if not path.exists():
        raise FileNotFoundError(
            f"[prompts] Template '{name}.j2' not found in {_PROMPTS_DIR}.\n"
            f"Available: {[p.stem for p in _PROMPTS_DIR.glob('*.j2')]}"
        )
    text = path.read_text(encoding="utf-8")
    logger.debug("[prompts] Loaded template '%s' (%d chars)", name, len(text))
    return text


def load_prompt(name: str, **variables: Any) -> str:
    """
    Load and optionally render a Jinja2 prompt template.

    Args:
        name:       Template filename without extension, e.g. "orchestrator_system".
        **variables: Keyword arguments injected as Jinja2 template variables.
                     If none are provided, the raw template text is returned as-is
                     (static system prompts don't need rendering).

    Returns:
        Rendered (or raw) prompt string.

    Example:
        # Static system prompt
        system = load_prompt("orchestrator_system")

        # User prompt with variables
        user = load_prompt("sql_generator_user", question="top 5 employees", schema_ctx=ctx)
    """
    raw = _read_template(name)

    if not variables:
        return raw

    try:
        from jinja2 import Environment, StrictUndefined
        env = Environment(undefined=StrictUndefined)
        template = env.from_string(raw)
        return template.render(**variables)
    except Exception as exc:
        logger.warning(
            "[prompts] Jinja2 render failed for '%s' (%s) — returning raw template", name, exc
        )
        return raw
