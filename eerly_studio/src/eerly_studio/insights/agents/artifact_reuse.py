"""
agents/artifact_reuse.py
────────────────────────
Lightweight helpers for the **hybrid history + reuse pattern**.

Design Goals
────────────
• Use real embedding similarity (AzureCohereEmbeddings) to decide whether the
  current standalone_query is semantically close enough to a previous artifact
  to skip re-execution.
• Do NOT store the full raw message history inside agent nodes — that is for
  the orchestrator / messages list only.
• Storage keys:
    "last_sql"    → last DataEngineer (SQL path) artifact
    "last_python" → last DataScientist (Python path) artifact

Artifact Schema (one per key in state["previous_artifacts"])
────────────────────────────────────────────────────────────
{
    "standalone_query": str,          # query that produced this artifact
    "generated_sql"   : str | None,   # SQL path only
    "python_code"     : str | None,   # Python path only
    "query_result"    : str | None,   # preview / hash when payload > _MAX_PREVIEW_CHARS
    "execution_result": str | None,   # Python path only
    "chart_config"    : dict | None,
    "timestamp"       : float,        # time.time() when artifact was saved
}

API
───
• compute_embedding_similarity(text1, text2) → float [0, 1]
• should_reuse(previous_artifact, current_standalone, threshold) → bool
• get_most_similar_previous(state, current_standalone, path_type) → dict | None
• build_artifact_record(...)  → dict   (helper to build the dict uniformly)
• truncate_for_storage(value, max_chars) → str   (safe preview truncation)
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Literal, Optional

logger = logging.getLogger(__name__)

# Large payloads (query_result / execution_result) are trimmed to this length
# before being stored in previous_artifacts, to avoid checkpoint bloat.
_MAX_PREVIEW_CHARS = 2_000  # ~2 KB per artifact

# Per-path similarity thresholds.
# SQL results are more sensitive to small filter changes (e.g., "last 7 days" vs
# "last 30 days"), so a slightly higher bar prevents stale SQL being reused.
# Python artifacts are more forgiving — the same analysis code often applies to
# lightly rephrased questions.
DEFAULT_THRESHOLD_SQL    = 0.88
DEFAULT_THRESHOLD_PYTHON = 0.85

# Maximum age of an artifact before it is considered stale (seconds).
# Override per-call via should_reuse(max_age_seconds=...) or
# get_most_similar_previous(max_age_seconds=...).
DEFAULT_MAX_AGE_SECONDS = 3600 * 4  # 4 hours


# ═══════════════════════════════════════════════════════════════════════════
# Embedding Similarity
# ═══════════════════════════════════════════════════════════════════════════

def compute_embedding_similarity(text1: str, text2: str) -> float:
    """
    Return cosine similarity ∈ [0, 1] between *text1* and *text2* using
    the project's real AzureCohereEmbeddings model.

    Falls back to 0.0 on any error so callers always get a usable float.
    """
    try:
        from eerly_studio.insights.utils.embeddings import AzureCohereEmbeddings
        import math

        embedder = AzureCohereEmbeddings()

        # Batch both texts in a single API call (2 × cheaper than two calls)
        vectors = embedder.embed_documents([text1, text2])
        v1, v2 = vectors[0], vectors[1]

        # Cosine similarity — manually computed to avoid extra dependencies
        dot   = sum(a * b for a, b in zip(v1, v2))
        norm1 = math.sqrt(sum(a * a for a in v1))
        norm2 = math.sqrt(sum(b * b for b in v2))

        if norm1 == 0.0 or norm2 == 0.0:
            return 0.0

        # Clamp to [0, 1] — floating-point noise can push slightly above 1.0
        return min(1.0, max(0.0, dot / (norm1 * norm2)))

    except Exception as exc:
        logger.warning("[artifact_reuse] compute_embedding_similarity failed: %s", exc)
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Reuse Decision
# ═══════════════════════════════════════════════════════════════════════════

def should_reuse(
    previous_artifact: Dict[str, Any],
    current_standalone: str,
    threshold: float = 0.87,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
) -> bool:
    """
    Return True if *current_standalone* is semantically similar enough to the
    query that produced *previous_artifact* to justify skipping re-execution.

    Args:
        previous_artifact: One entry from state["previous_artifacts"].
        current_standalone: The orchestrator-produced standalone_query for this turn.
        threshold: Cosine similarity threshold.
                   Tuning guidance:
                     ≥ 0.92 → very tight match (almost identical wording)
                     0.87   → good general default: catches slight rephrasing
                     ≤ 0.80 → looser — may reuse across loosely related questions
                   Use DEFAULT_THRESHOLD_SQL / DEFAULT_THRESHOLD_PYTHON for
                   path-appropriate defaults (set automatically by
                   get_most_similar_previous).
        max_age_seconds: Reject artifacts older than this many seconds (freshness
                         guard). Defaults to DEFAULT_MAX_AGE_SECONDS (4 hours).
                         Pass 0 to disable the freshness check entirely.

    Returns False (safe default) if any field is missing or an error occurs.
    """
    if not previous_artifact or not current_standalone:
        return False

    prev_query = previous_artifact.get("standalone_query", "")
    if not prev_query:
        return False

    # ── Freshness guard ─────────────────────────────────────────────
    # Reject stale artifacts before spending an embedding API call.
    # Skipping this check (max_age_seconds=0) is useful in tests.
    if max_age_seconds > 0:
        timestamp = previous_artifact.get("timestamp")
        if timestamp is not None:
            age_seconds = time.time() - timestamp
            if age_seconds > max_age_seconds:
                logger.info(
                    "[artifact_reuse] Artifact too old (age: %.1f min, limit: %.1f min) — skipping reuse",
                    age_seconds / 60,
                    max_age_seconds / 60,
                )
                return False

    # ── Semantic similarity check ───────────────────────────────────
    similarity = compute_embedding_similarity(current_standalone, prev_query)
    logger.info(
        "[artifact_reuse] Similarity %.4f (threshold %.2f) | prev='%s...' | curr='%s...'",
        similarity,
        threshold,
        prev_query[:60],
        current_standalone[:60],
    )
    return similarity >= threshold


# ═══════════════════════════════════════════════════════════════════════════
# Artifact Selector
# ═══════════════════════════════════════════════════════════════════════════

def get_most_similar_previous(
    state: Dict[str, Any],
    current_standalone: str,
    path_type: Literal["sql", "python"],
    threshold: Optional[float] = None,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
) -> Optional[Dict[str, Any]]:
    """
    Look up the most recent artifact for *path_type* in state["previous_artifacts"]
    and return it if it clears the freshness + similarity checks.

    Currently there is only one stored artifact per route ("last_sql" /
    "last_python"), but this function is designed to extend to a list later.

    Args:
        state:             The current AgentState dict.
        current_standalone: The orchestrator standalone_query for this turn.
        path_type:         "sql" or "python".
        threshold:         Cosine similarity threshold. When None (default), uses
                           DEFAULT_THRESHOLD_SQL for "sql" and
                           DEFAULT_THRESHOLD_PYTHON for "python" automatically.
        max_age_seconds:   Reject artifacts older than this. Forwarded to
                           should_reuse(). Defaults to DEFAULT_MAX_AGE_SECONDS
                           (4 h). Pass 0 to disable the freshness check.

    Returns:
        The artifact dict if reuse is appropriate, else None.
    """
    # Map path_type → storage key
    key = "last_sql" if path_type == "sql" else "last_python"

    # Resolve path-specific threshold when caller did not supply one explicitly
    effective_threshold = (
        threshold
        if threshold is not None
        else (DEFAULT_THRESHOLD_SQL if path_type == "sql" else DEFAULT_THRESHOLD_PYTHON)
    )

    artifacts: Dict[str, Any] = state.get("previous_artifacts") or {}
    previous = artifacts.get(key)

    if not previous:
        logger.debug("[artifact_reuse] No previous artifact for key='%s'", key)
        return None

    if should_reuse(
        previous,
        current_standalone,
        threshold=effective_threshold,
        max_age_seconds=max_age_seconds,
    ):
        logger.info(
            "[artifact_reuse] ✅ Reusing artifact key='%s' (threshold=%.2f)",
            key,
            effective_threshold,
        )
        return previous

    logger.debug(
        "[artifact_reuse] ❌ Similarity too low or artifact stale — fresh run for key='%s'",
        key,
    )
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Artifact Record Builder
# ═══════════════════════════════════════════════════════════════════════════

def build_artifact_record(
    standalone_query: str,
    *,
    generated_sql:    Optional[str]  = None,
    python_code:      Optional[str]  = None,
    query_result:     Optional[str]  = None,
    execution_result: Optional[str]  = None,
    chart_config:     Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Construct a well-formed artifact dict for saving into
    state["previous_artifacts"]["last_sql" | "last_python"].

    Large result payloads are automatically trimmed via truncate_for_storage()
    to keep checkpoint sizes manageable.
    """
    return {
        "standalone_query": standalone_query,
        "generated_sql":    generated_sql,
        "python_code":      python_code,
        # Trim large payloads — full data lives in AgentState.query_result / execution_result
        "query_result":     truncate_for_storage(query_result),
        "execution_result": truncate_for_storage(execution_result),
        "chart_config":     chart_config,
        "timestamp":        time.time(),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Storage Safety
# ═══════════════════════════════════════════════════════════════════════════

def truncate_for_storage(value: Optional[str], max_chars: int = _MAX_PREVIEW_CHARS) -> Optional[str]:
    """
    Trim *value* to *max_chars* characters for safe checkpoint storage.

    If the value exceeds *max_chars* a truncation marker is appended so
    readers know the preview is incomplete.

    Returns None unchanged (so callers can pass None safely).
    """
    if value is None:
        return None

    if not isinstance(value, str):
        # Graceful coercion — callers might pass dicts/lists
        try:
            import json
            value = json.dumps(value, default=str)
        except Exception:
            value = str(value)

    if len(value) <= max_chars:
        return value

    return value[:max_chars] + f"…[truncated {len(value) - max_chars} chars]"
