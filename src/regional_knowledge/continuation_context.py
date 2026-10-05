"""Conservative retrieval-time expansion for strong chunk continuations."""
from __future__ import annotations

import re
from typing import Any

_TERMINATORS = (".", "!", "?", "…")
_CLOSERS = '»”"\')]}'
STRONG_THRESHOLD = 4

_CONNECTORS = {
    "и","а","но","или","что","как","к","в","во","на","с","со","из","от","до",
    "по","для","при","о","об","за","под","над","между","перед","после","через",
    "and","or","but","of","to","in","on","with","from","for","by","und","oder",
    "aber","von","zu","in","mit","aus","für",
}


def compact_text(value: str | None) -> str:
    return " ".join((value or "").split())



_QUERY_STOPWORDS = {
    "какие","какой","какая","какое","каких","когда","почему","зачем","который","которые",
    "этого","этой","этот","было","были","была","будет","между","после","перед",
    "about","what","when","which","where","were","was","with","from","that","this",
    "welche","welcher","welches","wann","warum",
}

_LIST_CUES = {
    "какие","каких","перечисли","перечислите","назови","назовите",
    "which","list","name",
    "welche","welcher","welches","nenne","nennen",
}

_ADDITIVE_CONNECTORS = {"и","или","and","or","und","oder"}


def _word_terms(value: str | None) -> list[str]:
    return [
        token.casefold()
        for token in re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", compact_text(value))
    ]


def boundary_query_profile(
    query: str,
    left: str | None,
    right: str | None,
) -> dict[str, int | bool]:
    """Inspect exact query evidence on each side of a candidate boundary."""
    query_words=_word_terms(query)
    query_terms={
        token for token in query_words
        if len(token)>=4 and token not in _QUERY_STOPWORDS
    }
    left_terms={
        token for token in _word_terms(left)[-36:] if len(token)>=4
    }
    right_terms={
        token for token in _word_terms(right)[:24] if len(token)>=4
    }
    left_overlap=len(query_terms & left_terms)
    right_overlap=len(query_terms & right_terms)
    return {
        "left_overlap":left_overlap,
        "right_overlap":right_overlap,
        "total_overlap":len(query_terms & (left_terms | right_terms)),
        "list_cue":bool(set(query_words) & _LIST_CUES),
    }


def boundary_query_overlap(query: str, left: str | None, right: str | None) -> int:
    """Backward-compatible diagnostic: total meaningful boundary overlap."""
    return int(boundary_query_profile(query,left,right)["total_overlap"])


def query_allows_continuation_expansion(
    query: str,
    left: str | None,
    right: str | None,
    *,
    neighbor_side: str = "right",
) -> bool:
    """Require evidence that the added side can contribute answer context.

    left and right always describe source order. neighbor_side identifies
    which side is absent from the ranked window. This prevents source-only terms
    from being mistaken for evidence that a previous neighbor is useful.
    """
    if neighbor_side not in {"left","right"}:
        raise ValueError("neighbor_side must be left or right")
    profile=boundary_query_profile(query,left,right)
    left_overlap=int(profile["left_overlap"])
    right_overlap=int(profile["right_overlap"])
    total=int(profile["total_overlap"])
    neighbor_overlap=right_overlap if neighbor_side=="right" else left_overlap
    source_overlap=left_overlap if neighbor_side=="right" else right_overlap

    # Explicitly naming material on the missing side is the strongest signal.
    if neighbor_overlap>=1 and total>=2:
        return True

    left_text=compact_text(left)
    core=left_text.rstrip(_CLOSERS)
    words=re.findall(r"[A-Za-zА-Яа-яЁё]+",core)
    last=words[-1].casefold() if words else ""
    is_list_query=bool(profile["list_cue"])

    # A list question can legitimately ask for unseen items after/before an
    # additive continuation even though it cannot name those items in advance.
    if last in _ADDITIVE_CONNECTORS and is_list_query and source_overlap>=1:
        return True

    # Hyphenated lexical continuations remain conservative: prefer explicit
    # evidence on the missing side, with list-shaped source context as fallback.
    if core.endswith("-") and (
        neighbor_overlap>=1 or (is_list_query and source_overlap>=1)
    ):
        return True
    return False


def strong_continuation_boundary(
    left: str | None,
    right: str | None,
    *,
    left_has_illustrations: bool = False,
    right_has_illustrations: bool = False,
) -> int:
    """Return a small strength score for a high-confidence textual continuation.

    Visual-bearing chunks are deliberately excluded: continuation expansion is
    intended for printed prose, not caption/gallery adjacency.
    """
    if left_has_illustrations or right_has_illustrations:
        return 0
    left_text = compact_text(left)
    right_text = compact_text(right)
    if len(left_text) < 20 or len(right_text) < 10:
        return 0
    core = left_text.rstrip(_CLOSERS)
    if not core:
        return 0
    score = 0
    if core.endswith("-"):
        score += 4
    words = re.findall(r"[A-Za-zА-Яа-яЁё]+", core)
    if words and words[-1].casefold() in _CONNECTORS:
        score += 4
    first = next((char for char in right_text if char.isalpha()), "")
    if not core.endswith(_TERMINATORS) and first and first.islower():
        score += 3
    return score


def continuation_signal(*, direction: str, source_chunk_id: str, strength: int) -> dict[str, Any]:
    return {
        "branch": "continuation_neighbor",
        "direction": direction,
        "source_chunk_id": source_chunk_id,
        "strength": strength,
    }
