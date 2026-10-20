"""Kaggle-local fused passage search; no authority, model loading or dispatcher.

The caller owns admission, pinned-token validation, model residency, verified
identity and output freshness. The connection must be exclusively checked out,
idle, and use tuple rows. Never transmit intermediate vectors to DevCoveer.
"""
from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class SearchEvidence:
    passage_id: str
    source_id: str
    revision: int
    locator: str
    body: str
    bibliography: str


@dataclass(frozen=True)
class SearchResult:
    evidence: tuple[SearchEvidence, ...]
    # ANN budget or current-pointer filtering may omit matches, even with full K.
    incomplete: bool = True
    limitation: str = "bounded_ann_recall_unverified"


def vector_literal(values: Sequence[float]) -> str:
    if len(values) != 1024:
        raise ValueError("BGE baseline requires 1024 dimensions")
    floats = [float(v) for v in values]
    if not all(math.isfinite(v) for v in floats) or not any(floats):
        raise ValueError("query vector must be finite and nonzero")
    return "[" + ",".join(map(str, floats)) + "]"


async def fused_search(connection, query: str, *,
                       encode: Callable[[str], Awaitable[Sequence[float]]],
                       top_k: int = 10, candidate_budget: int = 100,
                       timeout_seconds: float = 2.0) -> SearchResult:
    """Encode + independent FTS/dense + RRF + one current-eligibility hydrate.

    Deadline failure raises TimeoutError, never an empty successful response.
    Encoder cancellation must discard this consumer without releasing a shared
    inference permit until its actual forward completes. Psycopg cancellation
    and transaction cleanup finish before the checked-out connection is returned.
    """
    if not query.strip() or len(query) > 16000:
        raise ValueError("query must contain 1..16000 characters")
    if not 1 <= top_k <= 50 or not top_k <= candidate_budget <= 200:
        raise ValueError("require 1 <= top_k <= 50 and top_k <= budget <= 200")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 2:
        raise ValueError("deadline must be within 2 seconds")
    async with asyncio.timeout(timeout_seconds):
        encoding = asyncio.create_task(encode(query))
        try:
            async with connection.transaction():
                # SET LOCAL keeps knobs out of the pool's next request.
                async with connection.cursor() as cursor:
                    await cursor.execute("SET TRANSACTION READ ONLY")
                    await cursor.execute("SELECT set_config('statement_timeout', %s, true)",
                                         (str(max(1, int(timeout_seconds * 1000))),))
                    await cursor.execute("SET LOCAL hnsw.iterative_scan = strict_order")
                    await cursor.execute("SET LOCAL hnsw.max_scan_tuples = 1000")
                    await cursor.execute("SET LOCAL hnsw.ef_search = 100")
                    await cursor.execute("SELECT * FROM rkb_v4.lexical_candidates(%s, %s)",
                                         (query, candidate_budget))
                    lexical = await cursor.fetchall()
                    vector = vector_literal(await encoding)
                    await cursor.execute("SELECT * FROM rkb_v4.dense_candidates(%s::public.vector, %s)",
                                         (vector, candidate_budget))
                    dense = await cursor.fetchall()
                    scores: dict[UUID, float] = defaultdict(float)
                    for branch in (lexical, dense):
                        for rank, (passage_id, _) in enumerate(branch, 1):
                            scores[passage_id] += 1 / (60 + rank)
                    ids = sorted(scores, key=lambda key: (-scores[key], str(key)))[:top_k]
                    await cursor.execute("SELECT * FROM rkb_v4.hydrate(%s::uuid[])", (ids,))
                    rows = await cursor.fetchall()
                    return SearchResult(tuple(SearchEvidence(str(r[0]), str(r[1]), *r[2:])
                                              for r in rows))
        finally:
            if not encoding.done():
                encoding.cancel()
            await asyncio.gather(encoding, return_exceptions=True)
