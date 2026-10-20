"""Kaggle-local fused passage search; no authority, model loading or dispatcher."""
from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from uuid import UUID

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .bge_contract import validate_vector

# Strong references only until encoder completion; no dispatch/retry/job semantics.
_PENDING_ENCODERS: set[asyncio.Task] = set()


def _observe_encoder(task: asyncio.Task) -> None:
    _PENDING_ENCODERS.discard(task)
    if not task.cancelled():
        task.exception()  # Observe even a late, discarded inference failure.


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
    incomplete: bool = True
    limitation: str = "bounded_ann_recall_unverified"


def vector_literal(values: Sequence[float]) -> str:
    # validate_vector performs dimension/finite/L2 checks, but accepts coercions;
    # reject wire booleans and strings before using the shared contract.
    if not isinstance(values, list) or any(type(v) not in (int, float) for v in values):
        raise ValueError("vector values must be numeric, excluding booleans")
    return "[" + ",".join(map(str, validate_vector(values))) + "]"


async def fused_search(connection, query: str, *,
                       encode: Callable[[str], Awaitable[Sequence[float]]],
                       top_k: int = 3, candidate_budget: int = 100,
                       timeout_seconds: float = 2.0) -> SearchResult:
    """Execute on an exclusively checked-out idle async psycopg connection.

    Encoder adapter contract: cancellation discards this consumer; the adapter
    retains its inference permit until the actual shared forward finishes. An
    adapter may suppress cancellation while finishing; this wrapper returns on
    deadline and observes its eventual result/exception without waiting for it.
    SQL cancellation and transaction rollback finish before pool reuse. Deadline
    expiry raises TimeoutError, never a successful empty response. The caller
    owns admission, pinned token count, actual residency and output freshness.
    """
    if (not isinstance(query, str) or not query.strip() or len(query) > 1000
            or len(query.encode("utf-8")) > 4096):
        raise ValueError("query requires 1..1000 characters and <=4096 UTF-8 bytes")
    if (type(top_k) is not int or type(candidate_budget) is not int
            or not 1 <= top_k <= 5 or not top_k <= candidate_budget <= 200):
        raise ValueError("require integer 1 <= top_k <= 5 and top_k <= budget <= 200")
    if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 2):
        raise ValueError("deadline must be within 2 seconds")
    if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
            or connection.row_factory is not tuple_row):
        raise ValueError("connection must be open, idle and use tuple_row")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    encoding = asyncio.create_task(encode(query))
    _PENDING_ENCODERS.add(encoding)
    encoding.add_done_callback(_observe_encoder)
    try:
        async with asyncio.timeout_at(deadline):
            async with connection.transaction():
                async with connection.cursor() as cursor:
                    async def execute(statement, params=()):
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            raise TimeoutError("search deadline exhausted")
                        # Refresh before every SQL operation. The outer absolute
                        # deadline also bounds this setting query and fetches.
                        await cursor.execute("SELECT set_config('statement_timeout', %s, true)",
                                             (str(max(1, int(remaining * 1000))),))
                        await cursor.execute(statement, params)

                    # This is the first SQL after BEGIN; read-only must precede SELECT.
                    await cursor.execute("SET TRANSACTION READ ONLY")
                    await execute("SET LOCAL hnsw.iterative_scan = strict_order")
                    await execute("SET LOCAL hnsw.max_scan_tuples = 1000")
                    await execute("SET LOCAL hnsw.ef_search = 100")
                    await execute("SELECT * FROM rkb_v4.lexical_candidates(%s, %s)",
                                  (query, candidate_budget))
                    lexical = await cursor.fetchall()
                    vector = vector_literal(await asyncio.shield(encoding))
                    await execute("SELECT * FROM rkb_v4.dense_candidates(%s::public.vector, %s)",
                                  (vector, candidate_budget))
                    dense = await cursor.fetchall()
                    scores: dict[UUID, float] = defaultdict(float)
                    for branch in (lexical, dense):
                        for rank, (passage_id, _) in enumerate(branch, 1):
                            scores[passage_id] += 1 / (60 + rank)
                    ids = sorted(scores, key=lambda key: (-scores[key], str(key)))[:top_k]
                    await execute("SELECT * FROM rkb_v4.hydrate(%s::uuid[])", (ids,))
                    rows = await cursor.fetchall()
                    return SearchResult(tuple(SearchEvidence(str(r[0]), str(r[1]), *r[2:])
                                              for r in rows))
    finally:
        if not encoding.done():
            encoding.cancel()
