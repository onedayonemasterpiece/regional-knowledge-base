"""Kaggle-local fused passage search; no authority, model loading or dispatcher."""
from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from uuid import UUID

from psycopg.errors import QueryCanceled
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .bge_contract import validate_vector
from .kaggle_protocol import QueryFilters

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
    source_provenance: dict | None = None


@dataclass(frozen=True)
class StorySearchEvidence:
    record: dict

    @property
    def source_id(self):return self.record['source']['source_id']

    @property
    def revision(self):return self.record['source']['content_revision']


@dataclass(frozen=True)
class SearchResult:
    evidence: tuple[SearchEvidence, ...]
    incomplete: bool = True
    limitation: str = "bounded_ann_recall_unverified"
    branch_ranks: tuple[tuple[str, ...], ...] | None = None


def vector_literal(values: Sequence[float]) -> str:
    # validate_vector performs dimension/finite/L2 checks, but accepts coercions;
    # reject wire booleans and strings before using the shared contract.
    if not isinstance(values, list) or any(type(v) not in (int, float) for v in values):
        raise ValueError("vector values must be numeric, excluding booleans")
    return "[" + ",".join(map(str, validate_vector(values))) + "]"


async def fused_search(connection, query: str, *,
                       encode: Callable[[str], Awaitable[Sequence[float]]],
                       top_k: int = 3, candidate_budget: int = 100,
                       timeout_seconds: float = 2.0,
                       filters: QueryFilters | None = None,
                       snapshot_guard=None, snapshot_finish=None,
                       stage_timings: dict[str, float] | None = None) -> SearchResult:
    return await _search(connection, query, encode=encode, top_k=top_k,
        candidate_budget=candidate_budget, timeout_seconds=timeout_seconds,
        filters=filters, snapshot_guard=snapshot_guard, snapshot_finish=snapshot_finish,
        stage_timings=stage_timings, observe=False)


async def acceptance_search(connection, query: str, *, encode, filters,
                            timeout_seconds, snapshot_guard, snapshot_finish, stage_timings=None):
    """Internal measurement only; controller fixture authorization is upstream.

    Fixed source scope, candidate budget100, RRF60, HNSW settings and one encode
    match normal source retrieval. Hydrate the entire <=30 rank union in the
    same transaction before exposing any rank to the current output gate.
    """
    if (type(filters) is not QueryFilters or filters.kinds != ('source',)
            or filters.source_ids is None or len(filters.source_ids) != 2):
        raise ValueError('exact two-source fixture required')
    return await _search(connection, query, encode=encode, top_k=3,
        candidate_budget=100, timeout_seconds=timeout_seconds, filters=filters,
        snapshot_guard=snapshot_guard, snapshot_finish=snapshot_finish,
        stage_timings=stage_timings, observe=True)


async def _search(connection, query, *, encode, top_k, candidate_budget,
                  timeout_seconds, filters, snapshot_guard, snapshot_finish,
                  stage_timings, observe):
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
    source_ids = None
    story = type(filters) is QueryFilters and filters.kinds == ("story",)
    if filters is not None:
        if type(filters) is not QueryFilters:
            raise ValueError("filters must be QueryFilters")
        # Revalidate even constructed instances before inference or SQL.
        filters = QueryFilters.model_validate(filters.model_dump())
        if filters.kinds is not None and filters.kinds not in (("source",),("story",)):
            raise ValueError("unsupported search kinds")
        if filters.source_ids is not None:
            try:
                source_ids = [UUID(value) for value in filters.source_ids]
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError("source filters require UUIDs") from exc
            if len(set(source_ids)) != len(source_ids):
                raise ValueError("duplicate source UUIDs")
    if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
            or connection.row_factory is not tuple_row):
        raise ValueError("connection must be open, idle and use tuple_row")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    encoding = None
    deadline_query_cancel = None
    try:
        async with asyncio.timeout_at(deadline):
            async with connection.transaction():
                async with connection.cursor() as cursor:
                    async def execute(statement, params=()):
                        nonlocal deadline_query_cancel
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            raise TimeoutError("search deadline exhausted")
                        # Refresh before every SQL operation. The outer absolute
                        # deadline also bounds this setting query and fetches.
                        await cursor.execute("SELECT set_config('statement_timeout', %s, true)",
                                             (str(max(1, int(remaining * 1000))),))
                        try:
                            await cursor.execute(statement, params)
                        except QueryCanceled as exc:
                            # Integer statement_timeout can expire <1ms before
                            # asyncio's absolute deadline. Capture the receipt
                            # time before rollback, which may itself take time.
                            if (loop.time() + .001 >= deadline
                                    and exc.diag.message_primary ==
                                    "canceling statement due to statement timeout"):
                                deadline_query_cancel = exc
                            raise

                    # This is the first SQL after BEGIN; read-only must precede SELECT.
                    await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    if snapshot_guard is not None:
                        await snapshot_guard(cursor, execute)
                    editorial=False
                    if story:
                        await execute('SELECT schema_revision FROM rkb_v4.recovery_state WHERE singleton')
                        semantic=await cursor.fetchone()
                        editorial=semantic in [('rkb-v4-sql001-009-v1',),('rkb-v4-sql001-011-v1',)]
                    async def timed_encode():
                        began = loop.time()
                        try:
                            return await encode(query)
                        finally:
                            if stage_timings is not None:
                                stage_timings["encode_ms"] = (loop.time() - began) * 1000

                    encoding = asyncio.create_task(timed_encode())
                    _PENDING_ENCODERS.add(encoding)
                    encoding.add_done_callback(_observe_encoder)
                    await execute("SET LOCAL hnsw.iterative_scan = strict_order")
                    await execute("SET LOCAL hnsw.max_scan_tuples = 1000")
                    await execute("SET LOCAL hnsw.ef_search = 100")
                    began = loop.time()
                    if story:
                        await execute("SELECT * FROM rkb_v4.editorial_lexical_candidates_v1(%s,%s,%s::uuid[])" if editorial else
                                      "SELECT * FROM rkb_v4.story_lexical_candidates_v1(%s,%s,%s::uuid[])",
                                      (query,candidate_budget,source_ids))
                    elif source_ids is None:
                        await execute("SELECT * FROM rkb_v4.lexical_candidates(%s, %s)",
                                      (query, candidate_budget))
                    else:
                        await execute("SELECT * FROM rkb_v4.lexical_candidates_scoped(%s, %s, %s::uuid[])",
                                      (query, candidate_budget, source_ids))
                    lexical = await cursor.fetchall()
                    if stage_timings is not None:
                        stage_timings["lexical_ms"] = (loop.time() - began) * 1000
                    vector = vector_literal(await asyncio.shield(encoding))
                    began = loop.time()
                    if story:
                        await execute("SELECT * FROM rkb_v4.editorial_dense_candidates_v1(%s::public.vector,%s,%s::uuid[])" if editorial else
                                      "SELECT * FROM rkb_v4.story_dense_candidates_v1(%s::public.vector,%s,%s::uuid[])",
                                      (vector,candidate_budget,source_ids))
                    elif source_ids is None:
                        await execute("SELECT * FROM rkb_v4.dense_candidates(%s::public.vector, %s)",
                                      (vector, candidate_budget))
                    else:
                        await execute("SELECT * FROM rkb_v4.dense_candidates_scoped(%s::public.vector, %s, %s::uuid[])",
                                      (vector, candidate_budget, source_ids))
                    dense = await cursor.fetchall()
                    if stage_timings is not None:
                        stage_timings["dense_ms"] = (loop.time() - began) * 1000
                    scores: dict[UUID, float] = defaultdict(float)
                    for branch in (lexical, dense):
                        for rank, (passage_id, _) in enumerate(branch, 1):
                            scores[passage_id] += 1 / (60 + rank)
                    fused_ids = sorted(scores, key=lambda key: (-scores[key], str(key)))[:10 if observe else top_k]
                    ranks = (tuple(str(row[0]) for row in dense[:10]),
                             tuple(str(row[0]) for row in lexical[:10]),
                             tuple(str(value) for value in fused_ids)) if observe else None
                    ids = list(dict.fromkeys(UUID(value) for branch in ranks for value in branch)) if observe else fused_ids
                    began = loop.time()
                    if story:
                        await execute("SELECT * FROM rkb_v4.editorial_hydrate_v1(%s::uuid[],%s::uuid[])" if editorial else
                                      "SELECT * FROM rkb_v4.story_hydrate_v1(%s::uuid[],%s::uuid[])", (ids,source_ids))
                    elif source_ids is None:
                        await execute("SELECT * FROM rkb_v4.hydrate(%s::uuid[])", (ids,))
                    else:
                        await execute("SELECT * FROM rkb_v4.hydrate_scoped(%s::uuid[], %s::uuid[])",
                                      (ids, source_ids))
                    rows = await cursor.fetchall()
                    if observe and (len(rows) != len(ids) or {row[0] for row in rows} != set(ids)):
                        raise ValueError('acceptance rank hydration incomplete')
                    if stage_timings is not None:
                        stage_timings["hydrate_ms"] = (loop.time() - began) * 1000
                    result = SearchResult(tuple(StorySearchEvidence(r[1]) if story else
                                              SearchEvidence(str(r[0]), str(r[1]), *r[2:])
                                              for r in rows if not story or r[1] is not None), limitation=(
                        "bounded_candidate_fusion_unverified" if source_ids is not None
                        else "bounded_ann_recall_unverified"), branch_ranks=ranks)
                    if snapshot_finish is not None:
                        await snapshot_finish(cursor, execute, result)
                    return result
    except QueryCanceled as exc:
        # The transaction context has rolled back before exposing this stable
        # public timeout. Other cancellations and SQL errors retain their type.
        if exc is deadline_query_cancel:
            raise TimeoutError("search deadline exhausted") from exc
        raise
    finally:
        if encoding is not None and not encoding.done():
            encoding.cancel()
