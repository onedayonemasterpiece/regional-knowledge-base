"""Kaggle-local ephemeral QUERY boundary; transport/admission/auth remain upstream."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .bge_contract import MAX_TOKENS, REVISION, SPACE, TOKENIZER_SHA256
from .kaggle_protocol import (
    EvidenceItem, EvidenceMetadata, ItemDependencies, PassageContinuation, Query,
    QueryResult, SourceDependency, PassageReadRequest, PassageReadResult, encode_envelope,
)
from .kaggle_search import SearchEvidence, fused_search, vector_literal
from .kaggle_acceptance_contracts import AcceptanceReadRequest, AcceptanceReadResult, BranchRanks


class DenseUnavailable(RuntimeError):
    pass


class TokenizerUnavailable(RuntimeError):
    pass


class ReadUnavailable(RuntimeError):
    def __init__(self, reason):
        self.reason = reason


PassageRead = PassageReadResult  # Preserve the internal resolver's import contract.


@dataclass(frozen=True)
class PinnedTokenizer:
    """Trusted nonblocking adapter: count exact no-prefix input with special tokens.

    Metadata is validated, not inferred from a callback. Deployment must verify
    actual loaded tokenizer bytes/settings; no tokenizer/model is loaded here.
    """
    count: Callable[[str], Awaitable[int]]
    revision: str
    tokenizer_sha256: str
    space: str

    def __post_init__(self):
        if (not callable(self.count) or self.revision != REVISION
                or self.tokenizer_sha256 != TOKENIZER_SHA256 or self.space != SPACE):
            raise ValueError("exact pinned BGE tokenizer/space contract required")


def _identity(query):
    return dict(request_id=query.request_id, connection_generation=query.connection_generation,
                turn_generation=query.turn_generation)


def _unavailable(query, reason, observed, *, invalid=False):
    result_type = AcceptanceReadResult if type(query) is AcceptanceReadRequest else QueryResult
    return result_type(**_identity(query), status="invalid_query" if invalid else "unavailable",
                       mode=None, reason=reason, observed_data_seq=observed.get("data"),
                       observed_projection_seq=observed.get("projection"),
                       observed_visibility_epoch=observed.get("visibility"),
                       observed_database_generation=observed.get("database"))


def _scope(query):
    if query.filters.kinds is not None and query.filters.kinds not in (("source",),("story",)):
        raise ReadUnavailable("invalid_filters")
    if query.filters.source_ids is not None:
        try:
            values = tuple(UUID(value) for value in query.filters.source_ids)
        except (ValueError, TypeError, AttributeError):
            raise ReadUnavailable("invalid_filters") from None
        if len(set(values)) != len(values):
            raise ReadUnavailable("invalid_filters")


async def _frontier(cursor, execute, query, observed, *, require_dense=True):
    await execute("""SELECT s.applied_seq,f.projection_seq,f.visibility_epoch,f.text_ready,f.dense_ready,s.database_generation
        FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
        WHERE s.singleton AND f.singleton""")
    row = await cursor.fetchone()
    if row is None:
        raise ReadUnavailable("not_ready")
    data, projection, visibility, text_ready, dense_ready, database = row
    observed.update(data=data, projection=projection, visibility=visibility, database=database)
    if projection is None or not text_ready or (require_dense and not dense_ready):
        raise ReadUnavailable("not_ready")
    if projection != data or data < query.min_data_seq or projection < query.min_data_seq:
        raise ReadUnavailable("projection_lag")


def _item(hit: SearchEvidence, source: SourceDependency, epoch: int, *,
          database_generation: str, text_offset=0, bibliography_offset=0, text_budget=8000):
    # Characters are Unicode offsets into the exact immutable passage/bibliography.
    text = hit.body[text_offset:text_offset + min(8000, text_budget)]
    # Worst-case JSON escaping is6bytes/character. Together with the1000-char
    # SQL locator this stays below12KiB even for accepted control characters.
    citation = hit.bibliography[bibliography_offset:bibliography_offset + 1024]
    next_text = text_offset + len(text)
    next_bib = bibliography_offset + len(citation)
    partial = next_text < len(hit.body) or next_bib < len(hit.bibliography)
    excerpt = partial or text_offset > 0 or bibliography_offset > 0
    locator = PassageContinuation(database_generation=database_generation,
                                  source=source, passage_id=hit.passage_id, visibility_epoch=epoch,
                                  text_offset=next_text, bibliography_offset=next_bib) if excerpt else None
    # This is a typed immutable-row locator, never an opaque capability or URL.
    continuation = f"passage:{hit.passage_id}:r{hit.revision}:t{next_text}:b{next_bib}" if excerpt else None
    return EvidenceItem(id=hit.passage_id, kind="source", text=text,
        metadata=EvidenceMetadata(citation=citation, locator=hit.locator,source_provenance=hit.source_provenance),
        dependencies=ItemDependencies(source_ids=(hit.source_id,), variant_refs=(), grant_refs=(),
                                      source_revisions=(source,)),
        text_form="excerpt" if excerpt else "complete", continuation=continuation,
        source_continuation=locator, continuation_complete=excerpt and not partial)


async def execute_query(query: Query, *, acquire, tokenizer: PinnedTokenizer, encode,
                        telemetry: asyncio.Queue | None = None) -> QueryResult:
    """Acquire from the existing pool; return one bounded typed QUERY_RESULT.

    acquire is an existing async pool-lease context manager factory. Input is an
    authenticated controller Query, not a public request or authorization proof.
    Callbacks must not block the event loop; shared encode retains its permit
    until the actual forward finishes even when this consumer is cancelled.
    """
    if type(query) not in (Query, AcceptanceReadRequest) or type(tokenizer) is not PinnedTokenizer:
        raise TypeError("validated Query and pinned tokenizer adapter required")
    if type(query) is AcceptanceReadRequest:
        # Private fixture scope must also survive constructed/model_copy inputs.
        type(query).model_validate(query.model_dump())
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + min(query.deadline_ms, 2000) / 1000
    observed = {}
    stages = {}
    mark = started
    try:
        async with asyncio.timeout_at(deadline):
            _scope(query)  # Unsupported kinds/source IDs do not acquire SQL or inference.
            try:
                count = await tokenizer.count(query.query)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise TokenizerUnavailable("loaded tokenizer unavailable") from None
            stages["tokenize_ms"] = (loop.time() - mark) * 1000
            if type(count) is not int or count < 1:
                raise TokenizerUnavailable("token count contract failed")
            if count > MAX_TOKENS:
                result = _unavailable(query, "token_limit_exceeded", observed, invalid=True)
            else:
                mark = loop.time()
                async with acquire() as connection:
                    stages["acquire_ms"] = (loop.time() - mark) * 1000
                    dependencies = {}

                    async def checked_encode(text):
                        try:
                            values = await encode(text)
                            vector_literal(values)  # Invalid dense output cannot become hybrid success.
                            return values
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            raise DenseUnavailable("resident encoder is unavailable") from None

                    async def guard(cursor, execute):
                        await _frontier(cursor, execute, query, observed)
                        if type(query) is AcceptanceReadRequest:
                            # Verify BOTH accepted fixture revisions even when
                            # neither branch returns a hit. A changed source is
                            # unavailable, never a benchmark zero-hit result.
                            await execute('''SELECT a.source_id,a.revision,v.eligibility_revision
                                FROM rkb_v4.active_revision a JOIN rkb_v4.source_visibility v USING(source_id)
                                JOIN rkb_v4.sources s USING(source_id)
                                WHERE a.source_id=ANY(%s::uuid[]) AND s.accepted AND NOT s.withdrawn''',
                                ([UUID(s.source_id) for s in query.fixture_sources],))
                            current=tuple(SourceDependency(source_id=str(s),content_revision=r,eligibility_revision=e)
                                for s,r,e in await cursor.fetchall())
                            if len(current)!=2 or set(current)!=set(query.fixture_sources):
                                raise ReadUnavailable('evidence_withdrawn')

                    async def finish(cursor, execute, searched):
                        ids = [UUID(hit.source_id) for hit in searched.evidence]
                        await execute("""SELECT a.source_id,a.revision,v.eligibility_revision
                            FROM rkb_v4.active_revision a JOIN rkb_v4.source_visibility v USING(source_id)
                            JOIN rkb_v4.sources s USING(source_id)
                            WHERE a.source_id=ANY(%s::uuid[]) AND s.accepted AND NOT s.withdrawn""", (ids,))
                        for source_id, revision, eligibility in await cursor.fetchall():
                            dependencies[str(source_id)] = SourceDependency(source_id=str(source_id),
                                content_revision=revision, eligibility_revision=eligibility)
                        if any(hit.source_id not in dependencies or dependencies[hit.source_id].content_revision != hit.revision
                               for hit in searched.evidence):
                            raise ReadUnavailable("not_ready")

                    mark = loop.time()
                    acceptance = type(query) is AcceptanceReadRequest
                    if acceptance:
                        from .kaggle_search import acceptance_search
                    searched = await (acceptance_search if acceptance else fused_search)(connection, query.query, encode=checked_encode,
                        **({} if acceptance else {'top_k':query.evidence_limit}), filters=query.filters,
                        timeout_seconds=max(.000001, deadline - loop.time()),
                        snapshot_guard=guard, snapshot_finish=finish, stage_timings=stages)
                    stages["search_ms"] = (loop.time() - mark) * 1000
                    budget = 24000
                    evidence = []
                    for hit in searched.evidence:
                        # Share finite text budget across remaining items; preserve all<=10.
                        per_item = min(8000, budget // (len(searched.evidence) - len(evidence)))
                        if query.filters.kinds == ("story",):
                            from .kaggle_story_read import story_item
                            entry = story_item(hit.record)
                            if entry.dependencies.source_revisions != (dependencies[hit.source_id],):
                                raise ReadUnavailable("not_ready")
                        else:
                            entry = _item(hit, dependencies[hit.source_id], observed["visibility"],
                                          database_generation=observed["database"], text_budget=per_item)
                        budget -= len(entry.text)
                        evidence.append(entry)
                    # Bounded top-k/ANN is the normal search policy, not an
                    # observed runtime failure. Recall acceptance is separate.
                    result = (AcceptanceReadResult if acceptance else QueryResult)(**_identity(query),
                        status="degraded" if acceptance else "ok", mode="hybrid",
                        **({'ranks':BranchRanks(bge=searched.branch_ranks[0],lexical=searched.branch_ranks[1],
                            fusion=searched.branch_ranks[2])} if acceptance else {}),
                        reason="bounded_retrieval" if acceptance else None,
                        evidence=tuple(evidence), observed_data_seq=observed["data"],
                        observed_projection_seq=observed["projection"], observed_visibility_epoch=observed["visibility"],
                        observed_database_generation=observed["database"])
            mark = loop.time()
            encode_envelope(result)  # Includes strict aggregate, UTF-8 and byte caps.
            stages["serialize_ms"] = (loop.time() - mark) * 1000
            if loop.time() >= deadline:
                raise TimeoutError
            return result
    except ReadUnavailable as exc:
        return _unavailable(query, exc.reason, observed, invalid=exc.reason == "invalid_filters")
    except TimeoutError:
        return _unavailable(query, "deadline_exceeded", observed)
    except DenseUnavailable:
        return _unavailable(query, "dense_unavailable", observed)
    except TokenizerUnavailable:
        return _unavailable(query, "not_ready", observed)
    except psycopg.errors.UndefinedTable:
        return _unavailable(query, "not_ready", observed)
    except psycopg.Error:
        return _unavailable(query, "internal_error", observed)
    finally:
        if telemetry is not None:
            from .query_telemetry import emit_executor_stage
            stages["total_ms"] = (loop.time() - started) * 1000
            emit_executor_stage(telemetry, query, stages)


async def execute_passage_read(request: PassageReadRequest, *, acquire, source_provenance=False, public_illustrations=False) -> PassageReadResult:
    if type(request) is not PassageReadRequest:
        raise TypeError("validated PassageReadRequest required")
    return await read_continuation(request, request.locator, acquire=acquire,
        source_provenance=source_provenance,public_illustrations=public_illustrations)


async def read_continuation(query: Query | PassageReadRequest, locator: PassageContinuation, *, acquire,source_provenance=False,public_illustrations=False) -> PassageRead:
    """Internal continuation resolver after renewed upstream authorization.

    An unchanged immutable passage/source fence is required. The
    locator itself grants no rights; the Dev output gate must still reauthorize.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + min(query.deadline_ms, 2000) / 1000
    observed = {}

    def unavailable(reason, invalid=False):
        return PassageRead(**_identity(query), status="invalid_query" if invalid else "unavailable",
            reason=reason, observed_data_seq=observed.get("data"),
            observed_projection_seq=observed.get("projection"), observed_visibility_epoch=observed.get("visibility"),
            observed_database_generation=observed.get("database"))

    try:
        async with asyncio.timeout_at(deadline):
            if type(query) is Query:
                _scope(query)
            try:
                source_id = UUID(locator.source.source_id)
                UUID(locator.passage_id)
            except ValueError:
                raise ReadUnavailable("invalid_input") from None
            if (type(query) is Query and query.filters.source_ids is not None
                    and source_id not in {UUID(v) for v in query.filters.source_ids}):
                raise ReadUnavailable("invalid_filters")
            async with acquire() as connection:
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                        or connection.row_factory is not tuple_row):
                    raise ValueError("continuation requires an exclusively checked-out idle tuple_row connection")
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

                        async def execute(statement, params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1, int((deadline - loop.time()) * 1000))),))
                            await cursor.execute(statement, params)

                        await _frontier(cursor, execute, query, observed, require_dense=False)
                        await execute("""SELECT p.passage_id,p.source_id,p.revision,p.locator,p.body,s.bibliography
                            FROM rkb_v4.passage_search p
                            JOIN rkb_v4.active_revision a USING(source_id,revision)
                            JOIN rkb_v4.sources s USING(source_id)
                            WHERE p.passage_id=%s::uuid AND p.source_id=%s::uuid AND p.revision=%s
                            AND s.accepted AND NOT s.withdrawn""",
                            (locator.passage_id, locator.source.source_id, locator.source.content_revision))
                        row = await cursor.fetchone()
                        if row is None or observed["database"] != locator.database_generation:
                            raise ReadUnavailable("evidence_withdrawn")
                        hit = SearchEvidence(str(row[0]), str(row[1]), *row[2:6])
                        illustrations, illustrations_has_more = (), False
                        if source_provenance:
                            await execute('SELECT metadata FROM rkb_v4.source_passage_metadata_v1(%s::uuid[])',([locator.passage_id],))
                            metadata=await cursor.fetchone()
                            if metadata is None or type(metadata[0]) is not dict:
                                raise ReadUnavailable('not_ready')
                            provenance = dict(metadata[0])
                            if public_illustrations:
                                from .kaggle_passage_illustrations import read_passage_illustrations
                                illustrations, illustrations_has_more = await read_passage_illustrations(
                                    cursor, execute, source_id=locator.source.source_id,
                                    passage_id=locator.passage_id)
                            from dataclasses import replace
                            hit=replace(hit,source_provenance=provenance)
                        if locator.text_offset>len(hit.body) or locator.bibliography_offset>len(hit.bibliography):
                            raise ReadUnavailable("invalid_input")
                        if locator.text_offset==len(hit.body) and locator.bibliography_offset==len(hit.bibliography):
                            raise ReadUnavailable("invalid_input")
                        # A completed text with remaining bibliography still carries
                        # an explicit one-character overlapping passage anchor.
                        text_start = min(locator.text_offset, len(hit.body) - 1)
                        entry = _item(hit, locator.source, observed["visibility"],
                                      database_generation=observed["database"],
                                      text_offset=text_start, bibliography_offset=locator.bibliography_offset)
                        result = PassageRead(**_identity(query), status="ok",
                            illustrations=illustrations, illustrations_has_more=illustrations_has_more,
                            evidence=(entry,), observed_data_seq=observed["data"],
                            observed_projection_seq=observed["projection"], observed_visibility_epoch=observed["visibility"],
                            observed_database_generation=observed["database"])
                encode_envelope(result)
                if loop.time() >= deadline:
                    raise TimeoutError
                return result
    except ReadUnavailable as exc:
        return unavailable(exc.reason, invalid=exc.reason in ("invalid_filters", "invalid_input"))
    except TimeoutError:
        return unavailable("deadline_exceeded")
    except psycopg.errors.QueryCanceled as exc:
        if loop.time() + .001 >= deadline and exc.diag.message_primary == "canceling statement due to statement timeout":
            return unavailable("deadline_exceeded")
        return unavailable("internal_error")
    except psycopg.Error:
        return unavailable("internal_error")
