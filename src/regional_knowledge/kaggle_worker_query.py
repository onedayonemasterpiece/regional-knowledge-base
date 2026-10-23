"""Trusted callback adapter for the existing resident Kaggle WSS worker.

No pool, tokenizer, model, transport or identity authority is created here.
The factory must verify the actual loaded artifacts before supplying its existing
PinnedTokenizer; matching metadata alone is not runtime artifact verification.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math

from .bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from .kaggle_protocol import (
    QUERY_MAX_BYTES, Query, QueryResult, PassageReadRequest, PassageReadResult,
    StoryReadRequest, StoryReadResult, ReadIdentity, decode_envelope, encode_envelope,
    SqlHealthRequest, SqlHealthResult, SqlHealthObservation,
)
from .kaggle_query import PinnedTokenizer, execute_query, execute_passage_read
from .query_telemetry import NativeReadTelemetry
from .kaggle_fact_contracts import FactReadRequest, FactReadResult
from .kaggle_source_read import SourceReadRequest, SourceReadResult
from .kaggle_case_contracts import CaseReadRequest, CaseReadResult
from .kaggle_geo_contracts import GeoReadRequest, GeoReadResult
from .kaggle_editorial_contracts import EditorialReadRequest, EditorialReadResult
from .kaggle_source_graph_read import SourceGraphReadRequest, SourceGraphReadResult
from .kaggle_acceptance_contracts import AcceptanceReadRequest, AcceptanceReadResult


class WorkerQueryInputError(ValueError):
    """No valid reply identity exists; the transport must fail closed."""


def _identity(frame):
    try:
        if type(frame) is not dict:
            raise TypeError
        identity = ReadIdentity.model_validate({key: frame.get(key) for key in (
            'request_id', 'connection_generation', 'turn_generation')})
        if len(identity.request_id.encode('utf-8')) > 256:
            raise ValueError
        return identity
    except (TypeError, ValueError):
        # Never invent IDs or copy validation diagnostics/private input into logs.
        raise WorkerQueryInputError('invalid worker QUERY identity') from None


def _failure(identity, reason, *, invalid=False, observed=None, exact=False,story=False,fact=False,source=False,case=False,geo=False,editorial=False,graph=False,acceptance=False):
    result_type = AcceptanceReadResult if acceptance else SourceGraphReadResult if graph else EditorialReadResult if editorial else CaseReadResult if case else GeoReadResult if geo else SourceReadResult if source else FactReadResult if fact else StoryReadResult if story else PassageReadResult if exact else QueryResult
    return result_type(**identity.model_dump(), status='invalid_query' if invalid else 'unavailable',
        **({} if exact else {'mode': None}), evidence=(), reason=reason,
        observed_data_seq=observed.observed_data_seq if observed else None,
        observed_projection_seq=observed.observed_projection_seq if observed else None,
        observed_visibility_epoch=observed.observed_visibility_epoch if observed else None,
        observed_database_generation=observed.observed_database_generation if observed else None)


def _check_tokenizer(tokenizer):
    if (type(tokenizer) is not PinnedTokenizer or not callable(getattr(tokenizer, 'count', None))
            or getattr(tokenizer, 'revision', None) != REVISION or getattr(tokenizer, 'space', None) != SPACE
            or getattr(tokenizer, 'tokenizer_sha256', None) != TOKENIZER_SHA256):
        raise ValueError('existing pinned BGE tokenizer/space contract required')


def _wire(result):
    # Validate, enforce aggregate/UTF-8 bounds and return an independent JSON dict.
    return json.loads(encode_envelope(result))


@dataclass(frozen=True)
class KaggleWorkerQuery:
    """Injected async handler; the caller owns the existing pool/model lifecycle.

    acquire is the already initialized pool's exclusive idle-connection lease
    factory. tokenizer is its already verified nonblocking pinned count adapter.
    encode is supplied per invocation by BgeWssTransport, preserving its actual
    forward ownership and shield; this class never cancels that native Task.
    """
    acquire: object
    tokenizer: PinnedTokenizer
    telemetry: asyncio.Queue | None = None
    source_provenance: bool = False

    def __post_init__(self):
        if not callable(self.acquire):
            raise ValueError('existing PG lease factory required')
        _check_tokenizer(self.tokenizer)
        if self.telemetry is not None and (
                not isinstance(self.telemetry, asyncio.Queue)
                or type(self.telemetry.maxsize) is not int or self.telemetry.maxsize <= 0):
            raise ValueError('finite dropping telemetry queue required')

    async def __call__(self, frame_dict, *, encode, lease_current=None):
        with NativeReadTelemetry(self.telemetry, frame_dict) as diagnostics:
            try:
                result = await self._call(frame_dict, encode=encode, lease_current=lease_current)
            except asyncio.CancelledError:
                diagnostics.finish(status='cancelled', reason='consumer_cancelled')
                raise
            except WorkerQueryInputError:
                diagnostics.finish(status='invalid_query', reason='invalid_input')
                raise
            except Exception:
                diagnostics.finish(status='error', reason='internal_error')
                raise
            diagnostics.finish(result)
            # The controller already accepts this fixed, bounded sidecar.
            # No optional drain/export callback runs before the read reply.
            if (result.get('type') == 'QUERY_RESULT' and diagnostics.runtime is not None
                    and self.telemetry is not None):
                from .query_telemetry import drain_native_read_events
                try:
                    events = drain_native_read_events(self.telemetry, result)
                except Exception:
                    events = []  # Optional observations cannot replace the read.
                if events:
                    result['read_events'] = events
            return result

    async def _call(self, frame_dict, *, encode, lease_current=None):
        loop = asyncio.get_running_loop()
        started = loop.time()
        identity = _identity(frame_dict)
        if frame_dict.get('type')=='SQL_HEALTH':
            return await self._health(frame_dict,identity,lease_current=lease_current,started=started)
        acceptance = frame_dict.get('type')=='ACCEPTANCE_READ'
        graph = frame_dict.get('type')=='SOURCE_GRAPH_READ'
        editorial = frame_dict.get('type')=='EDITORIAL_READ'
        case = frame_dict.get('type')=='CASE_READ'
        geo = frame_dict.get('type')=='GEO_READ'
        source = frame_dict.get('type')=='SOURCE_READ'
        fact = frame_dict.get('type')=='FACT_READ'
        story = frame_dict.get('type')=='STORY_READ'
        exact = graph or editorial or case or geo or source or fact or story or frame_dict.get('type') == 'PASSAGE_READ'
        def failure(reason, **kwargs):
            return _failure(identity, reason, exact=exact, story=story, fact=fact, source=source, case=case, geo=geo, editorial=editorial, graph=graph, acceptance=acceptance, **kwargs)
        try:
            raw = json.dumps(frame_dict, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
            if len(raw) > QUERY_MAX_BYTES:
                return _wire(failure('query_too_large', invalid=True))
            query = decode_envelope(raw)
            if (type(query) not in (Query, AcceptanceReadRequest, PassageReadRequest, StoryReadRequest, FactReadRequest, SourceReadRequest, CaseReadRequest, GeoReadRequest, EditorialReadRequest, SourceGraphReadRequest) or query.compute_claim is None
                    or len(query.compute_claim.encode('utf-8')) > 256
                    or not 1 <= query.deadline_ms <= 2000):
                raise ValueError
        except (TypeError, ValueError, RecursionError, OverflowError):
            return _wire(failure('invalid_input', invalid=True))
        if not exact:
            try:
                _check_tokenizer(self.tokenizer)
            except ValueError:
                return _wire(failure('not_ready'))
            if not callable(encode):
                return _wire(failure('not_ready'))
        # The worker's remaining budget includes this adapter, not a new per-hop
        # deadline. Round elapsed time up so parsing cannot extend the budget.
        remaining = query.deadline_ms - math.ceil((loop.time() - started) * 1000)
        if remaining <= 0:
            return _wire(failure('deadline_exceeded'))
        deadline = started + query.deadline_ms / 1000
        query = query.model_copy(update={'deadline_ms': remaining})
        try:
            if graph:
                from .kaggle_source_graph_read import execute_source_graph_read
                result=await execute_source_graph_read(query,acquire=self.acquire)
            elif editorial:
                from .kaggle_editorial_read import execute_editorial_read
                result=await execute_editorial_read(query,acquire=self.acquire)
            elif case:
                from .kaggle_case_read import execute_case_read
                result=await execute_case_read(query,acquire=self.acquire)
            elif geo:
                from .kaggle_geo_read import execute_geo_read
                result=await execute_geo_read(query,acquire=self.acquire)
            elif source:
                from .kaggle_source_read import execute_source_read
                result=await execute_source_read(query,acquire=self.acquire)
            elif fact:
                from .kaggle_fact_read import execute_fact_read
                result=await execute_fact_read(query,acquire=self.acquire)
            elif story:
                from .kaggle_story_read import execute_story_read
                result=await execute_story_read(query,acquire=self.acquire)
            else:
                result = (await execute_passage_read(query, acquire=self.acquire,
                    **({'source_provenance':True} if self.source_provenance else {})) if exact else
                await execute_query(query, acquire=self.acquire, tokenizer=self.tokenizer,
                                    encode=encode, telemetry=self.telemetry))
            result_type = AcceptanceReadResult if acceptance else SourceGraphReadResult if graph else EditorialReadResult if editorial else CaseReadResult if case else GeoReadResult if geo else SourceReadResult if source else FactReadResult if fact else StoryReadResult if story else PassageReadResult if exact else QueryResult
            if type(result) is not result_type or any(getattr(result, key) != getattr(identity, key)
                    for key in ('request_id', 'connection_generation', 'turn_generation')):
                raise ValueError('executor result identity mismatch')
            encoded = _wire(result)
            if loop.time() >= deadline:
                return _wire(failure('deadline_exceeded', observed=result))
            return encoded
        except asyncio.CancelledError:
            # execute_query/fused_search unwind their transaction and pool lease
            # before control returns. The worker still owns the shielded forward.
            raise
        except Exception:
            return _wire(failure('internal_error'))

    async def _health(self,frame,identity,*,lease_current,started):
        def failure(reason,*,invalid=False):
            return _wire(SqlHealthResult(**identity.model_dump(),
                status='invalid_query' if invalid else 'unavailable',reason=reason))
        try:
            raw=json.dumps(frame,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()
            if len(raw)>QUERY_MAX_BYTES:return failure('query_too_large',invalid=True)
            request=decode_envelope(raw)
            if (type(request) is not SqlHealthRequest or request.compute_claim is None
                    or len(request.compute_claim.encode())>256):
                return failure('invalid_input',invalid=True)
        except (TypeError,ValueError,RecursionError,OverflowError):
            return failure('invalid_input',invalid=True)
        if not callable(lease_current):return failure('not_ready')
        remaining=request.deadline_ms/1000-(asyncio.get_running_loop().time()-started)
        if remaining<=0:return failure('deadline_exceeded')
        from .kaggle_database_probe import probe_sql_health
        observed=await probe_sql_health(acquire=self.acquire,
            database_generation=request.database_generation,min_data_seq=request.min_data_seq,
            lease_current=lease_current,timeout_seconds=remaining)
        health=SqlHealthObservation.model_validate(observed)
        return _wire(SqlHealthResult(**identity.model_dump(),
            status='ok' if health.ready else 'unavailable',
            reason=None if health.ready else 'not_ready',health=health,
            observed_data_seq=health.applied_seq,observed_projection_seq=health.projection_seq,
            observed_database_generation=request.database_generation if health.schema_version is not None else None))
