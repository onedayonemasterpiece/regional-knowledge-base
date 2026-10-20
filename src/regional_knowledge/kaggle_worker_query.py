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
)
from .kaggle_query import PinnedTokenizer, execute_query, execute_passage_read
from .query_telemetry import NativeReadTelemetry
from .kaggle_fact_contracts import FactReadRequest, FactReadResult
from .kaggle_source_read import SourceReadRequest, SourceReadResult


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


def _failure(identity, reason, *, invalid=False, observed=None, exact=False,story=False,fact=False,source=False):
    result_type = SourceReadResult if source else FactReadResult if fact else StoryReadResult if story else PassageReadResult if exact else QueryResult
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

    async def __call__(self, frame_dict, *, encode):
        with NativeReadTelemetry(self.telemetry, frame_dict) as diagnostics:
            try:
                result = await self._call(frame_dict, encode=encode)
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
            return result

    async def _call(self, frame_dict, *, encode):
        loop = asyncio.get_running_loop()
        started = loop.time()
        identity = _identity(frame_dict)
        source = frame_dict.get('type')=='SOURCE_READ'
        fact = frame_dict.get('type')=='FACT_READ'
        story = frame_dict.get('type')=='STORY_READ'
        exact = source or fact or story or frame_dict.get('type') == 'PASSAGE_READ'
        def failure(reason, **kwargs):
            return _failure(identity, reason, exact=exact, story=story, fact=fact, source=source, **kwargs)
        try:
            raw = json.dumps(frame_dict, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
            if len(raw) > QUERY_MAX_BYTES:
                return _wire(failure('query_too_large', invalid=True))
            query = decode_envelope(raw)
            if (type(query) not in (Query, PassageReadRequest, StoryReadRequest, FactReadRequest, SourceReadRequest) or query.compute_claim is None
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
            if source:
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
            result_type = SourceReadResult if source else FactReadResult if fact else StoryReadResult if story else PassageReadResult if exact else QueryResult
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
