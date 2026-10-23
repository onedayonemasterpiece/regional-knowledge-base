"""Fixed read contract for source facts, independent of narrative existence."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from .kaggle_protocol import (ReadIdentity, TrustedActorContext, Nonnegative,
    Identifier, EvidenceItem)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def observation_position(value):
    """Exact native observation locator, never a query expression."""
    if type(value) is not str or len(value) != 39:
        raise ValueError('canonical observation locator required')
    assertion, separator, suffix = value.partition(':')
    if (str(UUID(assertion)) != assertion or separator != ':' or
            len(suffix) != 2 or suffix[0] != 'o' or suffix[1] not in '01234567'):
        raise ValueError('canonical observation locator required')
    return assertion, int(suffix[1])


class FactReadRequest(ReadIdentity):
    type: Literal['FACT_READ'] = 'FACT_READ'
    protocol_version: Literal[2] = 2
    operation: Literal['source_fact_read'] = 'source_fact_read'
    operation_version: Literal[1] = 1
    action: Literal['assertions', 'calendar', 'observations', 'observation_compare']
    source_id: str | None = None
    story_id: str | None = None
    left_id: str | None = None
    right_id: str | None = None
    month: Annotated[int, Field(ge=1, le=12)] | None = None
    day: Annotated[int, Field(ge=1, le=31)] | None = None
    calendar: Literal['gregorian', 'julian', 'unspecified'] | None = None
    metric_key: Annotated[str, Field(pattern=r'^[a-z][a-z0-9_]{1,63}$')] | None = None
    unit_code: Annotated[str, Field(pattern=r'^[a-z][a-z0-9_]{1,63}$')] | None = None
    cursor: Annotated[str, Field(max_length=1600)] | None = None
    limit: Annotated[int, Field(ge=1, le=10)] = 3
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int, Field(gt=0)]
    compute_claim: Identifier | None = None

    @model_validator(mode='after')
    def exact_filter(self):
        if self.source_id is not None and str(UUID(self.source_id)) != self.source_id:
            raise ValueError('canonical source UUID required')
        if self.action == 'observation_compare':
            if (self.story_id is None or str(UUID(self.story_id)) != self.story_id or
                    self.left_id == self.right_id or self.limit != 2 or self.cursor is not None or
                    any(v is not None for v in (self.source_id, self.month, self.day,
                        self.calendar, self.metric_key, self.unit_code))):
                raise ValueError('exact Story and two distinct observations required')
            observation_position(self.left_id); observation_position(self.right_id)
            return self
        if any(v is not None for v in (self.story_id, self.left_id, self.right_id)):
            raise ValueError('comparison selectors require comparison action')
        if self.action == 'calendar':
            if self.month is None or self.day is None or self.calendar is None:
                raise ValueError('explicit calendar/day required')
            if self.day > (29 if self.month == 2 else 30 if self.month in (4, 6, 9, 11) else 31):
                raise ValueError('invalid calendar month/day')
            if self.metric_key is not None or self.unit_code is not None:
                raise ValueError('calendar cannot carry metric filters')
        else:
            if any(v is not None for v in (self.month, self.day, self.calendar)):
                raise ValueError('calendar filters require calendar action')
            if self.action == 'observations' and self.metric_key is None:
                raise ValueError('exact metric required')
            if self.action == 'assertions' and (self.source_id is None or
                    self.metric_key is not None or self.unit_code is not None):
                raise ValueError('assertion page requires exact source only')
        return self


class FactReadResult(ReadIdentity):
    type: Literal['FACT_READ_RESULT'] = 'FACT_READ_RESULT'
    protocol_version: Literal[2] = 2
    status: Literal['ok', 'unavailable', 'invalid_query', 'denied', 'cancelled']
    evidence: tuple[EvidenceItem, ...] = Field(default=(), max_length=10)
    records: tuple[dict, ...] = Field(default=(), max_length=10)
    next_cursor: Annotated[str, Field(max_length=1600)] | None = None
    observed_data_seq: Nonnegative | None
    observed_projection_seq: Nonnegative | None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal['projection_lag', 'deadline_exceeded', 'not_ready', 'disconnected',
        'caller_cancelled', 'turn_replaced', 'query_too_large', 'invalid_input', 'access_denied',
        'prior_write_pending', 'evidence_withdrawn', 'internal_error'] | None = None
    prior_write_pending: bool = False

    @model_validator(mode='after')
    def complete_dependencies(self):
        if self.status == 'ok' and (self.reason is not None or self.prior_write_pending or
                any(v is None for v in (self.observed_data_seq,self.observed_projection_seq,
                    self.observed_visibility_epoch,self.observed_database_generation))):
            raise ValueError('fact read requires exact native frontier')
        if self.status != 'ok' and (self.records or self.evidence or self.next_cursor):
            raise ValueError('failed fact read cannot carry records')
        if len(self.records) != len(self.evidence):
            raise ValueError('every fact record requires exact evidence dependencies')
        for record, evidence in zip(self.records, self.evidence):
            if (evidence.kind != 'fact' or record.get('id') != evidence.id or
                    evidence.dependencies.source_ids != (record.get('source_id'),) or
                    evidence.text != record.get('proposition') or
                    len(evidence.dependencies.source_revisions)!=1 or
                    evidence.dependencies.source_revisions[0].content_revision != record.get('source_revision')):
                raise ValueError('fact record/evidence pairing required')
        if any('story_id' in record or 'story_revision' in record for record in self.records):
            if (len(self.records)!=2 or self.next_cursor is not None or
                    len({r.get('story_id') for r in self.records})!=1 or
                    len({r.get('story_revision') for r in self.records})!=1 or
                    len({r.get('source_id') for r in self.records})!=1 or
                    len({r.get('id') for r in self.records})!=2):
                raise ValueError('complete same Story observation pair required')
            for record in self.records:
                if (record.get('kind')!='observations' or
                        str(UUID(record['story_id']))!=record['story_id'] or
                        type(record['story_revision']) is not int or not 1<=record['story_revision']<2**63):
                    raise ValueError('exact current Story observation required')
                observation_position(record['id'])
        if len(canonical(self.model_dump(mode='json'))) > 128 * 1024:
            raise ValueError('fact read result bound')
        return self


def cursor_scope(request, observed):
    return hashlib.sha256(canonical(dict(action=request.action, source_id=request.source_id,
        month=request.month, day=request.day, calendar=request.calendar,
        metric_key=request.metric_key, unit_code=request.unit_code,
        actor=request.actor.model_dump(mode='json'), database=observed['database'],
        data=observed['data'], projection=observed['projection'],
        visibility=observed['visibility']))).hexdigest()


def encode_cursor(scope, assertion_id, ordinal, year=0):
    return base64.urlsafe_b64encode(canonical(dict(v=1, scope=scope,
        assertion_id=assertion_id, ordinal=ordinal, year=year))).decode().rstrip('=')


def decode_cursor(cursor, scope):
    try:
        if len(cursor) > 1600:
            raise ValueError
        value = json.loads(base64.b64decode(cursor + '=' * (-len(cursor) % 4),
                                          altchars=b'-_', validate=True))
        if (set(value) != {'v', 'scope', 'assertion_id', 'ordinal', 'year'} or
                type(value['v']) is not int or value['v'] != 1 or value['scope'] != scope or
                str(UUID(value['assertion_id'])) != value['assertion_id'] or
                type(value['ordinal']) is not int or not -1 <= value['ordinal'] <= 7 or
                type(value['year']) is not int or not 0 <= value['year'] <= 9999 or
                encode_cursor(**{k: value[k] for k in ('scope', 'assertion_id', 'ordinal', 'year')}) != cursor):
            raise ValueError
        return value['assertion_id'], value['ordinal'], value['year']
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise ValueError('invalid or stale fact cursor') from None
