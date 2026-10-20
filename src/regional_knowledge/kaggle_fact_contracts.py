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


class FactReadRequest(ReadIdentity):
    type: Literal['FACT_READ'] = 'FACT_READ'
    protocol_version: Literal[2] = 2
    operation: Literal['source_fact_read'] = 'source_fact_read'
    operation_version: Literal[1] = 1
    action: Literal['assertions', 'calendar', 'observations']
    source_id: str | None = None
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
        'caller_cancelled', 'turn_replaced', 'invalid_input', 'access_denied',
        'prior_write_pending', 'evidence_withdrawn', 'internal_error'] | None = None
    prior_write_pending: bool = False

    @model_validator(mode='after')
    def complete_dependencies(self):
        if self.status != 'ok' and (self.records or self.evidence or self.next_cursor):
            raise ValueError('failed fact read cannot carry records')
        if len(self.records) != len(self.evidence):
            raise ValueError('every fact record requires exact evidence dependencies')
        for record, evidence in zip(self.records, self.evidence):
            if (evidence.kind != 'fact' or record.get('id') != evidence.id or
                    record.get('source_id') not in evidence.dependencies.source_ids):
                raise ValueError('fact record/evidence pairing required')
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
