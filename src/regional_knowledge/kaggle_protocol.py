"""Bounded JSON contract for the future authenticated Kaggle read channel.

Models validate shape, never identity or permission. No transport or scheduler lives here.
"""
from __future__ import annotations

import json
import hashlib
import re
from uuid import UUID
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

PROTOCOL_VERSION = 2
QUERY_MAX_BYTES = 16 * 1024
RESULT_MAX_BYTES = 256 * 1024
CONTROL_MAX_BYTES = 16 * 1024
Identifier = Annotated[str, Field(min_length=1, max_length=256)]
Nonnegative = Annotated[int, Field(ge=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    @field_validator("protocol_version", "operation_version", mode="before", check_fields=False)
    @classmethod
    def strict_versions(cls, value: object) -> object:
        # Pydantic Literal[2] otherwise accepts JSON 2.0, even with strict=True.
        if type(value) is not int:
            raise ValueError("version must be an integer")
        return value


class TrustedActorContext(Contract):
    """Internal projection of existing verified authority; parsing does not verify it.

    Only the controller constructs this from its verified request context. A public
    tool handler must reject caller-supplied instances/fields, resolve authority
    itself and recheck current grants before delayed output or continuation.
    """

    person_id: Identifier
    audience: Identifier
    auth_epoch: Nonnegative
    visibility_epoch: Nonnegative
    context_ref: Identifier | None = None


class QueryFilters(Contract):
    # None means bounded SQL scope, never a request to enumerate the corpus.
    source_ids: tuple[Identifier, ...] | None = Field(default=None, min_length=1, max_length=32)
    kinds: tuple[Literal["source", "fact", "story", "entity", "geo", "case"], ...] | None = Field(
        default=None, min_length=1, max_length=6
    )

    @model_validator(mode="after")
    def unique_values(self) -> Self:
        for values in (self.source_ids, self.kinds):
            if values is not None and len(set(values)) != len(values):
                raise ValueError("duplicate filter values")
        return self


class ReadIdentity(Contract):
    request_id: Identifier
    connection_generation: Annotated[int, Field(ge=1)]
    turn_generation: Nonnegative


class Query(ReadIdentity):
    type: Literal["QUERY"] = "QUERY"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    operation: Literal["knowledge_search"] = "knowledge_search"
    operation_version: Literal[2] = 2
    query: Annotated[str, Field(min_length=1, max_length=1000)]
    filters: QueryFilters = Field(default_factory=QueryFilters)
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    # Remaining total tool-call budget, never a new per-hop timeout.
    deadline_ms: Annotated[int, Field(gt=0)]
    evidence_limit: Annotated[int, Field(ge=1, le=5)] = 3
    # Assigned only by the existing authenticated controller at dispatch.
    compute_claim: Identifier | None = None

    @model_validator(mode="after")
    def query_bounds(self) -> Self:
        if len(self.query.encode("utf-8")) > 4096:
            raise ValueError("query_too_large")
        if not self.query.strip():
            raise ValueError("empty query")
        return self


class CancelQuery(ReadIdentity):
    type: Literal["CANCEL_QUERY"] = "CANCEL_QUERY"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    reason: Literal["caller_cancelled", "deadline_exceeded", "turn_replaced", "disconnected"]


class SqlHealthRequest(ReadIdentity):
    """Fixed metadata read admitted with the existing verified actor/channel."""
    type: Literal['SQL_HEALTH'] = 'SQL_HEALTH'
    protocol_version: Literal[2] = PROTOCOL_VERSION
    operation: Literal['sql_health'] = 'sql_health'
    operation_version: Literal[1] = 1
    database_generation: Identifier
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int, Field(gt=0, le=2000)]
    compute_claim: Identifier | None = None


class SqlHealthObservation(Contract):
    sql_response: bool = False
    schema_version: Annotated[int,Field(strict=True,ge=1)] | None = None
    applied_seq: Nonnegative | None = None
    projection_seq: Nonnegative | None = None
    pending: bool | None = None
    reason: Literal['pg_binding_changed','pg_lease_invalid','pg_metadata_invalid',
        'pg_generation_mismatch','pg_prior_write_pending','pg_projection_lag',
        'pg_sequence_behind','pg_timeout','pg_query_failed'] | None = None
    elapsed_ms: Annotated[float,Field(ge=0,allow_inf_nan=False)] = 0

    @property
    def ready(self):
        return (self.sql_response and self.reason is None and self.schema_version is not None
            and self.applied_seq is not None and self.projection_seq==self.applied_seq
            and self.pending is False)


class SqlHealthResult(ReadIdentity):
    type: Literal['SQL_HEALTH_RESULT'] = 'SQL_HEALTH_RESULT'
    protocol_version: Literal[2] = PROTOCOL_VERSION
    status: Literal['ok','unavailable','invalid_query','denied','cancelled']
    evidence: tuple = Field(default=(),max_length=0)
    observed_data_seq: Nonnegative | None = None
    observed_projection_seq: Nonnegative | None = None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal['not_ready','deadline_exceeded','disconnected','caller_cancelled',
        'turn_replaced','query_too_large','invalid_input','access_denied','internal_error'] | None = None
    health: SqlHealthObservation | None = None

    @model_validator(mode='after')
    def health_semantics(self):
        if self.status=='ok':
            if (self.reason is not None or self.health is None or not self.health.ready
                    or self.observed_database_generation is None
                    or self.observed_data_seq!=self.health.applied_seq
                    or self.observed_projection_seq!=self.health.projection_seq):
                raise ValueError('complete current SQL health required')
        elif self.reason is None:
            raise ValueError('explicit health failure reason required')
        elif self.health is not None and self.health.ready:
            raise ValueError('unavailable response cannot claim ready SQL health')
        return self


class SourceDependency(Contract):
    source_id: Identifier
    content_revision: Annotated[int, Field(ge=1)]
    eligibility_revision: Annotated[int, Field(ge=1)]


class PassageContinuation(Contract):
    database_generation: Identifier
    source: SourceDependency
    passage_id: Identifier
    visibility_epoch: Nonnegative
    text_offset: Nonnegative
    bibliography_offset: Nonnegative


class PassageReadRequest(ReadIdentity):
    """Exact bounded source read; locator is data, never an authorization grant."""
    type: Literal["PASSAGE_READ"] = "PASSAGE_READ"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    operation: Literal["source_passage_read"] = "source_passage_read"
    operation_version: Literal[1] = 1
    locator: PassageContinuation
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int, Field(gt=0)]
    compute_claim: Identifier | None = None


class StoryReadRequest(ReadIdentity):
    type: Literal['STORY_READ'] = 'STORY_READ'
    protocol_version: Literal[2] = PROTOCOL_VERSION
    operation: Literal['story_read'] = 'story_read'
    operation_version: Literal[1] = 1
    story_id: Identifier
    revision: Annotated[int,Field(ge=1)] | None = None
    view: Literal['get','history','variants'] = 'get'
    after_revision: Nonnegative = 0
    limit: Annotated[int,Field(ge=1,le=10)] = 10
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int,Field(gt=0)]
    compute_claim: Identifier | None = None

    @model_validator(mode='after')
    def canonical_story(self):
        if str(UUID(self.story_id))!=self.story_id:raise ValueError('canonical story ID required')
        return self


class ItemDependencies(Contract):
    """Exhaustive per-item dependencies, including cross-source compositions.

    Empty variant/grant tuples explicitly mean none; missing lists are invalid.
    Completeness and current validity must be established by the retrieval/output gate.
    """

    source_ids: tuple[Identifier, ...] = Field(min_length=1)
    variant_refs: tuple[Identifier, ...]
    grant_refs: tuple[Identifier, ...]
    source_revisions: tuple[SourceDependency, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def unique_values(self) -> Self:
        for values in (self.source_ids, self.variant_refs, self.grant_refs):
            if len(set(values)) != len(values):
                raise ValueError("duplicate item dependencies")
        if (
            len({ref.source_id for ref in self.source_revisions}) != len(self.source_revisions)
            or set(self.source_ids) != {ref.source_id for ref in self.source_revisions}
        ):
            raise ValueError("exact source dependencies must cover every source")
        return self


class EvidenceMetadata(Contract):
    title: Annotated[str, Field(max_length=2048)] | None = None
    citation: Annotated[str, Field(max_length=8192)] | None = None
    locator: Annotated[str, Field(max_length=2048)] | None = None
    source_provenance: dict | None = None
    source_kind: Literal["book", "journal_issue", "article"] | None = None
    source_language: Annotated[str, Field(max_length=80)] | None = None
    passage_language: Literal["ru", "de", "simple"] | None = None

    @model_validator(mode='after')
    def bounded_provenance(self):
        if self.source_provenance is not None and len(json.dumps(self.source_provenance,
                ensure_ascii=False,allow_nan=False).encode())>128*1024:
            raise ValueError('bounded safe source provenance required')
        return self


class EvidenceItem(Contract):
    id: Identifier
    kind: Literal["source", "fact", "story", "entity", "geo", "case"]
    text: Annotated[str, Field(min_length=1, max_length=8000)]
    metadata: EvidenceMetadata = Field(default_factory=EvidenceMetadata)
    dependencies: ItemDependencies
    text_form: Literal["complete", "excerpt"] = "complete"
    # A locator only: the next read must reauthorize against current scope.
    continuation: Identifier | None = None
    exact_variant_ref: Identifier | None = None
    source_continuation: PassageContinuation | None = None
    continuation_complete: bool = False

    @model_validator(mode="after")
    def item_bounds(self) -> Self:
        if len(self.metadata.model_dump_json().encode("utf-8")) > 12 * 1024:
            raise ValueError("evidence metadata too large")
        if self.text_form == "excerpt" and self.continuation is None:
            raise ValueError("excerpt requires explicit continuation")
        if self.source_continuation is not None and (
            self.source_continuation.source not in self.dependencies.source_revisions
            or self.source_continuation.passage_id != self.id
        ):
            raise ValueError("passage continuation must match exact source dependencies")
        if self.continuation_complete and (self.text_form != "excerpt" or self.source_continuation is None):
            raise ValueError("terminal excerpt requires an exact source locator")
        if self.kind == "story" and self.exact_variant_ref is None:
            raise ValueError("story requires exact existing variant reference")
        if self.exact_variant_ref is not None and self.exact_variant_ref not in self.dependencies.variant_refs:
            raise ValueError("exact variant missing from dependencies")
        return self


class QueryResult(ReadIdentity):
    type: Literal["QUERY_RESULT"] = "QUERY_RESULT"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    status: Literal["ok", "degraded", "unavailable", "cancelled", "invalid_query", "denied"]
    mode: Literal["hybrid"] | None
    evidence: tuple[EvidenceItem, ...] = Field(default=(), max_length=5)
    # None means not observed, not zero / fresh / ready.
    observed_data_seq: Nonnegative | None
    observed_projection_seq: Nonnegative | None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal[
        "dense_unavailable", "projection_lag", "deadline_exceeded", "not_ready",
        "disconnected", "caller_cancelled", "turn_replaced", "query_too_large",
        "invalid_filters", "invalid_input", "token_limit_exceeded", "access_denied",
        "prior_write_pending", "context_gap", "evidence_withdrawn", "internal_error",
        "bounded_retrieval",
    ] | None = None
    prior_write_pending: bool = False
    context_gap: bool = False
    continuation: Identifier | None = None

    @model_validator(mode="after")
    def result_semantics(self) -> Self:
        searched = self.status in ("ok", "degraded")
        if searched != (self.mode is not None):
            raise ValueError("retrieval mode required exactly when search completed")
        if not searched and self.evidence:
            raise ValueError("non-search result cannot carry evidence")
        if searched and self.reason == "dense_unavailable":
            raise ValueError("dense failure cannot claim completed hybrid search")
        if self.status != "ok" and self.reason is None:
            raise ValueError("non-ok result requires explicit reason")
        if self.status == "ok" and (
            self.reason is not None or self.context_gap or self.prior_write_pending
            or self.mode != "hybrid"
        ):
            raise ValueError("ok must describe complete hybrid search")
        if searched and (self.observed_data_seq is None or self.observed_projection_seq is None):
            raise ValueError("completed search requires observed cursors")
        if sum(len(item.text) for item in self.evidence) > 24000:
            raise ValueError("total evidence text too large")
        return self


class PassageReadResult(ReadIdentity):
    """Exact source result has no search mode or inferred dense success."""
    type: Literal["PASSAGE_READ_RESULT"] = "PASSAGE_READ_RESULT"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    status: Literal["ok", "unavailable", "invalid_query", "denied", "cancelled"]
    evidence: tuple[EvidenceItem, ...] = Field(default=(), max_length=1)
    observed_data_seq: Nonnegative | None
    observed_projection_seq: Nonnegative | None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal["projection_lag", "deadline_exceeded", "not_ready", "disconnected",
        "caller_cancelled", "turn_replaced", "query_too_large", "invalid_filters", "invalid_input",
        "access_denied", "prior_write_pending", "context_gap", "evidence_withdrawn", "internal_error"] | None = None
    prior_write_pending: bool = False

    @model_validator(mode="after")
    def result_semantics(self) -> Self:
        if self.status == "ok":
            if (len(self.evidence) != 1 or self.reason is not None or self.prior_write_pending
                    or any(value is None for value in (self.observed_data_seq,
                        self.observed_projection_seq, self.observed_visibility_epoch,
                        self.observed_database_generation))):
                raise ValueError("exact read requires one item and observed source frontier")
        elif self.evidence or self.reason is None:
            raise ValueError("failed exact read requires reason and no evidence")
        return self


class StoryReadResult(PassageReadResult):
    """One exact shared basic narrative; publication/private variants excluded."""
    type: Literal['STORY_READ_RESULT'] = 'STORY_READ_RESULT'
    record: dict | None = None

    @model_validator(mode='after')
    def exact_narrative(self):
        if self.status!='ok':
            if self.record is not None:raise ValueError('failed story read has no record')
            return self
        record=self.record
        keys={'story_id','revision','title','summary','material_type','body','body_sha256','narrative_availability',
            'default_variant_ref','variant','bibliography','source','finding_ids','view','entries','next_revision'}
        if type(record) is not dict or set(record)!=keys or len(json.dumps(record,ensure_ascii=False,allow_nan=False).encode())>131072:
            raise ValueError('bounded exact story record required')
        item=self.evidence[0];ref=record['default_variant_ref'];variant=record['variant']
        if (type(ref) is not dict or set(ref)!={'variant_id','revision'} or ref['revision']!=1
                or type(ref['revision']) is not int or str(UUID(ref['variant_id']))!=ref['variant_id']
                or type(variant) is not dict
                or set(variant)!={'variant_id','revision','audience','format','language','body','blocks','input_tokens'}
                or variant['variant_id']!=ref['variant_id'] or variant['revision']!=ref['revision']
                or variant['body']!=record['body'] or variant['format']!='story'
                or type(record['body']) is not str or not 1<=len(record['body'])<=4000
                or hashlib.sha256(record['body'].encode()).hexdigest()!=record['body_sha256']
                or record['narrative_availability']!='available' or record['revision']!=1
                or type(record['revision']) is not int or str(UUID(record['story_id']))!=record['story_id']
                or item.id!=record['story_id'] or item.kind!='story' or item.text!=record['body']
                or item.text_form!='complete' or item.continuation is not None
                or item.exact_variant_ref!=f"variant:{ref['variant_id']}:r{ref['revision']}"
                or item.dependencies.variant_refs!=(item.exact_variant_ref,)
                or item.dependencies.grant_refs or len(item.dependencies.source_revisions)!=1
                or item.dependencies.source_revisions[0].model_dump()!=record['source']
                or record['view'] not in ('get','history','variants')
                or type(record['entries']) is not list or len(record['entries'])>10
                or type(record['title']) is not str or not 1<=len(record['title'])<=250
                or type(record['summary']) is not str or len(record['summary'])>1000
                or type(record['bibliography']) is not str or len(record['bibliography'])>16000
                or type(record['finding_ids']) is not list or not 1<=len(record['finding_ids'])<=12
                or len(set(record['finding_ids']))!=len(record['finding_ids'])
                or any(type(x) is not str or str(UUID(x))!=x for x in record['finding_ids'])
                or type(variant['input_tokens']) is not int or not 1<=variant['input_tokens']<=512
                or type(variant['audience']) is not str or not 1<=len(variant['audience'])<=500
                or type(variant['language']) is not str or not 2<=len(variant['language'])<=24):
            raise ValueError('exact source-bound default narrative required')
        if record['view']=='get' and record['entries']:raise ValueError('get has no version page')
        for entry in record['entries']:
            if type(entry) is not dict:raise ValueError('typed story version entry required')
            if record['view']=='variants':
                if entry!={**variant,'body_sha256':record['body_sha256']}:
                    raise ValueError('unsupported private/alternate variant')
            elif (set(entry)!={'story_id','revision','logical_time','action','payload_sha256','actor_id','request_id','operation_key','sequence','command_sha256'}
                    or entry['story_id']!=record['story_id'] or entry['revision']!=1
                    or entry['action']!='source_import' or type(entry['sequence']) is not int or entry['sequence']<1
                    or any(type(entry[k]) is not str or not 1<=len(entry[k])<=256 for k in ('actor_id','request_id','operation_key','logical_time'))
                    or any(type(entry[k]) is not str or re.fullmatch('[0-9a-f]{64}',entry[k]) is None for k in ('payload_sha256','command_sha256'))):
                raise ValueError('exact source-import history required')
        return self


class Hello(Contract):
    type: Literal["HELLO"] = "HELLO"
    protocol_version: Literal[2] = PROTOCOL_VERSION
    run_id: Identifier
    provider_version: Identifier
    launch_nonce: Identifier
    runtime_incarnation: Identifier
    connection_generation: Annotated[int, Field(ge=1)]
    role: Literal["search_worker"] = "search_worker"
    codec: Literal["json"] = "json"
    schema_version: Identifier | None
    model_version: Identifier | None
    observed_snapshot: Identifier | None
    observed_data_seq: Nonnegative | None
    text_search_ready: bool | None
    archive_read_ready: bool | None
    proof_ready: bool | None


Envelope = Annotated[Query | QueryResult | PassageReadRequest | PassageReadResult | StoryReadRequest | StoryReadResult | SqlHealthRequest | SqlHealthResult | CancelQuery | Hello, Field(discriminator="type")]
_ADAPTER = TypeAdapter(Envelope)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON field")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ValueError("non-finite JSON number")


def decode_envelope(data: bytes) -> Query | QueryResult | PassageReadRequest | PassageReadResult | CancelQuery | Hello:
    """Decode one bounded UTF-8 frame; never truncate, authorize, or admit work."""
    if not isinstance(data, bytes):
        raise TypeError("wire frame must be bytes")
    if len(data) > RESULT_MAX_BYTES:
        raise ValueError("response too large")
    # Reject ambiguous duplicate keys before Pydantic's JSON parsing.
    raw = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object,
                     parse_constant=_reject_constant)
    if not isinstance(raw, dict):
        raise ValueError("envelope must be an object")
    limit = RESULT_MAX_BYTES if raw.get("type") in ("QUERY_RESULT", "PASSAGE_READ_RESULT", "STORY_READ_RESULT", "FACT_READ_RESULT", "SOURCE_READ_RESULT", "CASE_READ_RESULT", "GEO_READ_RESULT", "EDITORIAL_READ_RESULT", "SOURCE_GRAPH_READ_RESULT", "ACCEPTANCE_READ_RESULT") else QUERY_MAX_BYTES
    if len(data) > limit:
        raise ValueError("query_too_large" if raw.get("type") == "QUERY" else "control frame too large")
    if raw.get('type') in ('ACCEPTANCE_READ','ACCEPTANCE_READ_RESULT'):
        from .kaggle_acceptance_contracts import AcceptanceReadRequest,AcceptanceReadResult
        return (AcceptanceReadRequest if raw['type']=='ACCEPTANCE_READ' else AcceptanceReadResult).model_validate_json(data)
    if raw.get('type') in ('SOURCE_GRAPH_READ','SOURCE_GRAPH_READ_RESULT'):
        from .kaggle_source_graph_read import SourceGraphReadRequest,SourceGraphReadResult
        return (SourceGraphReadRequest if raw['type']=='SOURCE_GRAPH_READ' else SourceGraphReadResult).model_validate_json(data)
    if raw.get('type') in ('EDITORIAL_READ','EDITORIAL_READ_RESULT'):
        from .kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
        return (EditorialReadRequest if raw['type']=='EDITORIAL_READ' else EditorialReadResult).model_validate_json(data)
    if raw.get('type') in ('CASE_READ','CASE_READ_RESULT'):
        from .kaggle_case_contracts import CaseReadRequest,CaseReadResult
        return (CaseReadRequest if raw['type']=='CASE_READ' else CaseReadResult).model_validate_json(data)
    if raw.get('type') in ('GEO_READ','GEO_READ_RESULT'):
        from .kaggle_geo_contracts import GeoReadRequest,GeoReadResult
        return (GeoReadRequest if raw['type']=='GEO_READ' else GeoReadResult).model_validate_json(data)
    if raw.get('type') in ('SOURCE_READ','SOURCE_READ_RESULT'):
        from .kaggle_source_read import SourceReadRequest,SourceReadResult
        return (SourceReadRequest if raw['type']=='SOURCE_READ' else SourceReadResult).model_validate_json(data)
    if raw.get('type') in ('FACT_READ', 'FACT_READ_RESULT'):
        # The fixed optional domain contract imports the base wire models; load
        # it after this module is initialized rather than adding an import cycle.
        from .kaggle_fact_contracts import FactReadRequest, FactReadResult
        return (FactReadRequest if raw['type']=='FACT_READ' else FactReadResult).model_validate_json(data)
    # JSON-mode strict validation accepts JSON arrays for immutable tuples.
    return _ADAPTER.validate_json(data)


def encode_envelope(envelope: Query | QueryResult | PassageReadRequest | PassageReadResult | CancelQuery | Hello) -> bytes:
    """Validate again at the codec boundary, including byte and aggregate caps."""
    data = envelope.model_dump_json().encode("utf-8")
    decode_envelope(data)
    return data
