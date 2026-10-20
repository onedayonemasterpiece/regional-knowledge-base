"""Bounded JSON contract for the future authenticated Kaggle read channel.

Models validate shape, never identity or permission. No transport or scheduler lives here.
"""
from __future__ import annotations

import json
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


Envelope = Annotated[Query | QueryResult | CancelQuery | Hello, Field(discriminator="type")]
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


def decode_envelope(data: bytes) -> Query | QueryResult | CancelQuery | Hello:
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
    limit = RESULT_MAX_BYTES if raw.get("type") == "QUERY_RESULT" else QUERY_MAX_BYTES
    if len(data) > limit:
        raise ValueError("query_too_large" if raw.get("type") == "QUERY" else "control frame too large")
    # JSON-mode strict validation accepts JSON arrays for immutable tuples.
    return _ADAPTER.validate_json(data)


def encode_envelope(envelope: Query | QueryResult | CancelQuery | Hello) -> bytes:
    """Validate again at the codec boundary, including byte and aggregate caps."""
    data = envelope.model_dump_json().encode("utf-8")
    decode_envelope(data)
    return data
