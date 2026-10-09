"""Typed, bounded Story Registry MCP inputs. No arbitrary JSON patch or model-asserted actor."""
from __future__ import annotations

from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field, model_validator

Key = Annotated[str, Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]+$")]
SmallText = Annotated[str, Field(min_length=1, max_length=4000)]
ShortText = Annotated[str, Field(min_length=1, max_length=500)]
Identifier = Annotated[str, Field(min_length=1, max_length=128)]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SeedInput(Strict):
    text: SmallText
    origin_status: Literal["known", "partial", "unknown"] = "unknown"
    origin_note: str | None = Field(default=None, max_length=500)
    speaker: str | None = Field(default=None, max_length=250)


class SourceRef(Strict):
    kind: Literal["document", "external"]
    source_id: Identifier
    source_revision: int = Field(ge=1)
    locator: dict[str, str | int | None] = Field(default_factory=dict, max_length=12)


class StoryMetadata(Strict):
    title: str | None = Field(default=None, max_length=250)
    summary: str | None = Field(default=None, max_length=1000)
    material_type: Literal[
        "event_narrative", "person_episode", "place_biography", "explanation",
        "comparison", "everyday_life", "mystery", "conflicting_accounts", "other",
    ] = "other"
    tags: list[Annotated[str, Field(max_length=80)]] = Field(default_factory=list, max_length=16)
    project_refs: list[Annotated[str, Field(max_length=250)]] = Field(default_factory=list, max_length=12)
    time_scope: str | None = Field(default=None, max_length=150)


class SetMetadata(Strict):
    op: Literal["set_metadata"]
    title: str | None = Field(default=None, max_length=250)
    summary: str | None = Field(default=None, max_length=1000)
    material_type: str | None = Field(default=None, max_length=80)
    tags: list[str] | None = Field(default=None, max_length=16)
    time_scope: str | None = Field(default=None, max_length=150)


class AddGap(Strict):
    op: Literal["add_gap"]
    text: ShortText
    priority: Literal["low", "normal", "high", "critical"] = "normal"


class ResolveGap(Strict):
    op: Literal["resolve_gap"]
    gap_id: Identifier
    resolution: ShortText


class SetAngle(Strict):
    op: Literal["set_angle"]
    angle: ShortText
    audience_value: str | None = Field(default=None, max_length=400)


class LinkEntity(Strict):
    op: Literal["link_entity"]
    entity_ref: Identifier
    kind: Literal["person", "place", "event", "thread", "poi_ref", "other"]
    time_scope: str | None = Field(default=None, max_length=100)


class UpsertAssertion(Strict):
    op: Literal["upsert_assertion"]
    assertion_id: str | None = None
    expected_assertion_revision: int | None = Field(default=None, ge=1)
    proposition: ShortText
    kind: Literal["historical_claim", "attributed_account", "interpretation", "hypothesis", "creative_material"]
    account_kind: Literal["legend", "tradition", "rumor", "recollection", "testimony", "other"] | None = None
    attributed_to: str | None = Field(default=None, max_length=250)
    reported_by: str | None = Field(default=None, max_length=250)
    time_scope: str | None = Field(default=None, max_length=150)
    @model_validator(mode="after")
    def attribution_kind(self):
        if self.kind != "attributed_account" and self.account_kind is not None:
            raise ValueError("account_kind requires attributed_account")
        if self.assertion_id and self.expected_assertion_revision is None:
            raise ValueError("editing an assertion requires expected_assertion_revision")
        return self


class EvidenceLocator(Strict):
    page_id: str | None = None
    region_id: str | None = None
    chunk_id: str | None = None
    physical_page_index: int | None = Field(default=None, ge=0)
    printed_page_number: str | None = Field(default=None, max_length=30)
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, ge=0)
    start_ms: int | None = Field(default=None, ge=0)
    end_ms: int | None = Field(default=None, ge=0)


class AttachEvidence(Strict):
    op: Literal["attach_evidence"]
    assertion_id: Identifier
    assertion_revision: int = Field(ge=1)
    source_kind: Literal["document", "external"]
    source_id: Identifier
    source_revision: int = Field(ge=1)
    relation: Literal["reports", "supports", "contradicts", "provides_context"]
    source_role_for_assertion: Literal["primary", "secondary", "tertiary", "unspecified"] = "unspecified"
    locator: EvidenceLocator = Field(default_factory=EvidenceLocator)
    original_excerpt: ShortText
    derived_from: str | None = Field(default=None, max_length=250)


class RecordAssessment(Strict):
    op: Literal["record_assessment"]
    assertion_id: Identifier
    assertion_revision: int = Field(ge=1)
    evidence_ids: list[Identifier] = Field(default_factory=list, max_length=20)
    support_status: Literal[
        "unassessed", "unsupported", "single_source", "corroborated", "contested", "contradicted",
    ] = "unassessed"
    independence: Literal["not_assessed", "dependent", "independent", "mixed", "unknown"] = "not_assessed"
    semantic_review: Literal["not_reviewed", "supported", "partially_supported", "unsupported", "contested"] = "not_reviewed"
    rationale: ShortText
    assessor_kind: Literal["human", "model", "application"] = "application"
    method_version: str | None = Field(default=None, max_length=80)
    supersedes_assessment_id: str | None = None


class VariantBlock(Strict):
    text: SmallText
    assertion_id: str | None = None
    assertion_revision: int | None = Field(default=None, ge=1)
    attribution: str | None = Field(default=None, max_length=500)


class UpsertVariant(Strict):
    op: Literal["upsert_variant"]
    variant_id: str | None = None
    expected_variant_revision: int | None = Field(default=None, ge=1)
    audience: ShortText
    format: Literal["story", "video", "short_video", "article", "post", "audio", "lecture", "other"]
    channel: str | None = Field(default=None, max_length=120)
    language: Annotated[str, Field(min_length=2, max_length=24)] = "ru"
    body: SmallText
    blocks: list[VariantBlock] = Field(default_factory=list, max_length=25)
    attributions: list[Annotated[str, Field(max_length=500)]] = Field(default_factory=list, max_length=15)
    media_refs: list[Annotated[str, Field(max_length=250)]] = Field(default_factory=list, max_length=12)
    @model_validator(mode="after")
    def revision_required(self):
        if self.variant_id and self.expected_variant_revision is None:
            raise ValueError("editing a variant requires expected_variant_revision")
        return self


class ScoreCriterion(Strict):
    value: int | None = Field(default=None, ge=0, le=4)
    rationale: str | None = Field(default=None, max_length=350)


class RecordInterest(Strict):
    op: Literal["record_interest_assessment"]
    unexpectedness: ScoreCriterion = Field(default_factory=ScoreCriterion)
    local_relevance: ScoreCriterion = Field(default_factory=ScoreCriterion)
    human_resonance: ScoreCriterion = Field(default_factory=ScoreCriterion)
    explanatory_value: ScoreCriterion = Field(default_factory=ScoreCriterion)
    visual_potential: ScoreCriterion = Field(default_factory=ScoreCriterion)
    channel_novelty: int | None = Field(default=None, ge=0, le=100)
    production_readiness: int | None = Field(default=None, ge=0, le=100)
    repetition_penalty: float = Field(default=0, ge=0, le=100)
    estimated_effort: str | None = Field(default=None, max_length=150)
    audience: str | None = Field(default=None, max_length=100)
    format: str | None = Field(default=None, max_length=80)
    rubric_version: str = Field(default="v1", max_length=40)
    assessor_kind: Literal["human", "model", "application"] = "human"


class SetContributors(Strict):
    op: Literal["set_contributors"]
    contributors: list[dict[Literal["name", "role"], str]] = Field(max_length=20)


StoryOperation = Annotated[Union[
    SetMetadata, AddGap, ResolveGap, SetAngle, LinkEntity, UpsertAssertion,
    AttachEvidence, RecordAssessment, UpsertVariant, RecordInterest, SetContributors,
], Field(discriminator="op")]


class ReviewDecision(Strict):
    semantic_checked: bool = False
    attribution_checked: bool = False
    rights_checked: bool = False
    reviewer_note: ShortText
    reviewer_kind: Literal["human", "model", "application"] = "application"
    model_version: str | None = Field(default=None, max_length=120)


class RegisteredSource(Strict):
    medium: Literal["text", "audio", "video", "image", "mixed", "unknown"] = "unknown"
    document_kind: Literal[
        "archival_document", "letter", "interview", "public_talk", "photo",
        "webpage", "audience_message", "user_note", "other",
    ] = "user_note"
    title: str | None = Field(default=None, max_length=250)
    origin_status: Literal["known", "partial", "unknown"] = "unknown"
    origin_note: str | None = Field(default=None, max_length=500)
    authors: list[str] = Field(default_factory=list, max_length=12)
    speaker: str | None = Field(default=None, max_length=250)
    language: str | None = Field(default=None, max_length=20)
    created_at: str | None = Field(default=None, max_length=32)
    recorded_at: str | None = Field(default=None, max_length=32)
    body: str = Field(default="", max_length=12000)
    # External bytes are NOT fetched by this operation. Only verified registered media refs may be used later.
    media_ref: str | None = Field(default=None, max_length=500)
    @model_validator(mode="after")
    def require_material(self):
        if not self.body.strip() and not self.media_ref:
            raise ValueError("an immutable text or registered media reference is required")
        return self


class ExtractGroundedEvidence(Strict):
    """Evidence in an already accepted source, not an inferred chunk reference."""
    page_id: Identifier
    region_id: Identifier
    original_excerpt: Annotated[str, Field(min_length=1, max_length=500)]
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def offsets_must_pair(self):
        if (self.start is None) != (self.end is None):
            raise ValueError("Both exact source offsets are required together")
        if self.start is not None and self.start >= self.end:
            raise ValueError("Source offsets must be ordered")
        return self


class ExtractGroundedCandidate(Strict):
    """One distinct source-attributed episode; no model call inside the server."""
    candidate_key: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")]
    title: Annotated[str, Field(min_length=3, max_length=250)]
    summary: Annotated[str, Field(min_length=5, max_length=1000)]
    material_type: Literal[
        "event_narrative", "person_episode", "place_biography", "explanation",
        "comparison", "everyday_life", "mystery", "conflicting_accounts", "other",
    ] = "other"
    proposition: Annotated[str, Field(min_length=5, max_length=500)]
    account_kind: Literal[
        "legend", "tradition", "rumor", "recollection", "testimony", "other",
    ] = "other"
    attributed_to: str | None = Field(default=None, max_length=250)
    reported_by: str | None = Field(default=None, max_length=250)
    evidence_refs: list[ExtractGroundedEvidence] = Field(min_length=1, max_length=4)


class ExtractStart(Strict):
    command: Literal["start"]
    document_id: Identifier
    source_revision: int = Field(ge=1)
    batch_size: int = Field(default=10, ge=1, le=20)


class ExtractClaim(Strict):
    command: Literal["claim"]
    job_id: Identifier
    expected_job_revision: int = Field(ge=1)


class ExtractStage(Strict):
    command: Literal["stage"]
    job_id: Identifier
    expected_job_revision: int = Field(ge=1)
    batch_id: Identifier
    lease_token: Identifier
    source_revision: int = Field(ge=1)
    candidates: list[SeedInput] = Field(default_factory=list, max_length=10)
    grounded_candidates: list[ExtractGroundedCandidate] = Field(default_factory=list, max_length=10)
    skipped: list[str] = Field(default_factory=list, max_length=20)


class ExtractCancel(Strict):
    command: Literal["cancel"]
    job_id: Identifier
    expected_job_revision: int = Field(ge=1)


ExtractRequest = Annotated[
    ExtractStart | ExtractClaim | ExtractStage | ExtractCancel,
    Field(discriminator="command"),
]


# New books are processed by the SAME importing model that reviews their pages.
# Each candidate represents one episode, not one chunk; region quotes must exist.
class StageStoryEvidenceInput(Strict):
    page_id: Annotated[str, Field(min_length=36, max_length=36)]
    region_key: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")]
    original_excerpt: Annotated[str, Field(min_length=1, max_length=500)]
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def span(self):
        if (self.start is None) != (self.end is None):
            raise ValueError("Both exact source offsets are required together")
        if self.start is not None and self.start >= self.end:
            raise ValueError("Source offsets must be ordered")
        return self


class StageStoryCandidateInput(Strict):
    candidate_key: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")]
    title: Annotated[str, Field(min_length=3, max_length=250)]
    summary: Annotated[str, Field(min_length=5, max_length=1000)]
    material_type: Literal[
        "event_narrative", "person_episode", "place_biography", "explanation",
        "comparison", "everyday_life", "mystery", "conflicting_accounts", "other",
    ] = "other"
    proposition: Annotated[str, Field(min_length=5, max_length=500)]
    account_kind: Literal[
        "legend", "tradition", "rumor", "recollection", "testimony", "other",
    ] = "other"
    attributed_to: str | None = Field(default=None, max_length=250)
    reported_by: str | None = Field(default=None, max_length=250)
    evidence_refs: list[StageStoryEvidenceInput] = Field(min_length=1, max_length=4)


# Bounded cross-book comparison. Model-authored conclusions are proposals,
# not identity or historical truth assigned by this deterministic backend.
class ReconcileCandidateRef(Strict):
    kind: Literal["story", "chunk"]
    ref_id: Identifier


class ReconcileEvidenceRef(Strict):
    document_id: Identifier
    source_revision: int = Field(ge=1)
    page_id: Identifier
    region_id: Identifier
    original_excerpt: Annotated[str, Field(min_length=1, max_length=500)]
    evidence_id: Identifier | None = None
    chunk_id: Identifier | None = None
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def span(self):
        if (self.start is None) != (self.end is None):
            raise ValueError("Offsets must be provided together")
        if self.start is not None and self.start >= self.end:
            raise ValueError("Ordered source offsets required")
        return self


class ReconcileDecision(Strict):
    identity_relation: Literal[
        "same_episode", "part_or_phase", "different_episode", "unrelated", "unresolved"
    ]
    contribution_kinds: list[Literal[
        "new_detail", "additional_evidence", "repeat", "contradiction",
        "interpretation", "thematic_context"
    ]] = Field(default_factory=list, max_length=6)
    independence: Literal["independent", "dependent", "mixed", "unknown"] = "unknown"
    independence_basis: Annotated[str, Field(min_length=5, max_length=500)]
    proposed_effect: Literal[
        "link_stories", "attach_evidence", "add_attributed_claim", "no_change"
    ]
    rationale: Annotated[str, Field(min_length=10, max_length=1000)]
    anchor_evidence: ReconcileEvidenceRef
    candidate_evidence: ReconcileEvidenceRef
    target_assertion_id: Identifier | None = None
    new_proposition: Annotated[str, Field(min_length=5, max_length=500)] | None = None
    attributed_to: str | None = Field(default=None, max_length=250)


class ReconcileStart(Strict):
    command: Literal["start"]
    anchor_story_id: Identifier
    expected_story_revision: int = Field(ge=1)
    query: str = Field(default="", max_length=350)
    policy_version: str = Field(default="cross-book-v1", max_length=80)
    max_candidate_pairs: int = Field(default=50, ge=1, le=50)
    refs: list[ReconcileCandidateRef] = Field(default_factory=list, max_length=50)


class ReconcileEnqueue(Strict):
    command: Literal["enqueue"]
    run_id: Identifier
    expected_job_revision: int = Field(ge=1)
    refs: list[ReconcileCandidateRef] = Field(default_factory=list, max_length=50)
    searched_channels: list[Literal[
        "story_lexical", "story_semantic", "source_lexical", "source_bge",
    ]] = Field(default_factory=list, max_length=4)


class ReconcileClaim(Strict):
    command: Literal["claim"]
    run_id: Identifier
    expected_job_revision: int = Field(ge=1)


class ReconcileStage(Strict):
    command: Literal["stage"]
    run_id: Identifier
    expected_job_revision: int = Field(ge=1)
    work_id: Identifier
    lease_token: Identifier
    decision: ReconcileDecision


class ReconcileApply(Strict):
    command: Literal["apply"]
    run_id: Identifier
    proposal_id: Identifier
    expected_job_revision: int = Field(ge=1)
    expected_target_revision: int = Field(ge=1)
    reviewer_note: Annotated[str, Field(min_length=10, max_length=500)]


class ReconcileCancel(Strict):
    command: Literal["cancel"]
    run_id: Identifier
    expected_job_revision: int = Field(ge=1)


ReconcileRequest = Annotated[
    ReconcileStart | ReconcileEnqueue | ReconcileClaim | ReconcileStage
    | ReconcileApply | ReconcileCancel,
    Field(discriminator="command"),
]
