"""Typed journal commands; semantic decisions belong to the calling researcher."""
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class JournalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Position(JournalInput):
    id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,79}$")
    text: str = Field(min_length=1, max_length=8000)
    speaker: str = Field(min_length=1, max_length=500)
    modality: str = Field(min_length=1, max_length=500)


class CaseCreate(JournalInput):
    question: str = Field(min_length=1, max_length=3000)
    context: str = Field(min_length=1, max_length=8000)
    classification: str = Field(min_length=1, max_length=200)
    rationale: str = Field(min_length=1, max_length=8000)
    positions: list[Position] = Field(min_length=1, max_length=40)

    @model_validator(mode="after")
    def unique_positions(self):
        if len({p.id for p in self.positions}) != len(self.positions):
            raise ValueError("position IDs must be unique")
        return self


class CaseRevise(JournalInput):
    question: str = Field(min_length=1, max_length=3000)
    context: str = Field(min_length=1, max_length=8000)
    classification: str = Field(min_length=1, max_length=200)
    rationale: str = Field(min_length=1, max_length=8000)
    reason: str = Field(min_length=1, max_length=3000)


class EvidenceAdd(JournalInput):
    document_id: UUID
    source_revision: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    page_id: UUID
    region_id: UUID
    text_start: int = Field(ge=0)
    quote: str = Field(min_length=1, max_length=8000)
    context: str = Field(min_length=1, max_length=8000)
    position_ids: list[str] = Field(min_length=1, max_length=40)
    relation: Literal["supports", "challenges", "context", "attributed_criticism"]
    speaker: str = Field(min_length=1, max_length=500)
    # This is a preserved claim about attribution, never a server adjudication.
    attribution: Literal["direct_statement", "attributed_report", "unknown_speaker"]


class ContributionAdd(JournalInput):
    kind: Literal["argument", "opinion", "question", "correction", "reply"]
    original_text: str = Field(min_length=1, max_length=12000)
    interpretation: str = Field(default="", max_length=8000)
    evidence_ids: list[UUID] = Field(default_factory=list, max_length=40)
    position_ids: list[str] = Field(default_factory=list, max_length=40)
    reply_to: UUID | None = None


class ContributionCorrect(JournalInput):
    original_text: str = Field(min_length=1, max_length=12000)
    interpretation: str = Field(default="", max_length=8000)
    reason: str = Field(min_length=1, max_length=3000)
    withdrawn: bool = False


class ConclusionAdd(JournalInput):
    kind: Literal["model_synthesis", "individual_opinion"]
    text: str = Field(min_length=1, max_length=16000)
    outcome: Literal["context_explains", "insufficient_support", "preferred_variant",
                     "multiple_interpretations", "insufficient_material"]
    allowed_wording: str = Field(min_length=1, max_length=8000)
    limitations: str = Field(min_length=1, max_length=8000)
    open_questions: list[str] = Field(max_length=40)
    based_on_seq: int = Field(ge=0)
    evidence_ids: list[UUID] = Field(default_factory=list, max_length=100)
    contribution_versions: dict[UUID, Annotated[int, Field(ge=1)]] = Field(default_factory=dict, max_length=100)
    weight_analysis: str = Field(min_length=1, max_length=8000)
    make_current: bool = False

    @model_validator(mode="after")
    def individual_is_separate(self):
        if self.kind == "individual_opinion" and self.make_current:
            raise ValueError("individual opinion cannot replace the shared research synthesis")
        return self


class CaseState(JournalInput):
    state: Literal["open", "paused", "archived"]
    reason: str = Field(min_length=1, max_length=3000)


class CaseGrant(JournalInput):
    grantee: UUID
    capability: Literal["viewer", "contributor", "curator", "manager"] | None
    reason: str = Field(min_length=1, max_length=3000)
