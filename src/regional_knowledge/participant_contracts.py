"""Participant administration payloads, separate from trusted request identity."""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


Category = Literal["demo", "student", "research_staff", "expert", "guide", "local_historian"]
Capability = Literal["public_search", "case_read", "case_contribute", "scan_proof_read",
                     "personal_conclusion", "support_exact_revision", "maintain_shared_summary",
                     "discussion_request", "discussion_approve", "participants_manage"]


class ParticipantInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Credential(ParticipantInput):
    description: str = Field(min_length=1, max_length=2000)
    status: Literal["claimed", "owner_attested", "independently_verified"]
    basis: str = Field(min_length=1, max_length=4000)
    verifier: str = Field(min_length=1, max_length=500)
    checked_at: datetime

    @field_validator("checked_at")
    @classmethod
    def aware_time(cls, value):
        if value.tzinfo is None:
            raise ValueError("timezone required")
        return value


class ParticipantUpsert(ParticipantInput):
    name: str = Field(min_length=1, max_length=500)
    description: str = Field(default="", max_length=4000)
    categories: set[Category] = Field(default_factory=set, max_length=6)
    credentials: list[Credential] = Field(default_factory=list, max_length=40)
    public_profile_consent: bool = False
    expires_at: datetime | None = None
    reason: str = Field(min_length=1, max_length=3000)

    @field_validator("expires_at")
    @classmethod
    def aware_expiry(cls, value):
        if value is not None and value.tzinfo is None:
            raise ValueError("timezone required")
        return value


class ParticipantDisable(ParticipantInput):
    disabled: bool
    reason: str = Field(min_length=1, max_length=3000)


class ParticipantGrant(ParticipantInput):
    capability: Capability
    granted: bool
    expires_at: datetime | None = None
    reason: str = Field(min_length=1, max_length=3000)

    _aware_expiry = field_validator("expires_at")(classmethod(ParticipantUpsert.aware_expiry.__func__))


class DomainWeight(ParticipantInput):
    domain: str = Field(min_length=1, max_length=500)
    weight: str = Field(min_length=1, max_length=500)
    basis: str = Field(min_length=1, max_length=4000)
    expires_at: datetime | None = None

    _aware_expiry = field_validator("expires_at")(classmethod(ParticipantUpsert.aware_expiry.__func__))


class AccountBegin(ParticipantInput):
    provider: Literal["slack", "vk", "projects_hub", "oidc"]
    tenant: str = Field(min_length=1, max_length=500)
    provider_subject: str = Field(min_length=1, max_length=500)
    reason: str = Field(min_length=1, max_length=3000)


class AccountConfirm(ParticipantInput):
    link_id: UUID
    expected_link_revision: int = Field(ge=1)
    method: Literal["owner_confirmed_slack", "provider_observation"]
    observation_id: UUID | None = None
    reason: str = Field(min_length=1, max_length=3000)


class AccountUnlink(ParticipantInput):
    link_id: UUID
    expected_link_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=3000)
    incorrect_binding: bool = False
