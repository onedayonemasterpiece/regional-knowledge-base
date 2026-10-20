"""Optional inputs on ordinary book_ingest, never identity or provider receipts."""
from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import RightsStatus, Visibility
from .rights import assert_visibility_allowed


class ImportInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class PublicImportRights(ImportInput):
    rights_status: RightsStatus = Field(strict=False)
    evidence: dict[str, Any] = Field(max_length=16)
    policy_version: str = Field(min_length=1, max_length=128)

    @model_validator(mode='after')
    def public_basis(self):
        if len(json.dumps(self.evidence, allow_nan=False).encode()) > 8192:
            raise ValueError('public rights evidence bound')
        assert_visibility_allowed(Visibility.PUBLIC, self.rights_status,
            evidence=self.evidence, policy_version=self.policy_version)
        return self


class SourceResearchInput(ImportInput):
    state: Literal['pending', 'complete']
    continuation: dict[str, Any] = Field(default_factory=dict, max_length=16)

    @model_validator(mode='after')
    def bounded_work(self):
        if len(json.dumps(self.continuation, allow_nan=False).encode()) > 8192:
            raise ValueError('research continuation bound')
        if self.state == 'pending' and not self.continuation:
            raise ValueError('pending research requires model-authored continuation')
        return self


class TargetSourceImportInput(ImportInput):
    public_rights: PublicImportRights | None = None
    research: SourceResearchInput | None = None

    @model_validator(mode='after')
    def explicit_input(self):
        if self.public_rights is None and self.research is None:
            raise ValueError('public rights or model-authored research required')
        return self
