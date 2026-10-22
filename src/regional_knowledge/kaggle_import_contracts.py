"""Optional inputs on ordinary book_ingest, never identity or provider receipts."""
from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

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
        if self.state == 'complete':
            if self.continuation:
                raise ValueError('completed research cannot retain an unfinished continuation')
            return self
        # A pending import must retain an actual, owner-scoped MCP read step.
        # It is not sufficient to save arbitrary prose, a URL, or a fabricated
        # tool name as a supposedly reachable post-activation frontier.
        step = self.continuation
        arguments = step.get('arguments')
        if (step.get('tool') != 'book_pages' or type(arguments) is not dict
                or set(arguments) - {'ingestion_id', 'cursor', 'batch_size'}):
            raise ValueError('pending research requires an exact book_pages continuation')
        ingestion_id = arguments.get('ingestion_id')
        try:
            if type(ingestion_id) is not str or str(UUID(ingestion_id)) != ingestion_id:
                raise ValueError
        except (ValueError, AttributeError, TypeError):
            raise ValueError('pending research requires a canonical ingestion ID') from None
        if 'cursor' in arguments:
            cursor = arguments['cursor']
            if (cursor is not None and (type(cursor) is not str or not 1 <= len(cursor) <= 256
                    or any(ord(char) < 32 or ord(char) > 126 for char in cursor))):
                raise ValueError('pending research requires a bounded page cursor')
        if 'batch_size' in arguments and (type(arguments['batch_size']) is not int
                                         or arguments['batch_size'] != 1):
            raise ValueError('pending research uses a single bounded page')
        # The caller's ingestion_id is still compared against the *owned*
        # source at stage admission by NativeSourceIntake; this input schema
        # does not replace that authority or promise that a page is available.
        return self


class TargetSourceImportInput(ImportInput):
    public_rights: PublicImportRights | None = None
    research: SourceResearchInput | None = None

    @model_validator(mode='after')
    def explicit_input(self):
        if self.public_rights is None and self.research is None:
            raise ValueError('public rights or model-authored research required')
        return self
