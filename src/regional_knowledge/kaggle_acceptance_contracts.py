"""Private acceptance observation, never a public search mode or permission.

Both branches always execute. These models describe bytes only; the retained
fixture in acceptance_observer is required at admission and final output.
"""
from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from .kaggle_protocol import Contract, EvidenceItem, Query, QueryResult, SourceDependency


def canonical_ids(values):
    if len(set(values)) != len(values) or any(str(UUID(v)) != v for v in values):
        raise ValueError('unique canonical IDs required')
    return values


class AcceptanceReadRequest(Query):
    type: Literal['ACCEPTANCE_READ'] = 'ACCEPTANCE_READ'
    operation: Literal['acceptance_rank_observation'] = 'acceptance_rank_observation'
    operation_version: Literal[1] = 1
    fixture_sha256: Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]
    fixture_sources: tuple[SourceDependency, ...] = Field(min_length=2,max_length=2)
    evidence_limit: Literal[3] = 3  # No caller-selectable product limit/mode.

    @field_validator('evidence_limit',mode='before')
    @classmethod
    def strict_fixed_limit(cls,value):
        if type(value) is not int:raise ValueError('integer fixed limit required')
        return value

    @model_validator(mode='after')
    def fixture_scope(self):
        if self.filters.kinds != ('source',) or self.filters.source_ids is None or len(self.filters.source_ids) != 2:
            raise ValueError('exact two-source acceptance fixture required')
        canonical_ids(self.filters.source_ids)
        if tuple(s.source_id for s in self.fixture_sources)!=self.filters.source_ids:
            raise ValueError('exact frozen source revisions required')
        return self


class BranchRanks(Contract):
    # Ordinals only: no query/document vector or arbitrary SQL output.
    bge: tuple[str, ...] = Field(default=(), max_length=10)
    lexical: tuple[str, ...] = Field(default=(), max_length=10)
    fusion: tuple[str, ...] = Field(default=(), max_length=10)

    @model_validator(mode='after')
    def unique_ranks(self):
        for values in (self.bge, self.lexical, self.fusion):
            canonical_ids(values)
        return self


class AcceptanceReadResult(QueryResult):
    type: Literal['ACCEPTANCE_READ_RESULT'] = 'ACCEPTANCE_READ_RESULT'
    evidence: tuple[EvidenceItem, ...] = Field(default=(), max_length=30)
    ranks: BranchRanks = Field(default_factory=BranchRanks)

    @model_validator(mode='after')
    def exact_hydrated_union(self):
        if self.status in ('ok','degraded') and (self.status!='degraded' or self.reason!='bounded_retrieval'):
            raise ValueError('observation is bounded retrieval, never exhaustive success')
        ids = tuple(item.id for item in self.evidence)
        canonical_ids(ids)
        union = set(self.ranks.bge + self.ranks.lexical + self.ranks.fusion)
        if union != set(ids) or (self.status not in ('ok', 'degraded') and union):
            raise ValueError('every observed rank requires exact hydrated evidence')
        if any(item.kind != 'source' for item in self.evidence):
            raise ValueError('acceptance observes only source passages')
        return self
