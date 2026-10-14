"""Bounded JSON transport for the existing model-authored stage operation.

This is not another importer: the exact existing stage validators and authority
write path run afterwards. The file cannot select another job, revision or action.
No source is inferred, extracted or marked reviewed by this transport.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import tempfile
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .contracts import (StagePageInput, StageChunkInput, StagePoiFactInput,
                        StagePoiMediaLinkInput)
from .entity_graph import GraphBundle
from .story_contracts import StageStoryCandidateInput

MAX_STAGE_FILE_BYTES = 1024 * 1024


class StageFileError(ValueError):
    """Safe generated diagnostic: field paths/types only, never input values."""


class StageFile(BaseModel):
    model_config = ConfigDict(extra='forbid')
    ingestion_id: UUID
    source_revision: int = Field(ge=1)
    pages: list[StagePageInput] = Field(default_factory=list, max_length=8)
    chunks: list[StageChunkInput] = Field(default_factory=list, max_length=100)
    poi_facts: list[StagePoiFactInput] = Field(default_factory=list, max_length=100)
    poi_media_links: list[StagePoiMediaLinkInput] = Field(default_factory=list, max_length=100)
    entity_candidates: GraphBundle | None = None
    story_candidates: list[StageStoryCandidateInput] = Field(default_factory=list, max_length=12)


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate stage JSON key')
        value[key] = item
    return value


def _constant(_value):
    raise ValueError('nonfinite stage JSON value')


def parse_stage_file(data: bytes, ingestion_id: str, revision: int) -> dict:
    if not data or len(data) > MAX_STAGE_FILE_BYTES:
        raise ValueError('stage_file_size_limit')
    try:
        value = json.loads(data.decode('utf-8-sig'), object_pairs_hook=_object,
                           parse_constant=_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError('stage_file_must_be_utf8_json') from exc
    try:
        staged = StageFile.model_validate(value)
    except ValidationError as exc:
        issues = []
        allowed = set(StageFile.model_fields) | {'region_key','kind','bbox','reading_order',
            'source_text','normalized_text','page_id','physical_page_index','printed_page_number',
            'layout_kind','source_material','source_review_note','knowledge_reviewed',
            'story_candidates_reviewed','regions','relations','illustrations','excluded_figure_regions',
            'left','top','right','bottom','entity_id','key','entities','entity_refs','canonical_label',
            'exact_source_spelling','evidence','aliases','candidate_key','title','summary',
            'proposition','account_kind','material_type','attributed_to','reported_by','evidence_refs',
            'event_dates','observations','entity_keys','original_excerpt','start','end',
            'year','month','day','precision','calendar','date_role','evidence_indexes','rationale'}
        for issue in exc.errors(include_input=False, include_context=False, include_url=False)[:8]:
            path = '.'.join(str(part) if isinstance(part,int) or part in allowed else '*' for part in issue['loc'])
            issues.append(path + ':' + issue['type'])
        raise StageFileError('stage_file_schema: ' + '; '.join(issues)) from exc
    if str(staged.ingestion_id) != str(UUID(ingestion_id)) or staged.source_revision != revision:
        raise StageFileError('stage_file_ingestion_or_revision_mismatch')
    return staged.model_dump(mode='json', exclude={'ingestion_id', 'source_revision'}, exclude_none=True)


async def read_stage_file(backend, principal, ingestion_id, file):
    if not ingestion_id:
        raise ValueError('ingestion_id is required for a stage file')
    # The normal owner lookup runs BEFORE fetching any user-controlled URL.
    row = await backend._ingestion_row(principal=principal, ingestion_id=ingestion_id)
    if not row or str(row.get('owner_user_id')) != str(principal.subject):
        raise PermissionError('owned_ingestion_required')
    mime = (file.mime_type or '').lower().split(';', 1)[0]
    if mime not in {'', 'application/json', 'text/plain', 'application/octet-stream'}:
        raise StageFileError('stage_file_must_be_json_not_original_source')
    root = Path(os.getenv('RKB_WORK_DIR') or Path.home()/'.local/state/regional-knowledge-base/work')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix='rkb-stage-', dir=root) as directory:
        downloaded = await backend.file_downloader.download(
            str(file.download_url), Path(directory), max_bytes=MAX_STAGE_FILE_BYTES,
            content_kind='stage_json')
        def parse():
            with downloaded.path.open('rb') as source:
                data = source.read(MAX_STAGE_FILE_BYTES + 1)
            return parse_stage_file(data, row['id'], int(row['staged_revision']))
        payload = await asyncio.to_thread(parse)
    return payload, downloaded.sha256
