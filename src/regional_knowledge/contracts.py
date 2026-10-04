from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, model_validator


class Visibility(StrEnum):
    PRIVATE = "private"
    WORKSPACE = "workspace"
    PUBLIC = "public"


class RightsStatus(StrEnum):
    UNKNOWN = "unknown"
    RESTRICTED = "restricted"
    LICENSED = "licensed"
    PERMISSION_GRANTED = "permission_granted"
    PUBLIC_DOMAIN_CANDIDATE = "public_domain_candidate"
    PUBLIC_DOMAIN_VERIFIED = "public_domain_verified"
    STATUTORY_ACCESS_VERIFIED = "statutory_access_verified"


PUBLIC_RIGHTS = frozenset(
    {
        RightsStatus.LICENSED,
        RightsStatus.PERMISSION_GRANTED,
        RightsStatus.PUBLIC_DOMAIN_VERIFIED,
        RightsStatus.STATUTORY_ACCESS_VERIFIED,
    }
)


class RegionKind(StrEnum):
    HEADING = "heading"
    BODY = "body"
    CAPTION = "caption"
    FOOTNOTE = "footnote"
    FIGURE = "figure"
    TABLE = "table"
    HEADER = "header"
    FOOTER = "footer"
    PAGE_NUMBER = "page_number"
    MARGINALIA = "marginalia"


class RelationKind(StrEnum):
    CAPTION_OF = "caption_of"
    FOOTNOTE_OF = "footnote_of"
    CONTINUES_TO = "continues_to"
    ILLUSTRATES = "illustrates"
    REFERS_TO = "refers_to"


class BBox(BaseModel):
    """Normalized page coordinates in a 0..1000 coordinate space."""

    left: int = Field(ge=0, le=1000)
    top: int = Field(ge=0, le=1000)
    right: int = Field(ge=0, le=1000)
    bottom: int = Field(ge=0, le=1000)

    @model_validator(mode="after")
    def ordered(self) -> "BBox":
        if self.left >= self.right or self.top >= self.bottom:
            raise ValueError("bbox must have positive area")
        return self


class Region(BaseModel):
    region_id: str
    page_id: str
    kind: RegionKind
    bbox: BBox
    reading_order: int = Field(ge=0)
    column_id: str | None = None
    source_text: str = ""
    normalized_text: str = ""
    confidence: float | None = Field(default=None, ge=0, le=1)
    needs_review: bool = False


class RegionRelation(BaseModel):
    kind: RelationKind
    source_region_id: str
    target_region_id: str


class Illustration(BaseModel):
    illustration_id: str
    document_id: str
    page_id: str
    source_region_id: str
    bbox: BBox
    kind: Literal["photo", "map", "drawing", "diagram", "facsimile", "other"]
    caption_region_ids: list[str] = Field(default_factory=list)
    nearby_region_ids: list[str] = Field(default_factory=list)
    media_ref: str | None = None
    source_crop_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    rights_status: RightsStatus = RightsStatus.UNKNOWN
    rights_evidence: dict[str, Any] = Field(default_factory=dict)
    rights_policy_version: str | None = None
    visibility: Visibility = Visibility.PRIVATE


class StartMetadataInput(BaseModel):
    duplicate_policy: Literal['reuse', 'new_revision'] = 'reuse'
    title: str | None = Field(default=None, max_length=500)
    authors: list[str] = Field(default_factory=list, max_length=50)
    publication_year: int | None = Field(default=None, ge=1, le=3000)
    language: str | None = Field(default=None, max_length=80)


class StageRegionInput(BaseModel):
    region_key: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
    kind: RegionKind
    bbox: BBox
    reading_order: int = Field(ge=0, le=10_000)
    column_id: str | None = Field(default=None, max_length=80)
    source_text: str = Field(default="", max_length=8_000)
    normalized_text: str = Field(default="", max_length=8_000)
    confidence: float | None = Field(default=None, ge=0, le=1)
    needs_review: bool = False

    @model_validator(mode="after")
    def fill_normalized_text(self) -> "StageRegionInput":
        if not self.normalized_text and self.source_text:
            self.normalized_text = self.source_text
        return self


class StageRelationInput(BaseModel):
    kind: RelationKind
    source_region_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )
    target_region_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )


class StageIllustrationInput(BaseModel):
    visual_description: str | None = Field(default=None, min_length=1, max_length=2000)
    visual_description_provenance: Literal['model_observation'] = 'model_observation'
    visual_description_language: str | None = Field(default=None, max_length=80)
    illustration_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )
    source_region_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )
    kind: Literal["photo", "map", "drawing", "diagram", "facsimile", "other"]
    caption_region_keys: list[str] = Field(default_factory=list, max_length=20)
    nearby_region_keys: list[str] = Field(default_factory=list, max_length=40)


class StagePageInput(BaseModel):
    excluded_figure_regions: dict[str, str] = Field(default_factory=dict, max_length=100)
    source_material: Literal["unreviewed", "preview", "full_native", "visual_reviewed"] = "unreviewed"
    source_review_note: str | None = Field(default=None, min_length=1, max_length=500)
    page_id: str = Field(min_length=36, max_length=36)
    physical_page_index: int = Field(ge=0)
    printed_page_number: str | None = Field(default=None, max_length=40)
    layout_kind: str | None = Field(default=None, max_length=80)
    regions: list[StageRegionInput] = Field(default_factory=list, max_length=500)
    relations: list[StageRelationInput] = Field(default_factory=list, max_length=1_000)
    illustrations: list[StageIllustrationInput] = Field(
        default_factory=list, max_length=100
    )

    @model_validator(mode="after")
    def validate_local_graph(self) -> "StagePageInput":
        keys = [region.region_key for region in self.regions]
        if len(keys) != len(set(keys)):
            raise ValueError("region_key must be unique within a page")
        orders = [region.reading_order for region in self.regions]
        if len(orders) != len(set(orders)):
            raise ValueError("reading_order must be unique within a page")
        known = set(keys)
        for relation in self.relations:
            if (
                relation.source_region_key not in known
                or relation.target_region_key not in known
            ):
                raise ValueError("relation references an unknown region_key")
        illustration_keys = [item.illustration_key for item in self.illustrations]
        if len(illustration_keys) != len(set(illustration_keys)):
            raise ValueError("illustration_key must be unique within a page")
        by_key = {region.region_key: region for region in self.regions}
        for illustration in self.illustrations:
            source = by_key.get(illustration.source_region_key)
            if source is None or source.kind is not RegionKind.FIGURE:
                raise ValueError("illustration source_region_key must be a figure region")
            if any(key not in known or by_key[key].kind is not RegionKind.CAPTION for key in illustration.caption_region_keys):
                raise ValueError("caption_region_keys must reference printed caption regions")
            if any(key not in known for key in illustration.nearby_region_keys):
                raise ValueError("nearby_region_keys contains an unknown region")
        for key, reason in self.excluded_figure_regions.items():
            if key not in known or by_key[key].kind is not RegionKind.FIGURE or not reason.strip() or len(reason)>500:
                raise ValueError('figure exclusion requires a figure region and bounded reason')
        return self


class StageRegionRef(BaseModel):
    page_id: str = Field(min_length=36, max_length=36)
    region_key: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")


class StageIllustrationRef(BaseModel):
    page_id: str = Field(min_length=36, max_length=36)
    illustration_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )


class StageChunkInput(BaseModel):
    chunk_key: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
    title: str = Field(min_length=1, max_length=500)
    region_refs: list[StageRegionRef] = Field(min_length=1, max_length=100)
    footnote_refs: list[StageRegionRef] = Field(default_factory=list, max_length=50)
    illustration_refs: list[StageIllustrationRef] = Field(
        default_factory=list, max_length=30
    )


class PoiLocatorInput(BaseModel):
    names: list[str] = Field(min_length=1, max_length=30)
    external_ids: dict[str, str] = Field(default_factory=dict)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    @model_validator(mode="after")
    def compact_locator(self) -> "PoiLocatorInput":
        self.names = [
            value.strip()[:300]
            for value in self.names
            if value.strip()
        ]
        if not self.names:
            raise ValueError("POI locator needs at least one non-empty name")
        if len(self.external_ids) > 20:
            raise ValueError("too many POI external ids")
        self.external_ids = {
            str(key).strip()[:80]: str(value).strip()[:500]
            for key, value in self.external_ids.items()
            if str(key).strip() and str(value).strip()
        }
        return self


class StagePoiFactInput(BaseModel):
    candidate_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )
    poi_locator: PoiLocatorInput
    kind: Literal[
        "construction",
        "architect",
        "foundation",
        "reconstruction",
        "demolition",
        "ownership",
        "visit",
        "use",
        "opening",
        "location",
        "structure",
        "other",
    ]
    text: str = Field(min_length=1, max_length=500)
    time_scope: str | None = Field(default=None, max_length=100)
    evidence_refs: list[StageRegionRef] = Field(min_length=1, max_length=50)
    contributor_names: list[str] = Field(default_factory=list, max_length=20)
    publication_method: Literal[
        "source_edition",
        "scholarly_monograph",
        "institutional_catalogue",
        "general_history",
        "memoir",
        "unknown",
    ] = "unknown"

    @model_validator(mode="after")
    def compact_contributors(self) -> "StagePoiFactInput":
        self.contributor_names = list(
            dict.fromkeys(
                value.strip()[:300]
                for value in self.contributor_names
                if value.strip()
            )
        )
        return self


class StagePoiMediaLinkInput(BaseModel):
    link_key: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$"
    )
    poi_locator: PoiLocatorInput
    illustration_ref: StageIllustrationRef
    relation: Literal["depicts", "illustrates", "map_of", "detail_of"]
    time_scope: str | None = Field(default=None, max_length=100)


class SearchResult(BaseModel):
    id: str
    title: str
    url: str
    ranking_signals: list[dict[str, Any]] = Field(default_factory=list)


class IndexingStatus(BaseModel):
    active_chunks: int = Field(ge=0)
    e5_ready: int = Field(ge=0)
    e5_missing: int = Field(ge=0)
    bge_ready: int = Field(ge=0)
    bge_missing: int = Field(ge=0)
    indexing_state: Literal['pending','running','ready','degraded']
    bge_worker_state: str
    indexing_owner_state: Literal['disabled','unavailable','running','ready','degraded']
    effective_retrieval_mode: Literal['bge_lexical','fast_e5','lexical_only']


class SearchOutput(BaseModel):
    results: list[SearchResult]
    mode: Literal["hybrid", "lexical_degraded"] = "hybrid"
    retrieval_mode: Literal["fast_e5", "lexical_only", "bge_lexical", "e5_bge_lexical", "bge", "e5_bge"] = "lexical_only"
    main_state: Literal['disabled','starting','pending','ready','unavailable'] = 'disabled'
    main_job_id: str | None = None
    indexing: IndexingStatus | None = None
    timings: dict[str, float] = Field(default_factory=dict, exclude=True)


class FetchOutput(BaseModel):
    id: str
    title: str
    text: str
    url: str
    metadata: dict[str, Any] | None = None


class EvidenceSearchOutput(BaseModel):
    evidence: list[FetchOutput]
    mode: Literal["hybrid", "lexical_degraded"] = "hybrid"
    retrieval_mode: Literal["fast_e5", "lexical_only", "bge_lexical", "e5_bge_lexical", "bge", "e5_bge"] = "lexical_only"
    main_state: Literal['disabled','starting','pending','ready','unavailable'] = 'disabled'
    main_job_id: str | None = None
    indexing: IndexingStatus | None = None
    timings: dict[str, float] = Field(default_factory=dict, exclude=True)


class ChatFile(BaseModel):
    download_url: HttpUrl
    file_id: str
    mime_type: str | None = None
    file_name: str | None = None


class Principal(BaseModel):
    subject: str
    client_id: str
    issuer: str
    access_token: str = Field(repr=False, exclude=True)


class ProfileOutput(BaseModel):
    id: str
    service: Literal["regional_knowledge"] = "regional_knowledge"


class BookFindResult(BaseModel):
    document_id: str
    title: str
    authors: list[str] = Field(default_factory=list)
    publication_year: int | None = None
    active_revision: int = Field(default=0, ge=0)
    source_format: str | None = None
    source_archive_status: Literal["pending", "verified"] | None = None


class BookFindOutput(BaseModel):
    query: str
    results: list[BookFindResult] = Field(default_factory=list, max_length=8)


class BookIngestOutput(BaseModel):
    ingestion_id: str
    state: Literal["staged", "processing", "needs_review", "ready", "finalized", "failed"]
    message: str
    document_id: str | None = None
    next_cursor: str | None = None
    next_action: Literal["continue_pages", "validate", "finalize", "wait", "resume_finalize", "done", "blocker"] | None = None
    warnings: list[str] = Field(default_factory=list)
    indexing: IndexingStatus | None = None
    source_archive_status: Literal["pending", "verified"] | None = None


class DocumentAccessOutput(BaseModel):
    document_id: str
    visibility: Visibility
    rights_status: RightsStatus
    changed: bool
    message: str
