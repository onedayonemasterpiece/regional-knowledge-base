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


PUBLIC_RIGHTS = frozenset(
    {
        RightsStatus.LICENSED,
        RightsStatus.PERMISSION_GRANTED,
        RightsStatus.PUBLIC_DOMAIN_VERIFIED,
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
    visibility: Visibility = Visibility.PRIVATE


class SearchResult(BaseModel):
    id: str
    title: str
    url: str


class SearchOutput(BaseModel):
    results: list[SearchResult]
    mode: Literal["hybrid", "lexical_degraded"] = "hybrid"


class FetchOutput(BaseModel):
    id: str
    title: str
    text: str
    url: str
    metadata: dict[str, Any] | None = None


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


class BookIngestOutput(BaseModel):
    ingestion_id: str
    state: Literal["staged", "processing", "needs_review", "ready", "finalized", "failed"]
    message: str
    document_id: str | None = None
    next_cursor: str | None = None
    warnings: list[str] = Field(default_factory=list)


class DocumentAccessOutput(BaseModel):
    document_id: str
    visibility: Visibility
    rights_status: RightsStatus
    changed: bool
    message: str