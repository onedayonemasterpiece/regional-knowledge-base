from __future__ import annotations

from collections import defaultdict
from typing import Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, Field, model_validator

from .contracts import (
    BBox,
    RegionKind,
    PoiLocatorInput,
    RelationKind,
    StageChunkInput,
    StagePageInput,
    StagePoiFactInput,
    StagePoiMediaLinkInput,
)


class StagedRegion(BaseModel):
    region_key: str
    region_id: UUID
    page_id: UUID
    kind: RegionKind
    bbox: BBox
    reading_order: int = Field(ge=0)
    column_id: str | None = Field(default=None, max_length=120)
    source_text: str = Field(default="", max_length=8_000)
    normalized_text: str = Field(default="", max_length=8_000)
    confidence: float | None = Field(default=None, ge=0, le=1)
    needs_review: bool = False


class StagedPage(BaseModel):
    source_material: Literal["unreviewed", "preview", "full_native", "visual_reviewed"] = "unreviewed"
    source_review_note: str | None = Field(default=None, min_length=1, max_length=500)
    page_id: UUID
    physical_page_index: int = Field(ge=0)
    printed_page_number: str | None = Field(default=None, max_length=80)
    width: int = Field(default=1000, gt=0, le=20_000)
    height: int = Field(default=1000, gt=0, le=20_000)
    layout_kind: str | None = Field(default=None, max_length=120)
    regions: list[StagedRegion] = Field(default_factory=list, max_length=500)

    @model_validator(mode="after")
    def region_page_ids_match(self) -> "StagedPage":
        if any(region.page_id != self.page_id for region in self.regions):
            raise ValueError("every region.page_id must match its staged page")
        return self


class StagedRelation(BaseModel):
    kind: RelationKind
    source_region_id: UUID
    target_region_id: UUID


class StagedIllustration(BaseModel):
    illustration_key: str
    illustration_id: UUID
    page_id: UUID
    source_region_id: UUID
    bbox: BBox
    kind: Literal["photo", "map", "drawing", "diagram", "facsimile", "other"]
    caption_region_ids: list[UUID] = Field(default_factory=list, max_length=50)
    nearby_region_ids: list[UUID] = Field(default_factory=list, max_length=100)


class StagedChunk(BaseModel):
    chunk_key: str
    chunk_id: UUID
    title: str = Field(min_length=1, max_length=500)
    region_ids: list[UUID] = Field(min_length=1, max_length=100)
    page_ids: list[UUID] = Field(min_length=1, max_length=20)
    illustration_ids: list[UUID] = Field(default_factory=list, max_length=30)
    footnote_region_ids: list[UUID] = Field(default_factory=list, max_length=50)
    text: str = Field(min_length=1, max_length=40_000)
    normalized_text: str = Field(min_length=1, max_length=40_000)


class StagedPoiFact(BaseModel):
    candidate_key: str
    candidate_id: UUID
    poi_locator: PoiLocatorInput
    kind: str
    text: str = Field(min_length=1, max_length=500)
    time_scope: str | None = Field(default=None, max_length=100)
    region_ids: list[UUID] = Field(min_length=1, max_length=50)
    page_ids: list[UUID] = Field(min_length=1, max_length=20)
    contributor_names: list[str] = Field(default_factory=list, max_length=20)
    publication_method: str


class StagedPoiMediaLink(BaseModel):
    link_key: str
    link_id: UUID
    poi_locator: PoiLocatorInput
    illustration_id: UUID
    page_id: UUID
    relation: Literal["depicts", "illustrates", "map_of", "detail_of"]
    time_scope: str | None = Field(default=None, max_length=100)


class IngestStagePayload(BaseModel):
    pages: list[StagedPage] = Field(default_factory=list, max_length=8)
    relations: list[StagedRelation] = Field(default_factory=list, max_length=2_000)
    illustrations: list[StagedIllustration] = Field(default_factory=list, max_length=200)
    chunks: list[StagedChunk] = Field(default_factory=list, max_length=500)
    poi_facts: list[StagedPoiFact] = Field(default_factory=list, max_length=500)
    poi_media_links: list[StagedPoiMediaLink] = Field(
        default_factory=list, max_length=500
    )


class StagedGraph(BaseModel):
    revision: int = Field(ge=1)
    pages: list[StagedPage] = Field(default_factory=list)
    relations: list[StagedRelation] = Field(default_factory=list)
    illustrations: list[StagedIllustration] = Field(default_factory=list)
    chunks: list[StagedChunk] = Field(default_factory=list)
    poi_facts: list[StagedPoiFact] = Field(default_factory=list)
    poi_media_links: list[StagedPoiMediaLink] = Field(default_factory=list)


class GraphValidation(BaseModel):
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.errors and not any(
            warning.startswith("review:") for warning in self.warnings
        )


def _dedupe_preserve(values: list[UUID]) -> list[UUID]:
    seen: set[UUID] = set()
    output: list[UUID] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


def compile_model_stage(
    graph: StagedGraph,
    *,
    document_id: str,
    revision: int,
    pages: list[StagePageInput],
    chunks: list[StageChunkInput],
    poi_facts: list[StagePoiFactInput] | None = None,
    poi_media_links: list[StagePoiMediaLinkInput] | None = None,
) -> IngestStagePayload:
    if graph.revision != revision:
        raise ValueError("staged graph revision mismatch")
    document_uuid = UUID(document_id)

    staged_pages: list[StagedPage] = []
    relations: list[StagedRelation] = []
    illustrations: list[StagedIllustration] = []

    for page in pages:
        expected_page_id = uuid5(
            document_uuid,
            f"page:{revision}:{page.physical_page_index}",
        )
        if str(expected_page_id) != page.page_id:
            raise ValueError(
                f"page_id does not match document/revision/index: {page.page_id}"
            )

        region_ids = {
            region.region_key: uuid5(
                expected_page_id,
                f"region:{region.region_key}",
            )
            for region in page.regions
        }
        staged_regions = [
            StagedRegion(
                region_key=region.region_key,
                region_id=region_ids[region.region_key],
                page_id=expected_page_id,
                kind=region.kind,
                bbox=region.bbox,
                reading_order=region.reading_order,
                column_id=region.column_id,
                source_text=region.source_text,
                normalized_text=region.normalized_text or region.source_text,
                confidence=region.confidence,
                needs_review=region.needs_review,
            )
            for region in page.regions
        ]
        staged_pages.append(
            StagedPage(
                page_id=expected_page_id,
                physical_page_index=page.physical_page_index,
                printed_page_number=page.printed_page_number,
                layout_kind=page.layout_kind,
                source_material=page.source_material,
                source_review_note=page.source_review_note,
                regions=staged_regions,
            )
        )

        relations.extend(
            StagedRelation(
                kind=relation.kind,
                source_region_id=region_ids[relation.source_region_key],
                target_region_id=region_ids[relation.target_region_key],
            )
            for relation in page.relations
        )

        for item in page.illustrations:
            source_id = region_ids[item.source_region_key]
            source_region = next(
                region
                for region in staged_regions
                if region.region_id == source_id
            )
            illustration_id = uuid5(
                expected_page_id,
                f"illustration:{item.illustration_key}",
            )
            illustrations.append(
                StagedIllustration(
                    illustration_key=item.illustration_key,
                    illustration_id=illustration_id,
                    page_id=expected_page_id,
                    source_region_id=source_id,
                    bbox=source_region.bbox,
                    kind=item.kind,
                    caption_region_ids=[
                        region_ids[key] for key in item.caption_region_keys
                    ],
                    nearby_region_ids=[
                        region_ids[key] for key in item.nearby_region_keys
                    ],
                )
            )

    partial = IngestStagePayload(
        pages=staged_pages,
        relations=relations,
        illustrations=illustrations,
    )
    merged_for_refs = merge_stage(graph, partial)

    region_by_ref: dict[tuple[str, str], StagedRegion] = {}
    for page in merged_for_refs.pages:
        for region in page.regions:
            region_by_ref[(str(page.page_id), region.region_key)] = region
    illustration_by_ref = {
        (str(item.page_id), item.illustration_key): item
        for item in merged_for_refs.illustrations
    }

    staged_chunks: list[StagedChunk] = []
    for chunk in chunks:
        main_regions: list[StagedRegion] = []
        for ref in chunk.region_refs:
            region = region_by_ref.get((ref.page_id, ref.region_key))
            if region is None:
                raise ValueError(
                    f"chunk {chunk.chunk_key} references unknown region "
                    f"{ref.page_id}#{ref.region_key}"
                )
            main_regions.append(region)

        footnotes: list[StagedRegion] = []
        for ref in chunk.footnote_refs:
            region = region_by_ref.get((ref.page_id, ref.region_key))
            if region is None:
                raise ValueError(
                    f"chunk {chunk.chunk_key} references unknown footnote "
                    f"{ref.page_id}#{ref.region_key}"
                )
            if region.kind is not RegionKind.FOOTNOTE:
                raise ValueError(
                    f"chunk {chunk.chunk_key} footnote_ref is not a footnote"
                )
            footnotes.append(region)

        chunk_illustrations: list[StagedIllustration] = []
        for ref in chunk.illustration_refs:
            item = illustration_by_ref.get((ref.page_id, ref.illustration_key))
            if item is None:
                raise ValueError(
                    f"chunk {chunk.chunk_key} references unknown illustration "
                    f"{ref.page_id}#{ref.illustration_key}"
                )
            chunk_illustrations.append(item)

        source_parts = [
            region.source_text.strip()
            for region in main_regions
            if region.source_text.strip()
        ]
        normalized_parts = [
            (region.normalized_text or region.source_text).strip()
            for region in main_regions
            if (region.normalized_text or region.source_text).strip()
        ]
        for footnote in footnotes:
            if footnote.source_text.strip():
                source_parts.append(f"[Footnote] {footnote.source_text.strip()}")
            normalized = (footnote.normalized_text or footnote.source_text).strip()
            if normalized:
                normalized_parts.append(f"[Footnote] {normalized}")
        text = "\n".join(source_parts).strip()
        normalized_text = "\n".join(normalized_parts).strip()
        if not text or not normalized_text:
            raise ValueError(f"chunk {chunk.chunk_key} resolves to empty text")

        page_ids = _dedupe_preserve(
            [region.page_id for region in [*main_regions, *footnotes]]
        )
        staged_chunks.append(
            StagedChunk(
                chunk_key=chunk.chunk_key,
                chunk_id=uuid5(
                    document_uuid,
                    f"chunk:{revision}:{chunk.chunk_key}",
                ),
                title=chunk.title,
                region_ids=_dedupe_preserve(
                    [region.region_id for region in main_regions]
                ),
                page_ids=page_ids,
                illustration_ids=_dedupe_preserve(
                    [item.illustration_id for item in chunk_illustrations]
                ),
                footnote_region_ids=_dedupe_preserve(
                    [region.region_id for region in footnotes]
                ),
                text=text,
                normalized_text=normalized_text,
            )
        )

    staged_poi_facts: list[StagedPoiFact] = []
    for fact in poi_facts or []:
        evidence_regions: list[StagedRegion] = []
        for ref in fact.evidence_refs:
            region = region_by_ref.get((ref.page_id, ref.region_key))
            if region is None:
                raise ValueError(
                    f"POI fact {fact.candidate_key} references unknown region "
                    f"{ref.page_id}#{ref.region_key}"
                )
            evidence_regions.append(region)
        staged_poi_facts.append(
            StagedPoiFact(
                candidate_key=fact.candidate_key,
                candidate_id=uuid5(
                    document_uuid,
                    f"poi-fact:{revision}:{fact.candidate_key}",
                ),
                poi_locator=fact.poi_locator,
                kind=fact.kind,
                text=fact.text.strip(),
                time_scope=fact.time_scope,
                region_ids=_dedupe_preserve(
                    [region.region_id for region in evidence_regions]
                ),
                page_ids=_dedupe_preserve(
                    [region.page_id for region in evidence_regions]
                ),
                contributor_names=fact.contributor_names,
                publication_method=fact.publication_method,
            )
        )

    staged_poi_media_links: list[StagedPoiMediaLink] = []
    for link in poi_media_links or []:
        illustration = illustration_by_ref.get(
            (
                link.illustration_ref.page_id,
                link.illustration_ref.illustration_key,
            )
        )
        if illustration is None:
            raise ValueError(
                f"POI media link {link.link_key} references unknown illustration "
                f"{link.illustration_ref.page_id}#"
                f"{link.illustration_ref.illustration_key}"
            )
        staged_poi_media_links.append(
            StagedPoiMediaLink(
                link_key=link.link_key,
                link_id=uuid5(
                    document_uuid,
                    f"poi-media:{revision}:{link.link_key}",
                ),
                poi_locator=link.poi_locator,
                illustration_id=illustration.illustration_id,
                page_id=illustration.page_id,
                relation=link.relation,
                time_scope=link.time_scope,
            )
        )

    return IngestStagePayload(
        pages=staged_pages,
        relations=relations,
        illustrations=illustrations,
        chunks=staged_chunks,
        poi_facts=staged_poi_facts,
        poi_media_links=staged_poi_media_links,
    )


def merge_stage(graph: StagedGraph, payload: IngestStagePayload) -> StagedGraph:
    replaced_page_ids = {str(page.page_id) for page in payload.pages}
    replaced_region_ids = {
        str(region.region_id)
        for page in graph.pages
        if str(page.page_id) in replaced_page_ids
        for region in page.regions
    }
    replaced_illustration_ids = {
        str(item.illustration_id)
        for item in graph.illustrations
        if str(item.page_id) in replaced_page_ids
    }

    pages = {
        str(page.page_id): page
        for page in graph.pages
        if str(page.page_id) not in replaced_page_ids
    }
    for page in payload.pages:
        pages[str(page.page_id)] = page

    relations = {
        (str(item.source_region_id), str(item.target_region_id), item.kind.value): item
        for item in graph.relations
        if str(item.source_region_id) not in replaced_region_ids
        and str(item.target_region_id) not in replaced_region_ids
    }
    for item in payload.relations:
        relations[
            (str(item.source_region_id), str(item.target_region_id), item.kind.value)
        ] = item

    illustrations = {
        str(item.illustration_id): item
        for item in graph.illustrations
        if str(item.page_id) not in replaced_page_ids
        and str(item.source_region_id) not in replaced_region_ids
    }
    for item in payload.illustrations:
        illustrations[str(item.illustration_id)] = item

    chunks = {
        str(item.chunk_id): item
        for item in graph.chunks
        if not any(str(page_id) in replaced_page_ids for page_id in item.page_ids)
        and not any(str(region_id) in replaced_region_ids for region_id in item.region_ids)
        and not any(
            str(region_id) in replaced_region_ids
            for region_id in item.footnote_region_ids
        )
        and not any(
            str(illustration_id) in replaced_illustration_ids
            for illustration_id in item.illustration_ids
        )
    }
    for item in payload.chunks:
        chunks[str(item.chunk_id)] = item

    poi_facts = {
        str(item.candidate_id): item
        for item in graph.poi_facts
        if not any(str(page_id) in replaced_page_ids for page_id in item.page_ids)
        and not any(str(region_id) in replaced_region_ids for region_id in item.region_ids)
    }
    for item in payload.poi_facts:
        poi_facts[str(item.candidate_id)] = item

    poi_media_links = {
        str(item.link_id): item
        for item in graph.poi_media_links
        if str(item.page_id) not in replaced_page_ids
        and str(item.illustration_id) not in replaced_illustration_ids
    }
    for item in payload.poi_media_links:
        poi_media_links[str(item.link_id)] = item

    return StagedGraph(
        revision=graph.revision,
        pages=sorted(pages.values(), key=lambda item: item.physical_page_index),
        relations=list(relations.values()),
        illustrations=list(illustrations.values()),
        chunks=list(chunks.values()),
        poi_facts=list(poi_facts.values()),
        poi_media_links=list(poi_media_links.values()),
    )


def validate_graph(graph: StagedGraph, *, expected_page_count: int) -> GraphValidation:
    errors: list[str] = []
    warnings: list[str] = []

    for page in graph.pages:
        if page.source_material != "visual_reviewed" or not (page.source_review_note or "").strip():
            errors.append(f"source completeness unreviewed: page {page.physical_page_index}; visual review and note required (page IDs/native previews are insufficient)")

    page_ids = [str(page.page_id) for page in graph.pages]
    page_indexes = [page.physical_page_index for page in graph.pages]
    if len(page_ids) != len(set(page_ids)):
        errors.append("duplicate page_id")
    if len(page_indexes) != len(set(page_indexes)):
        errors.append("duplicate physical_page_index")
    expected = set(range(expected_page_count))
    present = set(page_indexes)
    missing = sorted(expected - present)
    extra = sorted(present - expected)
    if missing:
        errors.append(f"missing pages: {missing[:30]}")
    if extra:
        errors.append(f"unexpected page indexes: {extra[:30]}")

    page_set = set(page_ids)
    regions: dict[str, StagedRegion] = {}
    region_page: dict[str, str] = {}
    orders: dict[str, set[int]] = defaultdict(set)
    for page in graph.pages:
        for region in page.regions:
            key = str(region.region_id)
            if key in regions:
                errors.append(f"duplicate region_id: {key}")
                continue
            regions[key] = region
            region_page[key] = str(page.page_id)
            if region.reading_order in orders[str(page.page_id)]:
                errors.append(
                    f"duplicate reading_order on page {page.page_id}: "
                    f"{region.reading_order}"
                )
            orders[str(page.page_id)].add(region.reading_order)
            if region.needs_review:
                warnings.append(f"review:region:{key}")
            if (
                region.kind
                in {
                    RegionKind.HEADING,
                    RegionKind.BODY,
                    RegionKind.CAPTION,
                    RegionKind.FOOTNOTE,
                }
                and not region.source_text.strip()
            ):
                errors.append(f"textual region has empty source_text: {key}")

    region_set = set(regions)
    for relation in graph.relations:
        source = str(relation.source_region_id)
        target = str(relation.target_region_id)
        if source not in region_set or target not in region_set:
            errors.append(f"relation references unknown region: {source}->{target}")

    illustration_ids: set[str] = set()
    for item in graph.illustrations:
        iid = str(item.illustration_id)
        if iid in illustration_ids:
            errors.append(f"duplicate illustration_id: {iid}")
        illustration_ids.add(iid)
        if str(item.page_id) not in page_set:
            errors.append(f"illustration references unknown page: {iid}")
        source = regions.get(str(item.source_region_id))
        if source is None:
            errors.append(f"illustration references unknown figure region: {iid}")
        elif source.kind is not RegionKind.FIGURE:
            errors.append(f"illustration source is not a figure region: {iid}")
        for region_id in [*item.caption_region_ids, *item.nearby_region_ids]:
            if str(region_id) not in region_set:
                errors.append(
                    f"illustration {iid} references unknown region {region_id}"
                )
        if not item.caption_region_ids:
            warnings.append(f"illustration_without_caption:{iid}")

    chunk_ids: set[str] = set()
    covered_regions: set[str] = set()
    for chunk in graph.chunks:
        cid = str(chunk.chunk_id)
        if cid in chunk_ids:
            errors.append(f"duplicate chunk_id: {cid}")
        chunk_ids.add(cid)
        chunk_page_set = {str(value) for value in chunk.page_ids}
        for page_id in chunk.page_ids:
            if str(page_id) not in page_set:
                errors.append(f"chunk {cid} references unknown page {page_id}")
        for region_id in chunk.region_ids:
            value = str(region_id)
            if value not in region_set:
                errors.append(f"chunk {cid} references unknown region {region_id}")
            else:
                covered_regions.add(value)
                if region_page[value] not in chunk_page_set:
                    errors.append(
                        f"chunk {cid} omits page for region {region_id}"
                    )
        for region_id in chunk.footnote_region_ids:
            value = str(region_id)
            region = regions.get(value)
            if region is None:
                errors.append(
                    f"chunk {cid} references unknown footnote region {region_id}"
                )
            else:
                covered_regions.add(value)
                if region.kind is not RegionKind.FOOTNOTE:
                    errors.append(
                        f"chunk {cid} footnote_region_id is not a footnote"
                    )
                if region_page[value] not in chunk_page_set:
                    errors.append(
                        f"chunk {cid} omits page for footnote {region_id}"
                    )
        for illustration_id in chunk.illustration_ids:
            if str(illustration_id) not in illustration_ids:
                errors.append(
                    f"chunk {cid} references unknown illustration {illustration_id}"
                )

    poi_candidate_ids: set[str] = set()
    for fact in graph.poi_facts:
        candidate_id = str(fact.candidate_id)
        if candidate_id in poi_candidate_ids:
            errors.append(f"duplicate POI fact candidate_id: {candidate_id}")
        poi_candidate_ids.add(candidate_id)
        if not fact.text.strip():
            errors.append(f"POI fact has empty text: {candidate_id}")
        for page_id in fact.page_ids:
            if str(page_id) not in page_set:
                errors.append(
                    f"POI fact {candidate_id} references unknown page {page_id}"
                )
        for region_id in fact.region_ids:
            value = str(region_id)
            if value not in region_set:
                errors.append(
                    f"POI fact {candidate_id} references unknown region {region_id}"
                )
            elif region_page[value] not in {str(page) for page in fact.page_ids}:
                errors.append(
                    f"POI fact {candidate_id} omits page for region {region_id}"
                )

    illustration_set = {
        str(item.illustration_id)
        for item in graph.illustrations
    }
    link_ids: set[str] = set()
    for link in graph.poi_media_links:
        link_id = str(link.link_id)
        if link_id in link_ids:
            errors.append(f"duplicate POI media link_id: {link_id}")
        link_ids.add(link_id)
        if str(link.page_id) not in page_set:
            errors.append(
                f"POI media link {link_id} references unknown page {link.page_id}"
            )
        if str(link.illustration_id) not in illustration_set:
            errors.append(
                f"POI media link {link_id} references unknown illustration "
                f"{link.illustration_id}"
            )

    if not graph.chunks:
        errors.append("no retrieval chunks staged")

    required_coverage = {
        region_id
        for region_id, region in regions.items()
        if region.kind
        in {
            RegionKind.HEADING,
            RegionKind.BODY,
            RegionKind.CAPTION,
            RegionKind.FOOTNOTE,
            RegionKind.TABLE,
            RegionKind.MARGINALIA,
        }
        and region.source_text.strip()
    }
    uncovered = sorted(required_coverage - covered_regions)
    if uncovered:
        errors.append(f"uncovered textual regions: {uncovered[:40]}")

    return GraphValidation(
        errors=list(dict.fromkeys(errors)),
        warnings=list(dict.fromkeys(warnings)),
    )
