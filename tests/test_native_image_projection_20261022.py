"""Native image lowering on the same checked source-image and SQL015 boundary.

These are synthetic image receipts and existing GH PG17 fixtures only, never
provider bytes, Telegram crop uploads or a live book import.
"""
import json
from uuid import UUID, uuid5

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical, digest
from test_native_source_materializer import PERSON, exact_batch
from test_source_stage import SOURCE, original


def illustrated_batch(*, foreign_reference=False, changed_receipt=False, missing_page=False):
    raw = exact_batch(count=1, receipt=original(page_count=1))
    page = raw["model_payload"]["pages"][0]
    page_id = page["page_id"]
    page["regions"].append(dict(region_key="figure-1", kind="figure",
        reading_order=1, bbox=dict(left=110, top=115, right=320, bottom=365),
        source_text=""))
    page["regions"].append(dict(region_key="caption-1", kind="caption",
        reading_order=2, bbox=dict(left=120, top=380, right=310, bottom=425),
        source_text="A picture from this synthetic original."))
    page["illustrations"] = [dict(illustration_key="picture", source_region_key="figure-1",
        kind="photo", visual_description="Model-observed picture in the synthetic scan.",
        visual_description_provenance="model_observation", visual_description_language="en",
        display_rotation_degrees=90, caption_region_keys=["caption-1"],
        nearby_region_keys=["body"])]
    reference = dict(page_id=page_id, illustration_key="picture")
    raw["model_payload"]["chunks"][0]["illustration_refs"] = [reference]
    raw["model_payload"]["poi_media_links"] = [dict(link_key="picture-place",
        poi_locator=dict(names=["Synthetic historic place"]), illustration_ref=reference,
        relation="depicts")]
    if foreign_reference:
        raw["model_payload"]["chunks"][0]["illustration_refs"][0]["page_id"] = str(
            uuid5(UUID(SOURCE), "foreign-page"))
    if changed_receipt:
        raw["model_payload"]["pages"][0]["source_image_receipt_sha256"] = "f" * 64
    if missing_page:
        raw["model_payload"]["poi_media_links"][0]["illustration_ref"]["page_id"] = str(
            uuid5(UUID(SOURCE), "missing-image-page"))
    return canonical(raw)


async def compile_image(raw):
    counted = []
    async def count(text):
        counted.append(text)
        return len(text.split()) + 2
    compiler = NativeSourceCompiler(count=count, current=lambda: True)
    projection, media = await compiler.compile([raw], source_id=SOURCE, person_id=PERSON,
        last_batch_sha256=digest(raw), include_illustrations=True)
    return projection, media, counted


@pytest.mark.asyncio
async def test_native_image_projection_retains_crop_source_and_exact_links():
    raw = illustrated_batch()
    projection, media, counted = await compile_image(raw)
    assert media["codec"] == "rkb.source-illustrations.v1"
    assert media["source_id"] == SOURCE and media["source_revision"] == 1
    assert len(media["illustrations"]) == len(media["passage_links"]) == len(media["poi_media_links"]) == 1
    image = media["illustrations"][0]
    binding = image["crop_binding"]
    issued = projection["image_manifest"]["pages"][0]
    assert binding == dict(source_id=SOURCE, source_revision=1,
        illustration_id=image["illustration_id"], page_id=issued["page_id"],
        source_region_id=image["source_region_id"], bbox=image["bbox"],
        display_rotation_degrees=90, source_image_receipt_sha256=issued["source_image_receipt_sha256"],
        source_raster_sha256=issued["image_sha256"],
        original_sha256=projection["original"]["sha256"], rendition="preview")
    assert image["caption_text"] == "A picture from this synthetic original."
    assert image["illustration_id"] == str(uuid5(UUID(issued["page_id"]), "illustration:picture"))
    assert media["passage_links"][0]["passage_id"] == projection["passages"][0]["passage_id"]
    assert media["passage_links"][0]["illustration_ids"] == [image["illustration_id"]]
    assert media["poi_media_links"][0]["illustration_id"] == image["illustration_id"]
    assert media["poi_media_links"][0]["page_id"] == issued["page_id"]
    assert counted == [projection["passages"][0]["body"]]
    assert "illustrations" not in projection and "source_media" not in projection


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["foreign_reference", "changed_receipt", "missing_page"])
async def test_native_image_projection_rejects_changed_or_cross_page_source(case):
    raw = illustrated_batch(**{case: True})
    with pytest.raises((ValueError, PermissionError)):
        await compile_image(raw)
