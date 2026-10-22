"""Native image lowering on the same checked source-image and SQL015 boundary.

These are synthetic image receipts and existing GH PG17 fixtures only, never
provider bytes, Telegram crop uploads or a live book import.
"""
import copy
import json
from pathlib import Path
from uuid import UUID, uuid5

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical, digest
from test_native_source_materializer import PERSON, exact_batch
from test_source_stage import SOURCE, original
from test_source_activation_pg import image_pg_sql18, image_pg
from test_kaggle_pg_driver import database


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
    # The printed caption is model-reviewed source text, so retain it in the
    # same exact passage instead of dropping textual coverage during activation.
    raw["model_payload"]["chunks"][0]["region_refs"].append(
        dict(page_id=page_id, region_key="caption-1"))
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


@pytest.mark.asyncio
async def test_pg_sql015_native_prepares_exact_illustration_candidate(image_pg_sql18):
    """SQL015 checks the actual accepted page+crop and durable preparation.

    Activation's SQL015 revision/handler rollout is owned by Shkiper. This
    scoped source regression never fabricates a version upgrade or route grant.
    """
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from psycopg.types.json import Jsonb
    from regional_knowledge.kaggle_source_materializer import (
        NativeSourceMaterializer, materializer_fingerprint)
    from test_source_activation_pg import (
        stage_all, stage_command, DB, WRITER, SEAL_ROUTE, seal)

    env = image_pg_sql18
    connection = env["connection"]
    sql015 = (Path(__file__).resolve().parents[1] /
              "sql/kaggle/015_source_illustrations.sql").read_text()
    async with connection.transaction():
        await connection.execute(sql015, prepare=False)

    raw = illustrated_batch()
    await stage_all(env, [json.loads(raw)])
    forwarded = []

    async def count(text):
        return len(text.split()) + 2

    async def encode(text):
        forwarded.append(text)
        return [1.0] + [0.0] * 1023

    @asynccontextmanager
    async def acquire():
        yield connection

    native = NativeSourceMaterializer(count=count, encode=encode, current=lambda: True)
    owned = SimpleNamespace(database_generation=DB, writer_generation=WRITER)
    command = stage_command(seal(1, digest(raw)), env["sequence"] + 1, SEAL_ROUTE)
    await env["journal"].seal([command],
        predecessor=await env["journal"].head(), writer_generation=WRITER)
    await native(command, acquire=acquire, database=owned)

    async with connection.transaction():
        row = await (await connection.execute("""
            SELECT c.knowledge,m.media,m.knowledge_sha256,m.media_sha256
            FROM rkb_v4.source_activation_candidates c
            JOIN rkb_v4.source_illustration_candidates m USING(source_id)
            WHERE c.source_id=%s
        """, (SOURCE,))).fetchone()
        assert row is not None
        knowledge, media, knowledge_hash, media_hash = row
        assert knowledge_hash == digest(canonical(knowledge))
        assert media_hash == digest(canonical(media))
        assert (await (await connection.execute(
            "SELECT rkb_v4.check_source_illustrations_v1(%s::jsonb,%s::jsonb,%s::uuid)",
            (Jsonb(knowledge), Jsonb(media), SOURCE))).fetchone()) == (True,)

        # The public crop metadata cannot be rebound to another source,
        # nonexistent page, different original or substituted issued raster.
        for mismatch in ("source_id", "page_id", "original_sha256", "source_raster_sha256"):
            changed = copy.deepcopy(media)
            item = changed["illustrations"][0]
            crop = item["crop_binding"]
            if mismatch == "source_id":
                changed["source_id"] = str(uuid5(UUID(SOURCE), "foreign-source"))
            elif mismatch == "page_id":
                item["page_id"] = str(uuid5(UUID(SOURCE), "missing-page"))
            else:
                crop[mismatch] = "f" * 64
            assert (await (await connection.execute(
                "SELECT rkb_v4.check_source_illustrations_v1(%s::jsonb,%s::jsonb,%s::uuid)",
                (Jsonb(knowledge), Jsonb(changed), SOURCE))).fetchone()) == (False,)
        assert await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s",
            (SOURCE,))).fetchone() == (0,)
    assert forwarded == []  # Preparing metadata did not run the encoder/media provider.


@pytest.mark.asyncio
async def test_existing_trusted_registry_refuses_image_activation_without_binding(image_pg_sql18):
    """Prove the remaining SQL015 handler wiring gap through real accepted ops.

    Do not replace the installed text-only activation handler with a synthetic
    image-aware handler just to make a fixture pass. The existing SQL015
    deferred receipt must abort this transaction without a half-Saved source.
    """
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from regional_knowledge.kaggle_apply import ApplyExecutionError
    from regional_knowledge.kaggle_source_materializer import (
        NativeSourceMaterializer, materializer_fingerprint,
    )
    from test_source_activation import activation_payload
    from test_source_activation_pg import (
        stage_all, stage_command, next_command, accepted, DB, WRITER,
        SEAL_ROUTE, seal,
    )

    env = image_pg_sql18
    connection = env["connection"]
    async with connection.transaction():
        await connection.execute(
            (Path(__file__).resolve().parents[1] /
             "sql/kaggle/015_source_illustrations.sql").read_text(), prepare=False)

    raw = illustrated_batch()
    await stage_all(env, [json.loads(raw)])
    forwards = []

    async def count(text):
        return len(text.split()) + 2

    async def encode(text):
        forwards.append(text)
        return [1.0] + [0.0] * 1023

    @asynccontextmanager
    async def acquire():
        yield connection

    native = NativeSourceMaterializer(count=count, encode=encode, current=lambda: True)
    owner = SimpleNamespace(database_generation=DB, writer_generation=WRITER)
    sealing = stage_command(seal(1, digest(raw)), env["sequence"] + 1, SEAL_ROUTE)
    await env["journal"].seal([sealing],
        predecessor=await env["journal"].head(), writer_generation=WRITER)
    await native(sealing, acquire=acquire, database=owner)
    assert (await accepted(env, sealing)).status == "applied"

    payload = {**activation_payload([json.loads(raw)], digest(raw)),
               "materializer_sha256": materializer_fingerprint()}
    materialize = next_command(payload, env["sequence"] + 1, ordinal=0)
    await env["journal"].seal([materialize],
        predecessor=await env["journal"].head(), writer_generation=WRITER)
    await native(materialize, acquire=acquire, database=owner)
    assert len(forwards) == 1
    assert (await accepted(env, materialize)).status == "applied"

    activation = next_command(payload, env["sequence"] + 1)
    installed = env["registry"].resolve(activation)
    assert installed.check.name == "check_source_activate_v1"
    assert installed.apply.name == "apply_source_activate_v1"
    # SQL015 requires the image-aware exact handler, but this is the actual
    # trusted text handler. Its deferred image receipt rejects at PG commit.
    with pytest.raises(ApplyExecutionError):
        await accepted(env, activation)

    async with connection.transaction():
        assert await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s",
            (SOURCE,))).fetchone() == (0,)
        assert await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.source_illustrations WHERE source_id=%s",
            (SOURCE,))).fetchone() == (0,)
        assert await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.source_illustration_candidates WHERE source_id=%s",
            (SOURCE,))).fetchone() == (1,)
        assert await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.source_activation_vectors WHERE source_id=%s",
            (SOURCE,))).fetchone() == (1,)
