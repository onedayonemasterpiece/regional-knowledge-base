"""Whole saved Story.body vs exact bounded derived narrative vector inputs.

Only the existing image-source compiler, native SQL activation and accepted
operation fixture are exercised. No provider/real model/Dev PG is involved.
"""
import json
from pathlib import Path
from uuid import UUID, uuid5

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_activation import knowledge_bytes
from regional_knowledge.source_stage import canonical, digest
from regional_knowledge.story_contracts import StageStoryCandidateInput
from test_native_source_materializer import PERSON, exact_batch
from test_source_stage import SOURCE, original, seal
from test_source_activation import activation_payload
from test_source_activation_pg import (
    PG, image_pg, stage_all, prepare, install, accepted, stage_command, next_command,
)
from test_kaggle_pg_driver import database
from test_kaggle_apply import pinned
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_native_source_bindings import native_source_handler
from regional_knowledge.source_activation import MATERIALIZE_ROUTE, ACTIVATE_ROUTE
from regional_knowledge.source_stage import STAGE_ROUTE, SEAL_ROUTE
from regional_knowledge.source_stage import SEAL_ROUTE

# 660 individually separated words fit the existing 4000-character Story.body
# limit but exceed the protected 512-token encoder input.
LONG_BODY = " ".join(f"w{n:03d}" for n in range(660))


def authored_batch():
    page = exact_batch(count=1, receipt=original(page_count=1))
    region = page["model_payload"]["pages"][0]["regions"][0]
    excerpt = region["source_text"]
    page["model_payload"]["story_candidates"] = [dict(
        candidate_key="long-body",
        title="One complete synthetic episode",
        summary="This is descriptive metadata, not the saved narrative.",
        proposition="The original contains the synthetic excerpt.",
        evidence_refs=[dict(page_id=page["model_payload"]["pages"][0]["page_id"],
                            region_key=region["region_key"],
                            original_excerpt=excerpt)],
        basic_variant=dict(body=LONG_BODY, audience="general", language="ru",
                           blocks=[dict(start=0, end=len(LONG_BODY),
                                        evidence_indexes=[0])]),
    )]
    StageStoryCandidateInput.model_validate(page["model_payload"]["story_candidates"][0])
    return canonical(page)


def assert_complete_derived_fragments(variant, count_calls):
    assert variant["body"] == LONG_BODY
    assert variant["input_tokens"] <= 512
    fragments = variant["fragments"]
    assert len(fragments) > 1
    assert fragments[0]["start"] == 0
    assert fragments[-1]["end"] == len(LONG_BODY)
    assert all(a["end"] == b["start"] for a, b in zip(fragments, fragments[1:]))
    assert all(0 <= f["start"] < f["end"] <= len(LONG_BODY) for f in fragments)
    assert len({f["fragment_id"] for f in fragments}) == len(fragments)
    assert all(1 <= f["input_tokens"] <= 512 for f in fragments)
    assert all(f["fragment_id"] == str(uuid5(UUID(variant["variant_id"]),
                        f"fragment:{f['start']}:{f['end']}")) for f in fragments)
    assert "".join(LONG_BODY[f["start"]:f["end"]] for f in fragments) == LONG_BODY
    assert all(f["input_tokens"] == len(LONG_BODY[f["start"]:f["end"]].split()) + 2
               for f in fragments)
    assert all(LONG_BODY[f["start"]:f["end"]] in count_calls for f in fragments)


@pytest.mark.asyncio
async def test_long_body_kept_exact_but_each_vector_input_is_bounded():
    raw = authored_batch()
    counts = []

    async def count(text):
        counts.append(text)
        return len(text.split()) + 2

    compiler = NativeSourceCompiler(count=count, current=lambda: True)
    one = await compiler.compile([raw], source_id=SOURCE,
        person_id=PERSON, last_batch_sha256=digest(raw))
    two = await compiler.compile([raw], source_id=SOURCE,
        person_id=PERSON, last_batch_sha256=digest(raw))
    assert knowledge_bytes(one) == knowledge_bytes(two)
    variant = one["semantic"]["stories"][0]["default_variant"]
    assert_complete_derived_fragments(variant, counts[:len(counts)//2])


@PG
@pytest.mark.asyncio
async def test_pg_long_body_exact_readback_and_replay_idempotency(image_pg):
    env = image_pg
    # Existing GH PG17 fixture only. Override the target SQL14 callables with
    # additive SQL18, then bind the real registry to their actual function pins.
    async with env["connection"].transaction():
        await env["connection"].execute(
            (Path(__file__).resolve().parents[1] /
             "sql/kaggle/pending/018_story_embedding_fragments.sql").read_text(), prepare=False)
    handlers = []
    for route, suffix in ((STAGE_ROUTE, "source_stage_v2"),
                          (SEAL_ROUTE, "source_stage_seal_v2"),
                          (MATERIALIZE_ROUTE, "source_materialize_v1"),
                          (ACTIVATE_ROUTE, "source_activate_v1")):
        check = await pinned(env["connection"], "check_" + suffix)
        apply = await pinned(env["connection"], "apply_" + suffix)
        handlers.append(native_source_handler(route,
            check_sha256=check.definition_sha256, apply_sha256=apply.definition_sha256))
    env["registry"] = HandlerRegistry(handlers)
    # Keep the existing accepted page, region, finding and source edition. Only
    # replace the authored body and its derived fragment metadata.
    sample = authored_batch()
    await stage_all(env, [json.loads(sample)])
    # The native compiler defines the exact candidate projection in production.
    async def count(text):
        return len(text.split()) + 2
    resolved = await NativeSourceCompiler(count=count, current=lambda: True).compile(
        [sample], source_id=SOURCE, person_id=PERSON, last_batch_sha256=digest(sample))
    h = await prepare(env, [json.loads(sample)], digest(sample), resolved)
    assert h == digest(knowledge_bytes(resolved))
    # A retry of the same candidate is immutable and does not duplicate data.
    assert await prepare(env, [json.loads(sample)], digest(sample), resolved) == h
    assert (await accepted(env, stage_command(
        seal(1, digest(sample)), env["sequence"]+1, SEAL_ROUTE))).status == "applied"
    variant = resolved["semantic"]["stories"][0]["default_variant"]
    offsets = variant["fragments"]
    count_inputs = len(resolved["passages"]) + len(offsets)
    for ordinal in range(count_inputs):
        assert await install(env, h, ordinal)
        command = next_command(activation_payload([json.loads(sample)], digest(sample)),
            env["sequence"]+1, ordinal=ordinal)
        result = await accepted(env, command)
        assert result.status == "applied"
        duplicate = await accepted(env, command)
        assert duplicate.duplicate and duplicate.outcome == result.outcome
    activate = next_command(activation_payload([json.loads(sample)], digest(sample)),
        env["sequence"]+1)
    first = await accepted(env, activate)
    second = await accepted(env, activate)
    assert first.status == "applied" and second.duplicate
    async with env["connection"].transaction():
        row = await (await env["connection"].execute("""
            SELECT b.body, b.body_sha256, e.body FROM rkb_v4.story_variant_versions b
            JOIN rkb_v4.stories s ON s.story_id=b.story_id
            JOIN rkb_v4.editorial_variant_versions e ON e.variant_id=b.variant_id
            WHERE s.source_id=%s
        """, (SOURCE,))).fetchone()
        assert row[0] == row[2] == LONG_BODY
        assert row[1] == digest(LONG_BODY.encode())
        fragments = await (await env["connection"].execute("""
            SELECT f.fragment_ordinal,f.start_offset,f.end_offset,f.input_tokens,f.input_sha256
            FROM rkb_v4.story_embedding_fragments f
            JOIN rkb_v4.story_variant_versions b ON b.variant_id=f.variant_id AND b.revision=f.revision
            WHERE b.story_id = (SELECT story_id FROM rkb_v4.stories WHERE source_id=%s)
            ORDER BY f.fragment_ordinal
        """, (SOURCE,))).fetchall()
        assert len(fragments) == len(offsets)
        assert "".join(LONG_BODY[row[1]:row[2]] for row in fragments) == LONG_BODY
        assert all(row[3] <= 512 and row[4] == digest(LONG_BODY[row[1]:row[2]].encode())
                   for row in fragments)
