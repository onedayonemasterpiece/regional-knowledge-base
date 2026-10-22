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
    PG, image_pg, stage_all, accepted, stage_command, next_command,
)
from test_kaggle_pg_driver import database
from test_kaggle_apply import pinned
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_native_source_bindings import native_source_handler
from regional_knowledge.source_activation import MATERIALIZE_ROUTE, ACTIVATE_ROUTE
from regional_knowledge.source_stage import STAGE_ROUTE, SEAL_ROUTE

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
async def test_pg_native_materializer_last_fragment_ordinal_full_body_and_replay(image_pg):
    """Exercise the real native preapply entrypoint before every accepted command.

    SQL-only install() can hide a mismatch between the generated legacy
    candidate.total_inputs and the new fragment-aware SQL018 input ordinals.
    NativeSourceMaterializer must admit even the final fragment, without
    weakening the accepted-command journal or its database/writer fences.
    """
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from regional_knowledge.kaggle_source_materializer import (
        NativeSourceMaterializer, materializer_fingerprint,
    )
    from test_source_activation_pg import DB, WRITER

    env = image_pg
    # Existing GH PG17 fixture only. Add uninstalled SQL018 explicitly and
    # pin the actual function definitions into the existing real registry.
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

    sample = authored_batch()
    await stage_all(env, [json.loads(sample)])
    counted, forwarded = [], []

    async def count(text):
        counted.append(text)
        return len(text.split()) + 2

    async def encode(text):
        forwarded.append(text)
        return [1.0] + [0.0] * 1023

    @asynccontextmanager
    async def acquire():
        yield env["connection"]

    native = NativeSourceMaterializer(count=count, encode=encode, current=lambda: True)
    owner = SimpleNamespace(database_generation=DB, writer_generation=WRITER)

    async def preapply(command):
        # Same actual accepted seal identity and PG handler path used by
        # the existing native worker test, not a fixture.install shortcut.
        seen = await env["journal"].locate_accepted(command)
        if seen is None:
            await env["journal"].seal([command],
                predecessor=await env["journal"].head(), writer_generation=WRITER)
        await native(command, acquire=acquire, database=owner)

    sealing = stage_command(seal(1, digest(sample)), env["sequence"]+1, SEAL_ROUTE)
    await preapply(sealing)
    async with env["connection"].transaction():
        row = await (await env["connection"].execute("""
            SELECT knowledge,knowledge_sha256,total_inputs,
                   rkb_v4.source_activation_total_inputs_v2(knowledge)
            FROM rkb_v4.source_activation_candidates WHERE source_id=%s
        """, (SOURCE,))).fetchone()
    assert row is not None
    knowledge, knowledge_hash, legacy_total, native_total = row
    assert knowledge_hash == digest(knowledge_bytes(knowledge))
    variant = knowledge["semantic"]["stories"][0]["default_variant"]
    assert_complete_derived_fragments(variant, counted)
    offsets = variant["fragments"]
    assert legacy_total == len(knowledge["passages"]) + 1
    assert native_total == len(knowledge["passages"]) + len(offsets)
    assert native_total > legacy_total
    assert (await accepted(env, sealing)).status == "applied"

    payload = {**activation_payload([json.loads(sample)], digest(sample)),
               "materializer_sha256": materializer_fingerprint()}
    assert (await accepted(env, next_command(payload, env["sequence"]+1))).status == "rejected_precondition"

    for ordinal in range(native_total):
        command = next_command(payload, env["sequence"]+1, ordinal=ordinal)
        before = len(forwarded)
        await preapply(command)
        assert len(forwarded) == before + 1
        async with env["connection"].transaction():
            expected = await (await env["connection"].execute("""
                SELECT input_id,input_sha256,body,installed
                FROM rkb_v4.read_source_activation_input_v1(%s,%s,%s)
            """, (SOURCE, knowledge_hash, ordinal))).fetchone()
        assert expected is not None and expected[3] is True
        assert forwarded[-1] == expected[2]
        assert digest(expected[2].encode()) == expected[1]
        assert len(expected[2].split()) + 2 <= 512
        # A lost private vector install ACK cannot duplicate encoder work.
        await preapply(command)
        assert len(forwarded) == before + 1
        saved = await accepted(env, command)
        assert saved.status == "applied"
        duplicate = await accepted(env, command)
        assert duplicate.duplicate and duplicate.outcome == saved.outcome
        await preapply(command)
        assert len(forwarded) == before + 1

    assert len(forwarded) == native_total
    assert forwarded[-1] == LONG_BODY[offsets[-1]["start"]:offsets[-1]["end"]]
    assert len(forwarded) > legacy_total
    async with env["connection"].transaction():
        assert await (await env["connection"].execute(
            "SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s",
            (SOURCE,))).fetchone() == (0,)

    activate = next_command(payload, env["sequence"]+1)
    first = await accepted(env, activate)
    second = await accepted(env, activate)
    assert first.status == "applied" and second.duplicate and second.outcome == first.outcome
    async with env["connection"].transaction():
        row = await (await env["connection"].execute("""
            SELECT b.body,b.body_sha256,e.body FROM rkb_v4.story_variant_versions b
            JOIN rkb_v4.stories s ON s.story_id=b.story_id
            JOIN rkb_v4.editorial_variant_versions e ON e.variant_id=b.variant_id
            WHERE s.source_id=%s
        """, (SOURCE,))).fetchone()
        assert row[0] == row[2] == LONG_BODY
        assert row[1] == digest(LONG_BODY.encode())
        fragments = await (await env["connection"].execute("""
            SELECT f.fragment_ordinal,f.start_offset,f.end_offset,
                   f.input_tokens,f.input_sha256
            FROM rkb_v4.story_embedding_fragments f
            JOIN rkb_v4.story_variant_versions b
              ON b.variant_id=f.variant_id AND b.revision=f.revision
            WHERE b.story_id=(SELECT story_id FROM rkb_v4.stories WHERE source_id=%s)
            ORDER BY f.fragment_ordinal
        """, (SOURCE,))).fetchall()
    assert len(fragments) == len(offsets)
    assert "".join(LONG_BODY[f[1]:f[2]] for f in fragments) == LONG_BODY
    assert all(f[0] == i and f[1] == offsets[i]["start"]
               and f[2] == offsets[i]["end"] for i, f in enumerate(fragments))
    assert all(f[3] <= 512 and f[4] == digest(LONG_BODY[f[1]:f[2]].encode())
               for f in fragments)


@PG
@pytest.mark.asyncio
async def test_native_source_requires_sql018_even_for_short_body(image_pg):
    """The resident materializer never selects an obsolete SQL014 ordinal path."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from psycopg.errors import UndefinedFunction
    from regional_knowledge.kaggle_source_materializer import NativeSourceMaterializer
    from test_source_activation_pg import DB, WRITER

    env = image_pg  # Explicit SQL014-only fixture: SQL018 is not installed.
    sample = json.loads(authored_batch())
    await stage_all(env, [sample])
    tokenized, encoded = [], []

    async def count(value):
        tokenized.append(value)
        return len(value.split()) + 2

    async def encode(value):
        encoded.append(value)
        return [1.0] + [0.0] * 1023

    @asynccontextmanager
    async def acquire():
        yield env["connection"]

    native = NativeSourceMaterializer(count=count, encode=encode, current=lambda: True)
    command = stage_command(seal(1, digest(canonical(sample))),
                            env["sequence"] + 1, SEAL_ROUTE)
    await env["journal"].seal([command], predecessor=await env["journal"].head(),
                              writer_generation=WRITER)
    with pytest.raises(UndefinedFunction):
        await native(command, acquire=acquire,
                     database=SimpleNamespace(database_generation=DB,
                                              writer_generation=WRITER))
    assert tokenized == encoded == []
    async with env["connection"].transaction():
        installed = await (await env["connection"].execute(
            "SELECT to_regprocedure('rkb_v4.source_activation_total_inputs_v2(jsonb)')"
        )).fetchone()
        assert installed == (None,)
        assert await (await env["connection"].execute(
            "SELECT count(*) FROM rkb_v4.source_activation_candidates"
        )).fetchone() == (0,)
