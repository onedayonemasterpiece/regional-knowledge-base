"""Independent accepted revision2 / rechunk changed-boundary on *real* PG17.

This exercises actual schema12 -> native schema14 -> accepted source revision1
-> additive SQL015/016/017 -> versioned stage/seal/materialize/activate revision2,
with actual HandlerRegistry/ApplyReceipt/RecoveryJournal/NativeSourceCompiler.
Only the pre-existing GitHub Actions RKB_KAGGLE_TEST_DSN guarded disposable
PG17 fixture is permitted. Synthetic one-page model/tokenizer/BGE input is
clearly not an original PDF/DjVu or a Dataset Saved/live provider receipt.
"""
from __future__ import annotations

import base64
from dataclasses import replace
import json
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry, ApplyConflict, StaleWriter
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceMigration, native_source_upgrade_handler, resolved_native_source_upgrade,
)
from regional_knowledge.kaggle_native_revision2_upgrade import (
    ARTIFACTS, REVISION2_REVISIONS, NativeRevision2Migration, NativeRevision2UpgradePins,
    revision2_upgrade_handler, resolved_revision2_upgrade, transition_sql, sha,
)
from regional_knowledge.kaggle_native_source_bindings import (
    native_source_handler, revision2_native_handler,
    resolved_revision2_native_command, validate_revision2_native_command,
)
from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler, materializer_fingerprint
from regional_knowledge.source_activation import (
    REVISION2_ACTIVATE_ROUTE, REVISION2_MATERIALIZE_ROUTE,
    REVISION2_ACTIVATION_CODEC, REVISION2_MATERIALIZE_CODEC, knowledge_bytes,
)
from regional_knowledge.source_stage import (
    REVISION2_STAGE_ROUTE, REVISION2_SEAL_ROUTE,
    REVISION2_BATCH_CODEC, REVISION2_SEAL_CODEC, canonical, digest, ZERO_HASH,
)
from test_kaggle_apply import PG, pinned
from test_kaggle_pg_driver import database  # guarded existing GH PG17 fixture
from test_native_source_upgrade_pg import (
    schema12, preflight as preflight14, apply as native_apply,
    DB, WRITER,
)
from test_source_activation import synthetic_knowledge, activation_payload, MATERIALIZER, PERSON
from test_source_activation_pg import VECTOR, stage_command, next_command
from test_source_stage import SOURCE, INGESTION, REQUEST, seal

ROOT = Path(__file__).resolve().parents[1]
TIME = "2026-10-22T13:00:00Z"


async def _accepted(env, command, *, writer=WRITER):
    """Real RecoveryJournal seal + real PgFunction/HandlerRegistry apply."""
    if writer == WRITER:
        receipt = await native_apply(env, command)
    else:
        from regional_knowledge.kaggle_apply import apply_accepted
        found = await env["journal"].locate_accepted(command)
        if found is None:
            found = await env["journal"].seal([command],
                predecessor=await env["journal"].head(),writer_generation=WRITER)
        receipt = await apply_accepted(env["connection"],journal=env["journal"],
            seal=found,command=command,database_generation=DB,
            writer_generation=writer,registry=env["registry"])
    env["sequence"] = max(env["sequence"], command.sequence)
    return receipt


async def _add_handlers(env, revision2=False):
    from regional_knowledge.source_stage import STAGE_ROUTE, SEAL_ROUTE
    from regional_knowledge.source_activation import MATERIALIZE_ROUTE, ACTIVATE_ROUTE
    old = [
        (STAGE_ROUTE, "source_stage_v2"), (SEAL_ROUTE, "source_stage_seal_v2"),
        (MATERIALIZE_ROUTE, "source_materialize_v1"),
        (ACTIVATE_ROUTE, "source_activate_v1"),
    ]
    new = [
        (REVISION2_STAGE_ROUTE, "source_stage_revision2_v1"),
        (REVISION2_SEAL_ROUTE, "source_stage_seal_revision2_v1"),
        (REVISION2_MATERIALIZE_ROUTE, "source_materialize_revision2_v1"),
        (REVISION2_ACTIVATE_ROUTE, "source_activate_revision2_v1"),
    ]
    handlers = list(env["registry"].handlers.values())
    for route, suffix in new if revision2 else old:
        c = await pinned(env["connection"], "check_" + suffix)
        a = await pinned(env["connection"], "apply_" + suffix)
        handlers.append(
            revision2_native_handler(route,check_sha256=c.definition_sha256,
                                     apply_sha256=a.definition_sha256)
            if revision2 else native_source_handler(
                route,check_sha256=c.definition_sha256,apply_sha256=a.definition_sha256))
    env["registry"] = HandlerRegistry(handlers)


async def _r14(env):
    pins = await preflight14(env["connection"])
    handler = native_source_upgrade_handler(pins)
    env["registry"] = HandlerRegistry([*env["registry"].handlers.values(),handler])
    command = resolved_native_source_upgrade(
        pins=pins,sequence=env["sequence"]+1,operation_key="rev2-predecessor-sql14",
        request_id=REQUEST,person_id=PERSON,logical_time=TIME)
    await NativeSourceMigration(sql_root=ROOT/"sql/kaggle",registry=env["registry"],
                                database_generation=DB,pins=pins).prepare(
                                    env["connection"],command,handler)
    receipt = await _accepted(env,command)
    assert receipt.status == "applied"
    await _add_handlers(env)


async def _v1(env):
    """Fully accept a single original source using the actual schema14 handlers."""
    chain, head, doc = synthetic_knowledge(1,story=True)
    for model in chain:
        assert (await _accepted(env,stage_command(
            model,env["sequence"]+1))).status == "applied"
    connection=env["connection"]
    async with connection.transaction():
        prepared = await (await connection.execute(
            "SELECT rkb_v4.prepare_source_activation_v1(%s,%s,%s,%s,%s,%s,%s,%s)",
            (SOURCE,INGESTION,1,head,MATERIALIZER,knowledge_bytes(doc),
             DB,WRITER))).fetchone()
        assert prepared == (digest(knowledge_bytes(doc)),)
    assert (await _accepted(env,stage_command(
        seal(1,head),env["sequence"]+1,route="source.stage.seal.v1"))).status=="applied"
    for ordinal in range(2):
        async with connection.transaction():
            result = await (await connection.execute(
                "SELECT rkb_v4.install_source_activation_vector_v1(%s,%s,%s,%s::public.vector,%s,%s)",
                (SOURCE,prepared[0],ordinal,VECTOR,DB,WRITER))).fetchone()
            assert result and len(result[0])==64
        assert (await _accepted(env,next_command(
            activation_payload(chain,head),env["sequence"]+1,ordinal=ordinal)
        )).status == "applied"
    accepted = next_command(activation_payload(chain,head),env["sequence"]+1)
    assert (await _accepted(env,accepted)).status == "applied"
    async with connection.transaction():
        rev = await (await connection.execute(
            "SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s",
            (SOURCE,))).fetchone()
        assert rev==(1,)
        old_story=await (await connection.execute(
            "SELECT v.body FROM rkb_v4.story_variant_versions v JOIN rkb_v4.stories s USING(story_id) "
            "WHERE s.source_id=%s",(SOURCE,))).fetchone()
        assert old_story==(doc["semantic"]["stories"][0]["default_variant"]["body"],)
        source_rows=await (await connection.execute(
            "SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s AND revision=1",
            (SOURCE,))).fetchone()
        assert source_rows==(1,)
        old_origin=await (await connection.execute(
            "SELECT command_sha256 FROM rkb_v4.source_image_origins WHERE source_id=%s",
            (SOURCE,))).fetchone()
        assert old_origin == (digest(accepted.encode()),)
    return chain, head, accepted, old_story[0]


async def _revisions17(env):
    """Real SQL015/016/017, ordered real transition and actual versioned registry."""
    connection=env["connection"]
    raw=[(ROOT/"sql/kaggle"/name).read_bytes() for name in ARTIFACTS]
    pins=NativeRevision2UpgradePins(*(sha(value) for value in raw),"1"*64,"2"*64)
    with pytest.raises(psycopg.Rollback):
        async with connection.transaction():
            for artifact in raw:
                await connection.execute(artifact.decode(),prepare=False)
            await connection.execute(transition_sql(pins),prepare=False)
            check=await pinned(connection,"check_native_revision2_upgrade_v1")
            apply=await pinned(connection,"apply_native_revision2_upgrade_v1")
            raise psycopg.Rollback()
    pins=replace(pins,transition_check_sha256=check.definition_sha256,
                 transition_apply_sha256=apply.definition_sha256)
    handler=revision2_upgrade_handler(pins)
    env["registry"]=HandlerRegistry([*env["registry"].handlers.values(),handler])
    command=resolved_revision2_upgrade(pins=pins,sequence=env["sequence"]+1,
        operation_key="rev2-schema17-transition",request_id=REQUEST,
        person_id=PERSON,logical_time=TIME)
    native=NativeRevision2Migration(sql_root=ROOT/"sql/kaggle",
        registry=env["registry"],database_generation=DB,pins=pins)
    await native.prepare(connection,command,handler)
    receipt=await _accepted(env,command)
    assert receipt.status=="applied"
    assert (await _accepted(env,command)).duplicate  # migration lost ACK
    async with connection.transaction():
        state=await (await connection.execute(
            "SELECT schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state"
        )).fetchone()
        assert state==REVISION2_REVISIONS.wire()
    await _add_handlers(env,revision2=True)


async def _check_stage(env, command):
    connection=env["connection"]
    async with connection.transaction():
        row=await (await connection.execute(
            "SELECT rkb_v4.check_source_stage_revision2_v1(%s,%s::jsonb,%s::jsonb,%s)",
            (command.payload,Jsonb(dict(command.ids)),
             Jsonb(dict(command.expected_revisions)),command.logical_time))).fetchone()
    return row==(True,)


@PG
@pytest.mark.asyncio
async def test_real_postgres_same_source_revision2_preserves_revision1_and_rejects_stale_boundaries(schema12):
    """Actual ordered PG/catalog, Command/Handler/Journal, and compiler v2.

    Dataset remote Saved is intentionally *not* claimed from PG APPLIED.
    """
    env=schema12
    roles=[]
    connection=env["connection"]
    try:
        for name in ("rkb_reader","rkb_writer"):
            async with connection.transaction():
                if await (await connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=%s",(name,))).fetchone():
                    raise AssertionError("temporary fixture role collides with existing role")
                await connection.execute("CREATE ROLE "+name+" NOLOGIN")
                roles.append(name)
        await _r14(env)
        chain,_,original_activation,body1=await _v1(env)
        before_seq=env["sequence"]
        await _revisions17(env)
        assert env["sequence"]==before_seq+1

        v2_ingestion=str(uuid4())
        doc={**chain[0],"codec":REVISION2_BATCH_CODEC,
            "source_revision":2,"prior_revision":1,
            "ingestion_id":v2_ingestion,"original_ingestion_id":INGESTION,
            "predecessor_command_sha256":digest(original_activation.encode())}
        command=resolved_revision2_native_command(
            route=REVISION2_STAGE_ROUTE,source_id=SOURCE,payload=doc,
            sequence=env["sequence"]+1,operation_key="accepted-rev2-stage",
            request_id=REQUEST,person_id=PERSON,logical_time=TIME)
        assert validate_revision2_native_command(command)==doc

        # Actual source SQL rejects a stale predecessor, wrong owner, edition,
        # original ingestion or a forged page/issued-image receipt.
        assert await _check_stage(env,command)
        stale=replace(command,expected_revisions={
            **dict(command.expected_revisions),"predecessor_command_sha256":"0"*64})
        assert not await _check_stage(env,stale)
        wrong_actor=replace(command,expected_revisions={
            **dict(command.expected_revisions),"owner_person_id":str(uuid4())})
        assert not await _check_stage(env,wrong_actor)
        for extra in (
            {"predecessor_command_sha256":"f"*64},
            {"original_ingestion_id":str(uuid4())},
            {"source":{**doc["source"],"edition_id":str(uuid4())}},
            {"issued_images_b64":[base64.b64encode(b'{"different":"receipt"}').decode()]},
        ):
            payload={**doc,**extra}
            forged=replace(command,payload=canonical(payload))
            assert not await _check_stage(env,forged)
        assert (await _accepted(env,command)).status=="applied"
        replay=await _accepted(env,command)
        assert replay.duplicate and replay.outcome
        different=replace(command,payload=canonical({**doc,"research":{
            **doc["research"],"state":"complete","continuation":{}}}))
        with pytest.raises(ApplyConflict):
            await _accepted(env,different)  # same operation key / different bytes

        stage_head=digest(canonical(doc))
        seal_payload=dict(codec=REVISION2_SEAL_CODEC,source_revision=2,prior_revision=1,
            ingestion_id=v2_ingestion,batch_count=1,last_batch_sha256=stage_head,
            predecessor_command_sha256=digest(original_activation.encode()))
        sealing=resolved_revision2_native_command(
            route=REVISION2_SEAL_ROUTE,source_id=SOURCE,payload=seal_payload,
            sequence=env["sequence"]+1,operation_key="accepted-rev2-seal",
            request_id=REQUEST,person_id=PERSON,logical_time=TIME)
        assert (await _accepted(env,sealing)).status=="applied"
        async with connection.transaction():
            assert await (await connection.execute(
                "SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s",
                (SOURCE,))).fetchone()==(1,)
            assert await (await connection.execute(
                "SELECT count(*) FROM rkb_v4.source_revision2_origins"
            )).fetchone()==(0,)

        async def count_inputs(texts):
            return [max(4,len(item.split())) for item in texts]
        compiler=NativeSourceCompiler(count=count_inputs,current=lambda:True,
                                     source_revision=2)
        resolved=await compiler.compile([canonical(doc)],source_id=SOURCE,
                                        person_id=PERSON,last_batch_sha256=stage_head)
        assert resolved["revision"]==2
        assert resolved["original"]["sha256"]=="a"*64
        knowledge=knowledge_bytes(resolved)
        fingerprint=materializer_fingerprint()
        async with connection.transaction():
            prepared=await (await connection.execute(
                "SELECT rkb_v4.prepare_source_revision2_activation_v1(%s,%s,%s,%s,%s,%s,%s,%s)",
                (SOURCE,v2_ingestion,1,stage_head,fingerprint,knowledge,DB,WRITER)
            )).fetchone()
            assert prepared==(digest(knowledge),)
            count=await (await connection.execute(
                "SELECT total_inputs FROM rkb_v4.source_revision2_candidates WHERE source_id=%s",
                (SOURCE,))).fetchone()
            assert count and count[0]>=1
        v2_projection=dict(source_revision=2,prior_revision=1,ingestion_id=v2_ingestion,
            batch_count=1,last_batch_sha256=stage_head,materializer_sha256=fingerprint,
            predecessor_command_sha256=digest(original_activation.encode()))
        activation=resolved_revision2_native_command(
            route=REVISION2_ACTIVATE_ROUTE,source_id=SOURCE,payload={
                **v2_projection,"codec":REVISION2_ACTIVATION_CODEC},
            sequence=env["sequence"]+1,operation_key="accepted-rev2-activate",
            request_id=REQUEST,person_id=PERSON,logical_time=TIME)
        pending=await _accepted(env,activation)
        assert pending.status=="rejected_precondition"
        async with connection.transaction():
            assert await (await connection.execute(
                "SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s",
                (SOURCE,))).fetchone()==(1,)
        # The earlier rejected key cannot later be resurrected as accepted.
        for ordinal in range(count[0]):
            async with connection.transaction():
                vector=await (await connection.execute(
                    "SELECT rkb_v4.install_source_revision2_vector_v1(%s,%s,%s,%s::public.vector,%s,%s)",
                    (SOURCE,prepared[0],ordinal,VECTOR,DB,WRITER))).fetchone()
                assert vector and len(vector[0])==64
            materialized=resolved_revision2_native_command(
                route=REVISION2_MATERIALIZE_ROUTE,source_id=SOURCE,payload={
                    **v2_projection,"codec":REVISION2_MATERIALIZE_CODEC,"ordinal":ordinal},
                sequence=env["sequence"]+1,operation_key=f"accepted-rev2-vector-{ordinal}",
                request_id=REQUEST,person_id=PERSON,logical_time=TIME)
            receipt=await _accepted(env,materialized)
            assert receipt.status=="applied"
            assert (await _accepted(env,materialized)).duplicate
        # A newly issued exact activation key, not the rejected operation,
        # switches PG active revision while retaining historical version 1.
        activated=resolved_revision2_native_command(
            route=REVISION2_ACTIVATE_ROUTE,source_id=SOURCE,payload={
                **v2_projection,"codec":REVISION2_ACTIVATION_CODEC},
            sequence=env["sequence"]+1,operation_key="accepted-rev2-activate-after-complete",
            request_id=REQUEST,person_id=PERSON,logical_time=TIME)
        active=await _accepted(env,activated)
        assert active.status=="applied" and not active.duplicate
        assert (await _accepted(env,activated)).duplicate
        assert active.status!="Saved"  # PG applied != Dataset-covered outcome.
        async with connection.transaction():
            assert await (await connection.execute(
                "SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s",
                (SOURCE,))).fetchone()==(2,)
            assert await (await connection.execute(
                "SELECT revision FROM rkb_v4.revisions WHERE source_id=%s ORDER BY revision",
                (SOURCE,))).fetchall()==[(1,),(2,)]
            assert await (await connection.execute(
                "SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s AND revision=1",
                (SOURCE,))).fetchone()==(1,)
            assert await (await connection.execute(
                "SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s AND revision=2",
                (SOURCE,))).fetchone()==(1,)
            prior_story=await (await connection.execute(
                "SELECT v.body FROM rkb_v4.story_variant_versions v JOIN rkb_v4.stories s USING(story_id) "
                "WHERE s.source_id=%s",(SOURCE,))).fetchone()
            assert prior_story==(body1,)
            origin=await (await connection.execute(
                "SELECT source_id,revision,ingestion_id,origin_ingestion_id,"
                "predecessor_command_sha256 FROM rkb_v4.source_revision2_origins WHERE source_id=%s",
                (SOURCE,))).fetchone()
            assert origin==(SOURCE,2,v2_ingestion,INGESTION,
                            digest(original_activation.encode()))
        # One PK source ID across revisions; no second archived original.
        assert env["sequence"]>=before_seq+3
    finally:
        # Bound the NOLOGIN roles to this existing isolated GH PG service.
        # Original fixture still owns schema and drops it after this test.
        await connection.rollback()
        if roles:
            async with connection.transaction():
                await connection.execute("DROP OWNED BY "+",".join(roles))
                for name in reversed(roles):
                    await connection.execute("DROP ROLE "+name)
