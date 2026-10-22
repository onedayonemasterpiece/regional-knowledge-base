"""Pending PR277 Story-fragment dense retrieval through actual PG17 and fused search.

All document/model vectors are deterministic **synthetic** unit vectors. This
proves source/active scoping, candidate identity, saved body and branch wiring;
it does NOT measure actual BGE relevance, HNSW recall or live p95.
"""
import json
from pathlib import Path
from uuid import UUID

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_native_source_bindings import native_source_handler
from regional_knowledge.kaggle_protocol import QueryFilters
from regional_knowledge.kaggle_search import fused_search
from regional_knowledge.kaggle_source_materializer import (
    NativeSourceCompiler, materializer_fingerprint,
)
from regional_knowledge.source_activation import (
    ACTIVATE_ROUTE, MATERIALIZE_ROUTE, knowledge_bytes,
)
from regional_knowledge.source_stage import STAGE_ROUTE, SEAL_ROUTE, ZERO_HASH, canonical, digest
from test_kaggle_apply import PG, pinned
from test_kaggle_pg_driver import database
from test_source_activation import MATERIALIZER, activation_payload, synthetic_knowledge
from test_source_activation_pg import (
    DB, WRITER, image_pg, accepted, install, next_command, prepare,
    stage_all, stage_command,
)
from test_source_stage import SOURCE, INGESTION, original, seal
from test_source_story_fragments_20261022 import LONG_BODY, authored_batch
from test_native_source_materializer import exact_batch

ROOT = Path(__file__).resolve().parents[1]
READ_SQL = ROOT / 'sql/kaggle/pending/story_fragment_dense_read_20261022.sql'
FRAGMENT_SQL = ROOT / 'sql/kaggle/018_story_embedding_fragments.sql'
GOOD = '[' + ','.join(['1'] + ['0'] * 1023) + ']'
BAD = '[' + ','.join(['0', '1'] + ['0'] * 1022) + ']'
GOOD_VALUES = [1.0] + [0.0] * 1023
NEAR_GOOD = '[' + ','.join(['0.98', '0.198997487'] + ['0'] * 1022) + ']'
OTHER_SOURCE = '00000000-0000-0000-0000-000000000099'


async def _prepare_reader(env):
    connection = env['connection']
    # Enforce the actual published SQL017 → ordinary018 source stack, not the
    # old draft pending file or a fabricated migration replacement.
    assert (ROOT / 'sql/kaggle/017_schema_version.sql').is_file()
    ordinary_sql18 = FRAGMENT_SQL.read_text()
    assert 'CREATE VIEW rkb_v4.story_embedding_fragments' in ordinary_sql18
    async with connection.transaction():
        # GH PG17 image fixture installs through SQL014. Apply ordinary
        # SQL018 explicitly, followed by the unnumbered own read candidate.
        await connection.execute(ordinary_sql18, prepare=False)
        await connection.execute(READ_SQL.read_text(), prepare=False)
    handlers = []
    for route, suffix in ((STAGE_ROUTE, 'source_stage_v2'),
                          (SEAL_ROUTE, 'source_stage_seal_v2'),
                          (MATERIALIZE_ROUTE, 'source_materialize_v1'),
                          (ACTIVATE_ROUTE, 'source_activate_v1')):
        check = await pinned(connection, 'check_' + suffix)
        apply = await pinned(connection, 'apply_' + suffix)
        handlers.append(native_source_handler(route,
            check_sha256=check.definition_sha256,
            apply_sha256=apply.definition_sha256))
    env['registry'] = HandlerRegistry(handlers)
    async with connection.transaction():
        proc = await (await connection.execute("""
            SELECT p.prosecdef,p.provolatile,
                EXISTS(SELECT 1 FROM pg_catalog.aclexplode(
                    coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl
                    WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE')
            FROM pg_catalog.pg_proc p WHERE p.oid=
                'rkb_v4.editorial_dense_candidates_v1(public.vector,integer,uuid[])'::regprocedure
        """)).fetchone()
        assert proc == (True, 's', False)
        assert await (await connection.execute("""
            SELECT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(
                coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl
                WHERE acl.grantee=0 AND acl.privilege_type='SELECT')
            FROM pg_catalog.pg_class c WHERE c.oid=
                'rkb_v4.source_activation_vectors'::regclass
        """)).fetchone() == (False,)


async def _seed_story(env, *, fragments, crowded_passages=False):
    connection = env['connection']
    if fragments:
        if crowded_passages:
            # Genuine 40-passage accepted source, not extra foreign/unsaved
            # vector rows: the top 32 vector neighbours can be passages.
            receipt = original(page_count=40)
            candidate = json.loads(authored_batch())['model_payload']['story_candidates'][0]
            chain = []
            previous = ZERO_HASH
            for index in range(5):
                batch = exact_batch(index, previous, start=8*index,
                                    count=8, receipt=receipt)
                if index == 0:
                    batch['model_payload']['story_candidates'] = [candidate]
                raw = canonical(batch)
                chain.append(json.loads(raw))
                previous = digest(raw)
            head = previous
        else:
            raw = authored_batch()
            chain = [json.loads(raw)]
            head = digest(raw)

        async def count(text):
            return len(text.split()) + 2

        document = await NativeSourceCompiler(
            count=count, current=lambda: True).compile(
                [canonical(batch) for batch in chain], source_id=SOURCE,
                person_id=__import__('test_native_source_materializer').PERSON,
                last_batch_sha256=head)
        vector_count = len(document['passages']) + len(
            document['semantic']['stories'][0]['default_variant']['fragments'])
        materializer = materializer_fingerprint()
    else:
        chain, head, document = synthetic_knowledge(1, story=True)
        vector_count = len(document['passages']) + 1
        materializer = MATERIALIZER
    await stage_all(env, chain)
    if fragments:
        async with connection.transaction():
            row = await (await connection.execute("""
                SELECT rkb_v4.prepare_source_activation_v1(
                    %s,%s,%s,%s,%s,%s,%s,%s)
            """, (SOURCE, INGESTION, len(chain), head, materializer,
                  knowledge_bytes(document), DB, WRITER))).fetchone()
        assert row == (digest(knowledge_bytes(document)),)
        knowledge_hash = row[0]
    else:
        knowledge_hash = await prepare(env, chain, head, document)
    sealed = stage_command(seal(len(chain), head), env['sequence'] + 1, SEAL_ROUTE)
    assert (await accepted(env, sealed)).status == 'applied'
    payload = {**activation_payload(chain, head), 'materializer_sha256': materializer}
    for ordinal in range(vector_count):
        # The ONLY close semantic vector in a long Story is in its final
        # fragment. Its representative first-fragment embedding is orthogonal.
        vector = ((GOOD if ordinal < len(document['passages']) else
                   NEAR_GOOD if ordinal == vector_count - 1 else BAD)
                  if crowded_passages else
                  GOOD if ordinal == vector_count - 1 else BAD)
        await install(env, knowledge_hash, ordinal, vector=vector)
        command = next_command(payload, env['sequence'] + 1, ordinal=ordinal)
        receipt = await accepted(env, command)
        assert receipt.status == 'applied', receipt.status
    receipt = await accepted(env, next_command(payload, env['sequence'] + 1))
    assert receipt.status == 'applied', receipt.status
    assert env['connection'].info.transaction_status.name == 'IDLE'
    return document['semantic']['stories'][0]['default_variant']['body']


async def _synthetic_query(_):
    return GOOD_VALUES


@PG
@pytest.mark.asyncio
async def test_saved_second_fragment_only_dense_hit_no_lexical_term_source_fence(image_pg):
    env = image_pg
    await _prepare_reader(env)
    body = await _seed_story(env, fragments=True)
    assert body == LONG_BODY
    connection = env['connection']
    query = 'periphrastic nebula secret not present in source'
    async with connection.transaction():
        lexical = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_lexical_candidates_v1(%s,200,%s::uuid[])
        """, (query, [SOURCE]))).fetchall()
        assert lexical == []
        rows = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,100,%s::uuid[])
        """, (GOOD, [SOURCE]))).fetchall()
        assert len(rows) == 1, rows  # one Story, not one hit per fragment
        assert rows[0][1] > .999, rows  # impossible with first-only orthogonal
        story_id = rows[0][0]
        parts = await (await connection.execute("""
            SELECT f.fragment_ordinal,f.embedding OPERATOR(public.<=>)
                %s::public.vector AS distance
            FROM rkb_v4.story_embedding_fragments f WHERE f.story_id=%s
            ORDER BY f.fragment_ordinal
        """, (GOOD, story_id))).fetchall()
        assert len(parts) >= 2 and parts[0][1] > .99 and parts[-1][1] < .001
        excluded = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,5,%s::uuid[])
        """, (GOOD, [OTHER_SOURCE]))).fetchall()
        assert excluded == []
    # Actual production Python branch keeps the existing 2-second total
    # budget; no new API, encoder or result DTO. Both candidate branches are
    # called by the real PG-backed fused search and exact Story body hydrates.
    found = await fused_search(connection, query, encode=_synthetic_query,
        top_k=3, timeout_seconds=2.0,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)))
    assert len(found.evidence) == 1
    assert found.evidence[0].record['body'] == LONG_BODY

    async with connection.transaction():
        # Accepted/withdrawn source is excluded by native candidate and
        # hydration, even though immutable vectors remain in storage.
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s', (SOURCE,))
        assert await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,5,%s::uuid[])
        """, (GOOD, [SOURCE]))).fetchall() == []
    withdrawn = await fused_search(connection, query, encode=_synthetic_query,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)),
        timeout_seconds=2.0)
    assert withdrawn.evidence == ()


@PG
@pytest.mark.asyncio
async def test_short_legacy_story_without_fragment_rows_keeps_dense_search(image_pg):
    env = image_pg
    await _prepare_reader(env)
    body = await _seed_story(env, fragments=False)
    connection = env['connection']
    async with connection.transaction():
        assert await (await connection.execute("""
            SELECT count(*) FROM rkb_v4.story_embedding_fragments
        """)).fetchone() == (0,)
        found = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,5,%s::uuid[])
        """, (GOOD, [SOURCE]))).fetchall()
        assert len(found) == 1 and found[0][1] > .999
    query = 'no lexical matches for this short story'
    result = await fused_search(connection, query, encode=_synthetic_query,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)),
        timeout_seconds=2.0)
    assert len(result.evidence) == 1
    assert result.evidence[0].record['body'] == body


@PG
@pytest.mark.asyncio
async def test_dense_story_fragment_survives_more_than_32_closer_passage_vectors(image_pg):
    """RED on the old SQL: 40 accepted near passages bury fragment at rank41.

    n=1 limits the native nearest probe to 32; the *eligible Story fragments*
    must be selected before that LIMIT. Synthetic direction vectors prove the
    routing bug, not production BGE relevance/ANN recall/p95.
    """
    env = image_pg
    await _prepare_reader(env)
    body = await _seed_story(env, fragments=True, crowded_passages=True)
    assert body == LONG_BODY
    connection = env['connection']
    query = 'periphrastic nebula phrase not in any saved source'
    async with connection.transaction():
        counts = await (await connection.execute("""
            SELECT (SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s),
                   (SELECT count(*) FROM rkb_v4.story_embedding_fragments WHERE source_id=%s)
        """, (SOURCE, SOURCE))).fetchone()
        assert counts[0] > 32 and counts[1] > 1
        lexical = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_lexical_candidates_v1(%s,1,%s::uuid[])
        """, (query, [SOURCE]))).fetchall()
        assert lexical == []
        slots = await (await connection.execute("""
            SELECT count(*) FROM rkb_v4.source_activation_vectors a
            JOIN rkb_v4.passage_search p
              ON p.source_id=a.source_id AND p.passage_id=a.input_id
            WHERE a.source_id=%s
              AND a.embedding OPERATOR(public.<=>) %s::public.vector <
                  %s::double precision
        """, (SOURCE, GOOD, 0.01))).fetchone()
        assert slots[0] > 32
        dense = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,1,%s::uuid[])
        """, (GOOD, [SOURCE]))).fetchall()
        # The existing first/default Story vector is orthogonal; score >0.97
        # proves the non-first fragment survived the pre-ranking passage crowd.
        assert len(dense) == 1 and dense[0][1] > 0.97, dense
        other = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,1,%s::uuid[])
        """, (GOOD, [OTHER_SOURCE]))).fetchall()
        assert other == []

    result = await fused_search(connection, query, encode=_synthetic_query,
        top_k=1, candidate_budget=1, timeout_seconds=2.0,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)))
    assert len(result.evidence) == 1
    assert result.evidence[0].record['body'] == LONG_BODY

    async with connection.transaction():
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s', (SOURCE,))
        denied = await (await connection.execute("""
            SELECT * FROM rkb_v4.editorial_dense_candidates_v1(
                %s::public.vector,1,%s::uuid[])
        """, (GOOD, [SOURCE]))).fetchall()
        assert denied == []
    withdrawn = await fused_search(connection, query, encode=_synthetic_query,
        top_k=1, candidate_budget=1, timeout_seconds=2.0,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)))
    assert withdrawn.evidence == ()
