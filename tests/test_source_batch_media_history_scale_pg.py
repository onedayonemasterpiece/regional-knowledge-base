"""Actual packet prepare with accepted history, using disposable CI PG17 only."""
import asyncio
import copy
from pathlib import Path
from time import perf_counter
import warnings

import psycopg
import pytest
from psycopg.types.json import Jsonb

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_batch_context import load_current
from regional_knowledge.source_stage import ZERO_HASH, canonical, digest
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_native_source_materializer import exact_batch
from test_source_activation_pg import image_pg_sql18, accepted, stage_command, DB, WRITER, TIME
from test_source_batch_pg import native, packet
from test_source_stage import SOURCE
from test_source_activation import PERSON

ROOT = Path(__file__).resolve().parents[1]


@PG
@pytest.mark.asyncio
async def test_empty_media_links_prepare_does_not_reconstruct_90_packet_history(image_pg_sql18):
    """Same owned data and exact prepare input before/after SQL033; no real BGE."""
    async with asyncio.timeout(240):
        env = image_pg_sql18
        conn = env['connection']
        head = ZERO_HASH
        for index in range(90):
            doc = exact_batch(index, head, start=index, count=1)
            model = doc['model_payload']
            template = model['chunks'][0]
            model['chunks'] = [dict(copy.deepcopy(template), chunk_key=f'packet-{index}-chunk-{n}')
                               for n in range(20)]
            raw = canonical(doc)
            assert (await accepted(env, stage_command(doc, env['sequence'] + 1))).status == 'applied'
            head = digest(raw)

        prepare, _ = native(env)
        text = packet(env, doc, 'text')
        await prepare(text)
        assert (await accepted(env, text)).status == 'applied'
        command = packet(env, doc, 'semantic')
        async with conn.transaction():
            view = await load_current(conn, raw, SOURCE, text_only=False)

        async def count(text):
            return len(text.split()) + 2

        knowledge, media = await NativeSourceCompiler(count=count, current=lambda: True).compile_current(
            raw, view, source_id=SOURCE, person_id=PERSON,
        )
        assert len(knowledge['passages']) == 20
        assert len(knowledge['image_manifest']['pages']) == 1
        assert media['passage_links'] == media['poi_media_links'] == []

        original = (ROOT / 'sql/kaggle/026_source_batch_activation.sql').read_text()
        start = original.index('CREATE FUNCTION rkb_v4.check_source_batch_media_v1(')
        end = original.index('END $$;', start) + len('END $$;')
        original = original[start:end].replace('CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION', 1)
        migration = (ROOT / 'sql/kaggle/033_source_batch_media_link_lookup.sql').read_text()
        timings = {}
        async with conn.transaction(force_rollback=True):
            # Diagnostic time budget belongs only to this disposable fixture.
            await conn.execute(original, prepare=False)
            calls = (
                ('semantic', '''SELECT rkb_v4.check_source_batch_semantic_v1(
                    %s,%s::jsonb,%s::jsonb,%s)''',
                 (canonical(dict({k: v for k, v in knowledge.items()
                                  if k not in ('image_manifest','source_graph','public_rights')},
                                 codec='rkb.source-activation-semantic.v1')),
                  Jsonb({'source_id': SOURCE}), Jsonb({'absent_source': 'true'}), TIME)),
                ('manifest', 'SELECT rkb_v4.check_source_batch_manifest_v1(%s::jsonb,%s::uuid)',
                 (Jsonb(knowledge), SOURCE)),
                ('graph', 'SELECT rkb_v4.check_source_batch_graph_v1(%s::jsonb,%s::uuid,90)',
                 (Jsonb(knowledge), SOURCE)),
                ('media', 'SELECT rkb_v4.check_source_batch_media_v1(%s::jsonb,%s::jsonb,%s::uuid)',
                 (Jsonb(knowledge), Jsonb(media), SOURCE)),
            )
            for name, sql, params in calls:
                started = perf_counter()
                try:
                    async with conn.transaction(force_rollback=True):
                        await conn.execute("SET LOCAL statement_timeout='25s'")
                        result = (await (await conn.execute(sql, params)).fetchone())[0]
                        assert result is True, name
                    timings[name] = round(1000 * (perf_counter() - started), 2)
                except psycopg.errors.QueryCanceled:
                    timings[name] = 'timeout_25000ms'

            sql = 'SELECT rkb_v4.prepare_source_batch_v1(%s,%s,%s::jsonb,%s,%s)'
            params = (command.encode(), canonical(knowledge), Jsonb(media), DB, WRITER)
            started = perf_counter()
            try:
                async with conn.transaction(force_rollback=True):
                    await conn.execute("SET LOCAL statement_timeout='25s'")
                    result = (await (await conn.execute(sql, params)).fetchone())[0]
                    assert result == digest(canonical(knowledge))
                timings['prepare_before'] = round(1000 * (perf_counter() - started), 2)
            except psycopg.errors.QueryCanceled:
                timings['prepare_before'] = 'timeout_25000ms'

            await conn.execute(migration, prepare=False)
            started = perf_counter()
            async with conn.transaction(force_rollback=True):
                # The actual worker's existing five-second deadline is unchanged.
                await conn.execute("SET LOCAL statement_timeout='5s'")
                result = (await (await conn.execute(sql, params)).fetchone())[0]
                assert result == digest(canonical(knowledge))
            timings['prepare_after'] = round(1000 * (perf_counter() - started), 2)
            assert timings['prepare_after'] < 5000

        warnings.warn(f'RKB_SQL033_HISTORY_SCALE packets=90 history_chunks=1800 '
                      f'current_passages=20 current_pages=1 timings_ms={timings}', UserWarning)
