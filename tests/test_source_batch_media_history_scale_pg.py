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


SQL036 = ROOT / 'sql/kaggle/036_source_batch_media_scoped_lookup.sql'


def test_sql036_preserves_every_existing_media_validation_check():
    """The new migration changes lookup scope, not the accepted media contract."""
    prior = (ROOT / 'sql/kaggle/033_source_batch_media_link_lookup.sql').read_text()
    prior = prior[prior.index('CREATE OR REPLACE FUNCTION'):]
    expected = prior.replace(
        'staged:=rkb_v4.source_illustration_stage_links_v1(source);',
        """staged:=rkb_v4.source_batch_media_stage_links_v1(source,
   ARRAY(SELECT value->>'chunk_key' FROM jsonb_array_elements(media->'passage_links')),
   ARRAY(SELECT value->>'link_key' FROM jsonb_array_elements(media->'poi_media_links')));""",
    )
    migration = SQL036.read_text()
    actual = migration[migration.index('CREATE OR REPLACE FUNCTION'):]
    assert actual.strip() == expected.strip()


def _illustrate_packet(doc):
    """Reuse the existing public synthetic figure fixture on this issued page."""
    import json
    from test_native_image_projection_20261022 import illustrated_batch
    sample = json.loads(illustrated_batch())['model_payload']
    model = doc['model_payload']
    page = model['pages'][0]
    page['regions'].extend(copy.deepcopy(sample['pages'][0]['regions'][1:]))
    page['illustrations'] = copy.deepcopy(sample['pages'][0]['illustrations'])
    model['chunks'][0]['region_refs'].append(
        dict(page_id=page['page_id'], region_key='caption-1'))
    model['chunks'][0]['illustration_refs'] = [
        dict(page_id=page['page_id'], illustration_key='picture')]
    model['poi_media_links'] = copy.deepcopy(sample['poi_media_links'])
    model['poi_media_links'][0]['illustration_ref']['page_id'] = page['page_id']


@PG
@pytest.mark.asyncio
async def test_nonempty_media_prepare_is_scoped_with_113_accepted_batches(image_pg_sql18):
    """Same actual prepare, before/after SQL036; neither model nor live PG used."""
    async with asyncio.timeout(240):
        env = image_pg_sql18
        conn = env['connection']
        head = ZERO_HASH
        for index in range(113):
            doc = exact_batch(index, head, start=index, count=1)
            model = doc['model_payload']
            if index < 112:
                template = model['chunks'][0]
                model['chunks'] = [
                    dict(copy.deepcopy(template), chunk_key=f'history-{index}-{n}')
                    for n in range(20)]
            else:
                _illustrate_packet(doc)
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

        async def count(body):
            return len(body.split()) + 2

        knowledge, media = await NativeSourceCompiler(count=count, current=lambda: True).compile_current(
            raw, view, source_id=SOURCE, person_id=PERSON)
        assert len(knowledge['passages']) == len(media['illustrations']) == 1
        assert len(media['passage_links']) == len(media['poi_media_links']) == 1
        sql = 'SELECT rkb_v4.prepare_source_batch_v1(%s,%s,%s::jsonb,%s,%s)'
        params = (command.encode(), canonical(knowledge), Jsonb(media), DB, WRITER)
        timings = {}
        async with conn.transaction(force_rollback=True):
            await conn.execute((ROOT / 'sql/kaggle/033_source_batch_media_link_lookup.sql').read_text(),
                               prepare=False)
            started = perf_counter()
            try:
                async with conn.transaction(force_rollback=True):
                    await conn.execute("SET LOCAL statement_timeout='25s'")
                    assert (await (await conn.execute(sql, params)).fetchone())[0] == digest(canonical(knowledge))
                timings['prepare_before'] = round(1000 * (perf_counter() - started), 2)
            except psycopg.errors.QueryCanceled:
                timings['prepare_before'] = 'timeout_25000ms'

            # The numbered runner already installed the helper; replace only the
            # validator for this rollback-only before/after comparison.
            replacement = SQL036.read_text()
            replacement = replacement[replacement.index('CREATE OR REPLACE FUNCTION'):]
            await conn.execute(replacement, prepare=False)
            for mode in ('passage', 'poi', 'both'):
                submitted = copy.deepcopy(media)
                if mode == 'passage':
                    submitted['poi_media_links'] = []
                if mode == 'poi':
                    submitted['passage_links'] = []
                started = perf_counter()
                async with conn.transaction(force_rollback=True):
                    await conn.execute("SET LOCAL statement_timeout='5s'")
                    assert (await (await conn.execute(sql, (
                        command.encode(), canonical(knowledge), Jsonb(submitted), DB, WRITER,
                    ))).fetchone())[0] == digest(canonical(knowledge))
                timings[mode] = round(1000 * (perf_counter() - started), 2)
                assert timings[mode] < 5000

            variants = []
            for field, key, value in (
                ('passage_links', 'chunk_key', 'missing'),
                ('passage_links', 'passage_id', '00000000-0000-0000-0000-000000000098'),
                ('passage_links', 'illustration_ids', ['00000000-0000-0000-0000-000000000098']),
                ('poi_media_links', 'link_key', 'missing'),
                ('poi_media_links', 'poi_locator', {'names': ['Different place']}),
                ('poi_media_links', 'relation', 'map_of'),
                ('poi_media_links', 'page_id', '00000000-0000-0000-0000-000000000098'),
            ):
                bad = copy.deepcopy(media)
                bad[field][0][key] = value
                variants.append(bad)
            for field in ('passage_links', 'poi_media_links'):
                bad = copy.deepcopy(media)
                bad[field].append(copy.deepcopy(bad[field][0]))
                variants.append(bad)
            for bad in variants:
                async with conn.transaction(force_rollback=True):
                    await conn.execute("SET LOCAL statement_timeout='5s'")
                    assert (await (await conn.execute(
                        'SELECT rkb_v4.check_source_batch_media_v1(%s::jsonb,%s::jsonb,%s::uuid)',
                        (Jsonb(knowledge), Jsonb(bad), SOURCE),
                    )).fetchone()) == (False,)
        warnings.warn('RKB_SQL036_HISTORY_SCALE packets=113 history_chunks=2240 '
                      f'current_passages=1 current_illustrations=1 timings_ms={timings}', UserWarning)


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['region_refs', 'footnote_refs', 'span_refs', 'illustration_refs'])
async def test_scoped_links_preserve_exact_page_correction_history(image_pg_sql18, field):
    """Check each dependency independently, identical reissue and A -> B -> A."""
    env = image_pg_sql18
    conn = env['connection']
    first = exact_batch(count=2)
    pages = first['model_payload']['pages']
    referenced_page = pages[1]['page_id']
    chunk = copy.deepcopy(first['model_payload']['chunks'][0])
    ref = dict(page_id=referenced_page, region_key='body')
    if field == 'span_refs':
        ref.update(start=0, end=9,
                   source_text_sha256=digest(pages[1]['regions'][0]['source_text'].encode()))
    if field == 'illustration_refs':
        ref = dict(page_id=referenced_page, illustration_key='picture')
    chunk.setdefault(field, []).append(ref)
    link = dict(link_key='page-link', poi_locator={'names': ['Synthetic place']},
                illustration_ref=dict(page_id=referenced_page, illustration_key='picture'),
                relation='depicts')
    unrelated_chunk = copy.deepcopy(first['model_payload']['chunks'][0])
    unrelated_chunk['chunk_key'] = 'unrequested-chunk'
    unrelated_link = copy.deepcopy(link)
    unrelated_link['link_key'] = 'unrequested-link'
    unrelated_link['illustration_ref']['page_id'] = pages[0]['page_id']
    first['model_payload']['chunks'] = [chunk, unrelated_chunk]
    first['model_payload']['poi_media_links'] = [link, unrelated_link]
    wanted_chunks = [chunk['chunk_key'], 'missing']
    wanted_media = [link['link_key'], 'missing']
    latest = None

    async def stage(doc):
        nonlocal latest
        doc = copy.deepcopy(doc)
        doc['batch_index'] = 0 if latest is None else latest['batch_index'] + 1
        doc['previous_batch_sha256'] = ZERO_HASH if latest is None else digest(canonical(latest))
        assert (await accepted(env, stage_command(doc, env['sequence'] + 1))).status == 'applied'
        latest = doc

    async def checked(present):
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout='5s'")
            legacy = (await (await conn.execute(
                'SELECT rkb_v4.source_illustration_stage_links_v1(%s)', (SOURCE,),
            )).fetchone())[0]
            scoped = (await (await conn.execute(
                'SELECT rkb_v4.source_batch_media_stage_links_v1(%s,%s::text[],%s::text[])',
                (SOURCE, wanted_chunks, wanted_media),
            )).fetchone())[0]
        assert 'unrequested-chunk' in legacy['chunks']
        assert 'unrequested-link' in legacy['poi_media_links']
        assert scoped == {
            'chunks': {k: v for k, v in legacy['chunks'].items() if k in wanted_chunks},
            'poi_media_links': {k: v for k, v in legacy['poi_media_links'].items() if k in wanted_media},
        }
        assert bool(scoped['chunks']) is present
        assert bool(scoped['poi_media_links']) is present
        return scoped

    await stage(first)
    await checked(True)
    repeated = copy.deepcopy(first)
    repeated['model_payload'].update(chunks=[], poi_media_links=[])
    await stage(repeated)  # Exact page reissue is not a correction.
    await checked(True)
    replacement = copy.deepcopy(first)
    replacement['model_payload']['chunks'][0]['title'] = 'Explicit replacement'
    await stage(replacement)
    assert (await checked(True))['chunks'][chunk['chunk_key']]['title'] == 'Explicit replacement'
    changed = copy.deepcopy(repeated)
    changed['model_payload']['pages'][1]['source_review_note'] = 'Explicit changed page B.'
    await stage(changed)
    await checked(False)
    await stage(repeated)  # Final A equality must not resurrect a removed link.
    await checked(False)
    changed['model_payload'].update(chunks=[chunk], poi_media_links=[link])
    await stage(changed)  # Invalidation happens before same-batch reinsertion.
    await checked(True)

    async with conn.transaction():
        foreign = (await (await conn.execute(
            'SELECT rkb_v4.source_batch_media_stage_links_v1(%s,%s::text[],%s::text[])',
            ('00000000-0000-0000-0000-000000000098', wanted_chunks, wanted_media),
        )).fetchone())[0]
        assert foreign == {'chunks': {}, 'poi_media_links': {}}
        definition = (await (await conn.execute("""
            SELECT p.prosecdef,p.provolatile,p.proconfig,
              EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
                     WHERE a.grantee=0 AND a.privilege_type='EXECUTE')
            FROM pg_proc p WHERE p.oid=
              'rkb_v4.source_batch_media_stage_links_v1(uuid,text[],text[])'::regprocedure
        """)).fetchone())
        assert definition == (False, 's', ['search_path=pg_catalog'], False)
