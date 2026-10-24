"""CI-only SQL028/032 comparison on identical synthetic input and storage forms.

Fixture construction and candidate SQL are pinned to PR394 1a1c383. This test
never stages source commands, changes live data or executes a local PG/model.
"""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from statistics import median
from time import perf_counter
from uuid import UUID, uuid5
import warnings

import pytest
from psycopg.types.json import Jsonb

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical, digest
from test_source_batch_compiler import passage_link_packet
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18
from test_native_source_materializer import SOURCE, PERSON


def ident(label, ordinal):
    return str(uuid5(UUID(SOURCE), f"pg17-validator:{label}:{ordinal}"))


async def large_projection():
    original = passage_link_packet()
    raw = canonical(original)

    async def count(text):
        return min(512, max(1, len(text.split()) + 2))

    doc, _media = await NativeSourceCompiler(count=count, current=lambda: True).compile_batch(
        [raw], batch_index=0, source_id=SOURCE, person_id=PERSON,
        last_batch_sha256=digest(raw),
    )
    passage_template = doc["passages"][0]
    region_template = doc["semantic"]["regions"][0]
    finding_template = doc["semantic"]["findings"][0]
    assert finding_template["evidence"] and doc["research"]["state"] == "pending"

    passages = []
    for ordinal in range(1600):
        item = copy.deepcopy(passage_template)
        item.update(passage_id=ident("passage", ordinal), ordinal=ordinal)
        passages.append(item)

    regions = []
    for ordinal in range(700):
        item = copy.deepcopy(region_template)
        item.update(region_id=ident("region", ordinal))
        regions.append(item)

    findings, pending, poi_facts, finding_entities = [], [], [], []
    for ordinal in range(800):
        finding = copy.deepcopy(finding_template)
        fid = ident("finding", ordinal)
        key = f"synthetic-candidate-{ordinal:04d}"
        region = regions[ordinal % len(regions)]
        passage = passages[ordinal % len(passages)]
        finding.update(assertion_id=fid, candidate_key=key, entity_keys=[])
        finding["evidence"] = [dict(
            region_id=region["region_id"], passage_id=passage["passage_id"],
        )]
        finding["event_dates"] = []
        finding["observations"] = []
        findings.append(finding)
        pending.append(dict(candidate_key=key, finding_ids=[fid], reason="needs_context"))
        poi_facts.append(dict(evidence=[dict(
            region_id=region["region_id"], page_id=region["page_id"],
        )]))
        finding_entities.append(dict(assertion_id=fid, entity_keys=[], entity_ids=[]))

    doc["passages"] = passages
    doc["semantic"].update(regions=regions, findings=findings, stories=[], pending=pending)
    # SQL026 passes the stripped semantic payload to its pure SQL028 helper.
    # Manifest reconciliation is a distinct owner-DB path; do not fake stage rows.
    doc.pop("image_manifest", None)
    doc.pop("source_graph", None)
    semantic = dict(doc, codec="rkb.source-activation-semantic.v1")
    base = dict(semantic, codec="rkb.source-activation-base.v1")
    base.pop("semantic")
    graph = dict(doc, source_graph=dict(
        source_id=SOURCE, owner_person_id=PERSON,
        entities=[], references=[], relations=[],
        poi_facts=poi_facts, finding_entities=finding_entities,
    ))
    return graph, canonical(semantic), canonical(base), findings[-1]["evidence"][0]


@PG
@pytest.mark.asyncio
async def test_graph_same_input_before_after_with_manifest_and_storage_forms(image_pg_sql18):
    async with asyncio.timeout(240):
        graph, _semantic, _base, _evidence = await large_projection()
        with_manifest = copy.deepcopy(graph)
        passages = graph['passages']
        regions = graph['semantic']['regions']
        selections = []
        for ordinal, passage in enumerate(passages):
            region = regions[ordinal % len(regions)]
            selections.append(dict(passage_id=passage['passage_id'],
                page_ids=[region['page_id']], region_ids=[region['region_id']],
                footnote_region_ids=[]))
        # These are exactly the fields SQL028's evidence helper consumes. This is
        # not an assertion that the independent owner/stage manifest path ran.
        with_manifest['image_manifest'] = dict(passages=selections)
        by_passage = {item['passage_id']: item for item in selections}
        for finding in with_manifest['semantic']['findings']:
            for proof in finding['evidence']:
                selected = by_passage[proof.get('passage_id', proof.get('chunk_id'))]
                assert proof['region_id'] in selected['region_ids']

        root = Path(__file__).resolve().parents[1]
        original = (root/'sql/kaggle/028_source_passage_evidence.sql').read_text()
        marker = 'CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_graph_v1('
        start = original.index(marker)
        end = original.index('\nEND $$;', start) + len('\nEND $$;')
        old_sql = original[start:end]
        new_sql = (root/'tests/fixtures/sql032_graph_candidate_1a1c383.sql').read_text()
        assert marker in new_sql and 'pg_get_functiondef' not in new_sql
        assert hashlib.sha256(new_sql.encode()).hexdigest()=='a5dfec6fab2346c392a7fbc3736bf91d7f9b144c17891effe237f9a517909846'
        conn = image_pg_sql18['connection']
        rows = []
        async with conn.transaction():
            # Diagnostic bound belongs only to this disposable CI transaction.
            await conn.execute("SET LOCAL statement_timeout='25000ms'")
            await conn.execute('CREATE TEMP TABLE graph_storage_profile '
                '(kind text PRIMARY KEY,doc jsonb,payload bytea) ON COMMIT DROP')
            for kind, doc in [('no_manifest', graph), ('with_manifest', with_manifest)]:
                payload=json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
                await conn.execute('INSERT INTO pg_temp.graph_storage_profile VALUES(%s,%s,%s)',
                    (kind, Jsonb(doc), payload))
            forms = [
                ('stored_jsonb', 'x.doc'),
                ('materialized_jsonb', "x.doc||'{}'::jsonb"),
                ('sql026_bytea_decode', "convert_from(x.payload,'UTF8')::jsonb"),
            ]
            for revision, sql in [('028', old_sql), ('032', new_sql)]:
                await conn.execute(sql, prepare=False)
                for kind in ['no_manifest', 'with_manifest']:
                    for form, expression in forms:
                        timings=[]
                        statement=('SELECT rkb_v4.check_source_batch_graph_v1('
                            +expression+',%s::uuid,89) FROM pg_temp.graph_storage_profile x '
                            'WHERE x.kind=%s')
                        for _ in range(2):
                            before=perf_counter()
                            value=(await(await conn.execute(statement,(SOURCE,kind))).fetchone())[0]
                            timings.append(round((perf_counter()-before)*1000,2))
                            assert value is True,(revision,kind,form)
                        rows.append(dict(revision=revision,kind=kind,form=form,
                            median_ms=round(median(timings),2)))
        warnings.warn('RKB_GRAPH_STORAGE_DIAGNOSTIC synthetic_disposable_pg17 '
            +json.dumps(dict(old_sql_sha256=hashlib.sha256(old_sql.encode()).hexdigest(),
                new_sql_sha256=hashlib.sha256(new_sql.encode()).hexdigest(),
                passages=len(passages),regions=len(regions),findings=len(graph['semantic']['findings']),
                measurements=rows),sort_keys=True),UserWarning,stacklevel=1)

