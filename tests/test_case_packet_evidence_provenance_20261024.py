"""PG17 regression: Case evidence on a later native source packet.

Synthetic source/Case only. No imported user books or live Case mutations.
"""
from uuid import UUID

import pytest

from regional_knowledge.disagreement_contracts import EvidenceAdd
from regional_knowledge.kaggle_domain_bindings import resolved_current_case_command
from regional_knowledge.kaggle_native_source_bindings import new_native_command
from regional_knowledge.source_stage import canonical, digest
from test_disagreement_native import CASE, OBJECT, create
from test_kaggle_apply import PG
from test_native_source_materializer import exact_batch
from test_source_activation_pg import image_pg_sql18, accepted, stage_command, TIME
from test_source_batch_pg import native, packet
from test_source_stage import SOURCE, original
from test_source_activation import PERSON


@PG
@pytest.mark.asyncio
async def test_late_packet_case_evidence_uses_all_accepted_manifest_pages(image_pg_sql18):
    env = image_pg_sql18
    receipt = original(page_count=2)
    first = exact_batch(count=1, receipt=receipt)
    later = exact_batch(1, digest(canonical(first)), start=1, count=1, receipt=receipt)
    prepare, _ = native(env)

    # Real staged page / text and semantic native packet commits in order.
    for doc in (first, later):
        assert (await accepted(env, stage_command(doc, env['sequence'] + 1))).status == 'applied'
        for component in ('text', 'semantic'):
            command = packet(env, doc, component)
            await prepare(command)
            assert (await accepted(env, command)).status == 'applied'

    later_page = later['model_payload']['pages'][0]['page_id']
    async with env['connection'].transaction():
        row = await (await env['connection'].execute(
            """SELECT r.region_id, r.source_text, o.original_sha256, v.eligibility_revision
               FROM rkb_v4.source_regions r
               JOIN rkb_v4.source_import_origins o ON o.source_id=r.source_id
               JOIN rkb_v4.source_visibility v ON v.source_id=r.source_id
               WHERE r.source_id=%s AND r.page_id=%s::uuid
               ORDER BY r.region_id LIMIT 1""", (SOURCE, later_page))).fetchone()
        assert row is not None and row[1]
        region_id, source_text, original_sha256, eligibility_revision = row
        raw = await (await env['connection'].execute(
            """SELECT count(*) FROM rkb_v4.source_manifests m
               CROSS JOIN LATERAL jsonb_array_elements(m.manifest->'pages') p
               WHERE m.source_id=%s AND p->>'page_id'=%s""",
            (SOURCE, later_page))).fetchone()
        published = await (await env['connection'].execute(
            """SELECT count(*) FROM rkb_v4.source_batch_publications b
               CROSS JOIN LATERAL jsonb_array_elements(b.manifest->'pages') p
               CROSS JOIN LATERAL jsonb_array_elements(b.manifest->'regions') r
               WHERE b.source_id=%s AND p->>'page_id'=%s AND r->>'region_id'=%s
               AND r->>'page_id'=p->>'page_id'""",
            (SOURCE, later_page, str(region_id)))).fetchone()
        folded = await (await env['connection'].execute(
            "SELECT rkb_v4.read_source_manifest_v1(%s::uuid)", (SOURCE,))).fetchone()
    assert raw == (0,), 'single first-packet manifest does not cover late pages'
    assert published[0] >= 1, 'late-page evidence has an applied packet provenance'
    manifest = folded[0]
    assert any(page['page_id'] == later_page for page in manifest['pages'])
    assert any(region['region_id'] == str(region_id) for region in manifest['regions'])

    def case_command(action,command,*,object_id,dependencies=(),revision=0):
        exact = resolved_current_case_command(
            sequence=env['sequence']+1, operation_key='packet-case-'+action,
            request_id='synthetic-packet-case-'+action, person_id=PERSON,
            logical_time=TIME, case_id=CASE, object_id=object_id,
            action=action, command=command,
            source_dependencies=list(dependencies), case_revision=revision,
            attribution=dict(person_id=PERSON,recorded_by=PERSON,route='personal',
                link_id=None,link_revision=None,human_reading_confirmed=False))
        return new_native_command(exact)

    created = case_command('create',create(),object_id=CASE)
    assert (await accepted(env, created)).status == 'applied'
    evidence = EvidenceAdd(
        document_id=UUID(SOURCE),source_revision=1,source_sha256=original_sha256,
        page_id=UUID(later_page),region_id=region_id,text_start=0,
        quote=source_text,context='Exact synthetic second packet passage.',
        position_ids=['phase-one'],relation='context',
        speaker='Synthetic book author',attribution='direct_statement')
    cmd = case_command('add_evidence',evidence,object_id=OBJECT,revision=1,
        dependencies=(dict(source_id=SOURCE,content_revision=1,
            eligibility_revision=eligibility_revision),))

    # The native precondition and effect must resolve the SAME existing
    # immutable packet provenance; no NoDataFound after a successful check.
    assert (await accepted(env, cmd)).status == 'applied'
    async with env['connection'].transaction():
        row = await (await env['connection'].execute(
            "SELECT data FROM rkb_v4.disagreement_evidence WHERE evidence_id=%s::uuid",
            (OBJECT,))).fetchone()
        verified = await (await env['connection'].execute(
            "SELECT rkb_v4.verify_disagreement_origins_v1(ARRAY[%s::uuid])",
            (CASE,))).fetchone()
    assert row is not None and row[0]['quote'] == source_text
    assert row[0]['page_provenance']['page_id'] == later_page
    assert row[0]['region_provenance']['region_id'] == str(region_id)
    assert verified == (True,), 'replayed Case proof must use the same packet manifest'
