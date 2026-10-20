"""Fixed domain admission reuses canonical Participants and one ordered spool."""
from dataclasses import replace
import pytest
from regional_knowledge.control_spool import ControlSpool,SpoolDenied
from regional_knowledge.kaggle_apply import HandlerRegistry,DependencySpec
from regional_knowledge.disagreement_native import CASE_EDIT,case_handler,resolved_case_command
from regional_knowledge.geo_manifest_commands import geo_manifest_handler,GEO_REVISIONS,plan_geo_manifest
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.kaggle_source_commands import decode_command
from test_participants import people,change
from test_bge_admission import state,auth
from test_disagreement_native import create
from test_geo_migration import snapshot
from test_recovery_journal import fixture as journal_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize('current',[False,True])
async def test_case_requires_exact_current_capabilities_attribution_and_actor_without_book_pending(state,current):
    s=state;_,journal=journal_fixture();actor=s.auth.p.other.subject
    handler=case_handler(CASE_EDIT,check_sha256='a'*64,apply_sha256='b'*64)
    spool=ControlSpool(s.q,s.auth.p.registry,journal,HandlerRegistry([handler]))
    c=resolved_case_command(sequence=1,operation_key='case-intake-1',request_id='case-request',person_id=actor,
        logical_time='2026-10-20T18:00:00Z',case_id='73000000-0000-0000-0000-000000000001',
        object_id='73000000-0000-0000-0000-000000000001',action='create',command=create(),source_dependencies=[],
        attribution=dict(person_id=actor,recorded_by=actor,route='personal',link_id=None,link_revision=None,human_reading_confirmed=False))
    if current:
        from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
        c=replace(c,schema_revision=DOMAIN_REVISIONS.schema,function_revision=DOMAIN_REVISIONS.function,executor_revision=DOMAIN_REVISIONS.executor)
        spool.registry=HandlerRegistry([current_handler(c.route,check_sha256='a'*64,apply_sha256='b'*64)])
    try:
        assert decode_command(c.encode())==c
        with pytest.raises(SpoolDenied):await spool.stage_domain_command(s.auth.p.personal,c,database_generation='case-db')
        for cap in ('case_read','case_contribute'):
            change(s.auth.p,ParticipantGrant(capability=cap,granted=True,reason='synthetic scoped test'))
        entry=await spool.stage_domain_command(s.auth.p.personal,c,database_generation='case-db')
        assert entry.command.encode()==c.encode()
        assert (await spool.scope_state())['pending'] and not (await spool.read_facts(None)).pending
        with pytest.raises(SpoolDenied):await spool.authorize_command(replace(s.auth.p.personal,public_only=True),c)
        with pytest.raises(SpoolDenied):await spool.authorize_command(s.auth.p.admin,c)
        change(s.auth.p,ParticipantGrant(capability='case_contribute',granted=False,reason='synthetic revoke'))
        with pytest.raises(SpoolDenied):await spool.seal(s.auth.p.personal,c.operation_key,predecessor=None,writer_generation='writer-1')
        assert (await spool.get(c.operation_key)).seal_state=='NOT_SENT'
    finally:spool.close()


@pytest.mark.asyncio
async def test_geo_scope_is_exact_actor_owned_manifest_and_import_remains_owner_personal(state):
    s=state;_,journal=journal_fixture()
    handler=geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)
    spool=ControlSpool(s.q,s.auth.p.registry,journal,HandlerRegistry([handler]))
    plan=plan_geo_manifest(build_geo_whitelist(**snapshot()))
    c=plan.commands(first_sequence=1,operation_key='geo-intake',request_id='geo-request',
        person_id=s.auth.p.owner.subject,logical_time='2026-10-20T18:00:00Z')[0]
    try:
        assert decode_command(c.encode())==c
        with pytest.raises(SpoolDenied):await spool.stage_domain_command(s.auth.p.personal,c,database_generation='geo-db')
        entry=await spool.stage_domain_command(s.auth.p.admin,c,database_generation='geo-db')
        assert entry.command.ids=={'manifest_id':plan.manifest_id}
        assert (await spool.scope_state())['pending'] and not (await spool.read_facts(None)).pending
        with spool._db() as db:
            assert db.execute('SELECT source_id FROM rkb_control_operations').fetchone()[0]=='geo:'+c.person_id+':'+plan.manifest_id
        with pytest.raises(ValueError):DependencySpec(c.route,'geo_manifest',frozenset({'manifest_id'}),'shared')
    finally:spool.close()
