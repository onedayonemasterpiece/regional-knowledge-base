"""Real spool/registry/history composition for the ordered maintenance transition."""
import base64,json
from dataclasses import replace
from types import SimpleNamespace
import pytest
from test_control_spool import env,people,handler,withdraw
from regional_knowledge.control_spool import SpoolDenied,SpoolConflict
from regional_knowledge.kaggle_apply import HandlerRegistry,ApplyConflict
from regional_knowledge.kaggle_native_source_upgrade import *
from regional_knowledge.kaggle_source_commands import fixed_registry,decode_command
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from regional_knowledge.kaggle_domain_bindings import current_handler
from regional_knowledge.geo_manifest_commands import GEO_REVISIONS,GEO_V2_REVISIONS,geo_dependency_transition_handler
from regional_knowledge.recovery_journal import ExactCommand
from regional_knowledge.source_history_wss import WssSourceHistoryVerifier

P=NativeSourceUpgradePins('a'*64,'b'*64,'c'*64,'d'*64)
G=GeoUpgradePins('1'*64,'2'*64,'3'*64,'4'*64,'5'*64,'6'*64,'7'*64)
def command(actor,sequence=26):
    return resolved_native_source_upgrade(pins=P,sequence=sequence,operation_key='book-upgrade',
        request_id='request',person_id=actor,logical_time='2026-10-21T00:00:00Z')
def entry():
    return dict(route=ROUTE,revisions=list(GEO_V2_REVISIONS.wire()),artifact_sha256=P.artifact_sha256,
        check_sha256=P.transition_check_sha256,apply_sha256=P.transition_apply_sha256,native_source_upgrade=P.wire())
def database():
    return SimpleNamespace(database_generation='db-source',writer_generation='writer-1',
        **dict(zip(('schema_revision','function_revision','executor_revision'),ACTIVATION_REVISIONS.wire())))
def setup(e):
    historical=[current_handler('source.import.v4',check_sha256='8'*64,apply_sha256='9'*64,revisions=r)
        for r in (GEO_REVISIONS,GEO_V2_REVISIONS)]
    geo=geo_dependency_transition_handler(artifact_sha256=G.artifact_sha256,
        check_sha256=G.transition_check_sha256,apply_sha256=G.transition_apply_sha256)
    e.spool.registry=HandlerRegistry([*historical,geo,native_source_upgrade_handler(P)])
    v=WssSourceHistoryVerifier(e.spool,acquire=None,binding_current=lambda b:True,
        routes={name:lambda:None for name in ('source.search','source.fetch','source.withdraw','source.import')},
        expected_catalog_sha256='e'*64,expected_acl_sha256='f'*64,writer_generation='writer-1',
        database=database(),observer_sha256='0'*64,geo_upgrade_pins=G,native_source_upgrade_pins=P)
    return v,historical,geo

def test_registry_and_decode_retain_exact_pins_and_reject_changed_tuple():
    r=fixed_registry(database(),[entry()]);c=command('00000000-0000-0000-0000-000000000001')
    assert r.resolve(c)==native_source_upgrade_handler(P)
    assert decode_command(c.encode())==c
    for bad in (dict(entry(),artifact_sha256='e'*64),dict(entry(),revisions=list(ACTIVATION_REVISIONS.wire())),
                dict(entry(),check_sha256='e'*64)):
        with pytest.raises(ValueError):fixed_registry(database(),[bad])
    bad=replace(c,expected_revisions={**c.expected_revisions,'migration':'wrong'})
    with pytest.raises(ValueError):decode_command(bad.encode())

@pytest.mark.asyncio
async def test_real_spool_maintenance_admission_null_roundtrip_and_classification(env):
    v,historical,geo=setup(env);c=command(env.p.owner.subject)
    # History holds the normal intake fence until its current proof, not a bypass.
    env.spool._native_history_unsettled=False
    result=await env.spool.stage_native_source_upgrade(env.p.admin,c,database_generation='db-source')
    assert result.command==c
    assert (await env.spool.get(c.operation_key)).command==c
    assert v._classify(c.encode(),None)[1]==native_source_upgrade_handler(P)
    assert all(env.spool.registry.handlers[h.key]==h for h in historical)
    with pytest.raises(SpoolDenied):
        await env.spool.stage_domain_command(env.p.admin,c,database_generation='db-source')
    with pytest.raises((SpoolDenied,ValueError)):
        await env.spool.stage_native_source_upgrade(env.p.admin,
            replace(c,person_id='00000000-0000-0000-0000-000000000002'),database_generation='db-source')

@pytest.mark.asyncio
async def test_arbitrary_null_dependency_is_not_a_history_escape(env):
    v,_,_=setup(env)
    env.spool.registry=HandlerRegistry([handler(None)])
    with pytest.raises((SpoolConflict,SpoolDenied,ValueError,ApplyConflict)):
        v._classify(withdraw(env.p.owner.subject).encode(),None)
    bad=replace(command(env.p.owner.subject),kind='domain_command')
    env.spool.registry=HandlerRegistry([native_source_upgrade_handler(P)])
    with pytest.raises((SpoolConflict,SpoolDenied,ValueError,ApplyConflict)):
        v._classify(bad.encode(),None)

def proof_body(c,h,before,after,status='applied'):
    body=canonical(dict(before=list(before.wire()),after=list(after.wire()),status=status,handler_sha256=h.fingerprint))
    return ('applied',sha(body),len(body)),SimpleNamespace(data=canonical(dict(outcome_b64=base64.b64encode(body).decode())))

@pytest.mark.asyncio
async def test_legacy_transition_metadata_is_not_a_schema_readiness_gate(env):
    v,_,_=setup(env)
    assert v._closure_transition({}, {}, {},None) is None
