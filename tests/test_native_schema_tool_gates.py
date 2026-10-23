"""Schema14 capability gates and real controller intent/retry seams, no live PG/BGE."""
import asyncio
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry, PgFunction
from regional_knowledge.source_activation import ACTIVATION_REVISIONS, ACTIVATE_ROUTE
from regional_knowledge.kaggle_native_source_bindings import (
    native_source_handler, new_native_command, validate_native_source_command,
)
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS, current_handler, _view
from regional_knowledge.source_read_gate import SourceReadGate
from regional_knowledge.case_read_gate import CaseReadGate
from regional_knowledge.geo_read_gate import GeoReadGate
from regional_knowledge.case_history import project_cases
from regional_knowledge.disagreement_native import DEPENDENCIES
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.geo_manifest_commands import plan_geo_manifest
from test_geo_migration import snapshot, uid
from test_disagreement_native import command as case_command, create
from test_kaggle_cases_backend import backend as case_backend
from test_kaggle_story_editorial_backend import editorial, context
from test_story_editorial import STORY
from test_control_spool import env
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


def handler(route):
    return native_source_handler(route, check_sha256='a'*64, apply_sha256='b'*64)


def bare_gate(cls, handlers):
    gate=object.__new__(cls)
    gate.spool=SimpleNamespace(registry=HandlerRegistry(handlers))
    return gate


def test_native_activation_alone_advertises_source_graph_and_story_domains_without_legacy_import():
    gate=bare_gate(SourceReadGate,[handler(ACTIVATE_ROUTE)])
    assert gate.catalog_supported() and gate.graph_supported()
    assert gate.facts_supported() and gate.story_supported()
    assert not gate.editorial_supported()
    gate.spool.registry=HandlerRegistry([handler(route) for route in
        (ACTIVATE_ROUTE,'story.edit.v1','story.access.v1')])
    assert gate.editorial_supported()
    assert all(h.route!='source.import.v4' for h in gate.spool.registry.handlers.values())


@pytest.mark.parametrize('change', ['function','revision','dependency'])
def test_native_activation_capability_requires_exact_reviewed_handler(change):
    value=handler(ACTIVATE_ROUTE)
    if change=='function':value=replace(value,apply=PgFunction('apply_source_import_v4','b'*64))
    if change=='revision':value=replace(value,revisions=DOMAIN_REVISIONS)
    if change=='dependency':value=replace(value,dependency=None)
    gate=bare_gate(SourceReadGate,[value])
    assert not gate.catalog_supported() and not gate.graph_supported()


def test_native_editorial_pair_cannot_borrow_catalog_capability_from_old_schema():
    old=current_handler('source.import.v4',check_sha256='a'*64,apply_sha256='b'*64)
    gate=bare_gate(SourceReadGate,[old,handler('story.edit.v1'),handler('story.access.v1')])
    assert gate.catalog_supported() and not gate.editorial_supported()


def test_original_domain11_profile_is_unchanged():
    gate=bare_gate(SourceReadGate,[current_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
        for route in ('source.import.v4','story.edit.v1','story.access.v1')])
    assert gate.catalog_supported() and gate.graph_supported() and gate.editorial_supported()


def test_case_gate_uses_exact_native_handler_not_route_name_only():
    value=handler('disagreement.edit.v1')
    assert bare_gate(CaseReadGate,[value]).cases_supported()
    assert not bare_gate(CaseReadGate,[replace(value,dependency=None)]).cases_supported()


def test_geo_gate_uses_native_v2_function_pins_and_ephemeral_validation_only():
    value=handler('geo.manifest.install.v1')
    assert value.check.name=='check_geo_manifest_install_v2'
    assert value.apply.name=='apply_geo_manifest_install_v2'
    gate=bare_gate(GeoReadGate,[value])
    assert gate.supported()
    commands=plan_geo_manifest(build_geo_whitelist(**snapshot())).commands(first_sequence=1,
        operation_key='native-geo',request_id='native-geo-request',person_id=uid(1),
        logical_time='2026-10-21T19:00:00Z')
    original=commands[0]; prior=original.encode()
    native=new_native_command(original); encoded=native.encode()
    assert gate._manifest_command(native)['action']=='open'
    assert native.encode()==encoded and original.encode()==prior
    with pytest.raises(ValueError):
        gate._manifest_command(replace(native,function_revision='unreviewed-native-function'))
    assert not bare_gate(GeoReadGate,[replace(value,check=PgFunction('check_geo_manifest_install_v1','a'*64))]).supported()


def test_native_case_history_projects_policy_without_relabeling_accepted_bytes():
    original=_view(case_command(),DOMAIN_REVISIONS); prior=original.encode()
    native=new_native_command(original); encoded=native.encode()
    doc=validate_native_source_command(native)
    result=project_cases([(native,handler(native.route),None)],{native.sequence:'applied'})
    case=result.cases[native.ids['case_id']]
    assert case['owner']==native.person_id and case['revision']==1
    assert doc['command']['question'] not in result.wire.decode()
    assert native.encode()==encoded and original.encode()==prior
    with pytest.raises(ValueError):
        project_cases([(replace(native,executor_revision='unknown'),handler(native.route),None)],{native.sequence:'applied'})


@pytest.mark.asyncio
async def test_native_case_create_uses_real_auth_spool_and_same_retry_identity(env,auth,monkeypatch):
    backend,principal,binding=await case_backend(env,auth,monkeypatch,current11=True)
    env.spool.registry=HandlerRegistry([handler(route) for route in DEPENDENCIES])
    with capture_read_output() as outputs:
        result=await backend.disagreement_edit('create','native14-case',principal,command=create())
        assert result['status']=='pending' and result['intent_remote_accepted'] and not result['saved']
        await outputs[0].gate.refresh(outputs[0].gated)
        assert outputs[0].gate.issuance_current(outputs[0].gated.ticket)
        outputs[0].release()
    entry=await env.spool.get(result['operation_key']); exact=entry.command_bytes
    assert (entry.command.schema_revision,entry.command.function_revision,
        entry.command.executor_revision)==ACTIVATION_REVISIONS.wire()
    with capture_read_output() as outputs:
        retry=await backend.disagreement_edit('create','native14-case',principal,command=create())
        assert retry['command_sha256']==result['command_sha256']
        outputs[0].release()
    assert (await env.spool.get(result['operation_key'])).command_bytes==exact
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==1


@pytest.mark.asyncio
async def test_native_editorial_manager_builds_schema14_intent_without_vector_or_relabelled_retry(editorial):
    s=editorial
    s.spool.registry=HandlerRegistry([handler(route) for route in
        (ACTIVATE_ROUTE,'story.edit.v1','story.access.v1','source.withdraw.v1')])
    with s.t.queue.connect() as db:
        row=db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',(s.t.run,)).fetchone()
        values=[]
        for raw in row:
            value=json.loads(raw)
            value.update(schema_version=14)
            values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',(*values,s.t.run))
    args=dict(story_id=STORY,expected_revision=1,idempotency_key='native14-access',
        grantee_user_id=s.t.auth.p.owner.subject,capability='editor')
    result=await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,**args)),role='manager')
    entry=await s.spool.get(result['operation_id']); exact=entry.command_bytes
    assert (entry.command.schema_revision,entry.command.function_revision,
        entry.command.executor_revision)==ACTIVATION_REVISIONS.wire()
    assert entry.command.route=='story.access.v1' and not s.counts
    retry=await s.workflow.mutate(s.t.principal,**args)
    assert retry['operation_id']==result['operation_id']
    assert (await s.spool.get(result['operation_id'])).command_bytes==exact
