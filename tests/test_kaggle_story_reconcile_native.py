"""Synthetic SQL016 command provenance; no PG/process/provider or registry mutation."""
import base64
from dataclasses import dataclass
from pathlib import Path
import sys
from types import ModuleType
from uuid import UUID

import pytest

from regional_knowledge.kaggle_story_reconcile import ReconcileIntent, ReconcileError, _digest
from regional_knowledge.kaggle_story_reconcile_native import (
    ROUTE,CODEC,REVISIONS,PRECONDITIONS,build_payload,canonical,
    parse_intent_bytes,intent_bytes,case_child_key,native_case_receipt_key,
    decode_payload,resolved_reconcile_command,progress_key,sha,validate_reconcile_command)
from test_kaggle_story_reconcile import ACTOR,CASE,comparison,snapshot,A,B


def intent(action='create'):
    a,b=snapshot(A),snapshot(B)
    value=comparison(a,b,action=action)
    parent='story-reconcile:'+_digest([ACTOR,a.frontier_id,value.comparison_key])
    return ReconcileIntent(ACTOR,parent,_digest(value.model_dump(mode='json')),a,b,value)


@dataclass(frozen=True)
class ExactCommand:
    sequence: int
    operation_key: str
    request_id: str
    person_id: str
    route: str
    logical_time: str
    schema_revision: str
    function_revision: str
    executor_revision: str
    kind: str
    ids: dict
    expected_revisions: dict
    payload: bytes

    def encode(self):
        from regional_knowledge.kaggle_story_reconcile_native import canonical, sha
        return canonical(dict(codec='rkb.exact-command.v1',sequence=self.sequence,
            operation_key=self.operation_key,request_id=self.request_id,person_id=self.person_id,
            route=self.route,logical_time=self.logical_time,schema_revision=self.schema_revision,
            function_revision=self.function_revision,executor_revision=self.executor_revision,
            kind=self.kind,ids=self.ids,expected_revisions=self.expected_revisions,
            payload_b64=base64.b64encode(self.payload).decode(),payload_sha256=sha(self.payload),
            payload_length=len(self.payload)))


@pytest.fixture(autouse=True)
def source_pinned_command_type(monkeypatch):
    # The real app imports the real recovery_journal.ExactCommand. No installs
    # are allowed for this isolated stdlib/Pydantic test environment.
    package=ModuleType('regional_knowledge.recovery_journal')
    package.ExactCommand=ExactCommand
    monkeypatch.setitem(sys.modules,package.__name__,package)


def test_canonical_intent_roundtrip_and_stable_case_effect_keys():
    value=intent()
    raw=intent_bytes(value)
    assert parse_intent_bytes(raw)==value
    assert len(raw)<192*1024
    assert value.operation_key.startswith('story-reconcile:')
    assert native_case_receipt_key(ACTOR,value.operation_key,0)==(
        'case-op:'+ACTOR+':'+sha(case_child_key(value.operation_key,0).encode()))
    assert case_child_key(value.operation_key,0)!=case_child_key(value.operation_key,1)
    assert progress_key(value,'begin',1,0)==progress_key(value,'begin',1,0)
    assert progress_key(value,'begin',1,0)!=progress_key(value,'advance',2,0)


def test_exact_accepted_command_builder_frozen_keys_and_progress():
    value=intent()
    first=resolved_reconcile_command(action='stage',intent=value,sequence=21,
        request_id='trusted-1',logical_time='2026-10-22T00:00:00+00:00',version=0,step=0)
    assert first.operation_key==value.operation_key
    assert (first.schema_revision,first.function_revision,first.executor_revision)==REVISIONS
    assert first.route==ROUTE and first.kind=='domain_command'
    assert first.expected_revisions==dict(progress_version='0',step_index='0')
    assert first.ids==dict(actor_id=ACTOR,source_id=A,peer_source_id=B)
    parsed=validate_reconcile_command(first)
    assert parsed['codec']==CODEC and parsed['action']=='stage'
    assert parsed['intent_sha256']==sha(intent_bytes(value))
    begin=resolved_reconcile_command(action='begin',intent=value,sequence=22,
        request_id='trusted-2',logical_time='2026-10-22T00:00:01+00:00',version=1,step=0)
    assert begin.operation_key==progress_key(value,'begin',1,0)
    assert validate_reconcile_command(begin)['action']=='begin'
    advance=resolved_reconcile_command(action='advance',intent=value,sequence=24,
        request_id='trusted-3',logical_time='2026-10-22T00:00:02+00:00',version=2,step=0,case_id=CASE)
    assert validate_reconcile_command(advance)['case_receipt_key']==native_case_receipt_key(
        ACTOR,value.operation_key,0)
    assert begin.payload!=advance.payload


@pytest.mark.parametrize('tamper', ['actor','source','sequence','payload','parent','revision','case_key'])
def test_mismatched_command_fails_closed(tamper):
    from dataclasses import replace
    value=intent()
    cmd=resolved_reconcile_command(action='advance',intent=value,sequence=10,
        request_id='r',logical_time='2026-10-22',version=2,step=0,case_id=CASE)
    if tamper=='actor':cmd=replace(cmd,person_id=str(UUID(int=17)))
    if tamper=='source':cmd=replace(cmd,ids={**cmd.ids,'source_id':B})
    if tamper=='sequence':cmd=replace(cmd,expected_revisions={'progress_version':'02','step_index':'0'})
    if tamper=='payload':cmd=replace(cmd,payload=cmd.payload+b'\n')
    if tamper=='parent':cmd=replace(cmd,operation_key='story-reconcile-progress:'+'a'*64)
    if tamper=='revision':cmd=replace(cmd,function_revision='unapproved')
    if tamper=='case_key':
        body=decode_payload(cmd.payload)
        body['case_receipt_key']='case-op:'+ACTOR+':'+'f'*64
        cmd=replace(cmd,payload=canonical(body))
    with pytest.raises(ReconcileError):validate_reconcile_command(cmd)


def test_modified_intent_proof_or_comparison_hash_is_not_accepted():
    from dataclasses import replace
    value=intent()
    doc=__import__('json').loads(intent_bytes(value))
    doc['anchor_source']['proofs'][0]['quote']='tampered'
    with pytest.raises(ReconcileError):parse_intent_bytes(canonical(doc))
    changed=replace(value,request_sha256='0'*64)
    with pytest.raises(ReconcileError):intent_bytes(changed)


def test_requested_step_bounds_and_no_case_crud_in_native_command():
    value=intent()
    for args in [dict(action='advance',version=1,step=7,case_id=CASE),
                 dict(action='reject',version=0,step=0),
                 dict(action='stage',version=1,step=0),
                 dict(action='begin',version=1,step=2,case_id=CASE)]:
        with pytest.raises(ReconcileError):build_payload(intent=value,**args)
    for key in ('create_case','case.insert','make_new_case'):
        assert key not in ROUTE


def test_sql016_has_only_accepted_pg_effects_and_narrow_reader_grants():
    sql=(Path(__file__).resolve().parents[1]/'sql/kaggle/016_story_reconcile.sql').read_text()
    for marker in ('CREATE TABLE rkb_v4.story_reconciliation_cursor',
                   'CREATE TABLE rkb_v4.story_reconciliation_intents',
                   'CREATE FUNCTION rkb_v4.check_story_reconcile_v1',
                   'CREATE FUNCTION rkb_v4.apply_story_reconcile_v1',
                   'CREATE FUNCTION rkb_v4.read_story_reconciliation_v1',
                   'FROM rkb_v4.applied_ops', 'rkb_v4.disagreement_receipts',
                   'rejected_precondition','progress_version=progress_version+1',
                   'FOR UPDATE','source_research_frontier'):
        assert marker in sql
    assert 'GRANT SELECT(source_id,revision,frontier_id,state)' in sql
    assert 'ON rkb_v4.source_research_frontier TO rkb_reader' in sql
    assert 'TO rkb_reader' in sql
    assert 'GRANT ALL' not in sql.upper()
    assert 'UPDATE rkb_v4.story_variant_versions' not in sql
    assert 'UPDATE rkb_v4.source_regions' not in sql
    assert 'UPDATE rkb_v4.disagreement_cases' not in sql
    assert 'UPDATE rkb_v4.source_research_frontier' not in sql
    assert 'DROP TABLE' not in sql.upper()
    assert 'INSERT INTO rkb_v4.story_reconciliation_cursor' in sql
    assert 'reconciliation_state text NOT NULL DEFAULT' in sql
    assert 'story_reconcile_case_receipt_v1' in sql
    assert 'expected_status=\'rejected_precondition\'' not in sql  # passed as explicit literal to fixed helper


def test_sql016_revision_parameter_has_unambiguous_binding_and_case_receipt_target():
    """Structural guard only; actual SQL execution belongs to existing PG fixture."""
    sql=(Path(__file__).resolve().parents[1]/'sql/kaggle/016_story_reconcile.sql').read_text()
    source=sql.split('CREATE FUNCTION rkb_v4.story_reconcile_source_current_v1(',1)[1].split('$$;',1)[0]
    assert 'ar.revision=$2' in source
    assert 'ar.revision=revision' not in source
    # Other new SQL funcs use no ambiguous unqualified `revision` symbol.
    assert sql.count('CREATE FUNCTION rkb_v4.') == 7
    for block in sql.split('CREATE FUNCTION rkb_v4.')[1:]:
        function_name=block.split('(',1)[0]
        if function_name!='story_reconcile_source_current_v1':
            assert 'ar.revision=revision' not in block.split('$$;',1)[0]
    check=sql.split('CREATE FUNCTION rkb_v4.check_story_reconcile_v1(',1)[1].split('$$;',1)[0]
    start=check.index("IF action='advance' THEN")
    end=check.index("IF action='reject' THEN",start)
    advance=check[start:end]
    assert "IF (d->>'case_id')::uuid<>wanted_case THEN RETURN false;END IF;" in advance
    assert "ELSIF plan->>'action' IS DISTINCT FROM 'create' OR step<>0" in advance
    assert "row.case_id IS NOT NULL" in advance
    assert "story_reconcile_case_receipt_v1" in advance
    assert advance.index("case_id')::uuid<>wanted_case") < advance.index('story_reconcile_case_receipt_v1')
    # No replacing selected Case with a second actor-valid Case receipt.
    assert "wanted_case:=coalesce(row.case_id" in check
    assert "case_role_v1(wanted_case,actor)" in check
