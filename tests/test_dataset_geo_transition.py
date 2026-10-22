import base64
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.geo_manifest_commands import (GEO_REVISIONS,GEO_V2_REVISIONS,
    geo_dependency_repair_commands,geo_dependency_transition_handler,plan_geo_manifest)
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_pg_snapshot_native import encode,sha
from regional_knowledge.source_history_wss import WssSourceHistoryVerifier
from test_dataset_recovery_store import prepared
from test_geo_manifest_commands import commands
from test_geo_migration import snapshot


@pytest.mark.asyncio
async def test_dataset_pending_migration_proves_native_closure_but_keeps_outcome_unsaved(tmp_path):
    p=prepared(tmp_path);original=commands(plan_geo_manifest(build_geo_whitelist(**snapshot())))
    outcomes={c.operation_key:('applied' if i==0 else 'rejected_precondition') for i,c in enumerate(original)}
    migration=geo_dependency_repair_commands(original,settled_outcomes=outcomes,artifact_sha256='d'*64)[0]
    handler=geo_dependency_transition_handler(artifact_sha256='d'*64,check_sha256='e'*64,apply_sha256='f'*64)
    body=encode(dict(status='applied',before=list(GEO_REVISIONS.wire()),after=list(GEO_V2_REVISIONS.wire()),
        handler_sha256=handler.fingerprint))
    envelope=encode(dict(operation_key=migration.operation_key,command_sha256=sha(migration.encode()),
        sequence=migration.sequence,database_generation='db',outcome_b64=base64.b64encode(body).decode()))
    key=p.prefix+'outcomes/'+sha(migration.operation_key.encode())
    assert await p.store.compare_and_swap(key,envelope,expected_token=None,writer_generation='writer')
    verifier=WssSourceHistoryVerifier.__new__(WssSourceHistoryVerifier)
    verifier.geo_upgrade_pins=NS(artifact_sha256='d'*64);verifier.origin_closure={}
    verifier.native_source_upgrade_pins=None
    verifier.catalog_hash='a'*64;verifier.acl_hash='b'*64;verifier.registry_hash='c'*64;verifier.routes={}
    verifier.database=NS(database_generation='db')
    verifier.spool=NS(journal=NS(prefix=p.prefix.rstrip('/'),store=p.store))
    known={migration.sequence:(migration,handler,None)}
    observed={migration.sequence:('applied',sha(body),len(body))}
    proof=verifier._closure_transition(known,observed,{},None)
    assert proof[:3]==('d'*64,sha(migration.encode()),migration.sequence)
    assert not p.store.remote_durable(key,envelope)
    with pytest.raises(SpoolConflict,match='differs from native'):
        verifier._closure_transition(known,{migration.sequence:('applied','0'*64,len(body))},{},None)
    verifier.spool.journal.store=object()
    with pytest.raises(SpoolConflict,match='not remotely durable'):
        verifier._closure_transition(known,observed,{},None)
