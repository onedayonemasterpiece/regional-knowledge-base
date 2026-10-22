"""Exact assembled native metadata + full55-wheel/source launch envelope.

GitHub-only explicit artifact input; no downloads, installs, PG or model calls
inside the test. Identity, signer and recovery store are synthetic fixtures.
"""
import ast
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest

from regional_knowledge.kaggle_bootstrap_config import build_bootstrap_target
from regional_knowledge.kaggle_original_native import OriginalIntakeConfig
from regional_knowledge.kaggle_owned_pg import canonical
from regional_knowledge.kaggle_stage0_urls import PinnedStage0Object
from regional_knowledge.kaggle_stage0 import Stage0Spec
from regional_knowledge.kaggle_target_launch import persist_empty_intent
from regional_knowledge.kaggle_target_launch import MAX_LAUNCH_CONFIG_BYTES, source_bundle
from first_book_registry_fixture import first_book_entries
from regional_knowledge.source_activation import ACTIVATION_REVISIONS
from test_import_dependency_packaging import packaging, inputs, placement, dataset_placement
from test_kaggle_target_runtime import generate, queue_run
from test_recovery_journal import fixture as journal_fixture
from test_stage0_url_renewal import signed, ORIGIN

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(not os.environ.get('RKB_IMPORT_NATIVE_METADATA'),
                    reason='explicit exact assembled native artifact metadata required')
@pytest.mark.asyncio
@pytest.mark.parametrize('dependency_transport',['https','dataset'])
@pytest.mark.parametrize('handler_profile',['baseline','current'])
async def test_full55_wheel_schema14_target_fits_actual_launch_envelope(tmp_path, monkeypatch,dependency_transport,handler_profile):
    artifact = Path(os.environ['RKB_IMPORT_NATIVE_METADATA'])
    raw = (artifact / 'native-manifest.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '84ab517abe83ca7f90b16290f30332415522eb4b355b6ef194c40df89cb264f5'
    native = json.loads((artifact / 'native-receipt.json').read_text())
    adapter = packaging()
    base, candidate = inputs(adapter)
    receipt=dataset_placement(candidate) if dependency_transport=='dataset' else placement(candidate)
    wheel_lock = adapter.merge_wheels(base, candidate, receipt)
    assert len(wheel_lock['packages']) == 55
    assert wheel_lock['packages'][:51] == base['packages']
    native_downloads = [dict(name=row['name'], size=row['size'], sha256=row['sha256'],
        executable=False, url=('kaggle-dataset://fixture-owner/rkb-bootstrap/7/'+row['name']
            if dependency_transport=='dataset' else ORIGIN+'/rkb-fixture/'+row['sha256']+'/'+row['name']))
        for row in native['artifacts']]
    target = build_bootstrap_target(wheel_lock=wheel_lock, native_receipt=native,
        native_manifest_bytes=raw, native_downloads=native_downloads,
        database_generation='synthetic-import-envelope', writer_generation='writer-1',
        uid=65534, gid=65534, identity_evidence='synthetic metadata fixture, not native identity attestation',
        original_intake=OriginalIntakeConfig(('https://archive.fixture.invalid',), (('pymupdf','1.28.2'),)))
    if handler_profile=='current':
        target=replace(target,command_handlers=tuple(first_book_entries(ROOT)),telegram_runtime=True)
        assert len(target.command_handlers)==13
    private_names = {row['name'] for row in native['artifacts']} | {'wheels/pyaes-1.6.1-py3-none-any.whl'}
    downloads = []
    for row in target.stage0.downloads:
        if row.name in private_names and dependency_transport=='https':
            pin = PinnedStage0Object(row.name, ORIGIN, 'rkb-fixture',
                'rkb/kaggle-v4/bootstrap-artifacts/'+row.sha256+'/'+Path(row.name).name,
                row.size, row.sha256)
            row = replace(row, url=signed(pin))
        downloads.append(row)
    target = replace(target, stage0=replace(target.stage0, downloads=tuple(downloads)))
    assert target.stage0.dataset_sources==(('fixture-owner/rkb-bootstrap/7',)
        if dependency_transport=='dataset' else ())
    queue, run = queue_run(tmp_path)
    _, journal = journal_fixture()
    await persist_empty_intent(queue, run, target, launch_nonce=queue.prepare_channel(run),
        recovery_journal=journal, sql_hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql')})
    from regional_knowledge import kaggle_target_launch
    def measured(value):
        raw = canonical(value)
        if isinstance(value, dict) and 'prepared_model' in value and 'target_runtime' in value:
            print(json.dumps(dict(actual_launch_config_bytes=len(raw),
                component_bytes={key:len(canonical(part)) for key,part in value.items()},
                metadata_only=True)))
        return raw
    monkeypatch.setattr(kaggle_target_launch, 'canonical', measured)
    source = generate(queue, run, target)
    config = next(ast.literal_eval(node.value) for node in ast.parse(source).body
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='RUN_CONFIG' for t in node.targets))
    expected_files=len(target.artifacts)+len(source_bundle(ROOT)[2])
    assert len(config['prepared_model']['entries']) == expected_files <= 192
    assert len(Stage0Spec.from_wire(config['stage0']).wheels) == 55
    assert len(canonical(config)) <= MAX_LAUNCH_CONFIG_BYTES <= 96*1024
    if handler_profile=='baseline':assert len(canonical(config))<=65536
    assert len(source.encode()) <= 1_000_000
    print(json.dumps(dict(prepared_entries=expected_files, wheel_count=55,
        handler_count=len(target.command_handlers),handler_profile=handler_profile,
        launch_json_bytes=len(canonical(config)), notebook_source_bytes=len(source.encode()),
        actual_native_manifest_sha256=hashlib.sha256(raw).hexdigest(),
        schema_version=target.database.schema_version, identities_and_signed_urls='synthetic_only',
        dependency_transport=dependency_transport,
        provider_effects=False, pg_or_model_executed=False)))
