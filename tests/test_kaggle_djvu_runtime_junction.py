"""B4: existing native bundle reaches original_pages in both prepare orders.

Exact owner/pool/handler path is used with synthetic PG/model and a verified
native package capability. No actual Kaggle, release install or document decode.
"""
from pathlib import Path
from types import SimpleNamespace as S

import pytest

from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_target_runtime import TargetRuntime
from regional_knowledge.kaggle_original_native import OriginalIntakeConfig
from regional_knowledge.kaggle_native_bundle import NativeBundleSpec
from test_kaggle_target_runtime import CONFIG, spec, candidate
from test_kaggle_native_bundle import fixture as native_fixture, prepare


def _make_runtime(monkeypatch,tmp_path,*,fresh,cap):
    events=[]
    launch=spec(fresh=fresh)
    target=dict(launch.wire(),sql_root='/unused',original_intake=
        OriginalIntakeConfig(('https://archive.invalid',),
            (('PyMuPDF','pinned-version'),)).wire())
    if fresh:
        target['native_bundle']=NativeBundleSpec('fixture.json','a'*64).wire()
    config=dict(CONFIG,target_runtime=target)
    class Owner:
        def __init__(self,_):pass
        async def start_empty(self,**opts):
            events.append(('initdb',opts.get('native_bundle')))
        async def aclose(self):events.append(('owner_closed',))
    class Pool:
        def __init__(self,**kwargs):events.append(('pool_created',))
        async def open(self,**kwargs):events.append(('pool_open',))
        async def close(self,**kwargs):events.append(('pool_closed',))
        def connection(self):return None
    async def query(*args,**kwargs):return dict(ok=True)
    async def make_prepared(*args,**kwargs):
        events.append(('model_prepared',))
        return PreparedRuntime(query,candidate())
    runtime=TargetRuntime(config,executed_source='/fixture',owner_factory=Owner,
         pool_factory=Pool,prepare=make_prepared)
    # The read maintenance lifecycle belongs to A2; these B4 fixtures only
    # test the decoder capability passing to existing original handler.
    runtime._bind_read_handler_maintenance=lambda: None

    class FakeIntake:
        def __init__(self,**kwargs):
            events.append(('intake',kwargs['decoder_bundle']))
        def attached(self,generation):pass
        def detached(self,generation):pass
        async def __call__(self,frame):
            raise AssertionError("No native page dispatched in offline fixture")
        async def aclose(self):events.append(('intake_closed',))
    monkeypatch.setattr('regional_knowledge.kaggle_original_native.NativeOriginalIntake',FakeIntake)
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg.ensure_private_runtime_root',lambda:tmp_path)
    if fresh:
        def native_bundle(*args,**kwargs):
            events.append(('bundle_verified',))
            return cap
        monkeypatch.setattr('regional_knowledge.kaggle_native_bundle.prepare_native_bundle',native_bundle)
    encoder=S(artifacts=S(root=cap.root,entries=[],role_hash=lambda roles:'f'*64))
    identity=dict(run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],
                  runtime_incarnation='inc')
    return runtime,encoder,identity,events


def _stub_worker(monkeypatch,*,identity,events,cap):
    class Restored:
        async def aclose(self):events.append(('restored_closed',))
        async def verify_for(self,database,run_id,launch_nonce,runtime_incarnation):
            assert (run_id,launch_nonce,runtime_incarnation)==tuple(identity.values())
            return database.connection()
    class Worker:
        async def wait_initialized(self,database,run_id,launch_nonce,incarnation):
            events.append(('wait_real_restore',))
            return Restored()
        async def aclose(self):events.append(('worker_closed',))
    async def build_recovery_worker(**kwargs):
        assert kwargs['native_bundle'] is cap
        assert kwargs['identity']==tuple(identity.values())
        events.append(('recovery_configured',))
        return Worker()
    monkeypatch.setattr('regional_knowledge.kaggle_recovery_phases.build_recovery_worker',build_recovery_worker)


async def _configure(runtime,encoder,identity,cap):
    return await runtime.configure_recovery(encoder,**identity,native_bundle=cap,
        tools={},native_uid=1000,native_gid=1000,
        current=lambda _:True,workspace_private_current=lambda _:True,
        workspace=lambda *a:None)


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh", [False,True])
async def test_one_verified_native_capability_passed_to_original_handler_in_both_orders(
        monkeypatch,tmp_path,native_fixture,fresh):
    cap=prepare(native_fixture())
    assert cap.current()
    runtime,encoder,identity,events=_make_runtime(monkeypatch,tmp_path,fresh=fresh,cap=cap)
    _stub_worker(monkeypatch,identity=identity,events=events,cap=cap)
    if fresh:
        # A2 NEW_EMPTY lifecycle prepares PG/handler before recovery config.
        await runtime.prepare(encoder,**identity)
        assert ('intake',cap) in events
        assert runtime._original_decoder_bundle is cap
        await _configure(runtime,encoder,identity,cap)
        assert events.count(('model_prepared',))==1
    else:
        # VERIFIED_INITIALIZED recovery config exists before PG initialization.
        await _configure(runtime,encoder,identity,cap)
        assert runtime._original_decoder_bundle is cap
        await runtime.prepare(encoder,**identity)
        assert ('intake',cap) in events
        assert ('wait_real_restore',) in events
    assert sum(item[0]=='intake' for item in events)==1
    await runtime.aclose()
    assert ('intake_closed',) in events
