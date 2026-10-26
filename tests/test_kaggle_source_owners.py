"""Production owner wiring with actual auth/SQLite and synthetic S3/native peers."""
from types import SimpleNamespace
from contextlib import asynccontextmanager
import hashlib

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_source_owners import compose_source_service,configured_source_service
from test_queue_writer import owner
from test_read_authority import auth
from test_participants import people
from test_control_spool import handler
from test_kaggle_service_startup import HistoryAdapter, service
from test_kaggle_public_backend import target


@pytest.mark.asyncio
async def test_cold_start_configures_admission_before_native_ready_and_writer_activation(auth,tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation
    from regional_knowledge.kaggle_target_launch import persist_empty_intent
    from regional_knowledge.recovery_journal import RecoveryJournal
    from regional_knowledge.recovery_s3 import S3ConditionalRecoveryStore
    from test_kaggle_target_runtime import spec,hashes
    from test_recovery_s3 import Client

    q=BgeQueue(tmp_path/'cold-control.sqlite',clock=lambda:1000.)
    q.enqueue(auth.p.owner.subject,'cold-fixture',['synthetic'],kind='document')
    run=q.maintenance()[0]['id'];nonce=q.prepare_channel(run)
    client=Client();client.meta.endpoint_url='https://existing-s3.fixture.invalid'
    stream='cold-start-fixture';bucket='existing-private'
    @asynccontextmanager
    async def deny_all(generation):
        pytest.fail('bootstrap journal cannot publish')
        yield
    journal=RecoveryJournal(S3ConditionalRecoveryStore(client=client,bucket=bucket,
        prefix='recovery/v1/'+hashlib.sha256(stream.encode()).hexdigest()+'/',writer_guard=deny_all),stream=stream)
    await persist_empty_intent(q,run,spec(fresh=True),launch_nonce=nonce,recovery_journal=journal,sql_hashes=hashes())
    expected=SearchRuntimeExpectation({key: 'a' * 64 for key in ('code', 'model', 'launch_config', 'executed_source', 'dependency', 'schema')}, 'synthetic-db', 0)
    q.expect_search_runtime(run,expected)
    token=q.launch_claim(run);q.launch_record(run,'owner/stable-slug',provider_version=88)
    assert getattr(q,'_shared_reads',None) is None
    history=HistoryAdapter();history.available=True
    startup=compose_source_service(queue=q,oauth_provider=auth.provider,participants=auth.p.registry,
        object_store=SimpleNamespace(client=client,bucket=bucket),stream=stream,registry=HandlerRegistry([handler()]),
        history_adapter=history,run_id=run,database_generation='synthetic-db',writer_generation='writer-1')
    assert q._shared_reads is startup.admission
    def writer_rows():
        with q.connect(write=False) as db:return db.execute('SELECT count(*) FROM rkb_writer_control').fetchone()[0]
    async with startup.lifespan(None):
        assert not startup.reads_current() and writer_rows()==0 and not history.calls
        generation=q.attach_channel(run,token,nonce,'cold-incarnation')['connection_generation']
        value=dict(hashes=dict(expected.hashes), database_generation='synthetic-db', schema_version=expected.schema_version, applied_seq=0, projection_seq=0, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None)
        assert q.accept_search_runtime(run,token,generation,value).ok
        q.heartbeat(run,token,ready=True,connection_generation=generation)
        assert not await startup.refresh() and writer_rows()==0 and not history.calls
        await startup.admission.resource_state(run,token,generation,runtime_incarnation='cold-incarnation',state='idle')
        assert await startup.refresh() and startup.reads_current() and writer_rows()==1
        assert history.calls[0][0]=='restore'
    assert startup.config.writer_owner.closed


@pytest.mark.asyncio
async def test_concrete_composition_reuses_actual_owners_and_journal_writer_fence(owner):
    e=owner;history=HistoryAdapter();history.available=True
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=HandlerRegistry([handler()]),
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1')
    assert startup.admission is e.a and startup.authority is e.auth.resolver
    assert startup.config.spool.queue is e.q
    assert startup.config.spool.journal.store.client is e.c
    assert startup.config.spool.journal.store.writer_guard is startup.config.writer_owner
    async with startup.lifespan(None):
        assert startup.reads_current()
        await startup.config.writer_owner.retire('writer-1')
        assert not startup.reads_current() and not await startup.refresh()
    assert startup.config.writer_owner.closed and e.a._fd is None


@pytest.mark.asyncio
async def test_concrete_composition_binds_existing_rkb1_without_reading_credentials(owner,tmp_path,monkeypatch):
    import json
    from regional_knowledge.telegram_runtime_owners import configured_rkb1_source
    from test_telegram_runtime_channel import locator
    monkeypatch.setattr('regional_knowledge.telegram_runtime_owners.existing_rkb1_credentials',
        lambda:pytest.fail('composition cannot acquire session credentials'))
    path=tmp_path/'archive-locator.json';path.write_text(json.dumps(locator().wire()));path.chmod(0o600)
    e=owner;source=configured_rkb1_source(e.q,str(path));history=HistoryAdapter();history.available=True
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=HandlerRegistry([handler()]),
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',
        telegram_runtime_source=source)
    assert startup.config.telegram_runtime_source is source
    assert e.q._telegram_runtime_source is source and source.startup is startup
    async with startup.lifespan(None):assert startup.reads_current()
    path.chmod(0o644)
    with pytest.raises(ValueError,match='private Telegram locator'):
        configured_rkb1_source(e.q,str(path))




def test_private_slack_ingress_config_reuses_exact_target_owners_and_fails_closed(service,tmp_path):
    """Only an explicitly installed, private synthetic scope enables the route."""
    import json
    from regional_knowledge.kaggle_source_owners import configured_expert_slack_ingress

    startup=service.startup
    signing=tmp_path/'not-a-provider-secret'
    signing.write_text('synthetic-signing-secret-for-only-unit-tests')
    signing.chmod(0o600)
    path=tmp_path/'slack-ingress.json'
    value=dict(codec='rkb.expert-slack-ingress.v1',app_id='A111',team_id='T111',
        bot_user_id='UBOT',allowed_channels=['C111'],signing_secret_file=str(signing))
    path.write_text(json.dumps(value));path.chmod(0o600)
    ingress=configured_expert_slack_ingress(startup,str(path))
    assert ingress.control.people is startup.config.participants
    assert ingress.control.spool is startup.config.spool
    assert ingress.control.allowed_channels=={'C111'}

    path.chmod(0o644)
    with pytest.raises(ValueError,match='private Slack config'):
        configured_expert_slack_ingress(startup,str(path))
    path.chmod(0o600)
    signing.chmod(0o644)
    with pytest.raises(ValueError,match='private Slack config'):
        configured_expert_slack_ingress(startup,str(path))
    signing.chmod(0o600)
    path.write_text(json.dumps(dict(value,allowed_channels=['C111','C111'])))
    with pytest.raises(ValueError,match='exact Slack ingress config'):
        configured_expert_slack_ingress(startup,str(path))
    path.write_text(json.dumps(dict(value,unknown_field=True)))
    with pytest.raises(ValueError,match='exact Slack ingress config'):
        configured_expert_slack_ingress(startup,str(path))
    path.write_text('{"codec":"v1","codec":"v2"}')
    with pytest.raises(ValueError,match='duplicate Slack config'):
        configured_expert_slack_ingress(startup,str(path))
    path.write_text(json.dumps(value))
    # A symlink is not an opt-in installation, even to the exact private file.
    alias=tmp_path/'slack-symlink.json'
    alias.symlink_to(path)
    with pytest.raises(OSError):
        configured_expert_slack_ingress(startup,str(alias))


def test_fixed_entrypoint_requires_private_exact_metadata_before_any_owner_creation(tmp_path,monkeypatch):
    monkeypatch.delenv('RKB_KAGGLE_SOURCE_CONFIG',raising=False)
    with pytest.raises(RuntimeError,match='RKB_KAGGLE_SOURCE_CONFIG'):configured_source_service()
    path=tmp_path/'source-owner.json';path.write_text('{"codec":"unexpected"}');path.chmod(0o600)
    monkeypatch.setenv('RKB_KAGGLE_SOURCE_CONFIG',str(path))
    with pytest.raises(ValueError,match='exact source owner'):configured_source_service()
    path.chmod(0o644)
    with pytest.raises(ValueError,match='private bounded'):configured_source_service()


@pytest.mark.asyncio
async def test_server_main_uses_fixed_owner_composition_without_legacy_backend(owner,monkeypatch):
    import regional_knowledge.server as server
    e=owner;history=HistoryAdapter()
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=HandlerRegistry([handler()]),
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1')
    monkeypatch.setenv('RKB_BACKEND_PROFILE','kaggle_source');monkeypatch.setenv('RKB_MCP_PROFILE','live')
    monkeypatch.setattr('regional_knowledge.kaggle_source_owners.configured_source_service',lambda:startup)
    monkeypatch.setattr(server,'backend_from_env',lambda:pytest.fail('legacy fallback'))
    called=[]
    real_app=server.KnowledgeMCPServer.streamable_http_app
    def capture_app(instance,**options):
        called.append(instance._target_startup)
        return real_app(instance,**options)
    launched=[]
    monkeypatch.setattr(server.KnowledgeMCPServer,'streamable_http_app',capture_app)
    monkeypatch.setattr('regional_knowledge.http_shutdown.run_http_server',lambda app,**kwargs:launched.append(app))
    server.main()
    assert called==[startup] and len(launched)==1
    await startup.source_dispatch.aclose();startup.config.spool.close();startup.config.writer_owner.close()


@pytest.mark.asyncio
async def test_optional_importer_reuses_all_control_owners_without_provider_or_subprocess(owner,tmp_path,monkeypatch):
    from regional_knowledge.kaggle_import_composition import SourceImportOwners
    from regional_knowledge.kaggle_import_tokenizer import TokenizerProcessSpec
    from regional_knowledge.source_import_v2 import STORY_REVISIONS,source_import_v2_handler
    import asyncio
    e=owner;history=HistoryAdapter()
    def forbidden(*args,**kwargs):pytest.fail('construction cannot contact provider or start a process')
    monkeypatch.setattr(asyncio,'create_subprocess_exec',forbidden)
    imports=SourceImportOwners('a'*64,'b'*64,('https://existing-s3.fixture.invalid',),str(tmp_path/'tokenizer'),
        TokenizerProcessSpec('/operator/versioned-venv/bin/python','c'*64,'d'*64))
    assert SourceImportOwners.from_wire(imports.wire())==imports
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=HandlerRegistry([
            source_import_v2_handler(revisions=STORY_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)]),
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',source_import=imports)
    try:
        assert startup.importer.originals.queue is e.q is startup.config.spool.queue
        assert startup.importer.originals.spool is startup.config.spool
        assert startup.importer.tokenizer.root==tmp_path/'tokenizer'
        assert startup.importer.parser_sha256=='b'*64
    finally:
        await startup.source_dispatch.aclose()
        startup.config.spool.close();startup.config.writer_owner.close()
