"""Same existing encrypted auth authority, synthetic credentials only."""
import json
import threading
from dataclasses import replace
from uuid import uuid4

import pytest

from regional_knowledge.personal_auth_cli import issue_service_to_file
from regional_knowledge.service_auth import new_service_token,service_scope
from regional_knowledge.oauth_provider import RegionalOAuthProvider
from regional_knowledge.personal_auth import PersonalPolicy
from test_read_authority import auth
from test_participants import people


def scope():return dict(operation='knowledge_search',provider='projects_hub',provider_tenant='ph.fixture',workspace_ids=['ws_fixture'])


def issue(a):
    ident,token=new_service_token()
    result=a.provider.issue_service_credential(a.p.admin,service_id='projects-hub-reader',token=token,
        scope=scope(),expires_at=a.now[0]+86400,reason='synthetic scoped service')
    return ident,token,result


def verify(a,ident,token,**changes):
    return a.provider.verify_service_credential(token,**dict(dict(service_id='projects-hub-reader',credential_id=ident,
        epoch=1,provider_tenant='ph.fixture',workspace_id='ws_fixture'),**changes))


@pytest.mark.asyncio
async def test_owner_service_issue_resume_uses_same_store_and_never_issues_person_rights(auth,tmp_path):
    a=auth;private=tmp_path/'private';private.mkdir(mode=0o700);output=private/'credential.json'
    before_key=a.provider.store.key_file.read_bytes()
    result=issue_service_to_file(a.provider,a.p.admin,'projects-hub-reader',scope(),output,reason='synthetic scoped service')
    resumed=issue_service_to_file(a.provider,a.p.admin,'projects-hub-reader',scope(),output,reason='synthetic scoped service',resume=True)
    assert result==resumed and output.stat().st_mode&0o077==0
    stored=json.loads(output.read_text());token=stored['service_token'];ident=result['credential_id']
    assert token not in repr(result) and 'secret_hash' not in repr(result) and a.provider.store.key_file.read_bytes()==before_key
    verified=verify(a,ident,token)
    assert verified and a.provider.service_credential_current(verified) and token not in repr(verified)
    assert await a.provider.load_access_token(token) is None
    with pytest.raises(Exception):await a.resolver.resolve_oauth(token,destination=a.destination)
    assert a.provider.service_credentials(a.p.admin)==[result]
    assert len(a.provider.service_audit(a.p.admin,credential_id=ident))==1
    assert not verify(a,ident,token,workspace_id='ws_other')
    assert not verify(a,ident,token,provider_tenant='ph.other')
    assert not verify(a,ident,token,epoch=2)
    assert not verify(a,str(uuid4()),token)
    assert not verify(a,ident,a.tokens.access_token)
    assert not a.provider.service_credential_current(replace(verified,epoch=2))
    with pytest.raises(Exception):a.provider.issue_service_credential(
        a.p.personal,service_id='other',token=token,scope=scope(),expires_at=a.now[0]+86400,reason='forbidden')


@pytest.mark.asyncio
async def test_durable_revoke_epoch_exact_retry_audit_and_restart_preserve_other_oauth(auth):
    a=auth;ident,token,metadata=issue(a);verified=verify(a,ident,token)
    revoked=a.provider.revoke_service_credential(a.p.admin,ident,expected_epoch=1,reason='synthetic revoke')
    assert revoked['epoch']==2 and not revoked['enabled'] and not a.provider.service_credential_current(verified)
    assert a.provider.revoke_service_credential(a.p.admin,ident,expected_epoch=1,reason='synthetic revoke')==revoked
    assert len(a.provider.service_audit(a.p.admin,credential_id=ident))==2
    with pytest.raises(ValueError):a.provider.revoke_service_credential(a.p.admin,ident,expected_epoch=1,reason='different revoke')
    restored=RegionalOAuthProvider(issuer=a.provider.issuer,resource=a.provider.resource,
        client_id=a.provider.client.client_id,client_secret=str(a.provider.client.client_secret),
        redirect_uris=list(a.provider.redirect_uris),owner_subject=a.p.owner.subject,owner_login_secret='synthetic-owner-login',
        state_file=a.provider.store.state_file,key_file=a.provider.store.key_file,clock=lambda:a.now[0])
    restored.personal_policy=PersonalPolicy(a.p.registry)
    assert not restored.service_credential_current(verified)
    assert restored.service_credentials(a.p.admin)==[revoked]
    assert await restored.load_access_token(a.tokens.access_token) is not None
    assert len(restored.service_audit(a.p.admin,credential_id=ident))==2


@pytest.mark.asyncio
async def test_current_epoch_fence_fails_closed_without_waiting_for_busy_auth_writer(auth):
    a=auth;ident,token,_=issue(a);verified=verify(a,ident,token)
    held=threading.Event();release=threading.Event()
    def busy():
        with a.provider.store._lock:held.set();release.wait(2)
    worker=threading.Thread(target=busy);worker.start();assert held.wait(1)
    try:
        import time
        start=time.monotonic()
        assert not a.provider.service_credential_current(verified)
        assert time.monotonic()-start<.1
    finally:release.set();worker.join()
    assert a.provider.service_credential_current(verified)
    a.now[0]+=86401
    assert not a.provider.service_credential_current(verified)


@pytest.mark.parametrize('change',[{'operation':'import'},{'provider':'slack'},{'workspace_ids':[]},
    {'workspace_ids':['ws_fixture','ws_fixture']},{'unexpected':True}])
def test_service_scope_cannot_expand_into_human_or_mutation_authority(change):
    with pytest.raises(ValueError):service_scope(dict(scope(),**change))


@pytest.mark.asyncio
async def test_lost_issue_and_revoke_response_reconcile_same_file_and_epoch(auth,tmp_path,monkeypatch):
    a=auth;private=tmp_path/'private';private.mkdir(mode=0o700);output=private/'credential.json'
    original_read=a.provider.store.read
    failed=[False]
    def read(callback):
        if not failed[0]:failed[0]=True;raise OSError('synthetic lost post-commit readback')
        return original_read(callback)
    monkeypatch.setattr(a.provider.store,'read',read)
    with pytest.raises(OSError):issue_service_to_file(a.provider,a.p.admin,'projects-hub-reader',scope(),output,reason='same exact issue')
    first=output.read_bytes()
    result=issue_service_to_file(a.provider,a.p.admin,'projects-hub-reader',scope(),output,reason='same exact issue',resume=True)
    assert output.read_bytes()==first
    ident=result['credential_id']
    assert len(a.provider.service_audit(a.p.admin,credential_id=ident))==1
    original_mutate=a.provider.store.mutate;failed[0]=False
    def mutate(callback):
        result=original_mutate(callback)
        if not failed[0]:failed[0]=True;raise OSError('synthetic lost revoke response')
        return result
    monkeypatch.setattr(a.provider.store,'mutate',mutate)
    with pytest.raises(OSError):a.provider.revoke_service_credential(a.p.admin,ident,expected_epoch=1,reason='same exact revoke')
    token=json.loads(first)['service_token']
    assert verify(a,ident,token) is None
    result=a.provider.revoke_service_credential(a.p.admin,ident,expected_epoch=1,reason='same exact revoke')
    assert result['epoch']==2 and not result['enabled']
    assert len(a.provider.service_audit(a.p.admin,credential_id=ident))==2


@pytest.mark.asyncio
async def test_real_cli_emits_metadata_only_and_reuses_same_authority(auth,tmp_path,monkeypatch,capsys):
    from regional_knowledge import personal_auth_cli as cli
    a=auth;private=tmp_path/'private';private.mkdir(mode=0o700)
    scope_file=private/'scope.json';scope_file.write_text(json.dumps(scope()));scope_file.chmod(0o600)
    output=private/'service.json';monkeypatch.setattr(cli,'_runtime',lambda:(a.provider,a.p.admin))
    cli.main(['service','issue','--service-id','projects-hub-reader','--scope-file',str(scope_file),
        '--output',str(output),'--reason','synthetic CLI provisioning'])
    result=json.loads(capsys.readouterr().out);raw=json.loads(output.read_text())
    assert result['credential_id']==raw['credential_id'] and 'service_token' not in result
    cli.main(['service','revoke','--credential',result['credential_id'],'--expected-epoch','1','--reason','synthetic CLI revocation'])
    revoked=json.loads(capsys.readouterr().out)
    assert revoked['epoch']==2 and raw['service_token'] not in repr(revoked)


@pytest.mark.asyncio
async def test_final_epoch_check_is_nonblocking_across_existing_auth_file_lock(auth):
    import fcntl
    import os
    a=auth;ident,token,_=issue(a);verified=verify(a,ident,token)
    path=a.provider.store.state_file.with_name(a.provider.store.state_file.name+'.lock')
    fd=os.open(path,os.O_RDWR)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert not a.provider.service_credential_current(verified)
    finally:fcntl.flock(fd,fcntl.LOCK_UN);os.close(fd)
    assert a.provider.service_credential_current(verified)
