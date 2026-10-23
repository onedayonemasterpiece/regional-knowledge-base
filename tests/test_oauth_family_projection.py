"""Actual encrypted synthetic issuance/SQL projection; no live credentials."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs,urlsplit
import hashlib
import json

import pytest
import pytest_asyncio

from regional_knowledge.oauth_provider import RegionalOAuthProvider,KNOWLEDGE_SCOPE,_digest
from regional_knowledge.personal_auth import PersonalPolicy
from regional_knowledge.oauth_family_projection import OwnerFamilyProjectionMigration,ProjectionMigrationError
from regional_knowledge.request_grants import reference
from test_oauth_provider import _params,_dynamic_client
from test_participants import people


@pytest_asyncio.fixture
async def legacy(people,tmp_path):
    p=people;p.corpus.path.chmod(0o600)
    provider=RegionalOAuthProvider(issuer='https://knowledge.example.test',resource='https://knowledge.example.test/mcp',
        client_id='synthetic-existing-client',client_secret='synthetic-'+64*'x',
        redirect_uris=['https://chatgpt.com/connector_platform_oauth_redirect'],owner_subject=p.owner.subject,
        owner_login_secret='synthetic-owner-login-secret',state_file=tmp_path/'oauth.state',key_file=tmp_path/'oauth.key')
    async def issue(client=None):
        client=client or provider.client
        url=await provider.authorize(client,_params())
        request_id=parse_qs(urlsplit(url).query)['id'][0]
        url=provider.approve(request_id)
        code=parse_qs(urlsplit(url).query)['code'][0]
        return await provider.exchange_authorization_code(client,await provider.load_authorization_code(client,code))
    tokens=await issue()
    assert await provider.load_access_token(tokens.access_token) is not None
    provider.personal_policy=PersonalPolicy(p.registry)
    migration=OwnerFamilyProjectionMigration(provider,corpus_path=p.corpus.path)
    return SimpleNamespace(p=p,provider=provider,tokens=tokens,issue=issue,migration=migration)


def rows(s):
    with s.p.corpus.connect() as db:return [dict(r) for r in db.execute('SELECT * FROM rkb_request_oauth_grants')]


def mutate(s,call):s.provider.store.mutate(call)


@pytest.mark.asyncio
async def test_exact_issued_access_continuity_without_refresh_and_idempotent_receipt(legacy):
    s=legacy;p=s.provider
    assert await p.load_access_token(s.tokens.access_token) is None
    state=p.store.state_file.read_bytes();key=p.store.key_file.read_bytes()
    review=s.migration.review()
    assert review.eligible_families==review.missing_projections==1 and not rows(s)
    assert p.store.state_file.read_bytes()==state and p.store.key_file.read_bytes()==key
    assert s.tokens.access_token not in repr(review) and s.tokens.refresh_token not in json.dumps(review.wire())
    with pytest.raises(ProjectionMigrationError,match='retained_operator'):s.migration.apply(replace(review))
    receipt=s.migration.apply(review)
    assert receipt['projected_families']==1 and receipt['issued_tokens']==receipt['changed_participant_grants']==0
    assert p.store.state_file.read_bytes()==state and p.store.key_file.read_bytes()==key
    assert await p.load_access_token(s.tokens.access_token) is not None
    state_rows=p.store.read(lambda state:[dict(r) for r in state['refresh'].values()])
    assert rows(s)[0]['expires_at']==state_rows[0]['expires_at']
    assert rows(s)[0]['id']==reference(state_rows[0]['family'])
    assert s.migration.apply(s.migration.review())==receipt
    with s.p.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_oauth_owner_projection_migrations').fetchone()[0]==1
        with pytest.raises(Exception,match='immutable'):db.execute("UPDATE rkb_oauth_owner_projection_migrations SET receipt_json='{}'")


@pytest.mark.asyncio
async def test_original_revoke_and_refresh_replay_still_close_projected_family(legacy):
    s=legacy;s.migration.apply(s.migration.review())
    old=await s.provider.load_refresh_token(s.provider.client,s.tokens.refresh_token)
    new=await s.provider.exchange_refresh_token(s.provider.client,old,[KNOWLEDGE_SCOPE])
    assert await s.provider.load_access_token(new.access_token) is not None
    # Normal rotation leaves used_refresh plus live current family; valid review.
    assert s.migration.review().eligible_families==1
    assert await s.provider.load_refresh_token(s.provider.client,s.tokens.refresh_token) is None
    assert await s.provider.load_access_token(new.access_token) is None
    assert rows(s)[0]['enabled']==0
    assert s.migration.review().eligible_families==0


@pytest.mark.asyncio
@pytest.mark.parametrize('change,reason',[
    (lambda r:r.pop('issuer',None),'unknown_issued_metadata'),
    (lambda r:r.update(issuer='https://other.invalid/'),'issued_scope_or_issuer_differs'),
    (lambda r:r.update(resource='https://other.invalid/mcp'),'issued_scope_or_issuer_differs'),
    (lambda r:r.update(client_id='unknown-unregistered-client'),'registered_client_unknown'),
    (lambda r:r.update(scopes=['participants:admin']),'issued_scope_or_issuer_differs'),
    (lambda r:r.update(subject='00000000-0000-4000-8000-000000000001'),'non_owner_family'),
])
async def test_unknown_or_changed_family_never_projects(legacy,change,reason):
    s=legacy
    mutate(s,lambda state:[change(r) for k in ('access','refresh') for r in state[k].values()])
    review=s.migration.review()
    assert review.eligible_families==0 and dict(review.rejected)[reason]==1 and not rows(s)


@pytest.mark.asyncio
async def test_mixed_family_and_revoke_or_owner_disable_after_review_fail_closed(legacy):
    s=legacy
    review=s.migration.review()
    record=await s.provider.load_refresh_token(s.provider.client,s.tokens.refresh_token)
    assert record is not None
    await s.provider.revoke_token(record)
    with pytest.raises(ProjectionMigrationError,match='changed_since_review'):s.migration.apply(review)
    assert not rows(s)
    # A revoked family leaves used-refresh/session metadata but no live records.
    assert s.migration.review().eligible_families==0


@pytest.mark.asyncio
async def test_disabled_or_conflicting_projection_blocks_all_rows_without_reenable(legacy):
    s=legacy
    review=s.migration.review();s.migration.apply(review)
    with s.p.corpus.connect() as db:db.execute('UPDATE rkb_request_oauth_grants SET enabled=0')
    with pytest.raises(ProjectionMigrationError,match='revoked_or_conflicting'):s.migration.review()
    assert rows(s)[0]['enabled']==0


@pytest.mark.asyncio
async def test_expired_access_is_not_revived_and_shorter_projection_is_not_extended(legacy):
    s=legacy;now=[s.provider.clock()];s.provider.clock=lambda:now[0]
    now[0]+=3601
    review=s.migration.review();assert review.eligible_families==1
    s.migration.apply(review)
    assert await s.provider.load_access_token(s.tokens.access_token) is None
    existing=rows(s)[0]['expires_at']
    with s.p.corpus.connect() as db:db.execute('UPDATE rkb_request_oauth_grants SET expires_at=?',(existing-100,))
    s.migration.apply(s.migration.review())
    assert rows(s)[0]['expires_at']==existing-100


@pytest.mark.asyncio
async def test_canonical_owner_disabled_after_review_prevents_any_projection(legacy):
    s=legacy;review=s.migration.review()
    s.p.corpus.put('rkb_users',[dict(id=s.p.owner.subject,status='disabled')])
    with pytest.raises(ProjectionMigrationError,match='active_canonical_owner'):s.migration.apply(review)
    assert not rows(s)


@pytest.mark.asyncio
async def test_registered_dynamic_client_is_exact_and_missing_metadata_stays_unprojected(legacy):
    s=legacy;s.provider.personal_policy=None
    client=_dynamic_client(redirect_uri=s.provider.redirect_uris[0]);await s.provider.register_client(client)
    token=await s.issue(client);s.provider.personal_policy=PersonalPolicy(s.p.registry)
    review=s.migration.review();assert review.eligible_families==2
    s.migration.apply(review)
    assert await s.provider.load_access_token(token.access_token) is not None
    # A typed registration stored under another digest is not this client.
    mutate(s,lambda state:state['clients'].__setitem__(_digest(client.client_id),dict(state['clients'][_digest(client.client_id)],client_id='changed-client')))
    review=s.migration.review()
    assert review.eligible_families==1 and dict(review.rejected)['registered_client_unknown']==1


@pytest.mark.asyncio
async def test_mixed_family_metadata_and_used_only_records_are_not_authority(legacy):
    s=legacy
    def mixed(state):next(iter(state['access'].values()))['client_id']='different-client'
    mutate(s,mixed)
    review=s.migration.review()
    assert review.eligible_families==0 and dict(review.rejected)['mixed_family_metadata']==1
    with pytest.raises(ProjectionMigrationError,match='no_current_owner'):s.migration.apply(review)
    def used_only(state):
        family=next(iter(state['refresh'].values()))['family']
        state['used_refresh']['synthetic-digest']={'family':family,'expires_at':s.provider.clock()+100}
        state['access'].clear();state['refresh'].clear()
    mutate(s,used_only)
    review=s.migration.review();assert review.eligible_families==0
    with pytest.raises(ProjectionMigrationError,match='no_current_owner'):s.migration.apply(review)
    assert not rows(s)
    with s.p.corpus.connect() as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='rkb_oauth_owner_projection_migrations'").fetchone()
