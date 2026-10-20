"""Current canonical read authority; all identities and credentials are synthetic."""
import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
import threading
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from mcp.server.auth.provider import AccessToken, AuthorizationParams
from pydantic import AnyUrl
import pytest
import pytest_asyncio

from regional_knowledge.oauth_provider import KNOWLEDGE_SCOPE, RegionalOAuthProvider
from regional_knowledge.participant_contracts import (
    AccountBegin, AccountConfirm, AccountUnlink, DomainWeight, ParticipantDisable,
    ParticipantGrant, ParticipantUpsert,
)
from regional_knowledge.participants import ParticipantError, RequestAuthority
from regional_knowledge.personal_auth import PersonalPolicy, new_login_code
from regional_knowledge.read_authority import ReadAuthority, ReadAuthorityError, TrustedReadDestination
from test_participants import people, change, bind, principal


async def issue(provider, code=None):
    url = await provider.authorize(provider.client, AuthorizationParams(
        state='synthetic', scopes=[KNOWLEDGE_SCOPE], code_challenge='synthetic-challenge',
        redirect_uri=AnyUrl(provider.redirect_uris[0]), redirect_uri_provided_explicitly=True,
        resource=provider.resource))
    request_id = parse_qs(urlsplit(url).query)['id'][0]
    record = None
    if code:
        raw = await asyncio.to_thread(provider._login, code)
        record = await asyncio.to_thread(provider._session_record, raw)
    target = await asyncio.to_thread(provider.approve, request_id, session_record=record)
    code = parse_qs(urlsplit(target).query)['code'][0]
    loaded = await provider.load_authorization_code(provider.client, code)
    return await provider.exchange_authorization_code(provider.client, loaded)


@pytest_asyncio.fixture
async def auth(people, tmp_path):
    p = people
    now = [time.time()]
    p.registry.clock = lambda: datetime.fromtimestamp(now[0], timezone.utc)
    for capability in ('public_search', 'scan_proof_read', 'case_read'):
        change(p, ParticipantGrant(capability=capability, granted=True, reason='synthetic read capability'))
    provider = RegionalOAuthProvider(issuer='https://knowledge.example.test',
        resource='https://knowledge.example.test/mcp', client_id='synthetic-authority',
        client_secret='synthetic-client-' + 'x'*40, redirect_uris=['https://chatgpt.com/connector_platform_oauth_redirect'],
        owner_subject=p.owner.subject, owner_login_secret='synthetic-owner-login',
        state_file=tmp_path/'oauth.state', key_file=tmp_path/'oauth.key', clock=lambda: now[0])
    provider.personal_policy = PersonalPolicy(p.registry)
    ident, code = new_login_code()
    provider.issue_personal_credential(p.admin, p.other.subject, login_code=code,
        expires_at=now[0]+86400, reason='synthetic enrollment')
    tokens = await issue(provider, code)
    resolver = ReadAuthority(p.registry, oauth_provider=provider, issuer=provider.issuer,
                             resource=provider.resource, clock=lambda: now[0])
    destination = TrustedReadDestination('mcp', 'personal', 'current-response')
    return SimpleNamespace(p=p, provider=provider, ident=ident, code=code, tokens=tokens,
                           resolver=resolver, destination=destination, now=now)


@pytest.mark.asyncio
async def test_real_opaque_oauth_identity_and_frozen_internal_context(auth):
    a = auth
    context = await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    assert context.person_id == a.p.other.subject
    assert context.auth_revision == a.p.registry.get(a.p.admin, a.p.other.subject)['revision']
    assert context.route == 'personal' and not context.public_only
    assert await a.resolver.require(context, 'public_search') == a.p.other.subject
    assert await a.resolver.artifact_eligibility(context, 'accepted_public_text')
    assert await a.resolver.artifact_eligibility(context, 'public_illustration')
    assert await a.resolver.artifact_eligibility(context, 'scan_proof')
    assert a.tokens.access_token not in repr(context) and a.code not in repr(context)
    with pytest.raises(FrozenInstanceError):
        context.person_id = a.p.owner.subject
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(replace(context))  # Same fields do not make an issued object.
    for supplied in (a.p.owner.subject, 'eyJhbGciOiJub25lIn0.' + a.p.owner.subject, 'unknown'):
        with pytest.raises(ReadAuthorityError):
            await a.resolver.resolve_oauth(supplied, destination=a.destination)
    loaded = await a.provider.load_access_token(a.tokens.access_token)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_oauth(loaded, destination=a.destination)


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [
    ('issuer','https://other.example/'), ('resource','https://other.example/mcp'),
    ('client_id','forged-client'), ('person_id',str(uuid4())),
])
async def test_exact_current_family_projection_cannot_be_forged(auth, field, value):
    a=auth
    loaded = await a.provider.load_access_token(a.tokens.access_token)
    ident = loaded.claims['rkb_request_grant']
    with a.p.corpus.connect() as db:
        # field names are static fixture parameters, not request SQL.
        db.execute(f'UPDATE rkb_request_oauth_grants SET {field}=? WHERE id=?', (value, ident))
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['grant', 'disable', 'profile', 'weight', 'user_status'])
async def test_current_changes_invalidate_inflight_authority(auth, kind):
    a=auth
    context=await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    if kind=='grant':
        change(a.p, ParticipantGrant(capability='scan_proof_read', granted=False, reason='revoke'))
    elif kind=='disable':
        change(a.p, ParticipantDisable(disabled=True, reason='disable'))
    elif kind=='profile':
        change(a.p, ParticipantUpsert(name='Synthetic student', categories={'student'}, reason='change'))
    elif kind=='weight':
        change(a.p, DomainWeight(domain='synthetic', weight='high', basis='not a capability'))
    else:
        a.p.corpus.put('rkb_users',[{'id':a.p.other.subject,'status':'disabled'}])
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(context)


@pytest.mark.asyncio
async def test_profile_grant_credential_and_context_expiry_are_independently_checked(auth):
    a=auth
    expires=datetime.fromtimestamp(a.now[0]+2, timezone.utc)
    change(a.p, ParticipantGrant(capability='scan_proof_read', granted=True, expires_at=expires, reason='short grant'))
    context=await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    a.now[0]+=3
    # Expiry is not a revision change: the current capability still disappears.
    assert await a.resolver.artifact_eligibility(context, 'public_illustration')
    with pytest.raises(ReadAuthorityError):
        await a.resolver.artifact_eligibility(context, 'scan_proof')
    ident,code=new_login_code()
    a.provider.issue_personal_credential(a.p.admin, a.p.other.subject, login_code=code,
        expires_at=a.now[0]+2, reason='short credential')
    tokens=await issue(a.provider,code)
    short=await a.resolver.resolve_oauth(tokens.access_token,destination=a.destination)
    a.now[0]+=3
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(short)
    change(a.p, ParticipantUpsert(name='Expiring person',categories={'expert'},
        expires_at=datetime.fromtimestamp(a.now[0]+2,timezone.utc),reason='profile expiry'))
    profile=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    a.now[0]+=3
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(profile)
    a.now[0]+=200
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(context)


@pytest.mark.asyncio
@pytest.mark.parametrize('revoke',['family','credential','refresh_replay'])
async def test_oauth_family_revocation_cancels_inflight_without_bearer_retention(auth,revoke):
    a=auth
    context=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    loaded=await a.provider.load_access_token(a.tokens.access_token)
    if revoke=='family':
        await a.provider.revoke_token(loaded)
    elif revoke=='credential':
        await asyncio.to_thread(a.provider.revoke_personal_credential,a.p.admin,a.ident,
                                expected_epoch=1,reason='synthetic revoke')
    else:
        refresh=await a.provider.load_refresh_token(a.provider.client,a.tokens.refresh_token)
        await a.provider.exchange_refresh_token(a.provider.client,refresh,[KNOWLEDGE_SCOPE])
        assert await a.provider.load_refresh_token(a.provider.client,a.tokens.refresh_token) is None
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(context)


@pytest.mark.asyncio
async def test_verified_multiple_providers_share_person_and_tenants_do_not_collide(auth):
    a=auth
    bind(a.p)
    link=change(a.p,AccountBegin(provider='projects_hub',tenant='project-1',provider_subject='usr_1',reason='link'))
    observation=a.p.registry.observe_account(link_id=link['link_id'],link_revision=1,provider='projects_hub',
        tenant='project-1',provider_subject='usr_1',audience='rkb-account-binding',
        expires_at=a.p.registry._time()+timedelta(minutes=5),receipt_id='synthetic-verified-transport')
    change(a.p,AccountConfirm(link_id=link['link_id'],expected_link_revision=1,method='provider_observation',
        observation_id=observation,reason='confirm exact verified provider'))
    one=await a.resolver.resolve_adapter(provider='slack',tenant='T111',provider_subject='U111',destination=a.destination)
    two=await a.resolver.resolve_adapter(provider='projects_hub',tenant='project-1',provider_subject='usr_1',destination=a.destination)
    assert one.person_id==two.person_id==a.p.other.subject
    other=principal()
    a.p.registry.change(a.p.admin,other.subject,0,ParticipantUpsert(name='Other tenant',reason='create'), 'other-person-created')
    change(a.p,ParticipantGrant(capability='public_search',granted=True,reason='public search'),person=other.subject)
    other_link=change(a.p,AccountBegin(provider='slack',tenant='T222',provider_subject='U111',reason='other tenant'),person=other.subject)
    change(a.p,AccountConfirm(link_id=other_link['link_id'],expected_link_revision=1,method='owner_confirmed_slack',reason='confirm'),person=other.subject)
    collision=await a.resolver.resolve_adapter(provider='slack',tenant='T222',provider_subject='U111',destination=a.destination)
    assert collision.person_id==other.subject
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_adapter(provider='slack',tenant='T333',provider_subject='U111',destination=a.destination)
    change(a.p,AccountUnlink(link_id=one.link_id,expected_link_revision=one.link_revision,reason='unlink'))
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(one)


@pytest.mark.asyncio
async def test_group_scope_never_unions_personal_proof_rights_and_originals_stay_independent(auth):
    a=auth
    group=TrustedReadDestination('slack','group','T111/C111')
    context=await a.resolver.resolve_oauth(a.tokens.access_token,destination=group)
    assert context.public_only
    assert await a.resolver.artifact_eligibility(context,'public_illustration')
    assert await a.resolver.artifact_eligibility(context,'accepted_public_text')
    for artifact in ('scan_proof','full_original','private_draft','caller-labelled-public-scan'):
        with pytest.raises(ReadAuthorityError):
            await a.resolver.artifact_eligibility(context,artifact)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.require(context,'case_read')
    # An explicit grant alone cannot turn a student into scan-proof eligibility.
    change(a.p,ParticipantUpsert(name='Student',categories={'student'},reason='student'))
    personal=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.artifact_eligibility(personal,'scan_proof')


@pytest.mark.asyncio
async def test_ready_reads_do_no_projection_write_fsync_or_event_loop_io(auth,monkeypatch):
    a=auth
    import regional_knowledge.request_grants as grants
    def forbidden(*args,**kwargs):
        raise AssertionError('ready read performed authority write')
    monkeypatch.setattr(grants,'save',forbidden)
    monkeypatch.setattr(a.provider.store,'_write',forbidden)
    loop_thread=threading.get_ident()
    original_read=a.provider.store.read
    def read(callback):
        assert threading.get_ident()!=loop_thread
        return original_read(callback)
    monkeypatch.setattr(a.provider.store,'read',read)
    original_state=a.resolver._state
    def state(*args,**kwargs):
        assert threading.get_ident()!=loop_thread
        return original_state(*args,**kwargs)
    monkeypatch.setattr(a.resolver,'_state',state)
    before=(a.provider.store.state_file.read_bytes(),a.provider.store.state_file.stat().st_mtime_ns)
    context=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    await a.resolver.recheck(context)
    assert before==(a.provider.store.state_file.read_bytes(),a.provider.store.state_file.stat().st_mtime_ns)
    with a.resolver._read() as db:
        with pytest.raises(Exception,match='readonly'):
            db.execute('DELETE FROM rkb_request_oauth_grants')


@pytest.mark.asyncio
async def test_context_release_during_recheck_and_capacity_are_fail_closed(auth,monkeypatch):
    a=auth
    small=ReadAuthority(a.p.registry,oauth_provider=a.provider,issuer=a.provider.issuer,
        resource=a.provider.resource,clock=lambda:a.now[0],max_contexts=1,ttl_seconds=1)
    first=await small.resolve_oauth(a.tokens.access_token,destination=a.destination)
    with pytest.raises(ReadAuthorityError):
        await small.resolve_oauth(a.tokens.access_token,destination=a.destination)
    a.now[0]+=2
    second=await small.resolve_oauth(a.tokens.access_token,destination=a.destination)
    assert len(small._contexts)==1 and first.context_ref!=second.context_ref
    entered,release=threading.Event(),threading.Event()
    original=small._state
    def blocked(*args,**kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args,**kwargs)
    monkeypatch.setattr(small,'_state',blocked)
    task=asyncio.create_task(small.recheck(second))
    assert await asyncio.to_thread(entered.wait,2)
    small.release(second)
    release.set()
    with pytest.raises(ReadAuthorityError):
        await task


@pytest.mark.asyncio
async def test_deferred_request_context_has_no_executor_or_uuid_authority(auth):
    a=auth
    context=RequestAuthority(a.p.other,'delegated',request_id='unavailable-durable-request',
                             request_generation=1,executor_id='missing-executor',execution_token='untrusted')
    with pytest.raises(ParticipantError) as error:
        a.p.registry.require(context,'public_search')
    assert error.value.code=='request_fence_denied'


@pytest.mark.asyncio
async def test_unprojected_legacy_family_needs_real_refresh_and_configuration_is_exact(auth):
    a=auth
    with a.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_request_oauth_grants')
    assert await a.provider.load_access_token(a.tokens.access_token) is None
    refresh=await a.provider.load_refresh_token(a.provider.client,a.tokens.refresh_token)
    rotated=await a.provider.exchange_refresh_token(a.provider.client,refresh,[KNOWLEDGE_SCOPE])
    context=await a.resolver.resolve_oauth(rotated.access_token,destination=a.destination)
    assert context.person_id==a.p.other.subject
    with pytest.raises(ValueError):
        ReadAuthority(a.p.registry,oauth_provider=a.provider,issuer=a.provider.issuer.rstrip('/'),resource=a.provider.resource)


@pytest.mark.asyncio
async def test_owner_group_is_public_only_and_destination_must_be_internal(auth):
    a=auth
    owner_tokens=await issue(a.provider)
    group=TrustedReadDestination('slack','group','T111/C111')
    owner=await a.resolver.resolve_oauth(owner_tokens.access_token,destination=group)
    assert owner.person_id==a.p.owner.subject and owner.public_only
    assert await a.resolver.artifact_eligibility(owner,'public_illustration')
    with pytest.raises(ReadAuthorityError):
        await a.resolver.artifact_eligibility(owner,'scan_proof')
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_oauth(a.tokens.access_token,destination={'audience':'personal','person':a.p.owner.subject})
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_adapter(provider='slack',tenant='T111',provider_subject='U111',
                                         destination={'audience':'personal'})
