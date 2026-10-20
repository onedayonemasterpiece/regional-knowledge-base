"""Synthetic regressions for the source-only exact-model authority audit."""
import asyncio
import base64
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from urllib.parse import parse_qs, urlsplit

import pytest
from starlette.testclient import TestClient

from regional_knowledge.participant_contracts import ParticipantDisable, ParticipantGrant, ParticipantUpsert
from regional_knowledge.participants import ParticipantError
from regional_knowledge.read_authority import ReadAuthority, ReadAuthorityError
from regional_knowledge.server import build_server, _transport_security
from regional_knowledge.backend import UnavailableBackend
from test_participants import people, bind, change
from test_read_authority import auth, issue


@contextmanager
def http(a):
    server = build_server(backend=UnavailableBackend(), issuer=a.provider.issuer,
                          resource_url=a.provider.resource, oauth_provider=a.provider)
    with TestClient(server.streamable_http_app(stateless_http=True, json_response=True,
            transport_security=_transport_security(a.provider.resource)),
            base_url=a.provider.origin) as client:
        credentials = f'{a.provider.client.client_id}:{a.provider.client.client_secret}'
        client.headers['Authorization'] = 'Basic ' + base64.b64encode(credentials.encode()).decode()
        yield client


def http_issue(a, client):
    p = a.provider
    verifier = 'v' * 64
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    start = client.get('/authorize', params={
        'client_id': p.client.client_id, 'redirect_uri': p.redirect_uris[0],
        'response_type': 'code', 'code_challenge': challenge, 'code_challenge_method': 'S256',
        'state': 'synthetic', 'resource': p.resource}, follow_redirects=False)
    assert start.status_code == 302
    consent = start.headers['location']
    request_id = parse_qs(urlsplit(consent).query)['id'][0]
    assert client.get(consent).status_code == 200
    login = client.post('/oauth/login', data={'id': request_id,
        'csrf': client.cookies.get('__Host-rkb_csrf'), 'secret': a.code},
        headers={'Origin': p.origin}, follow_redirects=False)
    assert login.status_code == 303
    assert client.get(login.headers['location']).status_code == 200
    approved = client.post('/oauth/consent', data={'id': request_id,
        'csrf': client.cookies.get('__Host-rkb_csrf'), 'decision': 'allow'},
        headers={'Origin': p.origin}, follow_redirects=False)
    assert approved.status_code == 303
    code = parse_qs(urlsplit(approved.headers['location']).query)['code'][0]
    result = client.post('/token', data={**client_fields(a), 'grant_type': 'authorization_code',
        'code': code, 'code_verifier': verifier, 'redirect_uri': p.redirect_uris[0],
        'resource': p.resource})
    assert result.status_code == 200, result.text
    return result.json()


def client_fields(a):
    return {'client_id': a.provider.client.client_id,
            'client_secret': a.provider.client.client_secret}


@pytest.mark.asyncio
@pytest.mark.parametrize('condition', ['disabled', 'unprojected'])
@pytest.mark.parametrize('token_kind', ['access_token', 'refresh_token'])
async def test_http_revoke_ineligible_family_cannot_resurrect(auth, condition, token_kind):
    a = auth
    with http(a) as client:
        tokens = http_issue(a, client)
        loaded = await a.provider.load_access_token(tokens['access_token'])
        family = loaded.claims['rkb_request_grant']
        if condition == 'disabled':
            change(a.p, ParticipantDisable(disabled=True, reason='synthetic suspension'))
        else:
            with a.p.corpus.connect() as db:
                db.execute('DELETE FROM rkb_request_oauth_grants WHERE id=?', (family,))
        result = client.post('/revoke', data={**client_fields(a), 'token': tokens[token_kind]})
        assert result.status_code == 200
        if condition == 'disabled':
            change(a.p, ParticipantDisable(disabled=False, reason='synthetic restore'))
            with a.p.corpus.connect() as db:
                assert db.execute('SELECT enabled FROM rkb_request_oauth_grants WHERE id=?', (family,)).fetchone()[0] == 0
        refreshed = client.post('/token', data={**client_fields(a), 'grant_type': 'refresh_token',
            'refresh_token': tokens['refresh_token'], 'resource': a.provider.resource})
        assert refreshed.status_code == 400 and refreshed.json()['error'] == 'invalid_grant'
        assert await a.provider.load_access_token(tokens['access_token']) is None
        assert await a.provider.load_refresh_token(a.provider.client, tokens['refresh_token']) is None
        with pytest.raises(ReadAuthorityError):
            await a.resolver.resolve_oauth(tokens['access_token'], destination=a.destination)


def test_owner_profile_cannot_expire(people):
    p = people
    with pytest.raises(ParticipantError) as error:
        p.registry.change(p.admin, p.owner.subject, 0, ParticipantUpsert(name='Owner',
            expires_at=p.clock[0], reason='expiry bypass'), 'owner-expiry-test')
    assert error.value.code == 'cannot_expire_owner'


@pytest.mark.asyncio
@pytest.mark.parametrize('gate', ['disabled_profile', 'canonical_user_status'])
async def test_legacy_owner_expiry_can_be_repaired_without_bypassing_disabled_policy(auth, gate):
    a = auth
    p = a.p
    p.registry.change(p.admin, p.owner.subject, 0, ParticipantUpsert(name='Owner', reason='setup'), 'owner-profile-test')
    with p.corpus.connect() as db:
        row = db.execute('SELECT payload FROM rkb_participant_profiles WHERE person_id=?', (p.owner.subject,)).fetchone()
        payload = json.loads(row[0]); payload['expires_at'] = datetime.fromtimestamp(a.now[0]-1, timezone.utc).isoformat()
        db.execute('UPDATE rkb_participant_profiles SET payload=? WHERE person_id=?', (json.dumps(payload), p.owner.subject))
    owner_tokens = await issue(a.provider)
    context = await a.resolver.resolve_oauth(owner_tokens.access_token, destination=a.destination)
    assert await a.resolver.require(context, 'public_search') == p.owner.subject
    assert a.provider.personal_policy.require_admin(p.admin) == p.owner.subject
    change(p, ParticipantUpsert(name='Owner repaired', reason='remove legacy expiry'), person=p.owner.subject)
    assert p.registry.get(p.admin, p.owner.subject)['profile']['expires_at'] is None
    # The recovery exception is only profile expiry, never disabled/user status.
    if gate == 'disabled_profile':
        with p.corpus.connect() as db:
            db.execute('UPDATE rkb_participant_profiles SET disabled=1 WHERE person_id=?', (p.owner.subject,))
    else:
        p.corpus.put('rkb_users', [{'id': p.owner.subject, 'status': 'disabled'}])
    with pytest.raises(ParticipantError):
        a.provider.personal_policy.require_admin(p.admin)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.resolve_oauth(owner_tokens.access_token, destination=a.destination)


@pytest.mark.asyncio
async def test_adapter_resolves_link_and_policy_in_one_read_snapshot(auth, monkeypatch):
    a = auth
    bind(a.p)
    original = a.resolver._read
    opened = []
    @contextmanager
    def counted():
        with original() as db:
            opened.append(db)
            yield db
    monkeypatch.setattr(a.resolver, '_read', counted)
    context = await a.resolver.resolve_adapter(provider='slack', tenant='T111', provider_subject='U111', destination=a.destination)
    assert context.person_id == a.p.other.subject
    assert len(opened) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['revoked', 'expired', 'state_error', 'revision'])
async def test_dead_context_recheck_frees_only_identical_retained_slot(auth, monkeypatch, failure):
    a = auth
    context = await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(replace(context))
    assert a.resolver._contexts[context.context_ref] is context
    if failure == 'revoked':
        change(a.p, ParticipantDisable(disabled=True, reason='revocation'))
    elif failure == 'expired':
        a.now[0] += 121
    elif failure == 'revision':
        change(a.p, ParticipantGrant(capability='scan_proof_read', granted=False, reason='revision change'))
    else:
        def fail(*args, **kwargs):
            raise ReadAuthorityError('synthetic read failure')
        monkeypatch.setattr(a.resolver, '_state', fail)
    with pytest.raises(ReadAuthorityError):
        await a.resolver.recheck(context)
    assert context.context_ref not in a.resolver._contexts


@pytest.mark.asyncio
async def test_canonical_person_cap_shared_across_oauth_and_adapter_preserves_other_person_room(auth):
    a = auth
    bind(a.p)
    resolver = ReadAuthority(a.p.registry, oauth_provider=a.provider, issuer=a.provider.issuer,
        resource=a.provider.resource, clock=lambda: a.now[0], max_contexts=4, max_contexts_per_person=2)
    first = await resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    second = await resolver.resolve_adapter(provider='slack', tenant='T111', provider_subject='U111', destination=a.destination)
    with pytest.raises(ReadAuthorityError):
        await resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    owner_tokens = await issue(a.provider)
    owner = await resolver.resolve_oauth(owner_tokens.access_token, destination=a.destination)
    assert owner.person_id == a.p.owner.subject and len(resolver._contexts) == 3
    resolver.release(first); resolver.release(second); resolver.release(owner)


@pytest.mark.parametrize('cap', [0, -1, True, 1.0, 5])
@pytest.mark.asyncio
async def test_per_person_cap_validation(auth, cap):
    a = auth
    with pytest.raises(ValueError, match='per-person'):
        ReadAuthority(a.p.registry, oauth_provider=a.provider, issuer=a.provider.issuer,
            resource=a.provider.resource, max_contexts=4, max_contexts_per_person=cap)


@pytest.mark.asyncio
async def test_recheck_cancellation_releases_slot_and_preserves_cancellation(auth, monkeypatch):
    a = auth
    context = await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    def cancelled(*args, **kwargs):
        raise asyncio.CancelledError()
    monkeypatch.setattr(a.resolver, '_state', cancelled)
    with pytest.raises(asyncio.CancelledError):
        await a.resolver.recheck(context)
    assert context.context_ref not in a.resolver._contexts


@pytest.mark.asyncio
async def test_ready_reads_deny_all_sqlite_mutations_with_authorizer(auth, monkeypatch):
    a = auth
    original = sqlite3.connect
    denied = []
    mutations = {getattr(sqlite3, name) for name in (
        'SQLITE_INSERT', 'SQLITE_UPDATE', 'SQLITE_DELETE', 'SQLITE_CREATE_INDEX', 'SQLITE_CREATE_TABLE',
        'SQLITE_CREATE_TRIGGER', 'SQLITE_CREATE_VIEW', 'SQLITE_DROP_INDEX', 'SQLITE_DROP_TABLE',
        'SQLITE_DROP_TRIGGER', 'SQLITE_DROP_VIEW', 'SQLITE_ALTER_TABLE', 'SQLITE_ATTACH', 'SQLITE_DETACH')}
    def connect(*args, **kwargs):
        db = original(*args, **kwargs)
        def authorize(action, arg1, arg2, database, source):
            if action in mutations or (action == sqlite3.SQLITE_PRAGMA and arg2 is not None
                                      and arg1 not in {'query_only', 'busy_timeout', 'foreign_keys'}):
                denied.append(action)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)
        return db
    monkeypatch.setattr(sqlite3, 'connect', connect)
    context = await a.resolver.resolve_oauth(a.tokens.access_token, destination=a.destination)
    await a.resolver.recheck(context)
    assert denied == []
