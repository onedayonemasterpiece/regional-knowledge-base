"""Offline projection and real OAuth refresh; every identity/token is synthetic."""
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from regional_knowledge.authority_projection import COLUMNS, export_authority
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.participants import Participants, ParticipantError
from regional_knowledge.personal_auth import PersonalPolicy
from test_participants import people, change, bind
from regional_knowledge.participant_contracts import ParticipantGrant, ParticipantDisable


def private(tmp_path):
    target = tmp_path / 'private'
    target.mkdir(mode=0o700)
    return target


def minimal(tmp_path):
    path = tmp_path / 'legacy.sqlite3'
    owner = str(uuid4())
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE corpus_rows(table_name TEXT NOT NULL,row_key TEXT NOT NULL,document_id TEXT,revision INTEGER,payload TEXT NOT NULL,PRIMARY KEY(table_name,row_key))')
        for ident in (owner, str(uuid4()), str(uuid4()), str(uuid4())):
            db.execute('INSERT INTO corpus_rows VALUES(?,?,NULL,NULL,?)', ('rkb_users', ident,
                json.dumps({'id': ident, 'status': 'active', 'metadata': {}})))
        db.execute("INSERT INTO corpus_rows VALUES('rkb_stories','old-story',NULL,NULL,?)", (json.dumps({'body': 'LEGACY_BODY_MUST_NOT_COPY'}),))
        db.execute('CREATE TABLE story_jobs(id TEXT,state TEXT,payload TEXT)')
        db.execute("INSERT INTO story_jobs VALUES('pending','awaiting_agent','PRIVATE_JOB_MUST_NOT_COPY')")
    return path, owner


def test_four_user_projection_never_copies_corpus_or_creates_grants(tmp_path):
    source, owner = minimal(tmp_path)
    before = source.read_bytes()
    target = private(tmp_path) / 'candidate.sqlite3'
    receipt = export_authority(source, target, owner_subject=owner)
    assert source.read_bytes() == before
    assert receipt['counts']['corpus_rows'] == 4
    assert receipt['missing_source_tables'] == list(COLUMNS)
    assert all(receipt['counts'][name] == 0 for name in COLUMNS)
    assert receipt['activated'] is False and receipt['legacy_control_disposition_required']
    assert target.stat().st_mode & 0o077 == 0 and receipt['bytes'] < 512 * 1024
    assert b'LEGACY_BODY_MUST_NOT_COPY' not in target.read_bytes()
    assert b'PRIVATE_JOB_MUST_NOT_COPY' not in target.read_bytes()
    with sqlite3.connect(target) as db:
        assert db.execute('SELECT DISTINCT table_name FROM corpus_rows').fetchall() == [('rkb_users',)]
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='story_jobs'").fetchone() is None
    with pytest.raises(ValueError, match='exclusive'):
        export_authority(source, target, owner_subject=owner)


def test_real_existing_rights_links_audit_and_revocations_preserved(people, tmp_path):
    p = people
    change(p, ParticipantGrant(capability='case_read', granted=True, reason='synthetic'))
    bind(p)
    # A persisted disabled family must not become enabled during projection.
    PersonalPolicy(p.registry)
    with p.corpus.connect() as db:
        db.execute('INSERT INTO rkb_request_oauth_grants VALUES(?,?,?,?,?,?,?,?,?)',
            ('f' * 64, p.other.subject, 'client', 'https://example.test/mcp', None, None, 9999999999, 0, 'https://example.test/'))
    target = private(tmp_path) / 'authority.sqlite3'
    receipt = export_authority(p.corpus.path, target, owner_subject=p.owner.subject)
    projected = Participants(SQLiteCorpus(target), p.owner.subject, clock=lambda: p.clock[0])
    assert projected.require(p.personal, 'case_read') == p.other.subject
    assert projected.get(p.admin, p.other.subject) == p.registry.get(p.admin, p.other.subject)
    with projected.corpus.connect() as db:
        assert db.execute('SELECT enabled FROM rkb_request_oauth_grants').fetchone()[0] == 0
    assert receipt['counts']['rkb_participant_events'] >= 3
    change(p, ParticipantDisable(disabled=True, reason='synthetic current source change'))
    # Prepared candidates are snapshots, not a second live writer. Re-export is
    # mandatory if authority changes before the eventual activation boundary.
    with pytest.raises(ParticipantError):
        p.registry.require(p.personal, 'case_read')
    assert projected.require(p.personal, 'case_read') == p.other.subject


@pytest.mark.parametrize('fault', ['bound', 'cell', 'unknown_column', 'unknown_table', 'dangling', 'workspace', 'symlink'])
def test_projection_refuses_unsafe_or_unreviewed_input(tmp_path, fault):
    source, owner = minimal(tmp_path)
    target = private(tmp_path) / 'candidate.sqlite3'
    kwargs = {}
    if fault == 'bound':
        kwargs['max_bytes'] = 1
    elif fault == 'cell':
        with sqlite3.connect(source) as db:
            db.execute("UPDATE corpus_rows SET payload=? WHERE row_key=?", ('x' * (1024 * 1024 + 1), owner))
    elif fault == 'unknown_table':
        with sqlite3.connect(source) as db:
            db.execute('CREATE TABLE rkb_request_unreviewed(id TEXT)')
    elif fault == 'unknown_column':
        with sqlite3.connect(source) as db:
            db.execute('ALTER TABLE corpus_rows ADD COLUMN surprise TEXT')
    elif fault == 'dangling':
        corpus = SQLiteCorpus(source)
        PersonalPolicy(Participants(corpus, owner))
        with corpus.connect() as db:
            db.execute('INSERT INTO rkb_participant_grants VALUES(?,?,?,?,?)', (str(uuid4()), 'case_read', None, owner, 'synthetic'))
    elif fault == 'workspace':
        with sqlite3.connect(source) as db:
            db.execute("INSERT INTO corpus_rows VALUES('rkb_workspace_members','member',NULL,NULL,'{}')")
    else:
        alias = tmp_path / 'alias.sqlite3'
        alias.symlink_to(source)
        source = alias
    with pytest.raises(ValueError):
        export_authority(source, target, owner_subject=owner, **kwargs)
    assert not target.exists()


@pytest.mark.asyncio
async def test_normal_http_refresh_reuses_old_owner_family_and_new_projection(tmp_path, monkeypatch):
    from test_oauth_provider import _provider, _params
    from urllib.parse import parse_qs, urlsplit
    from starlette.testclient import TestClient
    from regional_knowledge.backend import UnavailableBackend
    from regional_knowledge.server import build_server
    from regional_knowledge.read_authority import ReadAuthority, TrustedReadDestination
    from regional_knowledge.request_grants import reference
    provider = _provider(tmp_path)
    # Issue through the real legacy owner flow before attaching PersonalPolicy.
    consent = await provider.authorize(provider.client, _params())
    redirect = provider.approve(parse_qs(urlsplit(consent).query)['id'][0])
    code = await provider.load_authorization_code(provider.client, parse_qs(urlsplit(redirect).query)['code'][0])
    issued = await provider.exchange_authorization_code(provider.client, code)
    old_record = provider.store.read(lambda state: next(iter(state['refresh'].values())).copy())
    old_key = provider.store.key_file.read_bytes()
    source = tmp_path / 'legacy.sqlite3'
    corpus = SQLiteCorpus(source)
    corpus.put('rkb_users', [{'id': provider.owner_subject, 'status': 'active'}])
    target = private(tmp_path) / 'authority.sqlite3'
    export_authority(source, target, owner_subject=provider.owner_subject)
    projected = Participants(SQLiteCorpus(target), provider.owner_subject)
    provider.personal_policy = PersonalPolicy(projected)
    assert await provider.load_access_token(issued.access_token) is None
    with projected.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_request_oauth_grants').fetchone()[0] == 0
    monkeypatch.setenv('RKB_AUTH_MODE', 'embedded')
    server = build_server(backend=UnavailableBackend(), issuer=provider.issuer,
        resource_url=provider.resource, oauth_provider=provider)
    def refresh():
        with TestClient(server.streamable_http_app(), base_url=provider.origin) as client:
            response = client.post('/token', data={'client_id': provider.client.client_id, 'grant_type': 'refresh_token',
                'refresh_token': issued.refresh_token, 'resource': provider.resource},
                auth=(provider.client.client_id, provider.client.client_secret))
            assert response.status_code == 200
            return response.json()
    import asyncio
    rotated = await asyncio.to_thread(refresh)
    loaded = await provider.load_access_token(rotated['access_token'])
    assert loaded.subject == provider.owner_subject
    assert loaded.claims['rkb_request_grant'] == reference(old_record['family'])
    new_record = provider.store.read(lambda state: next(iter(state['refresh'].values())).copy())
    assert new_record['family'] == old_record['family']
    assert new_record['subject'] == old_record['subject']
    assert provider.store.key_file.read_bytes() == old_key
    authority = ReadAuthority(projected, oauth_provider=provider, issuer=provider.issuer, resource=provider.resource)
    context = await authority.resolve_oauth(rotated['access_token'], destination=TrustedReadDestination('mcp', 'personal', 'current-response'))
    assert context.person_id == provider.owner_subject
    await authority.require(context, 'public_search')
    # A consumed old token cannot mint again; existing replay protection revokes
    # the same family and the exact persisted SQL projection.
    assert await provider.load_refresh_token(provider.client, issued.refresh_token) is None
    assert await provider.load_access_token(rotated['access_token']) is None
    with projected.corpus.connect() as db:
        assert db.execute('SELECT enabled FROM rkb_request_oauth_grants').fetchone()[0] == 0
