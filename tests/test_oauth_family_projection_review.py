"""Independent black-box negatives; synthetic issuer state only."""
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4
import json

import pytest

from regional_knowledge.oauth_provider import KNOWLEDGE_SCOPE
from regional_knowledge.personal_auth import PersonalPolicy
from regional_knowledge.participants import Participants
from regional_knowledge.request_grants import reference,save,revoke
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from test_oauth_provider import _provider,_params


async def issued(tmp_path,*,dynamic=False):
    provider=_provider(tmp_path)
    client=provider.client
    if dynamic:
        from test_oauth_provider import _dynamic_client
        client=_dynamic_client(redirect_uri=provider.redirect_uris[0])
        await provider.register_client(client)
    consent=await provider.authorize(client,_params())
    redirect=provider.approve(parse_qs(urlsplit(consent).query)['id'][0])
    code=await provider.load_authorization_code(client,parse_qs(urlsplit(redirect).query)['code'][0])
    tokens=await provider.exchange_authorization_code(client,code)
    corpus=SQLiteCorpus(tmp_path/'authority.sqlite3')
    corpus.put('rkb_users',[{'id':provider.owner_subject,'status':'active'}])
    provider.personal_policy=PersonalPolicy(Participants(corpus,provider.owner_subject))
    corpus.path.chmod(0o600)
    return provider,tokens,corpus


def migrate(provider):
    from regional_knowledge.oauth_family_projection import OwnerFamilyProjectionMigration
    operation=OwnerFamilyProjectionMigration(provider,corpus_path=provider.personal_policy.participants.corpus.path)
    reviewed=operation.review()
    return reviewed,operation.apply(reviewed)


def rows(corpus):
    with corpus.connect() as db:
        return [dict(row) for row in db.execute('SELECT * FROM rkb_request_oauth_grants ORDER BY id')]


def poison_family(provider,changes):
    def write(state):
        for bucket in ('access','refresh'):
            for row in state[bucket].values():row.update(changes)
    provider.store.mutate(write)


@pytest.mark.asyncio
async def test_independent_same_old_access_no_refresh_and_exact_idempotent_projection(tmp_path):
    provider,tokens,corpus=await issued(tmp_path)
    before=provider.store.read(lambda state:state)
    state_bytes=provider.store.state_file.read_bytes();key_bytes=provider.store.key_file.read_bytes()
    assert await provider.load_access_token(tokens.access_token) is None
    reviewed,_=migrate(provider)
    loaded=await provider.load_access_token(tokens.access_token)
    assert loaded is not None and loaded.subject==provider.owner_subject
    after=rows(corpus);assert len(after)==1
    family=next(iter(before['refresh'].values()))
    assert after[0]['id']==reference(family['family'])
    assert after[0]['expires_at']==family['expires_at']
    assert provider.store.read(lambda state:state)==before
    assert provider.store.state_file.read_bytes()==state_bytes and provider.store.key_file.read_bytes()==key_bytes
    migrate(provider)
    assert rows(corpus)==after
    assert tokens.access_token not in repr(reviewed) and tokens.refresh_token not in repr(reviewed)
    serialized=json.dumps(reviewed.wire())
    assert tokens.access_token not in serialized and tokens.refresh_token not in serialized
    assert family['family'] not in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize('changes',[
    {'issuer':'https://foreign.example/'}, {'resource':'https://knowledge.example.test/other'},
    {'scopes':['knowledge:admin']}, {'client_id':'unregistered-client'},
    {'subject':str(uuid4())}, {'expires_at':float('inf')}, {'expires_at':float('nan')},
])
async def test_independent_foreign_or_malformed_issued_family_never_projects(tmp_path,changes):
    provider,tokens,corpus=await issued(tmp_path)
    poison_family(provider,changes)
    try:migrate(provider)
    except (ValueError,PermissionError):pass
    assert rows(corpus)==[]
    assert await provider.load_access_token(tokens.access_token) is None


@pytest.mark.asyncio
async def test_independent_missing_issuer_and_mixed_family_identity_do_not_infer_authority(tmp_path):
    provider,_,corpus=await issued(tmp_path)
    def write(state):
        for row in state['refresh'].values():row.pop('issuer')
    provider.store.mutate(write)
    try:migrate(provider)
    except (ValueError,PermissionError):pass
    assert rows(corpus)==[]


@pytest.mark.asyncio
async def test_independent_expired_access_live_refresh_not_revived_or_renewed(tmp_path):
    provider,tokens,corpus=await issued(tmp_path)
    record=provider.store.read(lambda state:next(iter(state['refresh'].values())).copy())
    provider.clock=lambda:record['expires_at']-86400
    migrate(provider)
    assert len(rows(corpus))==1 and rows(corpus)[0]['expires_at']==record['expires_at']
    assert await provider.load_access_token(tokens.access_token) is None
    assert await provider.load_refresh_token(provider.client,tokens.refresh_token) is not None


@pytest.mark.asyncio
async def test_independent_disabled_projection_and_current_owner_cannot_be_resurrected(tmp_path):
    provider,_,corpus=await issued(tmp_path)
    record=provider.store.read(lambda state:next(iter(state['refresh'].values())).copy())
    save(corpus,record,record['expires_at']);revoke(corpus,family=record['family'])
    before=rows(corpus)
    with pytest.raises((ValueError,PermissionError)):migrate(provider)
    assert rows(corpus)==before and before[0]['enabled']==0


@pytest.mark.asyncio
async def test_independent_normal_refresh_replay_still_revokes_migrated_family(tmp_path):
    provider,tokens,corpus=await issued(tmp_path)
    migrate(provider)
    refresh=await provider.load_refresh_token(provider.client,tokens.refresh_token)
    rotated=await provider.exchange_refresh_token(provider.client,refresh,[KNOWLEDGE_SCOPE])
    assert await provider.load_access_token(rotated.access_token) is not None
    # Rotation leaves used+live records; a second projection review is valid.
    migrate(provider)
    assert await provider.load_refresh_token(provider.client,tokens.refresh_token) is None
    assert await provider.load_access_token(rotated.access_token) is None
    assert rows(corpus)[0]['enabled']==0


@pytest.mark.asyncio
async def test_independent_owner_disabled_between_review_and_apply_has_no_projection(tmp_path):
    from regional_knowledge.oauth_family_projection import OwnerFamilyProjectionMigration
    provider,_,corpus=await issued(tmp_path)
    operation=OwnerFamilyProjectionMigration(provider,corpus_path=corpus.path)
    reviewed=operation.review()
    corpus.put('rkb_users',[{'id':provider.owner_subject,'status':'disabled'}])
    with pytest.raises((ValueError,PermissionError)):operation.apply(reviewed)
    assert rows(corpus)==[]


@pytest.mark.asyncio
async def test_independent_registered_dynamic_client_without_metadata_issuer_is_supported(tmp_path):
    provider,tokens,corpus=await issued(tmp_path,dynamic=True)
    migrate(provider)
    loaded=await provider.load_access_token(tokens.access_token)
    assert loaded is not None and loaded.subject==provider.owner_subject
    assert loaded.client_id==rows(corpus)[0]['client_id'] and loaded.client_id!=provider.client.client_id


@pytest.mark.asyncio
async def test_independent_shorter_existing_projection_lifetime_is_not_extended(tmp_path):
    provider,_,corpus=await issued(tmp_path)
    record=provider.store.read(lambda state:next(iter(state['refresh'].values())).copy())
    save(corpus,record,record['expires_at']-3600)
    before=rows(corpus);migrate(provider)
    assert rows(corpus)==before


@pytest.mark.asyncio
async def test_independent_used_refresh_only_after_actual_revoke_cannot_recreate_family(tmp_path):
    provider,tokens,corpus=await issued(tmp_path)
    # Use the normal legacy rotation, then revoke before any projection exists.
    provider.personal_policy=None
    refresh=await provider.load_refresh_token(provider.client,tokens.refresh_token)
    rotated=await provider.exchange_refresh_token(provider.client,refresh,[KNOWLEDGE_SCOPE])
    access=await provider.load_access_token(rotated.access_token)
    await provider.revoke_token(access)
    provider.personal_policy=PersonalPolicy(Participants(corpus,provider.owner_subject))
    try:migrate(provider)
    except (ValueError,PermissionError):pass
    assert rows(corpus)==[] and await provider.load_access_token(rotated.access_token) is None


@pytest.mark.asyncio
async def test_actual_migration_receipt_survives_tiny_authority_export_with_fixed_immutability(tmp_path):
    import sqlite3
    from regional_knowledge.authority_projection import export_authority
    provider,_,corpus=await issued(tmp_path)
    _,receipt=migrate(provider)
    directory=tmp_path/'private-export';directory.mkdir(mode=0o700)
    candidate=directory/'authority.sqlite3'
    exported=export_authority(corpus.path,candidate,owner_subject=provider.owner_subject)
    assert exported['counts']['rkb_oauth_owner_projection_migrations']==1
    assert candidate.stat().st_size<512*1024
    with sqlite3.connect(candidate) as db:
        raw=db.execute('SELECT plan_sha256,receipt_json FROM rkb_oauth_owner_projection_migrations').fetchone()
        assert raw[0]==receipt['plan_sha256'] and json.loads(raw[1])==receipt
        with pytest.raises(sqlite3.IntegrityError,match='immutable'):
            db.execute("UPDATE rkb_oauth_owner_projection_migrations SET receipt_json='{}'")
        with pytest.raises(sqlite3.IntegrityError,match='retained'):
            db.execute('DELETE FROM rkb_oauth_owner_projection_migrations')


@pytest.mark.asyncio
async def test_unknown_oauth_authority_table_or_nonreceipt_body_is_not_silently_exported(tmp_path):
    from regional_knowledge.authority_projection import export_authority
    provider,_,corpus=await issued(tmp_path)
    migrate(provider)
    directory=tmp_path/'private-export';directory.mkdir(mode=0o700)
    candidate=directory/'authority.sqlite3'
    with corpus.connect() as db:db.execute('CREATE TABLE rkb_oauth_unreviewed(payload TEXT)')
    with pytest.raises(ValueError,match='unknown authority'):
        export_authority(corpus.path,candidate,owner_subject=provider.owner_subject)
    assert not candidate.exists()
    with corpus.connect() as db:
        db.execute('DROP TABLE rkb_oauth_unreviewed')
        db.execute('DROP TRIGGER rkb_oauth_projection_receipt_immutable')
        db.execute("UPDATE rkb_oauth_owner_projection_migrations SET receipt_json=?",(json.dumps({'body':'NEVER_COPY_CORPUS'}),))
    with pytest.raises(ValueError,match='exact owner projection'):
        export_authority(corpus.path,candidate,owner_subject=provider.owner_subject)
    assert not candidate.exists()
