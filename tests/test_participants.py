"""Participant authority, temporal grants and verified account lifecycle."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from regional_knowledge.contracts import Principal
from regional_knowledge.participant_contracts import (
    AccountBegin, AccountConfirm, AccountUnlink, DomainWeight, ParticipantDisable,
    ParticipantGrant, ParticipantUpsert,
)
from regional_knowledge.participants import Participants, ParticipantError, RequestAuthority
from regional_knowledge.sqlite_corpus import SQLiteCorpus


def principal(subject=None):
    return Principal(subject=subject or str(uuid4()),client_id="test",issuer="test",access_token="synthetic")


def expect(code,fn,*args,**kwargs):
    with pytest.raises(ParticipantError) as e: fn(*args,**kwargs)
    assert e.value.code==code


@pytest.fixture
def people(tmp_path):
    corpus=SQLiteCorpus(tmp_path/"corpus.sqlite3")
    owner=principal(); other=principal(); stranger=principal()
    corpus.put("rkb_users",[{"id":owner.subject,"status":"active"}])
    clock=[datetime(2026,10,19,10,tzinfo=timezone.utc)]
    registry=Participants(corpus,owner.subject,clock=lambda:clock[0])
    admin=RequestAuthority(owner,"owner")
    registry.change(admin,other.subject,0,ParticipantUpsert(name="Synthetic expert",categories={"expert","guide"},reason="test"),"create-participant")
    return SimpleNamespace(corpus=corpus,owner=owner,other=other,stranger=stranger,registry=registry,admin=admin,personal=RequestAuthority(other,"personal"),clock=clock)


def change(p,command,key=None,person=None):
    person=person or p.other.subject
    revision=p.registry.get(p.admin,person)["revision"]
    return p.registry.change(p.admin,person,revision,command,key or "change-"+str(uuid4()))


def bind(p,person=None,user="U111"):
    person=person or p.other.subject
    link=change(p,AccountBegin(provider="slack",tenant="T111",provider_subject=user,reason="owner exact binding"),person=person)
    change(p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="owner_confirmed_slack",reason="confirmed exact tuple"),person=person)
    return p.registry.resolve_external(provider="slack",tenant="T111",provider_subject=user,principal_factory=principal)


def test_existing_uuid_categories_weights_never_grant_access(people):
    p=people
    assert p.corpus.one("rkb_users",p.other.subject)["status"]=="active"
    change(p,DomainWeight(domain="East Prussian architecture",weight="high",basis="Owner attested expertise"))
    expect("capability_denied",p.registry.require,p.personal,"case_contribute")
    profile=p.registry.get(p.admin,p.other.subject)
    assert set(profile["profile"]["categories"])=={"expert","guide"}
    assert not profile["grants"] and profile["domain_weights"][0]["payload"]["assigned_by"]==p.owner.subject
    # Source grants are not implicitly created by categories or weights.
    with p.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM corpus_rows WHERE table_name='rkb_document_grants'").fetchone()[0]==0


def test_grant_expiry_and_profile_expiry_rechecked_on_every_request(people):
    p=people
    expires=p.clock[0]+timedelta(minutes=10)
    change(p,ParticipantGrant(capability="case_contribute",granted=True,expires_at=expires,reason="short test"))
    assert p.registry.require(p.personal,"case_contribute")==p.other.subject
    p.clock[0]=expires
    expect("capability_denied",p.registry.require,p.personal,"case_contribute")
    change(p,ParticipantGrant(capability="case_contribute",granted=True,reason="new grant"))
    change(p,ParticipantUpsert(name="Synthetic expert",expires_at=expires,reason="demo ended"))
    expect("capability_denied",p.registry.get,p.personal,p.other.subject)


def test_owner_expert_route_cannot_import_or_administer(people):
    p=people
    p.registry.change(p.admin,p.owner.subject,0,ParticipantUpsert(name="Owner",reason="reuse canonical UUID"),"owner-profile")
    expert=bind(p,p.owner.subject,"UOWNER")
    assert p.registry.require(expert,"case_contribute")==p.owner.subject
    for capability in ("import","participants_manage","discussion_approve","maintain_shared_summary"):
        expect("capability_denied",p.registry.require,expert,capability)
    expect("capability_denied",p.registry.change,expert,p.other.subject,1,ParticipantDisable(disabled=True,reason="forged admin"),"expert-admin")
    assert p.registry.require(p.admin,"import")==p.owner.subject
    expect("capability_denied",p.registry.require,RequestAuthority(p.owner,"personal"),"import")


def test_pending_revoked_disabled_and_wrong_revision_never_resolve(people):
    p=people
    link=change(p,AccountBegin(provider="slack",tenant="T111",provider_subject="U111",reason="pending"))
    expect("identity_unverified",p.registry.resolve_external,provider="slack",tenant="T111",provider_subject="U111",principal_factory=principal)
    expect("identity_unverified",p.registry.require,RequestAuthority(p.other,"expert",link["link_id"],1),"case_read")
    change(p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="owner_confirmed_slack",reason="confirmed"))
    expert=p.registry.resolve_external(provider="slack",tenant="T111",provider_subject="U111",principal_factory=principal)
    change(p,ParticipantGrant(capability="case_read",granted=True,reason="test"))
    assert p.registry.require(expert,"case_read")==p.other.subject
    change(p,ParticipantDisable(disabled=True,reason="paused"))
    expect("capability_denied",p.registry.require,expert,"case_read")
    # Profile update does not revive a disabled participant.
    change(p,ParticipantUpsert(name="New spelling",reason="spelling only"))
    expect("capability_denied",p.registry.require,expert,"case_read")
    change(p,ParticipantDisable(disabled=False,reason="resumed"))
    change(p,AccountUnlink(link_id=link["link_id"],expected_link_revision=2,reason="wrong person",incorrect_binding=True))
    expect("identity_unverified",p.registry.require,expert,"case_read")
    old=p.registry.get(p.admin,p.other.subject)["accounts"][0]
    assert old["attribution_review_required"] and old["state"]=="revoked"
    assert p.registry.audit(p.admin,p.other.subject)["items"][-1]["payload"]["snapshot"]["accounts"][0]["state"]=="revoked"


def test_external_tuple_unique_and_rebinding_keeps_wrong_attribution_record(people):
    p=people; expert=bind(p)
    p.registry.change(p.admin,p.stranger.subject,0,ParticipantUpsert(name="Another person",reason="test"),"create-stranger")
    expect("identity_already_linked",change,p,AccountBegin(provider="slack",tenant="T111",provider_subject="U111",reason="collision"),person=p.stranger.subject)
    change(p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,reason="wrong binding",incorrect_binding=True))
    new=bind(p,p.stranger.subject)
    assert new.principal.subject==p.stranger.subject and new.link_id!=expert.link_id
    expect("identity_unverified",p.registry.require,expert,"case_read")
    assert p.registry.get(p.admin,p.other.subject)["accounts"][0]["attribution_review_required"]


def test_vk_person_requires_provider_proof_and_not_community_or_slack_attestation(people):
    p=people
    expect("invalid_external_identity",change,p,AccountBegin(provider="vk",tenant="community-1",provider_subject="-123",reason="not human"))
    link=change(p,AccountBegin(provider="vk",tenant="community-1",provider_subject="123",reason="human pending"))
    expect("invalid_identity_proof",change,p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="owner_confirmed_slack",reason="not provider proof"))
    expect("invalid_identity_proof",change,p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="provider_observation",observation_id=uuid4(),reason="invented observation"))
    observed=p.registry.observe_account(link_id=link["link_id"],link_revision=1,provider="vk",tenant="community-1",provider_subject="123",audience="rkb-account-binding",expires_at=p.clock[0]+timedelta(minutes=1),receipt_id="synthetic-provider-receipt")
    change(p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="provider_observation",observation_id=observed,reason="internal verifier fixture"))
    assert p.registry.resolve_external(provider="vk",tenant="community-1",provider_subject="123",principal_factory=principal).principal.subject==p.other.subject


def test_provider_observation_bound_to_challenge_audience_expiry_and_revision(people):
    p=people
    link=change(p,AccountBegin(provider="projects_hub",tenant="projects.example",provider_subject="usr_test",reason="pending"))
    params=dict(link_id=link["link_id"],link_revision=1,provider="projects_hub",tenant="projects.example",provider_subject="usr_test",audience="rkb-account-binding",expires_at=p.clock[0]+timedelta(minutes=1),receipt_id="provider-observation-1")
    for update in ({"provider_subject":"usr_other"},{"audience":"owner-invite"},{"link_revision":2}):
        expect("invalid_identity_proof",p.registry.observe_account,**{**params,**update})
    observed=p.registry.observe_account(**params)
    assert p.registry.observe_account(**params)==observed
    p.clock[0]+=timedelta(minutes=2)
    expect("invalid_identity_proof",change,p,AccountConfirm(link_id=link["link_id"],expected_link_revision=1,method="provider_observation",observation_id=observed,reason="expired"))


def test_operation_receipt_restart_cas_and_authorization_before_replay(people):
    p=people
    cmd=ParticipantGrant(capability="public_search",granted=True,reason="test")
    receipt=p.registry.change(p.admin,p.other.subject,1,cmd,"grant-public-search")
    reopened=Participants(p.corpus,p.owner.subject,clock=lambda:p.clock[0])
    assert reopened.operation_status(p.admin,"grant-public-search")==receipt
    assert reopened.change(p.admin,p.other.subject,1,cmd,"grant-public-search")==receipt
    expect("revision_conflict",reopened.change,p.admin,p.other.subject,1,cmd,"stale-search-grant")
    expect("idempotency_conflict",reopened.change,p.admin,p.other.subject,2,cmd,"grant-public-search")
    expect("capability_denied",reopened.operation_status,p.personal,"grant-public-search")
    p.corpus.put("rkb_users",[{"id":p.owner.subject,"status":"disabled"}])
    expect("capability_denied",reopened.operation_status,p.admin,"grant-public-search")


def test_concurrent_admin_updates_never_restore_stale_permissions(people):
    p=people
    def assign(cap):
        try: return p.registry.change(p.admin,p.other.subject,1,ParticipantGrant(capability=cap,granted=True,reason="concurrent"),"concurrent-"+cap)
        except ParticipantError as e: return e.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(assign,["public_search","case_contribute"]))
    assert results.count("revision_conflict")==1
    assert len(p.registry.get(p.admin,p.other.subject)["grants"])==1


def test_rollback_keeps_identity_event_and_receipt_atomic(people,monkeypatch):
    p=people
    original=p.registry._snapshot
    def failure(*args): raise RuntimeError("simulated before commit")
    monkeypatch.setattr(p.registry,"_snapshot",failure)
    with pytest.raises(RuntimeError): p.registry.change(p.admin,p.other.subject,1,AccountBegin(provider="slack",tenant="T111",provider_subject="U111",reason="rollback"),"rollback-link")
    monkeypatch.setattr(p.registry,"_snapshot",original)
    assert p.registry.get(p.admin,p.other.subject)["revision"]==1
    assert p.registry.get(p.admin,p.other.subject)["accounts"]==[]
    assert p.registry.operation_status(p.admin,"rollback-link")=={"state":"not_found"}


def test_private_profiles_history_and_forged_authority_fields(people):
    p=people
    expect("capability_denied",p.registry.get,p.personal,p.owner.subject)
    expect("capability_denied",p.registry.list,p.personal)
    expect("capability_denied",p.registry.audit,p.personal,p.other.subject)
    expect("capability_denied",p.registry.require,{"principal":p.owner,"route":"owner"},"import")
    with pytest.raises(ValidationError): ParticipantUpsert(name="Expert",reason="forged",actor_id=p.owner.subject,verified=True)
    with pytest.raises(ValidationError): ParticipantGrant(capability="import",granted=True,reason="wrong route")
    with pytest.raises(ValidationError): ParticipantUpsert(name="Test",reason="naive expiry",expires_at=datetime(2026,10,20))


def test_assignment_snapshot_and_late_unlink(people):
    p=people; expert=bind(p)
    change(p,DomainWeight(domain="Architecture",weight="experienced",basis="Synthetic assignment"))
    with p.corpus.connect() as db:
        before=p.registry.attribution(db,expert)
    assert before["external_link_id"]==expert.link_id
    assert before["domain_weights"][0]["weight"]=="experienced"
    change(p,DomainWeight(domain="Architecture",weight="new value",basis="Later assignment"))
    change(p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,
                          reason="incorrect attribution",incorrect_binding=True))
    assert before["domain_weights"][0]["weight"]=="experienced"
    expect("identity_unverified",p.registry.require,expert,"public_search")


def test_historical_attribution_review_is_bounded_and_never_rewrites_owner_records(people):
    p=people
    from uuid import uuid4
    expert=bind(p)
    key=(p.other.subject,expert.link_id,expert.link_revision)
    assert p.registry.historical_attribution_review((key,))=={key:False}
    # A routine unlink is not a correction of past authorship.
    change(p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,
        reason='Routine account deactivation',incorrect_binding=False))
    assert p.registry.historical_attribution_review((key,))=={key:False}
    corrected=bind(p,user='U222')
    other=(p.other.subject,corrected.link_id,corrected.link_revision)
    change(p,AccountUnlink(link_id=corrected.link_id,expected_link_revision=2,
        reason='Incorrect historical attribution',incorrect_binding=True))
    assert p.registry.historical_attribution_review((key,other))=={key:False,other:True}
    expect('attribution_link_unavailable',p.registry.historical_attribution_review,
           ((p.owner.subject,corrected.link_id,corrected.link_revision),))
    expect('attribution_link_unavailable',p.registry.historical_attribution_review,
           ((p.other.subject,str(uuid4()),corrected.link_revision),))
    expect('invalid_attribution_references',p.registry.historical_attribution_review,
           tuple([key]*21))
    expect('invalid_attribution_references',p.registry.historical_attribution_review,
           ((p.other.subject,'forged-link',1),))
    # An external identity row and its record of the correction stay put.
    assert any(row['id']==corrected.link_id and row['attribution_review_required']
        for row in p.registry.get(p.admin,p.other.subject)['accounts'])


def test_disabled_canonical_user_and_unknown_capability_fail_closed(people):
    p=people
    expect("capability_denied",p.registry.require,p.admin,"unknown-typo")
    change(p,ParticipantGrant(capability="public_search",granted=True,reason="test"))
    p.corpus.put("rkb_users",[{"id":p.other.subject,"status":"disabled"}])
    expect("capability_denied",p.registry.require,p.personal,"public_search")
