"""Synthetic cross-book reconciliation; no user books or private evidence."""
from __future__ import annotations

from hashlib import sha256
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.story_contracts import (
    AttachEvidence, EvidenceLocator, ReconcileApply, ReconcileCancel,
    ReconcileCandidateRef, ReconcileClaim, ReconcileDecision,
    ReconcileEnqueue, ReconcileEvidenceRef, ReconcileStage, ReconcileStart,
    SeedInput, UpsertAssertion,
)
from regional_knowledge.story_registry import StoryRegistry, StoryError
from regional_knowledge.story_reconcile import StoryReconciler


def principal():
    return Principal(subject=str(uuid4()), client_id="synthetic-test", issuer="unit",
                     access_token="fake-synthetic-value")


@pytest.fixture
def fixture(tmp_path):
    registry = StoryRegistry(SQLiteCorpus(tmp_path / "corpus.sqlite3"))
    return registry, principal(), principal()


def source(registry, owner, text):
    d, p, r, c = (str(uuid4()) for _ in range(4))
    sha = sha256(text.encode()).hexdigest()
    registry.corpus.put("rkb_documents", [{
        "id": d, "title": "Synthetic regional document", "owner_user_id": owner.subject,
        "active_revision": 1, "source_sha256": sha, "content_visibility": "private",
        "rights_status": "restricted",
    }])
    registry.corpus.put("rkb_pages", [{
        "id": p, "document_id": d, "revision": 1,
        "physical_page_index": 0, "printed_page_number": "10",
    }])
    registry.corpus.put("rkb_regions", [{
        "id": r, "page_id": p, "source_text": text, "reading_order": 0,
    }])
    registry.corpus.put("rkb_chunks", [{
        "id": c, "document_id": d, "revision": 1, "title": "Synthetic passage",
        "source_text": text, "text_sha256": sha, "search_material_sha256": sha,
        "page_ids": [p], "region_ids": [r],
    }])
    return d, p, r, c, text


def grounded_story(registry, owner, book, identifier):
    d,p,r,c,text = book
    story = registry.create(owner, SeedInput(text="Мост и его строительство"), None,
                            None, None, "create-"+identifier)
    sid = story["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion",kind="attributed_account",account_kind="other",
        attributed_to="Автор "+identifier,
        proposition="Автор сообщает, что горожане построили мост в 1535 году.",
    )],"assert-"+identifier)
    aid=registry.get(owner,sid,view="evidence")["snapshot"]["assertions"][0]["assertion_id"]
    registry.edit(owner,sid,2,[AttachEvidence(
        op="attach_evidence",assertion_id=aid,assertion_revision=1,
        source_kind="document",source_id=d,source_revision=1,
        relation="reports",original_excerpt=text,
        locator=EvidenceLocator(page_id=p,region_id=r,chunk_id=c),
    )],"attach-"+identifier)
    snap=registry.get(owner,sid,view="evidence")["snapshot"]
    eid=snap["assertions"][0]["evidence_ids"][0]
    return sid,aid,eid


def refs(a,b,books,story_ids):
    left,right=books
    return ReconcileDecision(
        identity_relation="same_episode",contribution_kinds=["new_detail"],
        independence="unknown",independence_basis="Source roots not independently verified",
        proposed_effect="link_stories",
        rationale="Two independent document versions describe the bridge project, "
                  "but same episode is a model-authored proposal, not a truth certification.",
        anchor_evidence=ReconcileEvidenceRef(
            evidence_id=story_ids[1][2], document_id=right[0],source_revision=1,
            page_id=right[1],region_id=right[2],original_excerpt=right[4]),
        candidate_evidence=ReconcileEvidenceRef(
            evidence_id=story_ids[0][2],document_id=left[0],source_revision=1,
            page_id=left[1],region_id=left[2],original_excerpt=left[4]),
    )


def test_reconcile_two_books_preserves_both_stories_and_citations(fixture):
    registry,owner,other=fixture
    a=source(registry,owner,"В 1535 году построили новый мост через реку.")
    b=source(registry,owner,"В 1535 году мост к острову построили по просьбе купцов.")
    sa=grounded_story(registry,owner,a,"one")
    sb=grounded_story(registry,owner,b,"two")
    reconciler=StoryReconciler(registry)
    started=reconciler.dispatch(owner,ReconcileStart(
        command="start",anchor_story_id=sb[0],expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="story",ref_id=sa[0])],
    ),"cross-start-0001")
    jid=started["job_id"]
    assert started["state"]=="awaiting_agent"
    channels=reconciler.dispatch(owner,ReconcileEnqueue(
        command="enqueue",run_id=jid,expected_job_revision=1,
        searched_channels=["story_lexical","source_bge"],
    ),"cross-search-0001")
    assert channels["candidate_count"]==1
    claim=reconciler.dispatch(owner,ReconcileClaim(
        command="claim",run_id=jid,expected_job_revision=2,
    ),"cross-claim-0001")
    assert claim["candidate"]["id"]==sa[0]
    decision=refs(a,b,[a,b],[sa,sb])
    staged=reconciler.dispatch(owner,ReconcileStage(
        command="stage",run_id=jid,expected_job_revision=3,
        work_id=claim["work_id"],lease_token=claim["lease_token"],decision=decision,
    ),"cross-stage-0001")
    assert staged["processed_pairs"]==1 and staged["state"]=="awaiting_review"
    assert reconciler.dispatch(owner,ReconcileStage(
        command="stage",run_id=jid,expected_job_revision=3,
        work_id=claim["work_id"],lease_token=claim["lease_token"],decision=decision,
    ),"cross-stage-0001")==staged

    persisted=registry.job_get(owner,jid)
    assert persisted["processed_pairs"]==1 and persisted["pending_proposals"]==1
    assert persisted["proposals"][0]["proposal_id"]==staged["proposal_id"]
    applied=reconciler.dispatch(owner,ReconcileApply(
        command="apply",run_id=jid,proposal_id=staged["proposal_id"],
        expected_job_revision=4,expected_target_revision=3,
        reviewer_note="Source texts and attribution reviewed within synthetic fixture.",
    ),"cross-apply-0001")
    assert applied["committed_effect"]=="link_stories"
    assert applied["state"]=="completed_under_policy"
    assert registry.get(owner,sa[0])["revision"]==3
    assert registry.get(owner,sb[0])["revision"]==3
    page=registry.get(owner,sa[0],view="relations_page",limit=2)
    assert page["items"][0]["related_story_id"]==sb[0]
    assert len(page["items"][0]["proofs"])==2
    assert all(x["original_excerpt"] for x in page["items"][0]["proofs"])
    assert registry.job_get(owner,jid)["applied_proposals"]==1
    with pytest.raises(StoryError) as exc:
        registry.job_get(other,jid)
    assert exc.value.code=="not_found_or_not_accessible"


def test_reconcile_old_chunk_without_story_attaches_old_source_to_current_story(fixture):
    registry,owner,_=fixture
    new=source(registry,owner,"Открыли мост через речной рукав в 1535 году.")
    old=source(registry,owner,"Постройка моста была разрешена городскому совету.")
    story=grounded_story(registry,owner,new,"anchor")
    run=StoryReconciler(registry)
    start=run.dispatch(owner,ReconcileStart(
        command="start",anchor_story_id=story[0],expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="chunk",ref_id=old[3])],
    ),"chunk-start-001")
    jid=start["job_id"]
    run.dispatch(owner,ReconcileEnqueue(command="enqueue",run_id=jid,
       expected_job_revision=1,searched_channels=["story_lexical","source_bge"]),
       "chunk-enqueue-001")
    claim=run.dispatch(owner,ReconcileClaim(command="claim",run_id=jid,
       expected_job_revision=2),"chunk-claim-001")
    decision=ReconcileDecision(
        identity_relation="same_episode",contribution_kinds=["new_detail"],
        independence="unknown",independence_basis="Root derivation not determined",
        proposed_effect="add_attributed_claim",
        new_proposition="Другой автор сообщает о разрешении построить мост.",
        attributed_to="Автор старой книги",
        rationale="Source chunk without a story adds a different attributed detail.",
        anchor_evidence=ReconcileEvidenceRef(evidence_id=story[2],
            document_id=new[0],source_revision=1,page_id=new[1],
            region_id=new[2],original_excerpt=new[4]),
        candidate_evidence=ReconcileEvidenceRef(chunk_id=old[3],
            document_id=old[0],source_revision=1,page_id=old[1],
            region_id=old[2],original_excerpt=old[4]),
    )
    staged=run.dispatch(owner,ReconcileStage(command="stage",run_id=jid,
        expected_job_revision=3,work_id=claim["work_id"],
        lease_token=claim["lease_token"],decision=decision),"chunk-stage-001")
    applied=run.dispatch(owner,ReconcileApply(
        command="apply",run_id=jid,proposal_id=staged["proposal_id"],
        expected_job_revision=4,expected_target_revision=3,
        reviewer_note="Synthetic curator verified both different document passages.",
    ),"chunk-apply-001")
    assert applied["committed_effect"]=="add_attributed_claim"
    assert applied["committed_story_revision"]==4
    snapshot=registry.get(owner,story[0],view="evidence")["snapshot"]
    assert len(snapshot["assertions"])==2
    evidence=registry.get(owner,story[0],view="evidence_page",limit=10)
    assert {item["source_id"] for item in evidence["items"]}=={new[0],old[0]}
    assert all(item["text_match"]=="exact" for item in evidence["items"])


def test_reconcile_wrong_quote_does_not_advance_checkpoint(fixture):
    registry,owner,_=fixture
    a=source(registry,owner,"Был построен один деревянный мост.")
    b=source(registry,owner,"На том же месте в 1535 году возвели мост.")
    sa=grounded_story(registry,owner,a,"sourceA")
    sb=grounded_story(registry,owner,b,"sourceB")
    reconciler=StoryReconciler(registry)
    start=reconciler.dispatch(owner,ReconcileStart(
        command="start",anchor_story_id=sb[0],expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="story",ref_id=sa[0])],
    ),"bad-start-0001")
    claim=reconciler.dispatch(owner,ReconcileClaim(command="claim",
        run_id=start["job_id"],expected_job_revision=1),"bad-claim-0001")
    decision=refs(a,b,[a,b],[sa,sb])
    decision.anchor_evidence.original_excerpt="This is invented text"
    with pytest.raises(StoryError) as exc:
        reconciler.dispatch(owner,ReconcileStage(
            command="stage",run_id=start["job_id"],expected_job_revision=2,
            work_id=claim["work_id"],lease_token=claim["lease_token"],
            decision=decision),"bad-stage-0001")
    assert exc.value.code=="invalid_evidence"
    status=registry.job_get(owner,start["job_id"])
    assert status["processed_pairs"]==0
    assert status["pending_proposals"]==0
