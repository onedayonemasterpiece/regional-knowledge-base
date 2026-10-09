"""Synthetic cross-book reconciliation; no user books or private evidence."""
from __future__ import annotations

from hashlib import sha256
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.story_contracts import (
    AttachEvidence, EvidenceLocator, ReconcileApply, ReconcileCancel,
    ReconcileCandidateRef, ReconcileClaim, ReconcileDecision, ReconcileNext,
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
    # Another session can inspect the typed decision and original proof
    # without relying on an earlier conversational context.
    detail=registry.job_get(owner,jid,proposal_id=staged["proposal_id"])
    assert detail["proposal_detail"]["candidate_proof"]["source_id"]==a[0]
    assert detail["proposal_detail"]["anchor_proof"]["source_id"]==b[0]
    assert detail["proposal_detail"]["decision"]["identity_relation"]=="same_episode"
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


def test_cross_book_enrichment_attaches_candidate_evidence_to_anchor_only(fixture):
    """Second book contributes its own quote to ONE existing dossier, not vice versa."""
    registry, owner, _ = fixture
    older = source(registry, owner, "В 1535 году горожане построили деревянный мост.")
    newer = source(registry, owner, "В 1535 году городской совет разрешил постройку моста.")
    old_story = grounded_story(registry, owner, older, "old")
    anchor = grounded_story(registry, owner, newer, "new")
    reconcile = StoryReconciler(registry)
    start = reconcile.dispatch(owner, ReconcileStart(
        command="start", anchor_story_id=anchor[0], expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="story", ref_id=old_story[0])],
    ), "enrich-stories-start-01")
    claim = reconcile.dispatch(owner, ReconcileClaim(
        command="claim", run_id=start["job_id"], expected_job_revision=1,
    ), "enrich-stories-claim-01")
    decision = ReconcileDecision(
        identity_relation="same_episode", contribution_kinds=["additional_evidence"],
        independence="unknown",
        independence_basis="May share a predecessor; source independence not established",
        proposed_effect="attach_evidence", target_assertion_id=anchor[1],
        rationale="The older publication also explicitly describes this bridge episode.",
        anchor_evidence=ReconcileEvidenceRef(
            document_id=newer[0], source_revision=1, page_id=newer[1],
            region_id=newer[2], original_excerpt=newer[4],
            evidence_id=anchor[2]),
        candidate_evidence=ReconcileEvidenceRef(
            document_id=older[0], source_revision=1, page_id=older[1],
            region_id=older[2], original_excerpt=older[4],
            evidence_id=old_story[2]),
    )
    stage = reconcile.dispatch(owner, ReconcileStage(
        command="stage", run_id=start["job_id"], expected_job_revision=2,
        work_id=claim["work_id"], lease_token=claim["lease_token"],
        decision=decision,
    ), "enrich-stories-stage-01")
    applied = reconcile.dispatch(owner, ReconcileApply(
        command="apply", run_id=start["job_id"], proposal_id=stage["proposal_id"],
        expected_job_revision=3, expected_target_revision=3,
        reviewer_note="The same bridge episode was checked in both printed sources.",
    ), "enrich-stories-apply-01")
    assert applied["target_story_id"] == anchor[0]
    assert applied["committed_story_revision"] == 4
    assert registry.get(owner, old_story[0])["revision"] == 3
    enriched = registry.get(owner, anchor[0], view="evidence_page", limit=10)
    assert {v["source_id"] for v in enriched["items"]} == {older[0], newer[0]}
    assert len(enriched["items"]) == 2
    assert {v["original_excerpt"] for v in enriched["items"]} == {older[4], newer[4]}
    assert {v["source_id"] for v in registry.get(owner, anchor[0], view="sources_page")["items"]} == {
        older[0], newer[0],
    }
    assert reconcile.dispatch(owner, ReconcileApply(
        command="apply", run_id=start["job_id"], proposal_id=stage["proposal_id"],
        expected_job_revision=3, expected_target_revision=3,
        reviewer_note="The same bridge episode was checked in both printed sources.",
    ), "enrich-stories-apply-01") == applied


def test_cross_book_link_is_not_automatic_merge(fixture):
    registry, owner, _ = fixture
    book_a = source(registry, owner, "В 1535 году построили первый мост.")
    book_b = source(registry, owner, "В 1540 году мост расширили.")
    first = grounded_story(registry, owner, book_a, "phaseA")
    second = grounded_story(registry, owner, book_b, "phaseB")
    reconciler = StoryReconciler(registry)
    started = reconciler.dispatch(owner, ReconcileStart(
        command="start", anchor_story_id=first[0], expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="story", ref_id=second[0])],
    ), "phase-link-start-001")
    claimed = reconciler.dispatch(owner, ReconcileClaim(
        command="claim", run_id=started["job_id"], expected_job_revision=1,
    ), "phase-link-claim-001")
    decision = ReconcileDecision(
        identity_relation="part_or_phase", contribution_kinds=["new_detail"],
        independence="unknown", independence_basis="Different primary source roots not verified",
        proposed_effect="link_stories", rationale="Second event follows first by five years; not same episode.",
        phase_of_source="candidate",
        anchor_evidence=ReconcileEvidenceRef(
            document_id=book_a[0], source_revision=1, page_id=book_a[1],
            region_id=book_a[2], original_excerpt=book_a[4], evidence_id=first[2]),
        candidate_evidence=ReconcileEvidenceRef(
            document_id=book_b[0], source_revision=1, page_id=book_b[1],
            region_id=book_b[2], original_excerpt=book_b[4], evidence_id=second[2]),
    )
    proposed = reconciler.dispatch(owner, ReconcileStage(
        command="stage", run_id=started["job_id"], expected_job_revision=2,
        work_id=claimed["work_id"], lease_token=claimed["lease_token"],
        decision=decision,
    ), "phase-link-stage-001")
    result = reconciler.dispatch(owner, ReconcileApply(
        command="apply", run_id=started["job_id"],
        proposal_id=proposed["proposal_id"], expected_job_revision=3,
        expected_target_revision=3,
        reviewer_note="This is a different temporal phase; retain both separate stories.",
    ), "phase-link-apply-001")
    assert result["committed_effect"] == "link_stories"
    assert registry.get(owner, first[0])["snapshot"]["state"] != "archived"
    assert registry.get(owner, second[0])["snapshot"]["state"] != "archived"
    related = registry.get(owner, first[0], view="relations_page")
    assert related["items"][0]["kind"] == "phase_of"
    assert related["items"][0]["related_story_id"] == second[0]
    direction = related["items"][0]["phase_direction"]
    assert related["items"][0]["phase_direction_status"] == "explicit"
    assert direction["from_story_id"] == second[0]
    assert direction["to_story_id"] == first[0]
    assert direction["relative_to_requested"] == "incoming"
    reverse = registry.get(owner, second[0], view="relations_page")
    assert reverse["items"][0]["phase_direction"] == {
        "from_story_id": second[0], "to_story_id": first[0],
        "relative_to_requested": "outgoing",
    }
    # An idempotent additive SQLite upgrade cannot erase the directed link.
    registry.migrate()
    assert registry.get(owner, first[0], view="relations_page")["items"][0]["phase_direction"] == direction


def test_next_reconciliation_from_accepted_story_is_resumable_without_old_chat(fixture):
    """After new evidence the MCP can find its pending story without client-side IDs."""
    registry, owner, other = fixture
    one = source(registry, owner, "Купцы построили деревянный мост в 1535 году.")
    story = grounded_story(registry, owner, one, "pending")
    reconciler = StoryReconciler(registry)
    first = reconciler.dispatch(owner, ReconcileNext(
        command="next", document_id=one[0],
    ), "reconcile-next-original-01")
    assert first["state"] == "awaiting_search"
    assert first["anchor_story_id"] == story[0]
    assert first["anchor_story_revision"] == 3
    assert first["next_action"] == "search_then_enqueue"
    assert reconciler.dispatch(owner, ReconcileNext(
        command="next", document_id=one[0],
    ), "reconcile-next-original-01") == first
    # A new chat with a distinct request key must find the SAME durable run.
    second = reconciler.dispatch(owner, ReconcileNext(
        command="next", document_id=one[0],
    ), "reconcile-next-fresh-session-02")
    assert second["job_id"] == first["job_id"]
    assert second["reused"] is True
    assert registry.job_get(owner, first["job_id"])["next_action"] == "search_then_enqueue"
    with pytest.raises(StoryError) as err:
        reconciler.dispatch(other, ReconcileNext(
            command="next", document_id=one[0],
        ), "reconcile-next-denied-other")
    assert err.value.code == "not_found_or_not_accessible"


def test_cancelled_reconciliation_cannot_apply_abandoned_proposal(fixture):
    """Correction/cancellation preserves audit but never permits a stale effect."""
    registry, owner, _ = fixture
    first = source(registry, owner, "В 1535 году жители построили мост.")
    second = source(registry, owner, "В 1535 году мост соединил два берега.")
    sa = grounded_story(registry, owner, first, "cancel-left")
    sb = grounded_story(registry, owner, second, "cancel-right")
    rec = StoryReconciler(registry)
    started = rec.dispatch(owner, ReconcileStart(
        command="start", anchor_story_id=sb[0], expected_story_revision=3,
        refs=[ReconcileCandidateRef(kind="story", ref_id=sa[0])],
    ), "cancel-first-start")
    claimed = rec.dispatch(owner, ReconcileClaim(
        command="claim", run_id=started["job_id"], expected_job_revision=1,
    ), "cancel-first-claim")
    proposed = rec.dispatch(owner, ReconcileStage(
        command="stage", run_id=started["job_id"], expected_job_revision=2,
        work_id=claimed["work_id"], lease_token=claimed["lease_token"],
        decision=refs(first, second, [first, second], [sa, sb]),
    ), "cancel-first-stage")
    cancelled = rec.dispatch(owner, ReconcileCancel(
        command="cancel", run_id=started["job_id"], expected_job_revision=3,
    ), "cancel-first-cancel")
    assert cancelled["state"] == "cancelled"
    status = registry.job_get(owner, started["job_id"])
    assert status["pending_proposals"] == 0
    assert status["next_action"] is None
    assert status["proposals"][0]["state"] == "cancelled"
    with pytest.raises(StoryError) as exc:
        rec.dispatch(owner, ReconcileApply(
            command="apply", run_id=started["job_id"],
            proposal_id=proposed["proposal_id"], expected_job_revision=4,
            expected_target_revision=3,
            reviewer_note="Discarded earlier comparison, do not apply it.",
        ), "cancel-first-invalid-apply")
    assert exc.value.code == "validation_failed"
    assert registry.get(owner, sb[0])["revision"] == 3
    assert not registry.get(owner, sb[0], view="relations_page")["items"]
