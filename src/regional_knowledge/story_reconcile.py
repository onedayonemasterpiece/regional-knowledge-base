"""Durable bounded cross-book evidence reconciliation for the existing SQLite corpus.

The calling model decides event identity and source meaning. The server only
finds/validates an authorized scope, checkpoints a finite frontier, verifies
exact evidence and applies explicit reviewed effects. No LLM or vector work is
performed under SQLite write locks. Existing story ids and versions survive.
"""
from __future__ import annotations

import hashlib
import json
import time
from uuid import uuid4

from .sqlite_corpus import canonical
from .story_contracts import (
    AttachEvidence, EvidenceLocator, RecordAssessment, ReconcileApply, ReconcileCancel,
    ReconcileClaim, ReconcileEnqueue, ReconcileStage, ReconcileStart, ReconcileNext,
    UpsertAssertion,
)
from .story_registry import StoryError, digest, fail, now

LEASE_SECONDS = 600
MAX_FRONTIER = 50


class StoryReconciler:
    def __init__(self, registry):
        self.registry = registry

    @staticmethod
    def _overflow_pending(db, run_id):
        return db.execute("""SELECT COUNT(*) FROM story_reconcile_overflow
            WHERE origin_run_id=? AND state='pending'""", (run_id,)).fetchone()[0]

    @staticmethod
    def _overflow_enqueue(db, run_id, refs):
        """Durable, idempotent overflow; no model packets over 50."""
        for ref in refs:
            db.execute("""INSERT OR IGNORE INTO story_reconcile_overflow(
                origin_run_id,ref_kind,ref_id,reference,state,added_at
            ) VALUES(?,?,?,?,'pending',?)""",
                (run_id, ref["kind"], ref["id"], canonical(ref), now()))

    def _run(self, db, ident):
        return db.execute("SELECT * FROM story_reconcile_runs WHERE id=?", (ident,)).fetchone()

    def _scoped_run(self, db, actor, ident, capability="researcher"):
        run = self._run(db, ident)
        if run is None or run["owner_id"] != actor:
            fail("not_found_or_not_accessible")
        self.registry._read_story(db, actor, run["anchor_story_id"], capability)
        return run

    def _candidate(self, db, actor, anchor_id, reference):
        kind, ident = reference.kind, str(reference.ref_id)
        if kind == "story":
            if ident == anchor_id:
                fail("validation_failed", "A story cannot compare itself")
            record, snapshot = self.registry._read_story(db, actor, ident)
            if record["archived"] or record["merged_into"]:
                fail("source_changed", "Target story is archived/merged")
            if not any(a.get("evidence_ids") for a in snapshot["assertions"]):
                fail("invalid_evidence", "Target story lacks printed source evidence")
            return {"kind": kind, "id": ident, "revision": int(record["revision"])}
        chunk = self.registry._corpus_row(db, "rkb_chunks", ident)
        if not chunk:
            fail("not_found_or_not_accessible")
        document = self.registry._document_allowed(db, actor, chunk["document_id"])
        if not document or int(chunk["revision"]) != int(document.get("active_revision") or 0):
            fail("not_found_or_not_accessible")
        return {"kind": "chunk", "id": ident, "revision": int(chunk["revision"]),
                "document_id": str(chunk["document_id"])}

    def _evidence_fingerprint(self, db, actor, kind, ident):
        """Fingerprint current accepted proof, not a story title/editorial revision.

        Source revision, accepted root revision, original source SHA and exact
        evidence IDs participate. A metadata-only story edit does not rerun a
        completed pair; new/changed evidence does. Called only for <=50 refs
        in a bounded reconciliation packet, never an unbounded corpus scan.
        """
        if kind == "chunk":
            chunk = self.registry._corpus_row(db, "rkb_chunks", ident)
            if not chunk:
                fail("not_found_or_not_accessible")
            doc = self.registry._document_allowed(db, actor, chunk["document_id"])
            if not doc or int(doc.get("active_revision") or 0) != int(chunk["revision"]):
                fail("source_changed", "Candidate chunk revision no longer active")
            return digest({
                "kind": "chunk", "id": ident, "revision": chunk["revision"],
                "text_sha": chunk.get("text_sha256"),
                "search_material_sha": chunk.get("search_material_sha256"),
                "source_sha": doc.get("source_sha256"),
                "document_revision": doc.get("active_revision"),
                "region_ids": sorted(str(x) for x in chunk.get("region_ids") or []),
            })
        if kind != "story":
            fail("validation_failed", "Unsupported reconciliation pair kind")
        record, snapshot = self.registry._read_story(db, actor, ident)
        roots = []
        for source in db.execute("""SELECT source_kind,source_id,source_revision
            FROM story_dependencies WHERE story_id=?
            ORDER BY source_kind,source_id,source_revision""", (ident,)):
            if source["source_kind"] == "document":
                doc = self.registry._document_allowed(db, actor, source["source_id"])
                if not doc:
                    fail("not_found_or_not_accessible")
                authority = {
                    "active_revision": doc.get("active_revision"),
                    "source_sha256": doc.get("source_sha256"),
                }
            elif source["source_kind"] == "external":
                row = db.execute("""SELECT content_sha256 FROM story_sources
                    WHERE id=? AND version=?""",
                    (source["source_id"], source["source_revision"])).fetchone()
                if not row:
                    fail("source_changed", "External source version disappeared")
                authority = {"content_sha256": row["content_sha256"]}
            else:
                fail("validation_failed", "Unknown source kind in story dependency")
            roots.append((source["source_kind"], source["source_id"],
                          source["source_revision"], authority))
        assertions = sorted(
            (a["assertion_id"], a["revision"],
             sorted(str(e) for e in a.get("evidence_ids") or []))
            for a in snapshot.get("assertions") or []
        )
        return digest({"kind": "story", "story_id": record["id"],
                       "assertions": assertions, "source_roots": roots})

    def _pair_already_decided(self, db, actor, anchor_id, reference, policy):
        anchor_hash = self._evidence_fingerprint(db, actor, "story", anchor_id)
        candidate_hash = self._evidence_fingerprint(
            db, actor, reference["kind"], reference["id"])
        return bool(db.execute("""SELECT 1 FROM story_reconcile_pair_decisions
            WHERE anchor_story_id=? AND ref_kind=? AND ref_id=?
              AND policy_version=? AND anchor_evidence_hash=?
              AND candidate_evidence_hash=?""",
            (anchor_id, reference["kind"], reference["id"], policy,
             anchor_hash, candidate_hash)).fetchone())

    def _exact(self, db, actor, data):
        """Return verified accepted provenance; a quote match is not truth."""
        doc = self.registry._document_allowed(db, actor, data.document_id)
        page = self.registry._corpus_row(db, "rkb_pages", data.page_id)
        region = self.registry._corpus_row(db, "rkb_regions", data.region_id)
        if (not doc or not page or not region
                or page.get("document_id") != data.document_id
                or int(page.get("revision") or 0) != data.source_revision
                or str(region.get("page_id")) != data.page_id
                or int(doc.get("active_revision") or 0) < data.source_revision):
            fail("invalid_evidence", "Document/page/region do not form one accepted revision")
        source = str(region.get("source_text") or "")
        excerpt = data.original_excerpt
        if data.start is not None:
            begin, end = data.start, data.end
            if source[begin:end] != excerpt:
                fail("invalid_evidence", "Source offsets do not match the exact excerpt")
        else:
            begin = source.find(excerpt)
            if begin < 0 or source.find(excerpt, begin + 1) >= 0:
                fail("invalid_evidence", "Missing or ambiguous excerpt")
            end = begin + len(excerpt)
        return {"source_kind": "document", "source_id": data.document_id,
                "source_revision": data.source_revision, "page_id": data.page_id,
                "region_id": data.region_id, "original_excerpt": excerpt,
                "start": begin, "end": end,
                "physical_page_index": int(page["physical_page_index"]),
                "printed_page_number": page.get("printed_page_number"),
                "source_region_sha256": hashlib.sha256(source.encode()).hexdigest()}

    def _story_evidence(self, db, actor, story_id, provided, verified):
        record, snap = self.registry._read_story(db, actor, story_id)
        if not provided.evidence_id:
            fail("invalid_evidence", "Story comparisons require a registered evidence ID")
        allowed = {str(eid) for a in snap["assertions"] for eid in a.get("evidence_ids") or []}
        if provided.evidence_id not in allowed:
            fail("invalid_evidence", "Evidence is not used by this story revision")
        evidence = self.registry._row(db, "story_evidence", provided.evidence_id)
        if not evidence or (
                evidence["source_kind"] != "document"
                or evidence["source_id"] != verified["source_id"]
                or int(evidence["source_revision"]) != verified["source_revision"]):
            fail("invalid_evidence", "Candidate is not bound to the claimed source")
        locator = json.loads(evidence["locator"])
        if (locator.get("page_id") != verified["page_id"]
                or locator.get("region_id") != verified["region_id"]
                or verified["original_excerpt"] not in evidence["original_excerpt"]):
            fail("invalid_evidence", "Candidate quote escapes registered evidence")
        return record

    def _validate_pair(self, db, actor, run, reference, decision, *, applying=False):
        anchor_rec, _ = self.registry._read_story(db, actor, run["anchor_story_id"])
        if (not applying and int(anchor_rec["revision"]) !=
                int(run["anchor_story_revision"])):
            fail("source_changed", "Anchor story changed; re-evaluate the comparison")
        if (decision.identity_relation in {"unrelated", "unresolved"}
                and decision.proposed_effect != "no_change"):
            fail("validation_failed", "No automatic effect for unresolved/unrelated pair")
        if decision.proposed_effect == "link_stories" and reference["kind"] != "story":
            fail("validation_failed", "Story relation requires two existing stories")
        if decision.phase_of_source is not None and (
                decision.identity_relation != "part_or_phase"
                or decision.proposed_effect != "link_stories"):
            fail("validation_failed", "Explicit phase direction requires a phase link")
        if (decision.proposed_effect in {"attach_evidence", "add_attributed_claim"}
                and decision.identity_relation != "same_episode"):
            fail("validation_failed", "Enrichment requires same episode; link distinct phases")
        if decision.proposed_effect == "attach_evidence" and not decision.target_assertion_id:
            fail("validation_failed", "Attach requires an explicit existing assertion")
        if decision.proposed_effect == "add_attributed_claim" and (
                not decision.new_proposition or not decision.attributed_to):
            fail("validation_failed", "New claim must be attributed and proposed explicitly")
        assessment = decision.effective_assessment
        if assessment is not None:
            if decision.proposed_effect not in {"attach_evidence", "add_attributed_claim"}:
                fail("validation_failed", "Effective assessment requires changed assertion evidence")
            if assessment.independence != decision.independence:
                fail("validation_failed", "Assessment independence conflicts with pair decision")
            if (assessment.support_status == "corroborated" and
                    (decision.independence != "independent"
                     or decision.proposed_effect != "attach_evidence")):
                fail("validation_failed", "Corroboration needs two explicitly independent sources")
            if decision.proposed_effect == "attach_evidence":
                anchor_evidence = self.registry._row(db, "story_evidence",
                                                     decision.anchor_evidence.evidence_id)
                if (not anchor_evidence or
                        anchor_evidence["assertion_id"] != decision.target_assertion_id):
                    fail("validation_failed", "Assessment must name the target assertion's original evidence")
        left = self._exact(db, actor, decision.anchor_evidence)
        self._story_evidence(db, actor, run["anchor_story_id"],
                             decision.anchor_evidence, left)
        right = self._exact(db, actor, decision.candidate_evidence)
        if reference["kind"] == "story":
            record = self._story_evidence(db, actor, reference["id"],
                                          decision.candidate_evidence, right)
            if record["revision"] != reference["revision"]:
                fail("source_changed", "Target story revision changed")
        else:
            chunk = self.registry._corpus_row(db, "rkb_chunks", reference["id"])
            if (not chunk or decision.candidate_evidence.chunk_id != reference["id"]
                    or chunk["document_id"] != right["source_id"]
                    or int(chunk["revision"]) != right["source_revision"]):
                fail("invalid_evidence", "Comparison source chunk changed")
            valid_regions = {str(x) for x in chunk.get("region_ids") or []}
            valid_regions.update(str(x.get("region_id")) for x in chunk.get("source_spans") or []
                                 if x.get("region_id"))
            if right["region_id"] not in valid_regions:
                fail("invalid_evidence", "Evidence is not part of the candidate chunk")
        if decision.independence == "independent" and left["source_id"] == right["source_id"]:
            fail("validation_failed", "One source cannot count as an independent second root")
        return left, right

    def dispatch(self, principal, request, idempotency_key):
        data = request.model_dump(mode="json")
        registry = self.registry
        if isinstance(request, ReconcileNext):
            def authorize(db, actor):
                if request.document_id and not registry._document_allowed(
                        db, actor, request.document_id):
                    fail("not_found_or_not_accessible")
                return None

            def apply(db, actor):
                scoped = """EXISTS (
                    SELECT 1 FROM story_dependencies dep
                    WHERE dep.story_id=s.id AND dep.source_kind='document'
                      AND dep.source_id=?
                )"""
                condition = " AND " + scoped if request.document_id else ""
                scope_args = [request.document_id] if request.document_id else []
                # A fresh executor can rediscover an interrupted run with no
                # previous chat context or duplicated second run.
                active = db.execute("""SELECT r.* FROM story_reconcile_runs r
                    JOIN story_records s ON s.id=r.anchor_story_id
                    WHERE r.owner_id=? AND s.owner_id=?
                      AND r.anchor_story_revision=s.revision
                      AND r.state IN ('awaiting_search','awaiting_agent',
                                      'awaiting_review','leased')
                      AND s.archived=0""" + condition +
                    " ORDER BY r.updated_at,r.id LIMIT 1",
                    [actor,actor,*scope_args],
                ).fetchone()
                if active:
                    state = active["state"]
                    return {
                        "job_id": active["id"], "job_revision":active["revision"],
                        "anchor_story_id": active["anchor_story_id"],
                        "anchor_story_revision": active["anchor_story_revision"],
                        "state":state,
                        "next_action":"wait_for_lease" if state=="leased"
                            else "read_proposals" if state=="awaiting_review"
                            else "claim" if state=="awaiting_agent"
                            else "search_then_enqueue",
                        "reused":True,
                    }

                pending = db.execute("""SELECT q.story_id,q.story_revision
                    FROM story_reconcile_queue q
                    JOIN story_records s ON s.id=q.story_id
                    WHERE q.state='awaiting_agent' AND s.owner_id=?
                      AND s.archived=0 AND s.revision=q.story_revision""" +
                    condition + " ORDER BY q.created_at,q.story_id LIMIT 1",
                    [actor,*scope_args],
                ).fetchone()
                if not pending:
                    return {"resource_type":"reconciliation","state":"idle",
                            "next_action":None,"commit_state":"saved"}
                record, snap = registry._read_story(db, actor, pending["story_id"],
                                                    "researcher")
                if not any(a.get("evidence_ids") for a in snap["assertions"]):
                    fail("invalid_evidence","Pending cross-book story lacks evidence")
                run_id = str(uuid4())
                query = str(snap["metadata"]["title"])[:350]
                plan = {"policy_version":request.policy_version,
                        "queries":[query], "searched_channels":[],
                        "required_channels":["story_lexical","source_bge"],
                        "search_completeness":"not_exhaustive",
                        "index_generation":None,
                        "candidate_budget":request.max_candidate_pairs}
                db.execute("""INSERT INTO story_reconcile_runs(
                    id,owner_id,anchor_story_id,anchor_story_revision,policy_version,
                    query,search_plan,frontier,position,max_candidate_pairs,revision,
                    state,work_id,lease_token,lease_deadline,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id,actor,record["id"],record["revision"],
                     request.policy_version,query,canonical(plan),"[]",0,
                     request.max_candidate_pairs,1,"awaiting_search",None,None,None,now()))
                db.execute("""UPDATE story_reconcile_queue SET state='run_started'
                    WHERE story_id=? AND story_revision=?""",
                    (record["id"],record["revision"]))
                return {
                    "resource_type":"reconciliation", "job_id":run_id,
                    "job_revision":1,"anchor_story_id":record["id"],
                    "anchor_story_revision":record["revision"],
                    "state":"awaiting_search","query":query,"search_plan":plan,
                    "next_action":"search_then_enqueue","reused":False,
                    "commit_state":"queued",
                }
            return registry._mutation(principal, "story_reconcile_next",
                                      idempotency_key,data,authorize,apply)

        if isinstance(request, ReconcileStart):
            def authorize(db, actor):
                record, _ = registry._read_story(db, actor, request.anchor_story_id, "researcher")
                return record["workspace_id"]
            def apply(db, actor):
                record, snap = registry._read_story(db, actor, request.anchor_story_id, "researcher")
                if record["revision"] != request.expected_story_revision:
                    fail("revision_conflict", "Current story revision changed")
                if not any(a.get("evidence_ids") for a in snap.get("assertions", [])):
                    fail("invalid_evidence", "Only source-backed stories can be reconciled")
                refs = []
                skipped_decided = 0
                for ref in request.refs:
                    item = self._candidate(db, actor, request.anchor_story_id, ref)
                    if (item["kind"], item["id"]) in {
                            (v["kind"], v["id"]) for v in refs}:
                        continue
                    if self._pair_already_decided(
                            db, actor, request.anchor_story_id, item,
                            request.policy_version):
                        skipped_decided += 1
                        continue
                    refs.append(item)
                cap = min(MAX_FRONTIER, request.max_candidate_pairs)
                first_frontier, overflow_refs = refs[:cap], refs[cap:]
                query = (request.query or snap["metadata"]["title"]).strip()
                run_id = str(uuid4())
                plan = {"policy_version": request.policy_version,
                        "queries": [query], "searched_channels": [],
                        "required_channels": ["story_lexical", "source_bge"],
                        "search_completeness": "not_exhaustive",
                        "index_generation": None,
                        "candidate_budget": request.max_candidate_pairs,
                        "skipped_decided_pairs": skipped_decided}
                state = "awaiting_agent" if first_frontier else "awaiting_search"
                db.execute("""INSERT INTO story_reconcile_runs(
                    id,owner_id,anchor_story_id,anchor_story_revision,policy_version,
                    query,search_plan,frontier,position,max_candidate_pairs,revision,
                    state,work_id,lease_token,lease_deadline,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, actor, request.anchor_story_id, record["revision"],
                     request.policy_version, query, canonical(plan), canonical(first_frontier), 0,
                     request.max_candidate_pairs, 1, state, None, None, None, now()))
                self._overflow_enqueue(db, run_id, overflow_refs)
                db.execute("""UPDATE story_reconcile_queue SET state='run_started'
                    WHERE story_id=? AND story_revision<=?""",
                    (request.anchor_story_id, record["revision"]))
                return {"resource_type": "reconciliation", "resource_ids": [run_id],
                        "job_id": run_id, "job_revision": 1, "state": state,
                        "candidate_count": len(first_frontier),
                        "overflow_pending": len(overflow_refs),
                        "coverage_state": "partial_budget_exhausted" if overflow_refs else
                                          "bounded_frontier_only",
                        "skipped_decided_pairs": skipped_decided,
                        "search_plan": plan,
                        "next_action": "claim" if first_frontier else "search_then_enqueue",
                        "commit_state": "queued"}
            return registry._mutation(principal, "story_reconcile", idempotency_key, data, authorize, apply)

        def authorize(db, actor):
            run = self._scoped_run(db, actor, request.run_id, "researcher")
            return registry._row(db, "story_records", run["anchor_story_id"])["workspace_id"]

        def apply(db, actor):
            run = self._scoped_run(db, actor, request.run_id, "researcher")
            if int(run["revision"]) != request.expected_job_revision:
                fail("revision_conflict", "Reconciliation revision has changed")
            frontier = json.loads(run["frontier"])
            plan = json.loads(run["search_plan"])
            revision = int(run["revision"]) + 1
            if isinstance(request, ReconcileEnqueue):
                if run["state"] in {"cancelled", "completed_under_policy",
                                    "already_compared_under_policy", "no_match_found_under_policy",
                                    "leased"}:
                    fail("validation_failed", "Finish current lease or start a new run")
                existing = {(x["kind"], x["id"]) for x in frontier}
                new_refs = []
                skipped_decided = 0
                for ref in request.refs:
                    item = self._candidate(db, actor, run["anchor_story_id"], ref)
                    key = item["kind"], item["id"]
                    if key in existing:
                        continue
                    if self._pair_already_decided(
                            db, actor, run["anchor_story_id"], item,
                            run["policy_version"]):
                        skipped_decided += 1
                        continue
                    new_refs.append(item)
                    existing.add(key)
                if len(frontier) + len(new_refs) > min(MAX_FRONTIER, run["max_candidate_pairs"]):
                    fail("validation_failed", "Candidate budget exhausted; continue under a new run")
                frontier.extend(new_refs)
                plan["skipped_decided_pairs"] = (
                    int(plan.get("skipped_decided_pairs") or 0) + skipped_decided)
                plan["searched_channels"] = sorted(set(plan["searched_channels"]) |
                                                  set(request.searched_channels))
                search_done = {"story_lexical", "source_bge"}.issubset(
                    set(plan["searched_channels"]))
                state = ("awaiting_agent" if len(frontier)>run["position"] else
                         "already_compared_under_policy" if not frontier
                             and search_done and plan["skipped_decided_pairs"] else
                         "no_match_found_under_policy" if not frontier and search_done else
                         "awaiting_search" if not frontier else "awaiting_review")
                db.execute("""UPDATE story_reconcile_runs
                    SET frontier=?,search_plan=?,revision=?,state=?,updated_at=? WHERE id=?""",
                    (canonical(frontier), canonical(plan), revision, state, now(), run["id"]))
                return {"job_id": run["id"], "job_revision": revision, "state": state,
                        "candidate_count": len(frontier), "processed_pairs": run["position"],
                        "searched_channels": plan["searched_channels"],
                        "skipped_decided_pairs": plan["skipped_decided_pairs"],
                        "next_action": "claim" if len(frontier)>run["position"]
                            else None if state in {"already_compared_under_policy",
                                                  "no_match_found_under_policy"}
                            else "review_or_expand"}

            if isinstance(request, ReconcileClaim):
                if (run["lease_token"] and run["lease_deadline"]
                        and float(run["lease_deadline"]) > time.time()):
                    fail("busy_retryable", "A previous batch is still leased")
                if run["state"] in {"cancelled", "completed_under_policy",
                                    "already_compared_under_policy",
                                    "no_match_found_under_policy"}:
                    fail("validation_failed", "Run was completed or cancelled")
                if run["position"] >= len(frontier):
                    state = "awaiting_review"
                    db.execute("""UPDATE story_reconcile_runs
                        SET revision=?,state=?,work_id=NULL,lease_token=NULL,
                            lease_deadline=NULL,updated_at=? WHERE id=?""",
                        (revision,state,now(),run["id"]))
                    return {"job_id":run["id"],"job_revision":revision,
                            "state":state,"next_action":"review_or_expand"}
                reference = frontier[run["position"]]
                # Always authorize the candidate again at lease time.
                self._candidate(db, actor, run["anchor_story_id"],
                                _ref_input(reference))
                work_id, lease_token = str(uuid4()), str(uuid4())
                deadline = time.time() + LEASE_SECONDS
                db.execute("""UPDATE story_reconcile_runs
                    SET revision=?,state='leased',work_id=?,lease_token=?,
                        lease_deadline=?,updated_at=? WHERE id=?""",
                    (revision,work_id,lease_token,deadline,now(),run["id"]))
                return {"job_id":run["id"],"job_revision":revision,"state":"leased",
                        "work_id":work_id,"lease_token":lease_token,
                        "lease_deadline":deadline,"candidate":reference,
                        "anchor_story_id":run["anchor_story_id"],
                        "anchor_story_revision":run["anchor_story_revision"],
                        "bundle_limits":{"max_pairs":2,"max_evidence_fragments":8,
                                         "max_bundle_bytes":65536,"max_bundle_tokens_estimate":6000},
                        "next_action":"read_both_evidence_then_stage"}

            if isinstance(request, ReconcileStage):
                if (run["state"] != "leased" or run["work_id"] != request.work_id
                        or run["lease_token"] != request.lease_token
                        or not run["lease_deadline"] or run["lease_deadline"] < time.time()):
                    fail("revision_conflict", "Lease was replaced or expired")
                reference = frontier[run["position"]]
                left, right = self._validate_pair(db, actor, run, reference, request.decision)
                if db.execute("""SELECT 1 FROM story_reconcile_proposals
                    WHERE run_id=? AND ref_kind=? AND ref_id=?""",
                    (run["id"],reference["kind"],reference["id"])).fetchone():
                    fail("revision_conflict", "This source pair already has a staged comparison")
                proposal_id = str(uuid4())
                payload = {"reference": reference,
                           "decision": request.decision.model_dump(mode="json"),
                           "anchor_proof": left, "candidate_proof": right,
                           "anchor_story_revision": run["anchor_story_revision"]}
                db.execute("""INSERT INTO story_reconcile_proposals
                   (id,run_id,ref_kind,ref_id,decision,state,actor_id,created_at,applied_at)
                   VALUES(?,?,?,?,?,'pending_review',?,?,NULL)""",
                   (proposal_id,run["id"],reference["kind"],reference["id"],
                    canonical(payload),actor,now()))
                pos = run["position"] + 1
                state = "awaiting_agent" if pos<len(frontier) else "awaiting_review"
                db.execute("""UPDATE story_reconcile_runs
                    SET revision=?,position=?,state=?,work_id=NULL,
                        lease_token=NULL,lease_deadline=NULL,updated_at=?
                    WHERE id=?""", (revision,pos,state,now(),run["id"]))
                return {"job_id":run["id"],"job_revision":revision,"state":state,
                        "processed_pairs":pos,"proposal_id":proposal_id,
                        "next_action":"claim" if pos<len(frontier) else "review_proposals"}

            if isinstance(request, ReconcileApply):
                if run["state"] == "cancelled":
                    fail("validation_failed", "Cancelled comparison cannot be applied")
                proposal = db.execute("SELECT * FROM story_reconcile_proposals WHERE id=? AND run_id=?",
                                      (request.proposal_id,run["id"])).fetchone()
                if not proposal:
                    fail("not_found_or_not_accessible")
                if proposal["state"] == "applied":
                    return {"job_id":run["id"],"job_revision":run["revision"],
                            "proposal_id":proposal["id"],"committed_effect":"already_applied",
                            "commit_state":"saved"}
                if proposal["state"] != "pending_review":
                    fail("validation_failed", "Proposal not pending review")
                stored = json.loads(proposal["decision"])
                reference = stored["reference"]
                decision = _decision_model(stored["decision"])
                left, right = self._validate_pair(db, actor, run, reference, decision,
                                                   applying=True)
                if stored["anchor_proof"] != left or stored["candidate_proof"] != right:
                    fail("source_changed", "Evidence changed after model comparison")
                # The anchor is the chosen editorial dossier. A candidate story
                # remains independently addressable, never silently becomes
                # the destination of a source enrichment.
                target_id = run["anchor_story_id"]
                target, snap = registry._read_story(db, actor, target_id, "editor")
                if target["revision"] != request.expected_target_revision:
                    fail("revision_conflict", "Target story revision changed")
                if target["archived"] or target["merged_into"]:
                    fail("source_changed", "Target story was archived")
                effect = decision.proposed_effect
                result_revision = target["revision"]
                if effect == "link_stories":
                    pair = sorted((run["anchor_story_id"], reference["id"]))
                    if pair[0] == pair[1]:
                        fail("validation_failed")
                    kind = ("same_episode" if decision.identity_relation=="same_episode"
                            else "phase_of" if decision.identity_relation=="part_or_phase"
                            else "related_theme")
                    phase_from = (run["anchor_story_id"] if decision.phase_of_source == "anchor"
                                  else reference["id"] if decision.phase_of_source == "candidate"
                                  else None) if kind == "phase_of" else None
                    phase_to = (reference["id"] if decision.phase_of_source == "anchor"
                                else run["anchor_story_id"] if decision.phase_of_source == "candidate"
                                else None) if kind == "phase_of" else None
                    existing = db.execute("""SELECT active,phase_from_story_id,phase_to_story_id
                        FROM story_relations WHERE left_story_id=? AND right_story_id=? AND kind=?""",
                        (pair[0], pair[1], kind)).fetchone()
                    if existing and not existing["active"]:
                        fail("revision_conflict", "Retracted link needs explicit reviewed restoration")
                    if existing and kind == "phase_of" and (
                            existing["phase_from_story_id"] != phase_from
                            or existing["phase_to_story_id"] != phase_to):
                        fail("revision_conflict", "Phase direction differs; reviewed correction required")
                    db.execute("""INSERT OR IGNORE INTO story_relations(
                        id,left_story_id,right_story_id,kind,rationale,
                        source_proposal_id,actor_id,created_at,
                        phase_from_story_id,phase_to_story_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (str(uuid4()),pair[0],pair[1],kind,
                         decision.rationale,proposal["id"],actor,now(),phase_from,phase_to))
                    registry._audit(db, principal, target_id, result_revision,
                                    "reconcile_link:"+kind, request.reviewer_note)
                elif effect in {"attach_evidence","add_attributed_claim"}:
                    # Always attach the candidate's original evidence, not the
                    # anchor's already existing proof. Applies equally to
                    # candidate stories and uncatalogued source chunks.
                    quoted = right
                    if effect == "add_attributed_claim":
                        operation = UpsertAssertion(
                            op="upsert_assertion",kind="attributed_account",
                            account_kind="other",attributed_to=decision.attributed_to,
                            proposition=decision.new_proposition)
                        registry._apply_op(db,principal,snap,operation)
                        assertion = snap["assertions"][-1]
                    else:
                        assertion = next((a for a in snap["assertions"]
                            if a["assertion_id"]==decision.target_assertion_id),None)
                        if not assertion:
                            fail("revision_conflict","Specified assertion no longer exists")
                    registry._apply_op(db,principal,snap,AttachEvidence(
                        op="attach_evidence",
                        assertion_id=assertion["assertion_id"],
                        assertion_revision=assertion["revision"],
                        source_kind="document",source_id=quoted["source_id"],
                        source_revision=quoted["source_revision"],
                        relation=decision.evidence_relation,
                        original_excerpt=quoted["original_excerpt"],
                        locator=EvidenceLocator(
                            page_id=quoted["page_id"],region_id=quoted["region_id"],
                            physical_page_index=quoted["physical_page_index"],
                            printed_page_number=quoted["printed_page_number"],
                            start=quoted["start"],end=quoted["end"])))
                    if decision.effective_assessment is not None:
                        # Only the two exact passages reviewed in this proposal,
                        # never every other unexamined citation of the dossier.
                        assessed_ids = [assertion["evidence_ids"][-1]]
                        if effect == "attach_evidence":
                            assessed_ids.insert(0, decision.anchor_evidence.evidence_id)
                        judgement = decision.effective_assessment
                        registry._apply_op(db, principal, snap, RecordAssessment(
                            op="record_assessment",
                            assertion_id=assertion["assertion_id"],
                            assertion_revision=assertion["revision"],
                            evidence_ids=assessed_ids,
                            support_status=judgement.support_status,
                            independence=judgement.independence,
                            semantic_review=judgement.semantic_review,
                            rationale=judgement.rationale,
                            assessor_kind="model",
                            method_version=judgement.method_version,
                        ))
                    result_revision = target["revision"]+1
                    registry._snapshot(db,target_id,result_revision,snap,principal,
                                       "reconcile_"+effect)
                    # Incremental follow-up, not an all-book reread.
                    db.execute("""INSERT OR IGNORE INTO story_reconcile_queue
                        (story_id,story_revision,state,created_at)
                        VALUES(?,?,'awaiting_agent',?)""",
                        (target_id,result_revision,now()))
                elif effect != "no_change":
                    fail("validation_failed","Unknown effect")
                # Record a post-apply proof fingerprint. For attach_evidence the
                # anchor has just gained the candidate proof; using the stale
                # pre-apply hash would re-enqueue the identical pair forever.
                db.execute("""INSERT OR IGNORE INTO story_reconcile_pair_decisions(
                    anchor_story_id,ref_kind,ref_id,policy_version,
                    anchor_evidence_hash,candidate_evidence_hash,proposal_id,
                    proposed_effect,decided_at
                ) VALUES(?,?,?,?,?,?,?,?,?)""", (
                    target_id, reference["kind"], reference["id"],
                    run["policy_version"],
                    self._evidence_fingerprint(db, actor, "story", target_id),
                    self._evidence_fingerprint(db, actor,
                                               reference["kind"], reference["id"]),
                    proposal["id"], effect, now(),
                ))
                db.execute("""UPDATE story_reconcile_proposals
                    SET state='applied',applied_at=? WHERE id=?""",(now(),proposal["id"]))
                remaining = db.execute("""SELECT COUNT(*) FROM story_reconcile_proposals
                    WHERE run_id=? AND state='pending_review'""",(run["id"],)).fetchone()[0]
                finished = (run["position"]>=len(frontier) and remaining==0
                            and {"story_lexical","source_bge"}.issubset(
                                set(plan.get("searched_channels") or [])))
                state = "completed_under_policy" if finished else (
                        "awaiting_agent" if run["position"]<len(frontier) else "awaiting_review")
                db.execute("""UPDATE story_reconcile_runs SET revision=?,state=?,updated_at=?
                    WHERE id=?""",(revision,state,now(),run["id"]))
                return {"job_id":run["id"],"job_revision":revision,"state":state,
                        "proposal_id":proposal["id"],"committed_effect":effect,
                        "target_story_id":target_id,"committed_story_revision":result_revision,
                        "next_action":"claim" if state=="awaiting_agent" else "review_or_expand"}

            if isinstance(request, ReconcileCancel):
                # Preserve the full comparison/audit as a cancelled proposal,
                # not an unreviewed actionable orphan after correction.
                db.execute("""UPDATE story_reconcile_proposals
                    SET state='cancelled' WHERE run_id=? AND state='pending_review'""",
                    (run["id"],))
                db.execute("""UPDATE story_reconcile_runs
                    SET revision=?,state='cancelled',work_id=NULL,
                        lease_token=NULL,lease_deadline=NULL,updated_at=?
                    WHERE id=?""",(revision,now(),run["id"]))
                return {"job_id":run["id"],"job_revision":revision,"state":"cancelled",
                        "next_action":None}
            fail("validation_failed", "Unknown reconcile operation")
        return registry._mutation(principal, "story_reconcile", idempotency_key, data, authorize, apply)

    def status(self, principal, run_id, cursor=None, limit=5, proposal_id=None):
        with self.registry.corpus.connect() as db:
            actor = self.registry._actor(db, principal)
            run = self._scoped_run(db, actor, run_id, "viewer")
            plan = json.loads(run["search_plan"])
            frontier = json.loads(run["frontier"])
            pending = db.execute("""SELECT COUNT(*) FROM story_reconcile_proposals
                WHERE run_id=? AND state='pending_review'""",(run_id,)).fetchone()[0]
            applied = db.execute("""SELECT COUNT(*) FROM story_reconcile_proposals
                WHERE run_id=? AND state='applied'""",(run_id,)).fetchone()[0]
            next_action = (
                None if run["state"] in {"cancelled","completed_under_policy",
                                         "already_compared_under_policy",
                                         "no_match_found_under_policy"}
                else "claim" if run["position"]<len(frontier)
                else "search_then_enqueue" if not frontier
                else "review_proposals" if pending
                else "expand_or_finish_under_policy"
            )
            after = str(cursor or "")
            if len(after)>128:
                fail("validation_failed", "Invalid proposal cursor")
            take = max(1, min(int(limit), 10))
            proposal_rows = db.execute("""SELECT id,state FROM story_reconcile_proposals
                WHERE run_id=? AND id>? ORDER BY id LIMIT ?""",
                (run_id,after,take+1)).fetchall()
            proposal_items = [{"proposal_id":row["id"],"state":row["state"]}
                              for row in proposal_rows[:take]]
            detail = None
            if proposal_id:
                proposal = db.execute("""SELECT * FROM story_reconcile_proposals
                    WHERE id=? AND run_id=?""", (proposal_id,run_id)).fetchone()
                if proposal is None:
                    fail("not_found_or_not_accessible")
                stored = json.loads(proposal["decision"])
                source = stored["reference"]
                self._candidate(db, actor, run["anchor_story_id"], _ref_input(source))
                decision = _decision_model(stored["decision"])
                self._validate_pair(db, actor, run, source, decision, applying=True)
                detail = {
                    "proposal_id":proposal["id"],"state":proposal["state"],
                    "candidate":source,"decision":stored["decision"],
                    "anchor_proof":stored["anchor_proof"],
                    "candidate_proof":stored["candidate_proof"],
                    "recorded_actor_id":proposal["actor_id"],
                    "created_at":proposal["created_at"],
                    "applied_at":proposal["applied_at"],
                }
            return {"job_id":run_id,"revision":run["revision"],"state":run["state"],
                    "executor_state":"external_model_required",
                    "anchor_story_id":run["anchor_story_id"],
                    "anchor_story_revision":run["anchor_story_revision"],
                    "policy_version":run["policy_version"],"query":run["query"],
                    "searched_channels":plan.get("searched_channels",[]),
                    "required_channels":plan.get("required_channels",[]),
                    "candidate_count":len(frontier),"processed_pairs":run["position"],
                    "pending_proposals":pending,"applied_proposals":applied,
                    "max_candidate_pairs":run["max_candidate_pairs"],
                    "skipped_decided_pairs":int(plan.get("skipped_decided_pairs") or 0),
                    "pair_decision_basis":"accepted_evidence_fingerprints_and_policy_v1",
                    "lease_active":bool(run["lease_token"] and run["lease_deadline"]
                                        and run["lease_deadline"]>time.time()),
                    "next_action":next_action,
                    "proposals":proposal_items,
                    "proposal_detail":detail,
                    "proposals_has_more":len(proposal_rows)>take,
                    "proposals_next_cursor":proposal_items[-1]["proposal_id"]
                        if len(proposal_rows)>take and proposal_items else None,
                    "coverage":"completed_under_declared_policy_is_not_global_semantic_completeness"}


def _ref_input(row):
    from .story_contracts import ReconcileCandidateRef
    return ReconcileCandidateRef(kind=row["kind"],ref_id=row["id"])


def _decision_model(data):
    from .story_contracts import ReconcileDecision
    return ReconcileDecision.model_validate(data)
