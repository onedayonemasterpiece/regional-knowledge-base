"""Same-pass book ingestion -> evidence-backed Story Registry candidates.

Semantic discovery is supplied by the *existing importing model* while it
visually reviews source pages and builds chunks. This module only validates
source offsets, persists bounded checkpoints and atomically accepts candidates
when the corresponding book revision activates. No hidden LLM/embedding calls,
no second corpus store and no independent long-running story assistant.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from types import SimpleNamespace
from uuid import UUID

from .contracts import Principal, StagePageInput
from .sqlite_corpus import canonical
from .story_contracts import (
    AttachEvidence, EvidenceLocator, StageStoryCandidateInput, UpsertAssertion,
)
from .story_registry import StoryRegistry, StoryError, digest, now


def _resolve_candidates(graph, candidates):
    regions = {
        (str(page.page_id), region.region_key):
            (page, region)
        for page in graph.pages for region in page.regions
    }
    resolved = []
    for candidate in candidates:
        proof = []
        for ref in candidate.evidence_refs:
            found = regions.get((ref.page_id, ref.region_key))
            if found is None:
                raise ValueError("story candidate evidence is not in a staged source page")
            page, region = found
            if page.source_material not in {"full_native", "visual_reviewed", "accepted_reuse"}:
                raise ValueError("story candidate evidence requires reviewed source")
            original = region.source_text or ""
            exact = ref.original_excerpt
            if ref.start is not None or ref.end is not None:
                if ref.start is None or ref.end is None or original[ref.start:ref.end] != exact:
                    raise ValueError("story evidence source span/quote mismatch")
                start, end = ref.start, ref.end
            else:
                first = original.find(exact)
                if first < 0 or original.find(exact, first + 1) >= 0:
                    raise ValueError("story evidence quote is missing or ambiguous; supply offsets")
                start, end = first, first + len(exact)
            proof.append({
                "page_id": str(page.page_id),
                "region_id": str(region.region_id),
                "physical_page_index": page.physical_page_index,
                "printed_page_number": page.printed_page_number,
                "source_region_sha256": hashlib.sha256(original.encode("utf8")).hexdigest(),
                "original_excerpt": exact,
                "start": start, "end": end,
            })
        data = candidate.model_dump(mode="json", exclude={"evidence_refs"})
        data["evidence"] = proof
        # Model-authored title is not a factual assertion; excerpt and location
        # are explicit, immutable provenance of what was observed.
        data["fingerprint"] = digest({
            "title": candidate.title.strip().casefold(),
            "material_type": candidate.material_type,
            "evidence": sorted([
                (e["physical_page_index"], e["source_region_sha256"],
                 e["start"], e["end"], e["original_excerpt"])
                for e in proof
            ]),
        })
        resolved.append(data)
    return resolved


def persist_stage(corpus, principal, ingestion_row, graph, pages, candidates):
    """Bounded DB transaction after the canonical staged-page graph is stored.

    If this call crashes between graph storage and this checkpoint, book
    activation remains independent and extraction is explicitly incomplete.
    Restaging the same batch is idempotent; restaging changed source pages
    invalidates older candidates linked to those pages.
    """
    actor = str(UUID(principal.subject))
    ing_id = str(ingestion_row["id"])
    doc_id = str(ingestion_row["document_id"])
    revision = int(ingestion_row["staged_revision"])
    if ingestion_row.get("owner_user_id") and str(ingestion_row["owner_user_id"]) != actor:
        raise PermissionError("source owner required for candidate extraction")
    if len(candidates) > 12 or len(pages) > 8:
        raise ValueError("bounded extraction batch exceeded")
    collected = _resolve_candidates(graph, candidates)
    changed = [page.page_id for page in pages]
    with corpus.connect() as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        # Revision and owner are checked inside the same write transaction.
        job_row = db.execute("""SELECT payload FROM corpus_rows
                 WHERE table_name='rkb_ingestion_jobs' AND row_key=?""", (ing_id,)).fetchone()
        if not job_row:
            raise ValueError("ingestion identity missing")
        job = json.loads(job_row[0])
        if (str(job.get("owner_user_id")) != actor or str(job.get("document_id")) != doc_id
                or int(job.get("staged_revision") or 0) != revision
                or job["state"] == "finalized"):
            raise ValueError("ingestion changed, was finalized or actor lacks rights")
        if changed:
            marks = ",".join("?" for _ in changed)
            db.execute(f"""DELETE FROM story_ingest_candidates
                  WHERE ingestion_id=? AND state='staged'
                    AND EXISTS (
                      SELECT 1 FROM json_each(story_ingest_candidates.payload,'$.evidence') e
                      WHERE json_extract(e.value,'$.page_id') IN ({marks})
                    )""", [ing_id, *changed])
            for page in pages:
                db.execute("""INSERT INTO story_ingest_coverage
                  (ingestion_id,document_id,source_revision,physical_page_index,state,at)
                  VALUES(?,?,?,?,?,?) ON CONFLICT(ingestion_id,physical_page_index)
                  DO UPDATE SET state=excluded.state,at=excluded.at""",
                           (ing_id, doc_id, revision, page.physical_page_index,
                            "reviewed" if page.story_candidates_reviewed else "unreviewed", now()))
        for candidate in collected:
            key = candidate["candidate_key"]
            existing = db.execute("""SELECT payload,state FROM story_ingest_candidates
                    WHERE ingestion_id=? AND candidate_key=?""", (ing_id, key)).fetchone()
            if existing and existing["state"] == "accepted":
                continue
            db.execute("""INSERT INTO story_ingest_candidates
                    (ingestion_id,candidate_key,document_id,source_revision,
                     payload,payload_hash,state,story_id,at)
                    VALUES(?,?,?,?,?,?,'staged',NULL,?)
                    ON CONFLICT(ingestion_id,candidate_key)
                    DO UPDATE SET payload=excluded.payload,payload_hash=excluded.payload_hash,
                                  state='staged',story_id=NULL,at=excluded.at""",
                       (ing_id, key, doc_id, revision,
                        canonical(candidate), digest(candidate), now()))
    return {"candidate_count": len(collected), "reviewed_in_batch":
            sum(bool(page.story_candidates_reviewed) for page in pages)}


def activate_stories(ctx, document, ingestion_id, revision):
    """Called INSIDE the accepted book's existing SQLite activation transaction.

    Any invalid optional candidate is skipped and logged in the staging record.
    Import and vector activation remain authoritative even if a story needs
    model review. A successful candidate cannot appear before book activation.
    """
    db = ctx.db
    corpus = ctx.corpus
    actor = str(UUID(str(document["owner_user_id"])))
    registry = StoryRegistry(corpus, migrate=False)
    application_actor = Principal(subject=actor, client_id="book_ingest",
                                  issuer="internal_source_activation", access_token="")
    rows = db.execute("""SELECT * FROM story_ingest_candidates
                    WHERE ingestion_id=? AND source_revision=? AND state='staged'
                    ORDER BY candidate_key""", (str(ingestion_id), int(revision))).fetchall()
    accepted, reused, skipped = 0, 0, 0
    for row in rows:
        db.execute("SAVEPOINT story_candidate")
        try:
            data = json.loads(row["payload"])
            proof = data["evidence"]
            # The current accepted source must match exactly the reviewed regions.
            for e in proof:
                page = registry._corpus_row(db, "rkb_pages", e["page_id"])
                region = registry._corpus_row(db, "rkb_regions", e["region_id"])
                if (not page or not region or page.get("document_id") != document["id"]
                        or int(page["revision"]) != int(revision)
                        or region.get("page_id") != e["page_id"]
                        or hashlib.sha256((region.get("source_text") or "").encode()).hexdigest()
                            != e["source_region_sha256"]):
                    raise ValueError("staged story source no longer matches accepted region")
            prior = db.execute("""SELECT story_id FROM story_ingest_identity
                  WHERE document_id=? AND source_sha256=? AND fingerprint=?""",
                               (document["id"], document["source_sha256"],
                                data["fingerprint"])).fetchone()
            if prior:
                story_id = prior["story_id"]
                reused += 1
            else:
                seed = {"text": data["summary"], "origin_status": "known",
                        "origin_note": "Автоматически выделено при импорте принятой книги",
                        "speaker": None}
                metadata = {"title": data["title"], "summary": data["summary"],
                            "material_type": data["material_type"]}
                story_id, snap = registry._create_record(
                    db, application_actor, seed, metadata,
                    document.get("workspace_id"),
                    [{"kind": "document", "source_id": document["id"],
                      "source_revision": revision}])
                assertion = UpsertAssertion(
                    op="upsert_assertion", kind="attributed_account",
                    account_kind=data["account_kind"],
                    proposition=data["proposition"],
                    attributed_to=data.get("attributed_to"),
                    reported_by=data.get("reported_by"))
                registry._apply_op(db, application_actor, snap, assertion)
                claim = snap["assertions"][-1]
                for evidence in proof:
                    registry._apply_op(db, application_actor, snap, AttachEvidence(
                        op="attach_evidence",
                        assertion_id=claim["assertion_id"], assertion_revision=claim["revision"],
                        source_kind="document", source_id=document["id"],
                        source_revision=revision, relation="reports",
                        source_role_for_assertion="unspecified",
                        original_excerpt=evidence["original_excerpt"],
                        locator=EvidenceLocator(
                            page_id=evidence["page_id"], region_id=evidence["region_id"],
                            physical_page_index=evidence["physical_page_index"],
                            printed_page_number=evidence["printed_page_number"],
                            start=evidence["start"], end=evidence["end"])))
                snap["state"] = "candidate"
                registry._snapshot(db, story_id, 2, snap, application_actor,
                                   "automated_source_candidate")
                db.execute("""INSERT INTO story_ingest_identity
                          (document_id,source_sha256,fingerprint,story_id,at)
                          VALUES(?,?,?,?,?)""",
                           (document["id"], document["source_sha256"],
                            data["fingerprint"], story_id, now()))
                accepted += 1
            db.execute("""UPDATE story_ingest_candidates SET state='accepted',story_id=?,at=?
                      WHERE ingestion_id=? AND candidate_key=?""",
                       (story_id, now(), ingestion_id, row["candidate_key"]))
            db.execute("RELEASE story_candidate")
        except (ValueError, KeyError, sqlite3.IntegrityError, StoryError):
            db.execute("ROLLBACK TO story_candidate")
            db.execute("RELEASE story_candidate")
            db.execute("""UPDATE story_ingest_candidates SET state='needs_review',at=?
                          WHERE ingestion_id=? AND candidate_key=?""",
                       (now(), ingestion_id, row["candidate_key"]))
            skipped += 1
    return {"accepted": accepted, "reused": reused, "needs_review": skipped}


def extraction_status(corpus, ingestion_row):
    doc_id = ingestion_row.get("document_id")
    ing_id = ingestion_row.get("id")
    if not doc_id or not ing_id:
        return None
    doc = corpus.one("rkb_documents", str(doc_id))
    if not doc:
        return None
    total = int(doc.get("page_count") or 0)
    with corpus.connect() as db:
        counts = db.execute("""SELECT state,COUNT(*) AS n FROM story_ingest_coverage
                    WHERE ingestion_id=? GROUP BY state""", (str(ing_id),)).fetchall()
        coverage = {row["state"]: row["n"] for row in counts}
        states = db.execute("""SELECT state,COUNT(*) AS n FROM story_ingest_candidates
                    WHERE ingestion_id=? GROUP BY state""", (str(ing_id),)).fetchall()
        candidate_states = {row["state"]: row["n"] for row in states}
    reviewed = coverage.get("reviewed", 0)
    staged = candidate_states.get("staged", 0)
    accepted = candidate_states.get("accepted", 0)
    needs_review = candidate_states.get("needs_review", 0)
    if total > 0 and reviewed >= total and not staged and not needs_review and ingestion_row.get("state") == "finalized":
        state = "completed"
    elif total > 0 and reviewed >= total and ingestion_row.get("state") != "finalized":
        state = "staged"
    elif reviewed:
        state = "reviewing" if ingestion_row.get("state") != "finalized" else "awaiting_agent"
    else:
        state = "not_reviewed" if ingestion_row.get("state") != "finalized" else "awaiting_agent"
    return {
        "state": state, "reviewed_pages": reviewed, "total_pages": total,
        "staged_candidates": staged, "saved_candidates": accepted,
        "needs_review_candidates": needs_review,
        "coverage_note": ("Просмотр страниц не гарантирует полноту выявления сюжетов; "
                          "непроверенные утверждения не становятся историческими фактами.")
    }
