"""Append-only research journal in the existing corpus SQLite authority.

No model, publisher, provider client or second IAM lives here. Calling code
supplies an authenticated principal. Every receipt and history read rechecks
current case and source access. Channel actors and scan leases are integrated
separately before exposing those routes.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import UUID, uuid4

from .disagreement_contracts import (
    CaseCreate, CaseGrant, CaseRevise, CaseState, ConclusionAdd,
    ContributionAdd, ContributionCorrect, EvidenceAdd,
)
from .sqlite_corpus import canonical
from .story_registry import StoryRegistry
from .request_authority import track_resources

ROLES = {"viewer": 1, "contributor": 2, "curator": 3, "manager": 4}


class JournalError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def fail(code):
    raise JournalError(code)


def now():
    return datetime.now(timezone.utc).isoformat()


class DisagreementJournal:
    def __init__(self, corpus, *, participants=None):
        self.corpus = corpus
        self.participants = participants
        self.sources = StoryRegistry(corpus, migrate=False)
        with corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS disagreement_cases(
              id TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
              revision INTEGER NOT NULL CHECK(revision>0), state TEXT NOT NULL
                CHECK(state IN ('open','paused','archived')),
              event_seq INTEGER NOT NULL, current_conclusion_id TEXT,
              payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS disagreement_case_versions(
              case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              revision INTEGER NOT NULL, payload TEXT NOT NULL,
              actor_id TEXT NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(case_id,revision));
            CREATE TABLE IF NOT EXISTS disagreement_grants(
              case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              actor_id TEXT NOT NULL, capability TEXT NOT NULL,
              PRIMARY KEY(case_id,actor_id));
            CREATE TABLE IF NOT EXISTS disagreement_evidence(
              id TEXT NOT NULL, version INTEGER NOT NULL,
              case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              document_id TEXT NOT NULL, source_revision INTEGER NOT NULL,
              payload TEXT NOT NULL, actor_id TEXT NOT NULL,
              event_seq INTEGER NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(id,version));
            CREATE INDEX IF NOT EXISTS disagreement_evidence_source
              ON disagreement_evidence(document_id,source_revision,case_id);
            CREATE INDEX IF NOT EXISTS disagreement_evidence_case
              ON disagreement_evidence(case_id,event_seq,id,version);
            CREATE TABLE IF NOT EXISTS disagreement_contributions(
              id TEXT NOT NULL, version INTEGER NOT NULL,
              case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              actor_id TEXT NOT NULL, recorded_by TEXT NOT NULL,
              payload TEXT NOT NULL, event_seq INTEGER NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(id,version));
            CREATE INDEX IF NOT EXISTS disagreement_contribution_case
              ON disagreement_contributions(case_id,event_seq,id,version);
            CREATE TABLE IF NOT EXISTS disagreement_conclusions(
              id TEXT PRIMARY KEY, case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              ordinal INTEGER NOT NULL, actor_id TEXT NOT NULL, payload TEXT NOT NULL,
              based_on_seq INTEGER NOT NULL, event_seq INTEGER NOT NULL, created_at TEXT NOT NULL,
              UNIQUE(case_id,ordinal));
            CREATE TABLE IF NOT EXISTS disagreement_events(
              case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              seq INTEGER NOT NULL, actor_id TEXT NOT NULL, kind TEXT NOT NULL,
              payload TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(case_id,seq));
            CREATE TABLE IF NOT EXISTS disagreement_operations(
              actor_id TEXT NOT NULL, operation_key TEXT NOT NULL, request_hash TEXT NOT NULL,
              operation_id TEXT NOT NULL UNIQUE, case_id TEXT NOT NULL REFERENCES disagreement_cases(id),
              result TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(actor_id,operation_key));
            """)
            if "request_id" not in {r["name"] for r in db.execute("PRAGMA table_info(disagreement_operations)")}:
                db.execute("ALTER TABLE disagreement_operations ADD COLUMN request_id TEXT")

    def _actor(self, db, principal, policy_capability="case_read"):
        if self.participants is not None:
            return self.participants.require(principal, policy_capability, connection=db)
        actor = str(UUID(str(principal.subject)))
        user = self.sources._corpus_row(db, "rkb_users", actor)
        if not user or user.get("status") != "active":
            fail("capability_denied")
        return actor

    def _case(self, db, actor, case_id, capability="viewer"):
        row = db.execute("SELECT * FROM disagreement_cases WHERE id=?", (str(case_id),)).fetchone()
        if not row:
            fail("not_found_or_not_accessible")
        grant = db.execute("SELECT capability FROM disagreement_grants WHERE case_id=? AND actor_id=?",
                           (row["id"], actor)).fetchone()
        role = "manager" if row["owner_id"] == actor else (grant[0] if grant else "")
        if ROLES.get(role, 0) < ROLES[capability]:
            fail("not_found_or_not_accessible")
        # First-release policy: all revealed revisions inherit every source ACL,
        # including old evidence versions. Removing a link cannot launder a summary.
        for dep in db.execute("SELECT DISTINCT document_id FROM disagreement_evidence WHERE case_id=?", (row["id"],)):
            if not self.sources._document_allowed(db, actor, dep[0]):
                fail("not_found_or_not_accessible")
        return row

    @staticmethod
    def _event(db, case_id, actor, kind, payload):
        db.execute("UPDATE disagreement_cases SET event_seq=event_seq+1 WHERE id=?", (case_id,))
        seq = db.execute("SELECT event_seq FROM disagreement_cases WHERE id=?", (case_id,)).fetchone()[0]
        db.execute("INSERT INTO disagreement_events VALUES(?,?,?,?,?,?)",
                   (case_id, seq, actor, kind, canonical(payload), now()))
        return seq

    def _mutate(self, principal, case_id, key, action, payload, apply, *, capability="contributor", expected=None, policy_capability="case_contribute", authorize=None):
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", key):
            fail("invalid_operation_key")
        request_hash = hashlib.sha256(canonical([case_id, action, payload, expected]).encode()).hexdigest()
        request_id=getattr(principal,"request_id",None)
        key=request_id+":"+key if request_id else key
        with self.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            actor = self._actor(db, principal, policy_capability)
            case = self._case(db, actor, case_id, capability) if case_id else None
            if authorize is not None:authorize(db,actor,case)
            if case:
                track_resources(db,principal,case_ids=[case_id],document_ids=[r[0] for r in db.execute("SELECT DISTINCT document_id FROM disagreement_evidence WHERE case_id=?",(case_id,))])
            saved = db.execute("SELECT * FROM disagreement_operations WHERE actor_id=? AND operation_key=?", (actor, key)).fetchone()
            if saved:
                # Authorization precedes both receipt disclosure and exact replay.
                self._case(db, actor, saved["case_id"])
                if saved["request_hash"] != request_hash:
                    fail("idempotency_conflict")
                return json.loads(saved["result"])
            if case and expected is not None and case["revision"] != expected:
                fail("revision_conflict")
            result = apply(db, actor, case)
            result.update(operation_id=str(uuid4()), state="applied")
            db.execute("INSERT INTO disagreement_operations(actor_id,operation_key,request_hash,operation_id,case_id,result,created_at,request_id) VALUES(?,?,?,?,?,?,?,?)",
                       (actor, key, request_hash, result["operation_id"], result["case_id"], canonical(result), now(),request_id))
            return result

    def operation_status(self, principal, operation_key):
        request_id=getattr(principal,"request_id",None)
        operation_key=request_id+":"+operation_key if request_id else operation_key
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE" if request_id else "BEGIN")
            actor = self._actor(db, principal)
            saved = db.execute("SELECT * FROM disagreement_operations WHERE actor_id=? AND operation_key=?", (actor, operation_key)).fetchone()
            if not saved:
                return {"state": "not_found"}
            self._case(db, actor, saved["case_id"])
            track_resources(db,principal,case_ids=[saved["case_id"]])
            return json.loads(saved["result"])

    def create(self, principal, command: CaseCreate, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, _):
            case_id = str(uuid4())
            db.execute("INSERT INTO disagreement_cases VALUES(?,?,1,'open',0,NULL,?,?)", (case_id, actor, canonical(payload), now()))
            db.execute("INSERT INTO disagreement_case_versions VALUES(?,1,?,?,?)", (case_id, canonical(payload), actor, now()))
            seq = self._event(db, case_id, actor, "created", {"revision": 1})
            return {"case_id": case_id, "case_revision": 1, "event_seq": seq}
        return self._mutate(principal, None, operation_key, "create", payload, apply)

    def revise(self, principal, case_id, expected_revision, command: CaseRevise | CaseState, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, case):
            current = json.loads(case["payload"])
            state = case["state"]
            if isinstance(command, CaseState):
                state = command.state
            else:
                current.update({k: v for k, v in payload.items() if k != "reason"})
            revision = case["revision"] + 1
            db.execute("UPDATE disagreement_cases SET revision=?,state=?,payload=? WHERE id=?",
                       (revision, state, canonical(current), case_id))
            snapshot = {**current, "state": state, "reason": command.reason,
                        "current_conclusion_id": case["current_conclusion_id"]}
            db.execute("INSERT INTO disagreement_case_versions VALUES(?,?,?,?,?)", (case_id, revision, canonical(snapshot), actor, now()))
            seq = self._event(db, case_id, actor, "revised", {"revision": revision, "reason": command.reason})
            return {"case_id": case_id, "case_revision": revision, "event_seq": seq}
        return self._mutate(principal, case_id, operation_key, "revise", payload, apply, capability="curator", expected=expected_revision, policy_capability="maintain_shared_summary")

    def grant(self, principal, case_id, expected_revision, command: CaseGrant, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, case):
            user = self.sources._corpus_row(db, "rkb_users", command.grantee)
            if not user or user.get("status") != "active":
                fail("invalid_grantee")
            if command.capability:
                db.execute("INSERT INTO disagreement_grants VALUES(?,?,?) ON CONFLICT(case_id,actor_id) DO UPDATE SET capability=excluded.capability",
                           (case_id, str(command.grantee), command.capability))
            else:
                db.execute("DELETE FROM disagreement_grants WHERE case_id=? AND actor_id=?", (case_id, str(command.grantee)))
            revision = case["revision"] + 1
            db.execute("UPDATE disagreement_cases SET revision=? WHERE id=?", (revision, case_id))
            snapshot = {**json.loads(case["payload"]), "state": case["state"], "current_conclusion_id": case["current_conclusion_id"]}
            db.execute("INSERT INTO disagreement_case_versions VALUES(?,?,?,?,?)", (case_id, revision, canonical(snapshot), actor, now()))
            seq = self._event(db, case_id, actor, "access_changed", payload)
            return {"case_id": case_id, "case_revision": revision, "event_seq": seq}
        return self._mutate(principal, case_id, operation_key, "grant", payload, apply, capability="manager", expected=expected_revision, policy_capability="maintain_shared_summary")

    @staticmethod
    def _references(db, case, evidence_ids, position_ids=()):
        positions = {p["id"] for p in json.loads(case["payload"])["positions"]}
        if not set(position_ids) <= positions:
            fail("invalid_position")
        for ident in evidence_ids:
            if not db.execute("SELECT 1 FROM disagreement_evidence WHERE case_id=? AND id=?", (case["id"], str(ident))).fetchone():
                fail("invalid_evidence")

    def add_evidence(self, principal, case_id, command: EvidenceAdd, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, case):
            self._references(db, case, (), command.position_ids)
            doc = self.sources._document_allowed(db, actor, str(command.document_id))
            page = self.sources._corpus_row(db, "rkb_pages", command.page_id)
            region = self.sources._corpus_row(db, "rkb_regions", command.region_id)
            if not doc or not page or not region or doc.get("source_sha256") != command.source_sha256:
                fail("invalid_evidence")
            if (str(page.get("document_id")) != str(command.document_id)
                    or page.get("revision") != command.source_revision
                    or str(region.get("page_id")) != str(command.page_id)):
                fail("invalid_evidence")
            text = region.get("source_text", "")
            if text[command.text_start:command.text_start + len(command.quote)] != command.quote:
                fail("quote_mismatch")
            track_resources(db,principal,document_ids=[command.document_id])
            ident = str(uuid4())
            seq = self._event(db, case_id, actor, "evidence_added", {"evidence_id": ident, "version": 1})
            value = {**payload, "text_end": command.text_start + len(command.quote),
                     "verification": "accepted_text_match", "visual_confirmation": None,
                     "physical_page_index": page["physical_page_index"]}
            db.execute("INSERT INTO disagreement_evidence VALUES(?,1,?,?,?,?,?,?,?)",
                       (ident, case_id, str(command.document_id), command.source_revision, canonical(value), actor, seq, now()))
            return {"case_id": case_id, "case_revision": case["revision"], "evidence_id": ident, "event_seq": seq}
        return self._mutate(principal, case_id, operation_key, "add_evidence", payload, apply)

    def contribute(self, principal, case_id, command: ContributionAdd, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, case):
            return self._append_contribution(db,principal,actor,case,command)
        return self._mutate(principal, case_id, operation_key, "contribute", payload, apply)

    def _append_contribution(self,db,principal,actor,case,command):
        """One atomic writer shared by direct and explicit draft publication."""
        case_id=case['id']
        self._references(db, case, command.evidence_ids, command.position_ids)
        if command.reply_to and not db.execute("SELECT 1 FROM disagreement_contributions WHERE id=? AND case_id=?", (str(command.reply_to), case_id)).fetchone():
            fail("invalid_reply")
        ident = str(uuid4())
        seq = self._event(db, case_id, actor, "contribution_added", {"contribution_id": ident, "version": 1})
        value = {**command.model_dump(mode='json'), "origin": "authenticated_mcp_account", "human_reading_confirmed": False, "withdrawn": False}
        if self.participants is not None:
            value["attribution"] = self.participants.attribution(db, principal)
            value["origin"] = "verified_external_account" if principal.route == "expert" else "authenticated_mcp_account"
        recorded_by=getattr(principal,"executor_id",None) or actor
        db.execute("INSERT INTO disagreement_contributions VALUES(?,1,?,?,?,?,?,?)", (ident, case_id, actor, recorded_by, canonical(value), seq, now()))
        return {"case_id": case_id, "case_revision": case["revision"], "contribution_id": ident, "version": 1, "event_seq": seq}

    def correct_contribution(self, principal, case_id, contribution_id, expected_version, command: ContributionCorrect, operation_key):
        payload = {**command.model_dump(mode="json"), "id": str(contribution_id), "expected_version": expected_version}
        def apply(db, actor, case):
            old = db.execute("SELECT * FROM disagreement_contributions WHERE id=? AND case_id=? ORDER BY version DESC LIMIT 1", (str(contribution_id), case_id)).fetchone()
            if not old or old["actor_id"] != actor:
                fail("not_found_or_not_accessible")
            if old["version"] != expected_version:
                fail("revision_conflict")
            version = expected_version + 1
            value = {**json.loads(old["payload"]), **command.model_dump(mode="json")}
            seq = self._event(db, case_id, actor, "contribution_corrected", {"contribution_id": str(contribution_id), "version": version})
            recorded_by=getattr(principal,"executor_id",None) or actor
            db.execute("INSERT INTO disagreement_contributions VALUES(?,?,?,?,?,?,?,?)", (str(contribution_id), version, case_id, actor, recorded_by, canonical(value), seq, now()))
            return {"case_id": case_id, "case_revision": case["revision"], "contribution_id": str(contribution_id), "version": version, "event_seq": seq}
        return self._mutate(principal, case_id, operation_key, "correct_contribution", payload, apply)

    def conclude(self, principal, case_id, expected_revision, command: ConclusionAdd, operation_key):
        payload = command.model_dump(mode="json")
        def apply(db, actor, case):
            if command.based_on_seq > case["event_seq"]:
                fail("invalid_material_snapshot")
            self._references(db, case, command.evidence_ids)
            for ident in command.evidence_ids:
                if db.execute("SELECT event_seq FROM disagreement_evidence WHERE id=?", (str(ident),)).fetchone()[0] > command.based_on_seq:
                    fail("material_outside_snapshot")
            for ident, version in command.contribution_versions.items():
                row = db.execute("SELECT event_seq FROM disagreement_contributions WHERE id=? AND version=? AND case_id=?", (str(ident), version, case_id)).fetchone()
                if not row or row[0] > command.based_on_seq:
                    fail("material_outside_snapshot")
            ident = str(uuid4())
            ordinal = db.execute("SELECT coalesce(max(ordinal),0)+1 FROM disagreement_conclusions WHERE case_id=?", (case_id,)).fetchone()[0]
            seq = self._event(db, case_id, actor, "conclusion_added", {"conclusion_id": ident, "ordinal": ordinal})
            db.execute("INSERT INTO disagreement_conclusions VALUES(?,?,?,?,?,?,?,?)", (ident, case_id, ordinal, actor, canonical({**payload, "human_support": []}), command.based_on_seq, seq, now()))
            revision = case["revision"]
            if command.make_current:
                revision += 1
                db.execute("UPDATE disagreement_cases SET revision=?,current_conclusion_id=? WHERE id=?", (revision, ident, case_id))
                db.execute("INSERT INTO disagreement_case_versions VALUES(?,?,?,?,?)", (case_id, revision, canonical({**json.loads(case["payload"]), "current_conclusion_id": ident, "state": case["state"]}), actor, now()))
            return {"case_id": case_id, "case_revision": revision, "conclusion_id": ident, "conclusion_ordinal": ordinal, "event_seq": seq}
        return self._mutate(principal, case_id, operation_key, "conclude", payload, apply,
                            capability="curator" if command.make_current else "contributor", expected=expected_revision,
                            policy_capability="personal_conclusion" if command.kind == "individual_opinion" else "maintain_shared_summary")

    def get(self, principal, case_id, *, section="case", after_seq=0, limit=20, revision=None):
        if not isinstance(after_seq, int) or after_seq < 0 or not 1 <= limit <= 100:
            fail("invalid_page")
        tables = {"evidence": "disagreement_evidence", "contributions": "disagreement_contributions",
                  "conclusions": "disagreement_conclusions", "history": "disagreement_events"}
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE" if getattr(principal,"request_id",None) else "BEGIN")
            actor = self._actor(db, principal)
            case = self._case(db, actor, case_id)
            track_resources(db,principal,case_ids=[case_id],document_ids=[r[0] for r in db.execute("SELECT DISTINCT document_id FROM disagreement_evidence WHERE case_id=?",(case_id,))])
            header = {**json.loads(case["payload"]), "case_id": case_id, "case_revision": case["revision"],
                      "state": case["state"], "event_seq": case["event_seq"], "current_conclusion_id": case["current_conclusion_id"]}
            if section == "case":
                if revision is not None:
                    old = db.execute("SELECT payload FROM disagreement_case_versions WHERE case_id=? AND revision=?", (case_id, revision)).fetchone()
                    if not old:
                        fail("not_found_or_not_accessible")
                    return {"case_id": case_id, "case_revision": revision, "state": "open", **json.loads(old[0])}
                current = db.execute("SELECT * FROM disagreement_conclusions WHERE id=? AND case_id=?", (case["current_conclusion_id"], case_id)).fetchone()
                header["current_conclusion"] = ({**dict(current), "payload": json.loads(current["payload"])} if current else None)
                reviewed = current["based_on_seq"] if current else 0
                latest = db.execute("SELECT coalesce(max(seq),0) FROM disagreement_events WHERE case_id=? AND kind IN ('evidence_added','contribution_added','contribution_corrected')", (case_id,)).fetchone()[0]
                header["new_material_since_conclusion"] = latest > reviewed
                return header
            if section not in tables:
                fail("invalid_section")
            col = "seq" if section == "history" else "event_seq"
            rows = db.execute(f"SELECT * FROM {tables[section]} WHERE case_id=? AND {col}>? ORDER BY {col} LIMIT ?", (case_id, after_seq, limit + 1)).fetchall()
            items = [{**dict(r), "payload": json.loads(r["payload"])} for r in rows[:limit]]
            if self.participants is not None and section == "contributions":
                for item in items:
                    link_id = item["payload"].get("attribution", {}).get("external_link_id")
                    review = db.execute("SELECT attribution_review_required FROM rkb_external_identities WHERE id=?", (link_id,)).fetchone() if link_id else None
                    item["attribution_review_required"] = bool(review and review[0])
            return {"case_id": case_id, "case_revision": case["revision"], "items": items,
                    "has_more": len(rows) > limit, "next_after_seq": items[-1][col] if len(rows) > limit else None,
                    "snapshot_seq": case["event_seq"]}
