"""Story Registry: one SQLite authority, immutable editorial versions, server-enforced provenance.

This module deliberately has no LLM, audio transport, web crawler, publisher or
external database. The current application principal is the only actor.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

from .sqlite_corpus import canonical
from .story_contracts import (
    AddGap, AttachEvidence, EvidenceLocator, ExtractCancel, ExtractClaim, ExtractStage, ExtractStart,
    UpsertAssertion, LinkEntity, RecordAssessment, RecordInterest, ResolveGap, SetAngle,
    SetContributors, SetMetadata, UpsertAssertion, UpsertVariant,
)

ROLE_LEVEL = {"viewer": 1, "contributor": 2, "researcher": 3, "editor": 4, "publisher": 5, "manager": 6}
WEIGHTS = {"unexpectedness": .25, "local_relevance": .25, "human_resonance": .20,
           "explanatory_value": .20, "visual_potential": .10}
KINDS = {"historical_claim", "attributed_account", "interpretation", "hypothesis", "creative_material"}


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def fail(code, detail=""):
    messages = {
        "not_found_or_not_accessible": "Материал не найден или отсутствует доступ.",
        "capability_denied": "Недостаточно полномочий для этого действия.",
        "revision_conflict": "Карточка изменена другим участником. Перечитайте версию.",
        "idempotency_conflict": "Ключ команды уже использован с другими аргументами.",
        "invalid_evidence": "Указанное свидетельство не соответствует принятому источнику.",
        "validation_failed": "Материал не удовлетворяет условиям редакционной готовности.",
        "busy_retryable": "Хранилище занято, повторите ту же команду.",
        "dependency_unavailable": "Необходимый источник или сервис недоступен.",
        "source_changed": "Источник или версия изменены; нужна повторная проверка.",
    }
    raise StoryError(code, messages.get(code, code), detail)


class StoryError(Exception):
    def __init__(self, code, message, detail=""):
        self.code, self.message, self.detail = code, message, str(detail)[:500]
        super().__init__(code)


class StoryRegistry:
    def __init__(self, corpus, *, migrate=True):
        if corpus is None:
            fail("dependency_unavailable", "SQLite authority is required")
        self.corpus = corpus
        if migrate:
            self.migrate()

    def migrate(self):
        # All domain data, receipts, audit and jobs share the corpus.sqlite3 WAL.
        with self.corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS story_records(
              id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, workspace_id TEXT,
              revision INTEGER NOT NULL CHECK(revision >= 1), state TEXT NOT NULL,
              archived INTEGER NOT NULL DEFAULT 0, merged_into TEXT,
              title TEXT NOT NULL, summary TEXT NOT NULL, seed_text TEXT NOT NULL,
              material_type TEXT NOT NULL, snapshot TEXT NOT NULL,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS story_scope_idx ON story_records(workspace_id,owner_id,archived,state,updated_at);
            CREATE VIRTUAL TABLE IF NOT EXISTS story_fts USING fts5(title,summary,seed_text,content='story_records',content_rowid='rowid',tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS story_fts_ai AFTER INSERT ON story_records BEGIN
              INSERT INTO story_fts(rowid,title,summary,seed_text) VALUES(new.rowid,new.title,new.summary,new.seed_text); END;
            CREATE TRIGGER IF NOT EXISTS story_fts_au AFTER UPDATE ON story_records BEGIN
              INSERT INTO story_fts(story_fts,rowid,title,summary,seed_text)
                VALUES('delete',old.rowid,old.title,old.summary,old.seed_text);
              INSERT INTO story_fts(rowid,title,summary,seed_text) VALUES(new.rowid,new.title,new.summary,new.seed_text); END;
            CREATE TRIGGER IF NOT EXISTS story_fts_ad AFTER DELETE ON story_records BEGIN
              INSERT INTO story_fts(story_fts,rowid,title,summary,seed_text)
                VALUES('delete',old.rowid,old.title,old.summary,old.seed_text); END;
            CREATE TABLE IF NOT EXISTS story_revisions(
              story_id TEXT NOT NULL REFERENCES story_records(id),
              revision INTEGER NOT NULL, snapshot TEXT NOT NULL,
              actor_id TEXT NOT NULL, action TEXT NOT NULL, at TEXT NOT NULL,
              PRIMARY KEY(story_id,revision));
            CREATE TABLE IF NOT EXISTS story_sources(
              id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version >= 1),
              owner_id TEXT NOT NULL, workspace_id TEXT, metadata TEXT NOT NULL,
              text_content TEXT NOT NULL, content_sha256 TEXT NOT NULL,
              created_at TEXT NOT NULL, PRIMARY KEY(id,version));
            CREATE TABLE IF NOT EXISTS story_assertion_versions(
              id TEXT NOT NULL, revision INTEGER NOT NULL,
              story_id TEXT NOT NULL REFERENCES story_records(id),
              proposition TEXT NOT NULL, data TEXT NOT NULL, actor_id TEXT NOT NULL,
              at TEXT NOT NULL, PRIMARY KEY(id,revision));
            CREATE TABLE IF NOT EXISTS story_evidence(
              id TEXT PRIMARY KEY, story_id TEXT NOT NULL REFERENCES story_records(id),
              assertion_id TEXT NOT NULL, assertion_revision INTEGER NOT NULL,
              source_kind TEXT NOT NULL, source_id TEXT NOT NULL,
              source_revision INTEGER NOT NULL, source_sha256 TEXT NOT NULL,
              locator TEXT NOT NULL, original_excerpt TEXT NOT NULL,
              excerpt_sha256 TEXT NOT NULL, relation TEXT NOT NULL, source_role TEXT NOT NULL,
              text_match TEXT NOT NULL, original_review TEXT NOT NULL,
              derived_from TEXT, actor_id TEXT NOT NULL, created_at TEXT NOT NULL,
              FOREIGN KEY(assertion_id,assertion_revision)
                REFERENCES story_assertion_versions(id,revision));
            CREATE INDEX IF NOT EXISTS story_evidence_src ON story_evidence(source_kind,source_id,source_revision);
            CREATE TABLE IF NOT EXISTS story_assessments(
              id TEXT PRIMARY KEY, story_id TEXT NOT NULL REFERENCES story_records(id),
              assertion_id TEXT NOT NULL, assertion_revision INTEGER NOT NULL,
              payload TEXT NOT NULL, actor_id TEXT NOT NULL, at TEXT NOT NULL,
              FOREIGN KEY(assertion_id,assertion_revision)
                REFERENCES story_assertion_versions(id,revision));
            CREATE INDEX IF NOT EXISTS story_assessment_claim ON story_assessments(assertion_id,assertion_revision,at);
            CREATE TABLE IF NOT EXISTS story_variants(
              id TEXT PRIMARY KEY, story_id TEXT NOT NULL REFERENCES story_records(id),
              revision INTEGER NOT NULL, state TEXT NOT NULL, approved_revision INTEGER,
              approval_fingerprint TEXT);
            CREATE TABLE IF NOT EXISTS story_variant_versions(
              id TEXT NOT NULL REFERENCES story_variants(id), revision INTEGER NOT NULL,
              data TEXT NOT NULL, actor_id TEXT NOT NULL, at TEXT NOT NULL,
              PRIMARY KEY(id,revision));
            CREATE TABLE IF NOT EXISTS story_dependencies(
              story_id TEXT NOT NULL REFERENCES story_records(id), source_kind TEXT NOT NULL,
              source_id TEXT NOT NULL, source_revision INTEGER NOT NULL,
              PRIMARY KEY(story_id,source_kind,source_id,source_revision));
            CREATE TABLE IF NOT EXISTS story_grants(
              story_id TEXT NOT NULL REFERENCES story_records(id), grantee_id TEXT NOT NULL,
              capability TEXT NOT NULL, granted_by TEXT NOT NULL,
              PRIMARY KEY(story_id,grantee_id));
            CREATE TABLE IF NOT EXISTS story_audit(
              id TEXT PRIMARY KEY, story_id TEXT, revision INTEGER,
              actor_id TEXT NOT NULL, client_id TEXT, action TEXT NOT NULL,
              summary TEXT NOT NULL, at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS story_audit_resource ON story_audit(story_id,revision);
            CREATE TABLE IF NOT EXISTS story_receipts(
              actor_scope TEXT NOT NULL, workspace_scope TEXT NOT NULL, method TEXT NOT NULL,
              idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL,
              result TEXT NOT NULL, at TEXT NOT NULL,
              PRIMARY KEY(actor_scope,workspace_scope,method,idempotency_key));
            CREATE TABLE IF NOT EXISTS story_jobs(
              id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, document_id TEXT NOT NULL,
              source_revision INTEGER NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL,
              executor_state TEXT NOT NULL, batch_size INTEGER NOT NULL,
              cursor_position INTEGER NOT NULL, total_chunks INTEGER NOT NULL,
              batch_id TEXT, lease_token TEXT, lease_deadline REAL,
              checkpoint TEXT NOT NULL, result_ids TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS story_review_decisions(
              id TEXT PRIMARY KEY,
              story_id TEXT NOT NULL REFERENCES story_records(id),
              variant_id TEXT NOT NULL REFERENCES story_variants(id),
              variant_revision INTEGER NOT NULL,
              approval_fingerprint TEXT NOT NULL,
              actor_id TEXT NOT NULL, client_id TEXT, review TEXT NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY(variant_id,variant_revision)
                REFERENCES story_variant_versions(id,revision));
            CREATE TABLE IF NOT EXISTS story_publications(
              id TEXT PRIMARY KEY, story_id TEXT NOT NULL REFERENCES story_records(id),
              variant_id TEXT NOT NULL, variant_revision INTEGER NOT NULL,
              data TEXT NOT NULL, confirmation TEXT NOT NULL, actor_id TEXT NOT NULL,
              created_at TEXT NOT NULL, FOREIGN KEY(variant_id,variant_revision)
                REFERENCES story_variant_versions(id,revision));
            CREATE TABLE IF NOT EXISTS story_outbox(
              id TEXT PRIMARY KEY, story_id TEXT NOT NULL REFERENCES story_records(id),
              story_revision INTEGER NOT NULL, payload_hash TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'awaiting_worker', created_at TEXT NOT NULL,
              UNIQUE(story_id,story_revision));
            CREATE TABLE IF NOT EXISTS story_ingest_candidates(
              ingestion_id TEXT NOT NULL, candidate_key TEXT NOT NULL,
              document_id TEXT NOT NULL, source_revision INTEGER NOT NULL,
              payload TEXT NOT NULL, payload_hash TEXT NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('staged','accepted','needs_review')),
              story_id TEXT REFERENCES story_records(id), at TEXT NOT NULL,
              PRIMARY KEY(ingestion_id,candidate_key));
            CREATE INDEX IF NOT EXISTS story_ingest_doc
              ON story_ingest_candidates(document_id,source_revision,state);
            CREATE TABLE IF NOT EXISTS story_ingest_coverage(
              ingestion_id TEXT NOT NULL, document_id TEXT NOT NULL,
              source_revision INTEGER NOT NULL, physical_page_index INTEGER NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('reviewed','unreviewed')),
              at TEXT NOT NULL,
              PRIMARY KEY(ingestion_id,physical_page_index));
            CREATE TABLE IF NOT EXISTS story_ingest_identity(
              document_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
              fingerprint TEXT NOT NULL, story_id TEXT NOT NULL REFERENCES story_records(id),
              at TEXT NOT NULL,
              PRIMARY KEY(document_id,source_sha256,fingerprint));
            CREATE TABLE IF NOT EXISTS story_reconcile_runs(
              id TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
              anchor_story_id TEXT NOT NULL REFERENCES story_records(id),
              anchor_story_revision INTEGER NOT NULL, policy_version TEXT NOT NULL,
              query TEXT NOT NULL, frontier TEXT NOT NULL,
              position INTEGER NOT NULL DEFAULT 0, max_candidate_pairs INTEGER NOT NULL,
              revision INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL,
              work_id TEXT, lease_token TEXT, lease_deadline REAL,
              updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS story_reconcile_anchor
              ON story_reconcile_runs(anchor_story_id,state,updated_at);
            CREATE TABLE IF NOT EXISTS story_reconcile_proposals(
              id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES story_reconcile_runs(id),
              ref_kind TEXT NOT NULL, ref_id TEXT NOT NULL,
              decision TEXT NOT NULL, state TEXT NOT NULL,
              actor_id TEXT NOT NULL, created_at TEXT NOT NULL, applied_at TEXT,
              UNIQUE(run_id,ref_kind,ref_id));
            CREATE INDEX IF NOT EXISTS story_reconcile_proposal_state
              ON story_reconcile_proposals(run_id,state,created_at);
            CREATE TABLE IF NOT EXISTS story_relations(
              id TEXT PRIMARY KEY,
              left_story_id TEXT NOT NULL REFERENCES story_records(id),
              right_story_id TEXT NOT NULL REFERENCES story_records(id),
              kind TEXT NOT NULL, rationale TEXT NOT NULL, source_proposal_id TEXT NOT NULL,
              actor_id TEXT NOT NULL, created_at TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 1,
              UNIQUE(left_story_id,right_story_id,kind));
            CREATE INDEX IF NOT EXISTS story_relations_right
              ON story_relations(right_story_id,kind,active);
            CREATE TABLE IF NOT EXISTS story_reconcile_queue(
              story_id TEXT NOT NULL REFERENCES story_records(id),
              story_revision INTEGER NOT NULL, state TEXT NOT NULL,
              created_at TEXT NOT NULL, PRIMARY KEY(story_id,story_revision));
            CREATE INDEX IF NOT EXISTS story_reconcile_queue_state
              ON story_reconcile_queue(state,created_at);
            CREATE TABLE IF NOT EXISTS story_schema_migrations(
              version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
            INSERT OR IGNORE INTO story_schema_migrations VALUES(1,datetime('now'));
            INSERT OR IGNORE INTO story_schema_migrations VALUES(2,datetime('now'));
            INSERT OR IGNORE INTO story_schema_migrations VALUES(3,datetime('now'));
            INSERT OR IGNORE INTO story_schema_migrations VALUES(4,datetime('now'));
            """)

    @staticmethod
    def _row(db, table, ident):
        return db.execute(f"SELECT * FROM {table} WHERE id=?", (str(ident),)).fetchone()

    @staticmethod
    def _corpus_row(db, table, ident):
        row = db.execute("SELECT payload FROM corpus_rows WHERE table_name=? AND row_key=?",
                         (table, str(ident))).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _matching_corpus(db, table, **filters):
        expr = ["table_name=?"]
        params = [table]
        for k, v in filters.items():
            if not re.fullmatch(r"[a-z_]+", k):
                raise ValueError("invalid filter")
            expr.append("json_extract(payload,'$." + k + "')=?")
            params.append(str(v))
        return [json.loads(r[0]) for r in
                db.execute("SELECT payload FROM corpus_rows WHERE " + " AND ".join(expr), params)]

    def _actor(self, db, principal, create=False):
        actor = str(UUID(str(principal.subject)))
        user = self._corpus_row(db, "rkb_users", actor)
        if user and user.get("status") != "active":
            fail("capability_denied")
        if not user and create:
            from .sqlite_data import defaults
            self.corpus.put("rkb_users", [{**defaults("rkb_users"), "id": actor}], connection=db)
        return actor

    def _membership(self, db, workspace_id, actor):
        if not workspace_id:
            return False
        space = self._corpus_row(db, "rkb_workspaces", workspace_id)
        if not space:
            return False
        return space.get("owner_user_id") == actor or bool(self._matching_corpus(
            db, "rkb_workspace_members", workspace_id=workspace_id, user_id=actor))

    def _document_allowed(self, db, actor, document_id):
        doc = self._corpus_row(db, "rkb_documents", document_id)
        if not doc:
            return None
        if doc.get("owner_user_id") == actor:
            return doc
        rights = doc.get("rights_evidence") or {}
        if (doc.get("content_visibility") == "public" and doc.get("rights_status") in
                {"licensed", "permission_granted", "public_domain_verified", "statutory_access_verified"}
                and doc.get("rights_policy_version") and rights.get("public_distribution") is True):
            return doc
        if self._matching_corpus(db, "rkb_document_grants", document_id=document_id, grantee_user_id=actor):
            return doc
        if doc.get("content_visibility") in {"workspace", "public"} and self._membership(
                db, doc.get("workspace_id"), actor):
            return doc
        return None

    def _permission(self, db, actor, record, required="viewer"):
        if record is None:
            fail("not_found_or_not_accessible")
        if actor == record["owner_id"]:
            role = "manager"
        else:
            grant = db.execute("SELECT capability FROM story_grants WHERE story_id=? AND grantee_id=?",
                               (record["id"], actor)).fetchone()
            role = str(grant[0]) if grant else (
                "viewer" if self._membership(db, record["workspace_id"], actor) else "")
        if ROLE_LEVEL.get(role, 0) < ROLE_LEVEL[required]:
            fail("not_found_or_not_accessible" if required == "viewer" else "capability_denied")
        # Dependents inherit source ACL, including revisions, exports, receipts and job results.
        for dep in db.execute("SELECT * FROM story_dependencies WHERE story_id=?", (record["id"],)):
            if dep["source_kind"] == "document":
                if not self._document_allowed(db, actor, dep["source_id"]):
                    fail("not_found_or_not_accessible")
            else:
                src = db.execute("SELECT owner_id FROM story_sources WHERE id=? AND version=?",
                                 (dep["source_id"], dep["source_revision"])).fetchone()
                if not src or (src[0] != actor and record["owner_id"] != actor):
                    fail("not_found_or_not_accessible")
        return role

    def _mutation(self, principal, method, key, payload, authorize, apply):
        actor_id = str(UUID(str(principal.subject)))
        if not key or not 8 <= len(key) <= 128:
            fail("validation_failed", "A stable idempotency_key is required")
        request_hash = digest(payload)
        try:
            with self.corpus.connect() as db:
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                actor = self._actor(db, principal, create=True)
                workspace_scope = authorize(db, actor)
                workspace_scope = str(workspace_scope or "personal:" + actor)
                receipt = db.execute("""SELECT request_hash,result FROM story_receipts
                    WHERE actor_scope=? AND workspace_scope=? AND method=? AND idempotency_key=?""",
                                     (actor, workspace_scope, method, key)).fetchone()
                if receipt:
                    if receipt["request_hash"] != request_hash:
                        fail("idempotency_conflict")
                    return json.loads(receipt["result"])
                output = apply(db, actor)
                output.setdefault("operation_id", str(uuid4()))
                output.setdefault("commit_state", "saved")
                output.setdefault("warnings", [])
                output.setdefault("next_actions", [])
                db.execute("INSERT INTO story_receipts VALUES(?,?,?,?,?,?,?)",
                           (actor, workspace_scope, method, key, request_hash, canonical(output), now()))
                return output
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                fail("busy_retryable")
            raise

    def _audit(self, db, principal, story_id, revision, action, summary):
        db.execute("INSERT INTO story_audit VALUES(?,?,?,?,?,?,?,?)",
                   (str(uuid4()), story_id, revision, str(principal.subject),
                    getattr(principal, "client_id", None), action, summary[:500], now()))

    def _snapshot(self, db, story_id, revision, content, principal, action):
        db.execute("INSERT INTO story_revisions VALUES(?,?,?,?,?,?)",
                   (story_id, revision, canonical(content), str(principal.subject), action, now()))
        meta = content["metadata"]
        db.execute("""UPDATE story_records SET revision=?,state=?,archived=?,merged_into=?,
          title=?,summary=?,seed_text=?,material_type=?,snapshot=?,updated_at=? WHERE id=?""",
                   (revision, content["state"], int(content.get("archived", False)),
                    content.get("merged_into"), meta["title"], meta.get("summary", ""),
                    content["seed"]["text"], meta["material_type"], canonical(content), now(), story_id))
        db.execute("""INSERT OR IGNORE INTO story_outbox(id,story_id,story_revision,payload_hash,created_at)
                      VALUES(?,?,?,?,?)""", (str(uuid4()), story_id, revision,
                                               digest({"title": meta["title"], "summary": meta.get("summary")}),
                                               now()))
        self._audit(db, principal, story_id, revision, action, action)
        # Accepted-source extraction creates a durable reconciliation delta
        # without running another model, or blocking book acceptance.
        if action in {"automated_source_candidate", "accepted_source_candidate"} or (
            action.startswith("edit:") and "evidence" in action):
            if content.get("assertions") and any(a.get("evidence_ids")
                                                   for a in content["assertions"]):
                db.execute("""INSERT OR IGNORE INTO story_reconcile_queue
                    (story_id,story_revision,state,created_at)
                    VALUES(?,?,'awaiting_agent',?)""",
                    (story_id, revision, now()))

    def _create_record(self, db, principal, seed, metadata, workspace_id=None, source_refs=None):
        story_id = str(uuid4())
        meta = {"title": metadata.get("title") or seed["text"][:90],
                "summary": metadata.get("summary") or "",
                "material_type": metadata.get("material_type", "other"),
                "tags": metadata.get("tags") or [], "project_refs": metadata.get("project_refs") or [],
                "time_scope": metadata.get("time_scope")}
        snap = {"story_id": story_id, "state": "raw_seed", "archived": False,
                "seed": seed, "metadata": meta, "angle": None, "assertions": [],
                "gaps": [], "variants": [], "links": [], "interest_assessments": [],
                "contributors": [], "source_refs": source_refs or []}
        db.execute("""INSERT INTO story_records(id,owner_id,workspace_id,revision,state,title,
          summary,seed_text,material_type,snapshot,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                   (story_id, str(principal.subject), workspace_id, 1, "raw_seed", meta["title"],
                    meta["summary"], seed["text"], meta["material_type"], canonical(snap), now(), now()))
        for ref in source_refs or []:
            self._dependency(db, story_id, ref["kind"], ref["source_id"], ref["source_revision"],
                             str(principal.subject))
        self._snapshot(db, story_id, 1, snap, principal, "create")
        return story_id, snap

    def _dependency(self, db, story_id, kind, source_id, source_revision, actor):
        if kind == "document":
            doc = self._document_allowed(db, actor, source_id)
            if not doc or source_revision > (doc.get("active_revision") or 0):
                fail("invalid_evidence", "No accessible accepted document revision")
        else:
            s = db.execute("SELECT * FROM story_sources WHERE id=? AND version=?",
                           (source_id, source_revision)).fetchone()
            if not s or s["owner_id"] != actor:
                fail("invalid_evidence", "External source is not registered for this actor")
        db.execute("INSERT OR IGNORE INTO story_dependencies VALUES(?,?,?,?)",
                   (story_id, kind, source_id, source_revision))

    @staticmethod
    def _receipt_story(story_id, revision, state, snap):
        return {"resource_type": "story", "resource_ids": [story_id],
                "story_id": story_id, "committed_revision": revision,
                "editorial_state": state, "readiness": StoryRegistry._readiness(snap)}

    @staticmethod
    def _readiness(snapshot):
        v = snapshot.get("variants", [])
        return {"ready_variants": [x["variant_id"] for x in v if x["state"] == "publish_ready"],
                "needs_revalidation": [x["variant_id"] for x in v if x["state"] == "needs_revalidation"],
                "total_variants": len(v)}

    def create(self, principal, seed, metadata, workspace_id, source_refs, idempotency_key):
        sd = seed.model_dump(mode="json") if hasattr(seed, "model_dump") else (
            {"text": seed, "origin_status": "unknown", "origin_note": None, "speaker": None})
        md = metadata.model_dump(mode="json") if metadata else {}
        refs = [x.model_dump(mode="json") for x in (source_refs or [])]
        if not sd.get("text") or len(sd["text"]) > 4000:
            fail("validation_failed", "A short seed is required")
        if workspace_id:
            workspace_id = str(UUID(workspace_id))
        def authorize(db, actor):
            if workspace_id and not self._membership(db, workspace_id, actor):
                fail("capability_denied")
            for ref in refs:
                if ref["kind"] == "document" and not self._document_allowed(db, actor, ref["source_id"]):
                    fail("capability_denied")
            return workspace_id
        def apply(db, actor):
            sid, snap = self._create_record(db, principal, sd, md, workspace_id, refs)
            return {**self._receipt_story(sid, 1, "raw_seed", snap), "change_summary": "Зацепка сохранена"}
        return self._mutation(principal, "story_create", idempotency_key,
                              {"seed": sd, "metadata": md, "workspace_id": workspace_id, "source_refs": refs},
                              authorize, apply)

    def _read_story(self, db, actor, story_id, capability="viewer"):
        rec = self._row(db, "story_records", story_id)
        self._permission(db, actor, rec, capability)
        return rec, json.loads(rec["snapshot"])

    def _effective_readiness(self, db, snap):
        # Derived current readiness: historical approval receipts stay immutable,
        # but changed source identities must not be advertised as ready.
        result = self._readiness(snap)
        stale = []
        for variant in snap.get("variants", []):
            if variant.get("state") != "publish_ready":
                continue
            approved = self._row(db, "story_variants", variant["variant_id"])
            if (approved is None or
                    approved["approval_fingerprint"] != self._fingerprint(db, snap, variant)):
                stale.append(variant["variant_id"])
        result["ready_variants"] = [x for x in result["ready_variants"] if x not in stale]
        result["needs_revalidation"] = sorted(set([*result["needs_revalidation"], *stale]))
        return result

    def _current_assessments(self, db, assertion):
        """Return only effective reviews of this exact assertion revision.

        A superseded review stays in the audit and history, but may not
        authorize approval. Multiple un-superseded conflicting assessments
        remain visible; order of writing is not a vote for the latest.
        """
        ids = [str(x) for x in assertion.get("assessment_ids") or []]
        if not ids:
            return []
        rows = db.execute("""
            SELECT a.id,a.payload,a.actor_id,a.at FROM story_assessments a
            JOIN json_each(?) ids ON a.id=ids.value
            WHERE a.assertion_id=? AND a.assertion_revision=?""",
            (canonical(ids), assertion["assertion_id"], assertion["revision"]),
        ).fetchall()
        assessments = {}
        for row in rows:
            payload = json.loads(row["payload"])
            payload["id"] = row["id"]
            payload["verified_actor_id"] = row["actor_id"]
            payload["recorded_at"] = row["at"]
            assessments[row["id"]] = payload
        replaced = {
            x.get("supersedes_assessment_id") for x in assessments.values()
            if x.get("supersedes_assessment_id") in assessments
        }
        return sorted(
            [value for key, value in assessments.items() if key not in replaced],
            key=lambda x: (x["recorded_at"], x["id"]),
        )

    def _dossier_page(self, db, actor, rec, snapshot, section, cursor, limit, assertion_id):
        """Bounded, source-authorized dossier; IDs survive controlled merges."""
        take = max(1, min(int(limit), 10))
        effective_rev = int(rec["revision"])
        after = ""
        if cursor:
            try:
                version, last = cursor.split(":", 1)
                if int(version) != effective_rev or len(last) > 128:
                    raise ValueError("stale or oversized cursor")
                after = last
            except (ValueError, TypeError):
                fail("validation_failed", "Stale or invalid dossier cursor")
        assertions = [
            a for a in snapshot.get("assertions", [])
            if assertion_id is None or a["assertion_id"] == assertion_id
        ]
        if assertion_id is not None and not assertions:
            fail("not_found_or_not_accessible")
        if section == "assertion_page":
            selected = [a for a in sorted(assertions, key=lambda x: x["assertion_id"])
                        if a["assertion_id"] > after][:take + 1]
            page = []
            for a in selected[:take]:
                assessments = self._current_assessments(db, a)
                page.append({
                    "assertion_id": a["assertion_id"], "revision": a["revision"],
                    "proposition": a["proposition"], "kind": a["kind"],
                    "account_kind": a.get("account_kind"),
                    "attributed_to": a.get("attributed_to"),
                    "reported_by": a.get("reported_by"),
                    "evidence_count": len(a.get("evidence_ids", [])),
                    "effective_assessments": assessments[:6],
                    "assessments_has_more": len(assessments) > 6,
                })
            more = len(selected) > take
            next_id = page[-1]["assertion_id"] if more and page else None
        elif section == "sources_page":
            # Merge retains foreign assertion IDs, so resolve actual evidence
            # by assertion ID/revision, not by ev.story_id=target_story_id.
            sources = db.execute("""
                WITH active AS (
                  SELECT json_extract(value,'$.assertion_id') aid,
                         json_extract(value,'$.revision') rev FROM json_each(?)
                ), source_keys AS (
                  SELECT source_kind,source_id,source_revision FROM story_dependencies
                    WHERE story_id=?
                  UNION SELECT ev.source_kind,ev.source_id,ev.source_revision
                    FROM story_evidence ev JOIN active a
                    ON ev.assertion_id=a.aid AND ev.assertion_revision=a.rev
                )
                SELECT source_kind,source_id,source_revision FROM source_keys
                WHERE (source_kind || ':' || source_id || ':' || source_revision)>?
                ORDER BY source_kind,source_id,source_revision LIMIT ?""",
                (canonical(assertions), rec["id"], after, take + 1),
            ).fetchall()
            page = []
            for src in sources[:take]:
                if src["source_kind"] == "document":
                    doc = self._document_allowed(db, actor, src["source_id"])
                    if not doc:
                        fail("not_found_or_not_accessible")
                    metadata = {"title": doc.get("title"), "authors": doc.get("authors"),
                                "source_sha256": doc.get("source_sha256")}
                elif src["source_kind"] == "external":
                    ext = db.execute(
                        "SELECT owner_id,metadata FROM story_sources WHERE id=? AND version=?",
                        (src["source_id"], src["source_revision"]),
                    ).fetchone()
                    if not ext or (ext["owner_id"] != actor and rec["owner_id"] != actor):
                        fail("not_found_or_not_accessible")
                    metadata = json.loads(ext["metadata"])
                else:
                    fail("not_found_or_not_accessible")
                page.append({"kind": src["source_kind"], "source_id": src["source_id"],
                             "source_revision": src["source_revision"], "metadata": metadata})
            more = len(sources) > take
            next_id = (page[-1]["kind"] + ":" + page[-1]["source_id"] + ":" +
                       str(page[-1]["source_revision"])) if more and page else None
        else:
            rows = db.execute("""
                WITH active AS (
                  SELECT json_extract(value,'$.assertion_id') aid,
                         json_extract(value,'$.revision') rev FROM json_each(?)
                )
                SELECT ev.* FROM story_evidence ev JOIN active a
                  ON ev.assertion_id=a.aid AND ev.assertion_revision=a.rev
                WHERE ev.id>? ORDER BY ev.id LIMIT ?""",
                (canonical(assertions), after, take + 1),
            ).fetchall()
            mapping = {(a["assertion_id"], a["revision"]): a for a in assertions}
            page = []
            for ev in rows[:take]:
                assertion = mapping[(ev["assertion_id"], ev["assertion_revision"])]
                if ev["source_kind"] == "document":
                    document = self._document_allowed(db, actor, ev["source_id"])
                    if not document:
                        fail("not_found_or_not_accessible")
                    source_info = {"title": document.get("title"),
                                   "authors": document.get("authors")}
                else:
                    external = db.execute(
                        "SELECT owner_id,metadata FROM story_sources WHERE id=? AND version=?",
                        (ev["source_id"], ev["source_revision"]),
                    ).fetchone()
                    if not external or (external["owner_id"] != actor and rec["owner_id"] != actor):
                        fail("not_found_or_not_accessible")
                    source_info = json.loads(external["metadata"])
                reviews = self._current_assessments(db, assertion)
                page.append({
                    "evidence_id": ev["id"], "assertion_id": ev["assertion_id"],
                    "assertion_revision": ev["assertion_revision"],
                    "proposition": assertion["proposition"],
                    "attribution": assertion.get("attributed_to") or assertion.get("reported_by"),
                    "source_kind": ev["source_kind"], "source_id": ev["source_id"],
                    "source_revision": ev["source_revision"], "source": source_info,
                    "locator": json.loads(ev["locator"]),
                    "original_excerpt": ev["original_excerpt"],
                    "text_match": ev["text_match"],
                    "relation": ev["relation"], "source_role": ev["source_role"],
                    "source_state": self._evidence_state(db, ev),
                    "independence": "unknown" if not reviews else reviews[-1].get("independence", "unknown"),
                    "effective_assessments": reviews[:6],
                    "assessments_has_more": len(reviews) > 6,
                    "recorded_by": ev["actor_id"],
                })
            more = len(rows) > take
            next_id = page[-1]["evidence_id"] if more and page else None
        return {
            "story_id": rec["id"], "story_revision": effective_rev,
            "section": section, "items": page, "has_more": more,
            "next_cursor": str(effective_rev) + ":" + next_id if next_id else None,
            "source_refs_authority": "dependencies_and_current_evidence",
            "coverage": "bounded_authorized_read_not_entire_dossier",
        }

    def get(self, principal, story_id, revision=None, view="compact", cursor=None, limit=8, assertion_id=None):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            rec, snapshot = self._read_story(db, actor, story_id)
            if revision is not None:
                hist = db.execute("SELECT snapshot FROM story_revisions WHERE story_id=? AND revision=?",
                                  (story_id, revision)).fetchone()
                if not hist:
                    fail("not_found_or_not_accessible")
                snapshot = json.loads(hist[0])
            permitted = self._available_actions(db, actor, rec)
            if view in {"assertion_page", "evidence_page", "sources_page"}:
                return self._dossier_page(db, actor, rec, snapshot, view, cursor, limit, assertion_id)
            if view == "compact":
                snapshot = {k: snapshot[k] for k in ("story_id", "state", "seed", "metadata", "gaps")}
                snapshot["assertions"] = [
                    {k: a.get(k) for k in ("assertion_id", "revision", "kind", "account_kind", "proposition", "attributed_to")}
                    for a in json.loads((hist[0] if revision is not None else rec["snapshot"]))["assertions"][:6]]
            if view == "editorial":
                snapshot.pop("source_refs", None)  # source metadata available in evidence view
            return {"story_id": story_id, "revision": revision or rec["revision"],
                    "snapshot": snapshot, "readiness": self._effective_readiness(db, json.loads(rec["snapshot"])),
                    "allowed_actions": permitted, "indexing_state": "local_fts_ready_vector_awaiting_worker"}

    def _available_actions(self, db, actor, rec):
        role = self._permission(db, actor, rec)
        base = ["story_get", "story_history", "story_validate"]
        if ROLE_LEVEL[role] >= ROLE_LEVEL["contributor"]:
            base += ["story_edit", "story_archive"]
        if ROLE_LEVEL[role] >= ROLE_LEVEL["editor"]:
            base += ["story_transition"]
        if ROLE_LEVEL[role] >= ROLE_LEVEL["publisher"]:
            base += ["story_export", "story_publication_record"]
        if role == "manager":
            base += ["story_access"]
        return base

    def history(self, principal, story_id, from_revision=None, to_revision=None, cursor=None, limit=10):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            rec, _ = self._read_story(db, actor, story_id)
            first = max(1, int(cursor or from_revision or 1))
            last = min(rec["revision"], int(to_revision or rec["revision"]))
            rows = db.execute("""SELECT revision,actor_id,action,at FROM story_revisions
                  WHERE story_id=? AND revision BETWEEN ? AND ? ORDER BY revision LIMIT ?""",
                              (story_id, first, last, min(int(limit), 20) + 1)).fetchall()
            page = [dict(x) for x in rows[:limit]]
            return {"story_id": story_id, "history": page, "has_more": len(rows) > limit,
                    "next_cursor": str(page[-1]["revision"] + 1) if len(rows) > limit else None}

    def _invalidate(self, db, snap, affected_assertion_ids=None):
        """Invalidate only variants depending on changed assertion identities.

        A legacy variant without explicit assertion-linked blocks still depends
        conservatively on the whole card. New variants with declared blocks do
        not become stale when an unrelated episode is enriched.
        """
        affected = set(affected_assertion_ids or [])
        for item in snap["variants"]:
            if item["state"] != "publish_ready":
                continue
            used = {str(block["assertion_id"]) for block in item.get("blocks") or []
                    if block.get("assertion_id")}
            if affected and used and not (used & affected):
                continue
            item["state"] = "needs_revalidation"
            db.execute("UPDATE story_variants SET state='needs_revalidation' WHERE id=?",
                       (item["variant_id"],))

    def _apply_op(self, db, principal, snap, op):
        if isinstance(op, SetMetadata):
            for field in ("title", "summary", "material_type", "tags", "time_scope"):
                value = getattr(op, field)
                if value is not None:
                    snap["metadata"][field] = value
            return "metadata"
        if isinstance(op, AddGap):
            snap["gaps"].append({"gap_id": str(uuid4()), "text": op.text,
                                 "priority": op.priority, "state": "open"})
            return "gap"
        if isinstance(op, ResolveGap):
            gap = next((g for g in snap["gaps"] if g["gap_id"] == op.gap_id), None)
            if not gap:
                fail("validation_failed", "Gap not found")
            gap.update(state="resolved", resolution=op.resolution)
            return "gap"
        if isinstance(op, SetAngle):
            snap["angle"] = {"angle": op.angle, "audience_value": op.audience_value}
            return "angle"
        if isinstance(op, LinkEntity):
            # Stable reference is navigation, not evidence of historical fact.
            if not any(x["entity_ref"] == op.entity_ref for x in snap["links"]):
                snap["links"].append(op.model_dump(mode="json", exclude={"op"}))
            return "entity_link"
        if isinstance(op, SetContributors):
            snap["contributors"] = op.contributors
            return "contributors"
        if isinstance(op, UpsertAssertion):
            a = next((x for x in snap["assertions"] if x["assertion_id"] == op.assertion_id), None)
            if op.assertion_id and (a is None or a["revision"] != op.expected_assertion_revision):
                fail("revision_conflict", "Assertion changed or belongs to another story")
            ident = op.assertion_id or str(uuid4())
            revision = (a["revision"] + 1) if a else 1
            values = op.model_dump(mode="json", exclude={"op", "assertion_id", "expected_assertion_revision"})
            values.update(assertion_id=ident, revision=revision, evidence_ids=[], assessment_ids=[])
            if a:
                snap["assertions"].remove(a)
            snap["assertions"].append(values)
            db.execute("INSERT INTO story_assertion_versions VALUES(?,?,?,?,?,?,?)",
                       (ident, revision, snap["story_id"], values["proposition"], canonical(values),
                        str(principal.subject), now()))
            self._invalidate(db, snap, {ident})
            return "assertion"
        if isinstance(op, AttachEvidence):
            assertion = next((a for a in snap["assertions"]
                              if a["assertion_id"] == op.assertion_id and a["revision"] == op.assertion_revision), None)
            if assertion is None:
                fail("revision_conflict", "Use the current assertion revision")
            locator = op.locator.model_dump(mode="json", exclude_none=True)
            match = "unverified"
            actor = str(principal.subject)
            if op.source_kind == "document":
                doc = self._document_allowed(db, actor, op.source_id)
                page = self._corpus_row(db, "rkb_pages", locator.get("page_id"))
                region = self._corpus_row(db, "rkb_regions", locator.get("region_id"))
                if not doc or not page or not region or page.get("document_id") != op.source_id or (
                        page.get("revision") != op.source_revision or region.get("page_id") != page.get("id")):
                    fail("invalid_evidence", "Page/region must belong to this exact document revision")
                if doc.get("active_revision", 0) < op.source_revision:
                    fail("source_changed")
                if locator.get("physical_page_index") is not None and (
                        locator["physical_page_index"] != page.get("physical_page_index")):
                    fail("invalid_evidence", "Physical source page mismatch")
                if locator.get("printed_page_number") is not None and (
                        str(locator["printed_page_number"]) != str(page.get("printed_page_number"))):
                    fail("invalid_evidence", "Printed page number mismatch")
                if locator.get("chunk_id"):
                    chunk = self._corpus_row(db, "rkb_chunks", locator["chunk_id"])
                    if (not chunk or chunk.get("document_id") != op.source_id
                            or chunk.get("revision") != op.source_revision
                            or locator["page_id"] not in (chunk.get("page_ids") or [])
                            or locator["region_id"] not in (chunk.get("region_ids") or [])):
                        fail("invalid_evidence", "Chunk pointer escapes source/region")
                source = region.get("source_text") or ""
                source_hash = doc.get("source_sha256") or ""
            else:
                source_row = db.execute("SELECT * FROM story_sources WHERE id=? AND version=?",
                                        (op.source_id, op.source_revision)).fetchone()
                if not source_row or source_row["owner_id"] != actor:
                    fail("invalid_evidence", "External original is unavailable to this actor")
                source = source_row["text_content"]
                source_hash = source_row["content_sha256"]
            begin = locator.get("start")
            end = locator.get("end")
            if begin is not None or end is not None:
                if begin is None or end is None or end <= begin or source[begin:end] != op.original_excerpt:
                    fail("invalid_evidence", "Invalid quoted source range")
                match = "exact"
            elif source:
                offsets = [m.start() for m in re.finditer(re.escape(op.original_excerpt), source)]
                if len(offsets) == 1:
                    locator["start"] = offsets[0]
                    locator["end"] = offsets[0] + len(op.original_excerpt)
                    match = "exact"
                elif len(offsets) > 1:
                    fail("invalid_evidence", "Repeated quote requires explicit source range")
                else:
                    fail("invalid_evidence", "The quote does not occur in this source")
            if match != "exact":
                fail("invalid_evidence", "Text-only external source needs exact quote")
            ident = str(uuid4())
            db.execute("""INSERT INTO story_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                       (ident, snap["story_id"], op.assertion_id, op.assertion_revision,
                        op.source_kind, op.source_id, op.source_revision, source_hash,
                        canonical(locator), op.original_excerpt, hashlib.sha256(op.original_excerpt.encode()).hexdigest(),
                        op.relation, op.source_role_for_assertion, match, "not_checked", op.derived_from, actor, now()))
            self._dependency(db, snap["story_id"], op.source_kind, op.source_id, op.source_revision, actor)
            assertion["evidence_ids"].append(ident)
            self._invalidate(db, snap, {op.assertion_id})
            return "evidence"
        if isinstance(op, RecordAssessment):
            a = next((x for x in snap["assertions"]
                      if x["assertion_id"] == op.assertion_id and x["revision"] == op.assertion_revision), None)
            if a is None:
                fail("revision_conflict")
            evid = db.execute("SELECT id FROM story_evidence WHERE assertion_id=? AND assertion_revision=?",
                              (op.assertion_id, op.assertion_revision)).fetchall()
            known = {r[0] for r in evid}
            if not set(op.evidence_ids).issubset(known):
                fail("invalid_evidence", "Assessment cites an unrelated evidence ID")
            if op.support_status == "corroborated" and op.independence != "independent":
                fail("validation_failed", "Independent provenance assessment is required")
            if op.assessor_kind == "model" and not op.method_version:
                fail("validation_failed", "Model/application assessment needs method version")
            if op.supersedes_assessment_id:
                active = {row["id"] for row in self._current_assessments(db, a)}
                if op.supersedes_assessment_id not in active:
                    fail("revision_conflict", "Supersession requires one current review of this assertion revision")
            ident = str(uuid4())
            data = op.model_dump(mode="json", exclude={"op"})
            data["id"] = ident
            # The authenticated actor is real; "human" or "model" in tool
            # arguments is at most a client claim, not verified authorship.
            data["assessor_claim_verified"] = False
            data["actual_executor"] = "application"
            data["client_id"] = getattr(principal, "client_id", None)
            db.execute("INSERT INTO story_assessments VALUES(?,?,?,?,?,?,?)",
                       (ident, snap["story_id"], op.assertion_id, op.assertion_revision,
                        canonical(data), str(principal.subject), now()))
            a["assessment_ids"].append(ident)
            self._invalidate(db, snap, {op.assertion_id})
            return "assessment"
        if isinstance(op, UpsertVariant):
            item = next((x for x in snap["variants"] if x["variant_id"] == op.variant_id), None)
            if op.variant_id and (not item or item["revision"] != op.expected_variant_revision):
                fail("revision_conflict", "Variant changed or belongs to another story")
            valid = {(a["assertion_id"], a["revision"]) for a in snap["assertions"]}
            for block in op.blocks:
                if block.assertion_id and (block.assertion_id, block.assertion_revision) not in valid:
                    fail("invalid_evidence", "A block refers to a stale/missing assertion")
            ident = op.variant_id or str(uuid4())
            revision = item["revision"] + 1 if item else 1
            values = op.model_dump(mode="json", exclude={"op", "variant_id", "expected_variant_revision"})
            if item:
                snap["variants"].remove(item)
                db.execute("""UPDATE story_variants SET revision=?,state='drafting',
                            approved_revision=NULL,approval_fingerprint=NULL WHERE id=?""", (revision, ident))
            else:
                db.execute("INSERT INTO story_variants(id,story_id,revision,state) VALUES(?,?,?,'drafting')",
                           (ident, snap["story_id"], revision))
            values.update(variant_id=ident, revision=revision, state="drafting")
            snap["variants"].append(values)
            db.execute("INSERT INTO story_variant_versions VALUES(?,?,?,?,?)",
                       (ident, revision, canonical(values), str(principal.subject), now()))
            return "variant"
        if isinstance(op, RecordInterest):
            values = op.model_dump(mode="json", exclude={"op"})
            complete = all(values[k]["value"] is not None for k in WEIGHTS)
            potential = round(25 * sum(WEIGHTS[k] * values[k]["value"] for k in WEIGHTS), 2) if complete else None
            if potential is not None and any(values[k]["rationale"] is None for k in WEIGHTS):
                fail("validation_failed", "Explain each scored criterion")
            novelty = values["channel_novelty"]
            readiness = values["production_readiness"]
            values.update(potential=potential, coverage=sum(values[k]["value"] is not None for k in WEIGHTS) / 5,
                          production_priority=round(.7 * potential + .2 * novelty + .1 * readiness -
                                                    values["repetition_penalty"], 2)
                          if None not in (potential, novelty, readiness) else None,
                          at=now(), actor_id=str(principal.subject),
                          story_revision=snap.get("current_revision", 0) + 1)
            snap["interest_assessments"].append(values)
            return "interest"
        fail("validation_failed", "Unsupported editorial operation")

    def edit(self, principal, story_id, expected_revision, operations, idempotency_key, reason=None):
        if not 1 <= len(operations) <= 12:
            fail("validation_failed", "1..12 typed operations required")
        ops = [x.model_dump(mode="json") for x in operations]
        def authorize(db, actor):
            rec, _ = self._read_story(db, actor, story_id, "contributor")
            if any(isinstance(op, (RecordAssessment, AttachEvidence, UpsertAssertion)) for op in operations):
                self._permission(db, actor, rec, "researcher")
            if any(isinstance(op, (UpsertVariant, SetAngle, SetContributors)) for op in operations):
                self._permission(db, actor, rec, "editor")
            return rec["workspace_id"]
        def apply(db, actor):
            rec, snap = self._read_story(db, actor, story_id, "contributor")
            if rec["revision"] != expected_revision:
                fail("revision_conflict", "Current revision " + str(rec["revision"]))
            if rec["archived"] or rec["merged_into"]:
                fail("validation_failed", "Restore archived material before editing")
            kinds = [self._apply_op(db, principal, snap, op) for op in operations]
            rev = rec["revision"] + 1
            self._snapshot(db, story_id, rev, snap, principal, "edit:" + ",".join(kinds))
            return {**self._receipt_story(story_id, rev, snap["state"], snap),
                    "change_summary": "Обновлено: " + ", ".join(kinds),
                    "next_actions": ["story_get", "story_validate"]}
        return self._mutation(principal, "story_edit", idempotency_key,
                              {"story_id": story_id, "expected_revision": expected_revision, "operations": ops, "reason": reason},
                              authorize, apply)

    def _evidence_state(self, db, evidence):
        """Check the accepted source, not its replaceable retrieval chunk.

        A rechunk of identical page/region text remains valid; corrected OCR
        or a changed source blocks approval. Ambiguous matching fails closed.
        """
        if evidence is None:
            return "missing"
        if evidence["source_kind"] == "external":
            source = db.execute("SELECT content_sha256 FROM story_sources WHERE id=? AND version=?",
                                (evidence["source_id"], evidence["source_revision"])).fetchone()
            return "unchanged" if source and source[0] == evidence["source_sha256"] else "changed"
        doc = self._corpus_row(db, "rkb_documents", evidence["source_id"])
        if not doc or doc.get("source_sha256") != evidence["source_sha256"]:
            return "changed"
        if doc.get("active_revision", 0) < evidence["source_revision"]:
            return "changed"
        loc = json.loads(evidence["locator"])
        old_page = self._corpus_row(db, "rkb_pages", loc.get("page_id"))
        old_region = self._corpus_row(db, "rkb_regions", loc.get("region_id"))
        if (not old_page or not old_region
                or old_page.get("document_id") != evidence["source_id"]
                or old_page.get("revision") != evidence["source_revision"]
                or old_region.get("page_id") != old_page.get("id")):
            return "changed"
        old_text = old_region.get("source_text") or ""
        excerpt = evidence["original_excerpt"]
        begin, finish = loc.get("start"), loc.get("end")
        if (begin is None or finish is None or old_text[begin:finish] != excerpt):
            return "changed"
        if doc.get("active_revision") == evidence["source_revision"]:
            return "unchanged"
        # A new accepted revision may have different page/region IDs after
        # rechunking. Match physical position AND entire original region text.
        pages = db.execute("""SELECT payload FROM corpus_rows WHERE table_name='rkb_pages'
                     AND document_id=? AND revision=?
                     AND json_extract(payload,'$.physical_page_index')=? LIMIT 2""",
                           (evidence["source_id"], doc["active_revision"],
                            old_page.get("physical_page_index"))).fetchall()
        if len(pages) != 1:
            return "changed"
        page_id = json.loads(pages[0][0])["id"]
        regions = db.execute("""SELECT payload FROM corpus_rows WHERE table_name='rkb_regions'
                     AND json_extract(payload,'$.page_id')=?""", (page_id,)).fetchall()
        matches = [1 for r in regions if (json.loads(r[0]).get("source_text") or "") == old_text]
        return "unchanged" if len(matches) == 1 else "changed"

    def _fingerprint(self, db, snap, variant):
        """Only declared variant-assertion dependencies affect scoped approval.

        Legacy variants with no explicit blocks conservatively depend on the
        entire card. Superseded reviews are not effective support.
        """
        used = {str(b["assertion_id"]) for b in variant.get("blocks") or []
                if b.get("assertion_id")}
        affected = [a for a in snap["assertions"]
                    if not used or a["assertion_id"] in used]
        ids = [(a["assertion_id"], a["revision"], a.get("evidence_ids"))
               for a in affected]
        proof = []
        for assertion in affected:
            for eid in assertion.get("evidence_ids") or []:
                ev = self._row(db, "story_evidence", eid)
                proof.append((eid, ev["source_sha256"] if ev else None,
                              self._evidence_state(db, ev), ev["source_revision"] if ev else None,
                              ev["excerpt_sha256"] if ev else None))
            for assessment in self._current_assessments(db, assertion):
                proof.append(("effective_assessment", assessment["id"], assessment))
        return digest({"variant": {k:v for k,v in variant.items() if k != "state"},
                       "assertions": ids, "proof": proof})

    def _validate(self, db, snap, variant, review=None):
        blockers, unknown = [], []
        if not variant:
            return ["variant_not_found"], []
        if not all(variant.get(k) for k in ("audience", "format", "language", "body")):
            blockers.append("missing_variant_fields")
        used = {str(block["assertion_id"]) for block in variant.get("blocks") or []
                if block.get("assertion_id")}
        relevant = [a for a in snap["assertions"]
                    if not used or a["assertion_id"] in used]
        if not relevant:
            blockers.append("no_reviewable_assertions")
        for a in relevant:
            for evidence_id in a.get("evidence_ids") or []:
                evidence = self._row(db, "story_evidence", evidence_id)
                if not evidence:
                    blockers.append("missing_evidence:" + evidence_id)
                    continue
                if self._evidence_state(db, evidence) != "unchanged":
                    blockers.append("source_changed:" + evidence_id)
            if a["kind"] == "attributed_account":
                if not a.get("account_kind"):
                    blockers.append("account_kind_required")
                if not a.get("attributed_to") and not a.get("reported_by"):
                    unknown.append("narrator_unknown_requires_explicit_wording:" + a["assertion_id"])
                if not variant.get("attributions"):
                    blockers.append("spoken_attribution_required:" + a["assertion_id"])
            if a["kind"] == "historical_claim":
                if not a.get("evidence_ids"):
                    blockers.append("historical_claim_without_evidence:" + a["assertion_id"])
                else:
                    assessments = self._current_assessments(db, a)
                    positions = {x.get("semantic_review") for x in assessments}
                    adverse = {"unsupported", "contested"}
                    positive = bool(positions & {"supported", "partially_supported"})
                    negative = bool(positions & adverse or any(
                        x.get("support_status") in {"contested", "contradicted", "unsupported"}
                        for x in assessments))
                    if positive and negative:
                        blockers.append("historical_claim_has_active_disagreement:" + a["assertion_id"])
                    elif not positive:
                        # Preserve the existing public blocker for an adverse-only
                        # or unassessed claim. A superseded old positive cannot
                        # satisfy the requirement.
                        blockers.append("historical_claim_without_semantic_review:" + a["assertion_id"])
            if a["kind"] in {"hypothesis", "creative_material"} and not variant.get("attributions"):
                blockers.append("hypothesis_or_reconstruction_not_marked:" + a["assertion_id"])
        if variant.get("media_refs"):
            # No trusted media/rights adapter is wired to the Story Registry yet.
            # An opaque ref supplied by a model is not evidence of a license.
            blockers.append("media_rights_not_verified")
        if any(x["state"] == "open" and x["priority"] == "critical" for x in snap["gaps"]):
            blockers.append("critical_unresolved_gap")
        if review is not None and not all((review.semantic_checked, review.attribution_checked, review.rights_checked)):
            blockers.append("review_decision_incomplete")
        if variant["state"] == "needs_revalidation":
            unknown.append("previous_approval_stale")
        return list(dict.fromkeys(blockers)), list(dict.fromkeys(unknown))

    def validate(self, principal, story_id, variant_revision_ids=None):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            rec, snap = self._read_story(db, actor, story_id)
            result = []
            for x in snap["variants"]:
                if variant_revision_ids and not any(x["variant_id"] == e.get("variant_id") and
                                                    x["revision"] == e.get("revision") for e in variant_revision_ids):
                    continue
                blockers, unknown = self._validate(db, snap, x)
                fingerprint = self._fingerprint(db, snap, x)
                row = self._row(db, "story_variants", x["variant_id"])
                if row["state"] == "publish_ready" and row["approval_fingerprint"] != fingerprint:
                    blockers.append("approval_fingerprint_stale")
                result.append({"variant_id": x["variant_id"], "revision": x["revision"],
                               "result": "failed" if blockers else "unknown" if unknown else "passed",
                               "blockers": blockers, "unknown": unknown,
                               "approval_current": row["state"] == "publish_ready" and
                                                   row["approval_fingerprint"] == fingerprint})
            return {"story_id": story_id, "story_revision": rec["revision"], "variants": result,
                    "all_passed": bool(result) and all(x["result"] == "passed" for x in result),
                    "allowed_actions": self._available_actions(db, actor, rec)}

    def transition(self, principal, story_id, expected_revision, target_state, variant_revision_ids,
                   review, idempotency_key):
        selected = [dict(x) for x in variant_revision_ids or []]
        def authorize(db, actor):
            rec, _ = self._read_story(db, actor, story_id, "editor")
            return rec["workspace_id"]
        def apply(db, actor):
            rec, snap = self._read_story(db, actor, story_id, "editor")
            if rec["revision"] != expected_revision:
                fail("revision_conflict", "Current revision " + str(rec["revision"]))
            if rec["archived"] or rec["merged_into"]:
                fail("validation_failed", "Archived material must be restored")
            if target_state == "publish_ready":
                if not selected or review is None:
                    fail("validation_failed", "Choose exact variant revisions and a recorded review")
                if len(set((v["variant_id"], v["revision"]) for v in selected)) != len(selected):
                    fail("validation_failed", "Duplicate variant")
                for v in selected:
                    item = next((x for x in snap["variants"]
                                 if x["variant_id"] == v["variant_id"] and x["revision"] == v["revision"]), None)
                    if item is None:
                        fail("revision_conflict", "Selected variant is no longer current")
                    blockers, unknown = self._validate(db, snap, item, review)
                    if blockers:
                        fail("validation_failed", ",".join(blockers))
                    # Assessor's review explicitly owns unresolved semantic questions; no automatic truth assertion.
                    fp = self._fingerprint(db, snap, item)
                    item["state"] = "publish_ready"
                    review_record = review.model_dump(mode="json")
                    review_record["actual_executor"] = "application"
                    review_record["reviewer_claim_verified"] = False
                    db.execute("""INSERT INTO story_review_decisions VALUES(?,?,?,?,?,?,?,?,?)""",
                               (str(uuid4()), story_id, item["variant_id"], item["revision"],
                                fp, actor, getattr(principal, "client_id", None),
                                canonical(review_record), now()))
                    db.execute("""UPDATE story_variants SET state='publish_ready',approved_revision=?,
                                  approval_fingerprint=? WHERE id=?""", (item["revision"], fp, item["variant_id"]))
                snap["state"] = "review"
            else:
                allowed = {"raw_seed": {"candidate", "rejected", "deferred"},
                           "candidate": {"researching", "drafting", "rejected", "deferred"},
                           "researching": {"drafting", "review", "deferred", "rejected"},
                           "drafting": {"review", "researching", "deferred"},
                           "review": {"drafting", "researching", "deferred"},
                           "deferred": {"candidate", "researching", "drafting"},
                           "rejected": {"candidate"}}
                if target_state not in allowed.get(snap["state"], set()):
                    fail("validation_failed", "Unsupported state transition")
                snap["state"] = target_state
            new_revision = rec["revision"] + 1
            self._snapshot(db, story_id, new_revision, snap, principal, "transition:" + target_state)
            return {**self._receipt_story(story_id, new_revision, snap["state"], snap),
                    "change_summary": "Состояние обновлено; одобрены только выбранные версии"}
        return self._mutation(principal, "story_transition", idempotency_key,
                              {"story_id": story_id, "expected_revision": expected_revision, "target_state": target_state,
                               "variants": selected, "review": review.model_dump(mode="json") if review else None},
                              authorize, apply)

    def archive(self, principal, story_id, expected_revision, action, idempotency_key):
        def authorize(db, actor):
            rec, _ = self._read_story(db, actor, story_id, "contributor")
            return rec["workspace_id"]
        def apply(db, actor):
            rec, snap = self._read_story(db, actor, story_id, "contributor")
            if rec["revision"] != expected_revision:
                fail("revision_conflict")
            if rec["merged_into"]:
                fail("validation_failed", "Merged history stays permanently redirected")
            snap["archived"] = action == "archive"
            if action == "archive":
                snap["previous_state"] = snap["state"]
                snap["state"] = "archived"
            else:
                snap["state"] = snap.pop("previous_state", "candidate")
            self._snapshot(db, story_id, rec["revision"] + 1, snap, principal, action)
            return {**self._receipt_story(story_id, rec["revision"] + 1, snap["state"], snap),
                    "change_summary": "Архивное состояние обновлено"}
        return self._mutation(principal, "story_archive", idempotency_key,
                              {"story_id": story_id, "expected_revision": expected_revision, "action": action},
                              authorize, apply)

    def register_source(self, principal, source, idempotency_key):
        data = source.model_dump(mode="json")
        def authorize(db, actor):
            return None
        def apply(db, actor):
            source_id = str(uuid4())
            db.execute("INSERT INTO story_sources VALUES(?,?,?,?,?,?,?,?)",
                       (source_id, 1, actor, None, canonical(data), data.get("body") or "",
                        hashlib.sha256((data.get("body") or "").encode()).hexdigest(), now()))
            self._audit(db, principal, None, None, "register_source", "immutable external source")
            return {"resource_type": "source", "resource_ids": [source_id], "source_id": source_id,
                    "source_version": 1, "change_summary": "Внешнее свидетельство зарегистрировано"}
        return self._mutation(principal, "story_source_register", idempotency_key, data, authorize, apply)

    def search(self, principal, query="", filters=None, mode="lexical", order="updated", limit=3, cursor=None):
        filters = filters or {}
        if any(k not in {"state", "material_type", "workspace_id", "assertion_kind", "source_id", "entity_ref", "format", "audience", "min_potential"} for k in filters):
            fail("validation_failed", "Unsupported story search filter")
        # The vector plane is deliberately not used until a separately scoped
        # index is installed. Never silently call a hybrid query semantic.
        actual_mode = "lexical_only" if mode == "lexical" else "lexical_degraded"
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            offset = max(0, int(cursor or 0))
            limit = max(1, min(int(limit), 20))
            if offset > 100000:
                fail("validation_failed", "Cursor outside bounded results")
            # Filter *all* story and source permissions in SQL BEFORE FTS BM25
            # orders candidates. A post-ranking check alone can reveal which
            # private content is relevant and displace authorized hits.
            # 'who' is a bound verified principal, never client JSON.
            visibility = """(
               (s.owner_id=(SELECT actor_id FROM who)
                OR EXISTS (SELECT 1 FROM story_grants g WHERE g.story_id=s.id
                      AND g.grantee_id=(SELECT actor_id FROM who))
                OR EXISTS (SELECT 1 FROM corpus_rows w
                  WHERE w.table_name='rkb_workspaces' AND w.row_key=s.workspace_id
                  AND json_extract(w.payload,'$.owner_user_id')=(SELECT actor_id FROM who))
                OR EXISTS (SELECT 1 FROM corpus_rows m
                  WHERE m.table_name='rkb_workspace_members'
                    AND json_extract(m.payload,'$.workspace_id')=s.workspace_id
                    AND json_extract(m.payload,'$.user_id')=(SELECT actor_id FROM who)))
               AND NOT EXISTS (
                 SELECT 1 FROM story_dependencies dep
                 WHERE dep.story_id=s.id AND (
                   (dep.source_kind='document' AND NOT EXISTS (
                     SELECT 1 FROM corpus_rows d
                     WHERE d.table_name='rkb_documents' AND d.row_key=dep.source_id
                       AND (
                         json_extract(d.payload,'$.owner_user_id')=(SELECT actor_id FROM who)
                         OR (json_extract(d.payload,'$.content_visibility')='public'
                             AND json_extract(d.payload,'$.rights_status') IN
                              ('licensed','permission_granted','public_domain_verified','statutory_access_verified')
                             AND json_extract(d.payload,'$.rights_policy_version') IS NOT NULL
                             AND json_extract(d.payload,'$.rights_evidence.public_distribution')=1)
                         OR EXISTS (SELECT 1 FROM corpus_rows grant_row
                             WHERE grant_row.table_name='rkb_document_grants'
                               AND grant_row.document_id=d.row_key
                               AND json_extract(grant_row.payload,'$.grantee_user_id')=(SELECT actor_id FROM who))
                         OR (json_extract(d.payload,'$.content_visibility') IN ('workspace','public')
                             AND EXISTS (SELECT 1 FROM corpus_rows w
                               WHERE w.table_name='rkb_workspaces'
                                 AND w.row_key=json_extract(d.payload,'$.workspace_id')
                                 AND json_extract(w.payload,'$.owner_user_id')=(SELECT actor_id FROM who)))
                         OR (json_extract(d.payload,'$.content_visibility') IN ('workspace','public')
                             AND EXISTS (SELECT 1 FROM corpus_rows member
                               WHERE member.table_name='rkb_workspace_members'
                                 AND json_extract(member.payload,'$.workspace_id')=
                                     json_extract(d.payload,'$.workspace_id')
                                 AND json_extract(member.payload,'$.user_id')=(SELECT actor_id FROM who)))
                       )))
                   OR (dep.source_kind='external' AND NOT EXISTS(
                     SELECT 1 FROM story_sources source
                     WHERE source.id=dep.source_id AND source.version=dep.source_revision
                       AND (source.owner_id=(SELECT actor_id FROM who)
                            OR s.owner_id=(SELECT actor_id FROM who))))
                   OR dep.source_kind NOT IN ('document','external')
                 )))"""
            params = [actor]
            predicates = [visibility, "s.archived=0", "s.merged_into IS NULL"]
            if filters.get("state"):
                predicates.append("s.state=?")
                params.append(str(filters["state"])[:80])
            if filters.get("material_type"):
                predicates.append("s.material_type=?")
                params.append(str(filters["material_type"])[:80])
            if filters.get("workspace_id"):
                predicates.append("s.workspace_id=?")
                params.append(str(filters["workspace_id"])[:128])
            if filters.get("source_id"):
                predicates.append("EXISTS (SELECT 1 FROM story_dependencies dep WHERE dep.story_id=s.id AND dep.source_id=?)")
                params.append(str(filters["source_id"])[:128])
            if filters.get("assertion_kind"):
                predicates.append("""EXISTS (SELECT 1 FROM json_each(s.snapshot,'$.assertions') a
                                     WHERE json_extract(a.value,'$.kind')=?)""")
                params.append(str(filters["assertion_kind"])[:80])
            if filters.get("entity_ref"):
                predicates.append("""EXISTS (SELECT 1 FROM json_each(s.snapshot,'$.links') l
                                     WHERE json_extract(l.value,'$.entity_ref')=?)""")
                params.append(str(filters["entity_ref"])[:128])
            if filters.get("format"):
                predicates.append("""EXISTS (SELECT 1 FROM json_each(s.snapshot,'$.variants') v
                                     WHERE json_extract(v.value,'$.format')=?)""")
                params.append(str(filters["format"])[:80])
            if filters.get("audience"):
                predicates.append("""EXISTS (SELECT 1 FROM json_each(s.snapshot,'$.variants') v
                                     WHERE json_extract(v.value,'$.audience')=?)""")
                params.append(str(filters["audience"])[:250])
            score = "json_extract(s.snapshot,'$.interest_assessments[#-1].potential')"
            if filters.get("min_potential") is not None:
                try:
                    min_score = float(filters["min_potential"])
                    if not (0 <= min_score <= 100):
                        raise ValueError("outside range")
                except (ValueError, TypeError):
                    fail("validation_failed", "min_potential must be 0..100")
                predicates.append(score + ">=?")
                params.append(min_score)
            tokens = re.findall(r"[^\W_]+", str(query), re.UNICODE)[:12]
            expression = " OR ".join('"' + t + '"' for t in tokens)
            if tokens:
                sql = """WITH who(actor_id) AS (VALUES (?))
                    SELECT s.* FROM story_fts JOIN story_records s ON s.rowid=story_fts.rowid
                    WHERE story_fts MATCH ? AND """ + " AND ".join(predicates)
                sql += (" ORDER BY COALESCE(" + score + ",-1) DESC,bm25(story_fts),s.id LIMIT ? OFFSET ?"
                        if order == "potential" else " ORDER BY bm25(story_fts),s.id LIMIT ? OFFSET ?")
                args = [actor, expression, *params[1:], limit + 1, offset]
            else:
                sql = """WITH who(actor_id) AS (VALUES (?))
                    SELECT s.* FROM story_records s WHERE """ + " AND ".join(predicates)
                sql += (" ORDER BY COALESCE(" + score + ",-1) DESC,s.updated_at DESC,s.id LIMIT ? OFFSET ?"
                        if order == "potential" else " ORDER BY s.updated_at DESC,s.id LIMIT ? OFFSET ?")
                args = [*params, limit + 1, offset]
            rows = db.execute(sql, args).fetchall()
            hits = []
            for r in rows[:limit]:
                try:
                    self._permission(db, actor, r)
                except StoryError:
                    # A concurrent revocation must still win at readback.
                    continue
                snap = json.loads(r["snapshot"])
                attribution = []
                kind_speech = {"legend": "По легенде", "tradition": "По преданию",
                               "rumor": "По неподтверждённому рассказу",
                               "recollection": "По воспоминанию", "testimony": "По свидетельству"}
                for a in snap["assertions"]:
                    if a["kind"] == "attributed_account":
                        kind = kind_speech.get(a.get("account_kind"), "По рассказу")
                        attributed = a.get("attributed_to") or a.get("reported_by")
                        attribution.append((kind + (", рассказчик: " + attributed if attributed
                                                    else ", рассказчик неизвестен"))[:160])
                    if len(attribution) == 3:
                        break
                seed_origin = snap.get("seed", {}).get("origin_status", "unknown")
                if not snap["assertions"] and seed_origin in {"unknown", "partial"}:
                    attribution.insert(0, "Непроверенная зацепка, происхождение " +
                                       ("неизвестно" if seed_origin == "unknown" else "установлено частично"))
                interest = (snap.get("interest_assessments") or [None])[-1]
                hits.append({"story_id": r["id"], "revision": r["revision"], "title": r["title"],
                             "potential": interest.get("potential") if interest else None,
                             "score_coverage": interest.get("coverage") if interest else 0,
                             "production_priority": interest.get("production_priority") if interest else None,
                             "summary": (r["summary"] or r["seed_text"])[:350],
                             "state": r["state"], "material_type": r["material_type"],
                             "kinds": list(dict.fromkeys(a["kind"] for a in snap["assertions"])),
                             "attributions": attribution,
                             "spoken_summary": "; ".join([*attribution, (r["summary"] or r["seed_text"])[:200]]),
                             "open_gaps": sum(g["state"] == "open" for g in snap["gaps"]),
                             "allowed_actions": self._available_actions(db, actor, r)[:6]})
            more = len(rows) > limit
            return {"results": hits, "has_more": more,
                    "next_cursor": str(offset + limit) if more else None,
                    "retrieval_mode": actual_mode,
                    "indexing_state": "local_fts_ready_vector_awaiting_worker"}

    def access(self, principal, story_id, expected_revision, grantee_user_id, capability, idempotency_key):
        grantee = str(UUID(grantee_user_id))
        def authorize(db, actor):
            rec, _ = self._read_story(db, actor, story_id, "manager")
            return rec["workspace_id"]
        def apply(db, actor):
            rec, snap = self._read_story(db, actor, story_id, "manager")
            if rec["revision"] != expected_revision:
                fail("revision_conflict")
            if capability == "revoke":
                db.execute("DELETE FROM story_grants WHERE story_id=? AND grantee_id=?", (story_id, grantee))
            elif capability in ROLE_LEVEL:
                db.execute("""INSERT INTO story_grants VALUES(?,?,?,?) ON CONFLICT(story_id,grantee_id)
                              DO UPDATE SET capability=excluded.capability,granted_by=excluded.granted_by""",
                           (story_id, grantee, capability, actor))
            else:
                fail("validation_failed")
            rev = rec["revision"] + 1
            self._snapshot(db, story_id, rev, snap, principal, "access_change")
            return {**self._receipt_story(story_id, rev, snap["state"], snap),
                    "change_summary": "Права истории изменены"}
        return self._mutation(principal, "story_access", idempotency_key,
                              {"story_id": story_id, "expected_revision": expected_revision,
                               "grantee": grantee, "capability": capability}, authorize, apply)

    def merge(self, principal, target_id, source_ids, expected_revisions, reason, idempotency_key):
        if not source_ids or len(source_ids) > 10 or target_id in source_ids or len(set(source_ids)) != len(source_ids):
            fail("validation_failed")
        def authorize(db, actor):
            target, _ = self._read_story(db, actor, target_id, "editor")
            for sid in source_ids:
                src, _ = self._read_story(db, actor, sid, "editor")
                if src["workspace_id"] != target["workspace_id"] or src["owner_id"] != target["owner_id"]:
                    fail("capability_denied", "Merge requires one ownership/scope")
            return target["workspace_id"]
        def apply(db, actor):
            target, snap = self._read_story(db, actor, target_id, "editor")
            if target["revision"] != expected_revisions.get(target_id):
                fail("revision_conflict")
            for sid in source_ids:
                r, s = self._read_story(db, actor, sid, "editor")
                if r["revision"] != expected_revisions.get(sid):
                    fail("revision_conflict")
                for dep in db.execute("SELECT * FROM story_dependencies WHERE story_id=?", (sid,)):
                    self._dependency(db, target_id, dep["source_kind"], dep["source_id"],
                                     dep["source_revision"], actor)
                for a in s["assertions"]:
                    if a not in snap["assertions"]:
                        # Preserve original IDs and versions; do NOT rewrite their ownership or approval.
                        snap["assertions"].append(a)
                for gap in s["gaps"]:
                    if gap not in snap["gaps"]:
                        snap["gaps"].append(gap)
                s["merged_into"] = target_id
                s["archived"] = True
                s["state"] = "archived"
                self._snapshot(db, sid, r["revision"] + 1, s, principal, "merged_into:" + target_id)
            self._invalidate(db, snap)
            rev = target["revision"] + 1
            self._snapshot(db, target_id, rev, snap, principal, "merge")
            return {**self._receipt_story(target_id, rev, snap["state"], snap),
                    "resource_ids": [target_id, *source_ids], "change_summary": "Истории объединены с сохранением исходных версий"}
        return self._mutation(principal, "story_merge", idempotency_key,
                              {"target": target_id, "sources": source_ids, "revisions": expected_revisions, "reason": reason},
                              authorize, apply)

    def export(self, principal, story_id, variant_revision_id, target="editorial"):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            rec, snap = self._read_story(db, actor, story_id, "publisher")
            variant = next((x for x in snap["variants"]
                            if x["variant_id"] == variant_revision_id and x["state"] == "publish_ready"), None)
            if not variant:
                fail("validation_failed", "Only a current approved variant can be exported")
            row = self._row(db, "story_variants", variant_revision_id)
            if row["approval_fingerprint"] != self._fingerprint(db, snap, variant):
                fail("source_changed")
            citations = []
            for eid in [e for a in snap["assertions"] for e in a["evidence_ids"]]:
                ev = self._row(db, "story_evidence", eid)
                if ev["source_kind"] == "document":
                    d = self._document_allowed(db, actor, ev["source_id"])
                    citations.append({"document_id": ev["source_id"], "source_revision": ev["source_revision"],
                                      "title": d.get("title"), "authors": d.get("authors"),
                                      "locator": json.loads(ev["locator"]), "text_match": ev["text_match"]})
                else:
                    source = db.execute("SELECT metadata FROM story_sources WHERE id=? AND version=?",
                                        (ev["source_id"], ev["source_revision"])).fetchone()
                    citations.append({"source_id": ev["source_id"],
                                      "source_revision": ev["source_revision"],
                                      "metadata": json.loads(source["metadata"]) if source else None})
            return {"target": target, "story_id": story_id, "story_revision": rec["revision"],
                    "variant_id": variant_revision_id, "variant_revision": variant["revision"],
                    "body": variant["body"], "blocks": variant["blocks"],
                    "attributions": variant["attributions"],
                    "narration_text": "\n".join([*variant["attributions"], variant["body"]]),
                    "media_refs": variant["media_refs"],
                    "bibliography": citations, "export_state": "read_only_package",
                    "publication_state": "not_sent"}

    def publication_record(self, principal, story_id, variant_revision_id, publication, idempotency_key):
        data = publication
        def authorize(db, actor):
            rec, _ = self._read_story(db, actor, story_id, "publisher")
            return rec["workspace_id"]
        def apply(db, actor):
            rec, snap = self._read_story(db, actor, story_id, "publisher")
            variant = next((x for x in snap["variants"] if x["variant_id"] == variant_revision_id
                            and x["state"] == "publish_ready"), None)
            if not variant:
                fail("validation_failed", "Only approved current variant can be reported published")
            if self._fingerprint(db, snap, variant) != self._row(
                    db, "story_variants", variant_revision_id)["approval_fingerprint"]:
                fail("source_changed")
            if not data.get("channel") or not (data.get("provider_id") or data.get("provider_url")):
                fail("validation_failed", "Channel and observable provider reference required")
            # Public callers cannot self-certify provider readback.
            if data.get("confirmation", "editor_reported") != "editor_reported":
                fail("capability_denied", "provider_verified requires a trusted provider adapter")
            ident = str(uuid4())
            db.execute("INSERT INTO story_publications VALUES(?,?,?,?,?,?,?,?)",
                       (ident, story_id, variant_revision_id, variant["revision"], canonical(data),
                        "editor_reported", actor, now()))
            self._audit(db, principal, story_id, rec["revision"], "publication_record", "editor reported")
            return {"resource_type": "publication", "resource_ids": [ident],
                    "publication_id": ident, "story_id": story_id, "variant_id": variant_revision_id,
                    "variant_revision": variant["revision"], "confirmation": "editor_reported",
                    "change_summary": "Сообщение о выпуске сохранено; провайдер не проверен"}
        return self._mutation(principal, "story_publication_record", idempotency_key,
                              {"story_id": story_id, "variant_id": variant_revision_id, "publication": data},
                              authorize, apply)

    def corpus_read(self, principal, document_id, source_revision, cursor=None, limit=3):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            d = self._document_allowed(db, actor, document_id)
            if not d or d.get("active_revision") < source_revision:
                fail("not_found_or_not_accessible")
            offset = max(0, int(cursor or 0))
            take = max(1, min(int(limit), 20))
            rows = db.execute("""SELECT chunk_id,source_text,text_sha256 FROM chunk_text
              WHERE document_id=? AND revision=? ORDER BY rowid LIMIT ? OFFSET ?""",
                              (document_id, source_revision, take + 1, offset)).fetchall()
            chunks = []
            for row in rows[:take]:
                source_refs = []
                chunk = self._corpus_row(db, "rkb_chunks", row["chunk_id"])
                if (chunk and chunk.get("document_id") == document_id
                        and int(chunk.get("revision") or 0) == source_revision):
                    seen = set()
                    refs = [str(x) for x in chunk.get("region_ids") or []]
                    refs += [str(x.get("region_id")) for x in chunk.get("source_spans") or []
                             if x.get("region_id")]
                    for region_id in refs[:24]:
                        if region_id in seen:
                            continue
                        seen.add(region_id)
                        region = self._corpus_row(db, "rkb_regions", region_id)
                        if not region:
                            continue
                        page = self._corpus_row(db, "rkb_pages", region.get("page_id"))
                        if (not page or page.get("document_id") != document_id
                                or int(page.get("revision") or 0) != source_revision):
                            continue
                        printed = str(region.get("source_text") or "")
                        source_refs.append({
                            "page_id": str(page["id"]), "region_id": region_id,
                            "physical_page_index": int(page["physical_page_index"]),
                            "printed_page_number": page.get("printed_page_number"),
                            "source_text": printed[:6000],
                            "source_text_truncated": len(printed) > 6000,
                        })
                chunks.append({
                    "chunk_id": row["chunk_id"], "text": row["source_text"][:6000],
                    "text_sha256": row["text_sha256"],
                    "truncated": len(row["source_text"]) > 6000,
                    "source_regions": source_refs,
                })
            return {
                "document_id": document_id, "source_revision": source_revision,
                "source_sha256": d.get("source_sha256"),
                "chunks": chunks, "has_more": len(rows) > take,
                "next_cursor": str(offset + take) if len(rows) > take else None,
            }

    def entity_list(self, principal, document_ids=None, kinds=None, query="", cursor=None, limit=20):
        """ACL-filtered keyset walk; never cut the catalog to the first 100 documents.

        Cursor freezes the maximum *authorized* entity id at the initial query;
        ACL and active revision are rechecked on every subsequent page.
        Changes to records during traversal are not a point-in-time snapshot:
        restart the walk to include new/updated entities before the cursor.
        """
        import base64
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            take = max(1, min(int(limit), 50))
            docs = [str(x) for x in (document_ids or [])]
            kind_list = [str(x) for x in (kinds or [])]
            if len(docs) > 400 or len(kind_list) > 40:
                fail("validation_failed", "Limit one explicit scope to 400 documents and 40 kinds")
            query = str(query or "")
            scope_hash = digest({"actor": actor, "docs": sorted(set(docs)),
                                 "kinds": sorted(set(kind_list)), "query": query})
            # The document-level source ACL is evaluated in SQL *before* keyset
            # ordering. Unauthorized entities never displace visible results.
            visible = """(
                json_extract(d.payload,'$.owner_user_id') = ?
                OR (json_extract(d.payload,'$.content_visibility') = 'public'
                    AND json_extract(d.payload,'$.rights_status') IN
                      ('licensed','permission_granted','public_domain_verified','statutory_access_verified')
                    AND json_extract(d.payload,'$.rights_policy_version') IS NOT NULL
                    AND json_extract(d.payload,'$.rights_evidence.public_distribution') = 1)
                OR EXISTS (SELECT 1 FROM corpus_rows grant_row
                  WHERE grant_row.table_name='rkb_document_grants'
                    AND grant_row.document_id=d.row_key
                    AND json_extract(grant_row.payload,'$.grantee_user_id') = ?)
                OR (json_extract(d.payload,'$.content_visibility') IN ('workspace','public')
                    AND (EXISTS (SELECT 1 FROM corpus_rows ws
                         WHERE ws.table_name='rkb_workspaces'
                           AND ws.row_key=json_extract(d.payload,'$.workspace_id')
                           AND json_extract(ws.payload,'$.owner_user_id') = ?)
                         OR EXISTS (SELECT 1 FROM corpus_rows member
                         WHERE member.table_name='rkb_workspace_members'
                           AND json_extract(member.payload,'$.workspace_id') =
                             json_extract(d.payload,'$.workspace_id')
                           AND json_extract(member.payload,'$.user_id') = ?)))
            )"""
            where = [
                "e.table_name='rkb_entities'",
                "d.table_name='rkb_documents'",
                "d.row_key=e.document_id",
                "CAST(e.revision AS INTEGER)=CAST(json_extract(d.payload,'$.active_revision') AS INTEGER)",
                visible,
            ]
            params = [actor, actor, actor, actor]
            if docs:
                where.append("e.document_id IN (" + ",".join("?" for _ in docs) + ")")
                params.extend(docs)
            if kind_list:
                where.append("json_extract(e.payload,'$.kind') IN (" + ",".join("?" for _ in kind_list) + ")")
                params.extend(kind_list)
            if query:
                where.append("instr(lower(json_extract(e.payload,'$.canonical_label')),lower(?))>0")
                params.append(query)
            where_sql = " AND ".join(where)
            if cursor is None:
                upper = db.execute(
                    "SELECT MAX(e.row_key) FROM corpus_rows e JOIN corpus_rows d "
                    "ON d.row_key=e.document_id WHERE " + where_sql,
                    params,
                ).fetchone()[0]
                after = ""
            else:
                try:
                    value = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
                    if (value.get("v") != 1 or value.get("scope") != scope_hash
                            or not isinstance(value.get("after"), str)
                            or not isinstance(value.get("upper"), str)):
                        raise ValueError("invalid cursor scope")
                    after, upper = value["after"], value["upper"]
                except (ValueError, UnicodeError, TypeError, KeyError):
                    fail("validation_failed", "Invalid or mismatched entity cursor")
            if upper is None:
                return {"items": [], "has_more": False, "next_cursor": None,
                        "coverage": "accepted_mentions_not_complete_extraction",
                        "cursor_policy": "authorized_keyset_highwater_acl_rechecked"}
            rows = db.execute(
                "SELECT e.row_key, e.payload, e.document_id, e.revision "
                "FROM corpus_rows e JOIN corpus_rows d ON d.row_key=e.document_id "
                "WHERE " + where_sql + " AND e.row_key>? AND e.row_key<=? "
                "ORDER BY e.row_key LIMIT ?",
                [*params, after, upper, take + 1],
            ).fetchall()
            items = []
            for row in rows[:take]:
                item = json.loads(row["payload"])
                items.append({"entity_id": row["row_key"], "kind": item.get("kind"),
                              "label": item.get("canonical_label"),
                              "document_id": row["document_id"], "revision": row["revision"]})
            more = len(rows) > take
            next_cursor = None
            if more and items:
                token = {"v": 1, "scope": scope_hash, "upper": upper, "after": items[-1]["entity_id"]}
                next_cursor = base64.urlsafe_b64encode(
                    canonical(token).encode("utf8")).decode("ascii").rstrip("=")
            return {"items": items, "has_more": more, "next_cursor": next_cursor,
                    "coverage": "accepted_mentions_not_complete_extraction",
                    "cursor_policy": "authorized_keyset_highwater_acl_rechecked"}

    def _accepted_story_candidate(self, db, principal, job, document, candidate, batch_region_ids):
        """Materialize an attributed episode in the same transaction as its lease.

        The model does the semantic selection; the server only checks immutable
        printed source bytes, ACL, revision, batch scope and duplicate identity.
        No second model or automatic historical truth assessment is involved.
        """
        doc_id = str(job["document_id"])
        rev = int(job["source_revision"])
        proof = []
        for ref in candidate.evidence_refs:
            page = self._corpus_row(db, "rkb_pages", ref.page_id)
            region = self._corpus_row(db, "rkb_regions", ref.region_id)
            if (not page or not region or str(page.get("document_id")) != doc_id
                    or int(page.get("revision") or 0) != rev
                    or str(region.get("page_id")) != ref.page_id):
                fail("invalid_evidence", "Candidate evidence is outside the accepted document revision")
            source = str(region.get("source_text") or "")
            if not source:
                fail("invalid_evidence", "Candidate needs printed source text")
            quote = ref.original_excerpt
            if ref.start is not None:
                begin, end = ref.start, ref.end
                if end > len(source) or source[begin:end] != quote:
                    fail("invalid_evidence", "Stale or invalid exact source offsets")
            else:
                begin = source.find(quote)
                if begin < 0 or source.find(quote, begin + 1) >= 0:
                    fail("invalid_evidence", "Quoted source absent or ambiguous; provide offsets")
                end = begin + len(quote)
            proof.append({
                "page_id": str(page["id"]), "region_id": str(region["id"]),
                "physical_page_index": int(page["physical_page_index"]),
                "printed_page_number": page.get("printed_page_number"),
                "source_region_sha256": hashlib.sha256(source.encode("utf8")).hexdigest(),
                "original_excerpt": quote, "start": begin, "end": end,
            })
        if not any(item["region_id"] in batch_region_ids for item in proof):
            fail("invalid_evidence", "Grounded story must cite at least one region in the current batch")
        fingerprint = digest({
            "title": candidate.title.strip().casefold(),
            "material_type": candidate.material_type,
            "evidence": sorted((
                e["physical_page_index"], e["source_region_sha256"],
                e["start"], e["end"], e["original_excerpt"],
            ) for e in proof),
        })
        source_sha = str(document["source_sha256"])
        prior = db.execute(
            "SELECT story_id FROM story_ingest_identity "
            "WHERE document_id=? AND source_sha256=? AND fingerprint=?",
            (doc_id, source_sha, fingerprint),
        ).fetchone()
        if prior is not None:
            return str(prior["story_id"]), True

        seed = {
            "text": candidate.summary,
            "origin_status": "known",
            "origin_note": "Модель выделила кандидат при просмотре принятой книги",
            "speaker": None,
        }
        metadata = {
            "title": candidate.title,
            "summary": candidate.summary,
            "material_type": candidate.material_type,
        }
        story_id, snap = self._create_record(
            db, principal, seed, metadata, document.get("workspace_id"),
            [{"kind": "document", "source_id": doc_id, "source_revision": rev}],
        )
        assertion = UpsertAssertion(
            op="upsert_assertion", kind="attributed_account",
            account_kind=candidate.account_kind,
            proposition=candidate.proposition,
            attributed_to=candidate.attributed_to,
            reported_by=candidate.reported_by,
        )
        self._apply_op(db, principal, snap, assertion)
        claim = snap["assertions"][-1]
        for evidence in proof:
            self._apply_op(db, principal, snap, AttachEvidence(
                op="attach_evidence",
                assertion_id=claim["assertion_id"], assertion_revision=claim["revision"],
                source_kind="document", source_id=doc_id, source_revision=rev,
                relation="reports", source_role_for_assertion="unspecified",
                original_excerpt=evidence["original_excerpt"],
                locator=EvidenceLocator(
                    page_id=evidence["page_id"], region_id=evidence["region_id"],
                    physical_page_index=evidence["physical_page_index"],
                    printed_page_number=evidence["printed_page_number"],
                    start=evidence["start"], end=evidence["end"],
                ),
            ))
        snap["state"] = "candidate"
        self._snapshot(db, story_id, 2, snap, principal, "accepted_source_candidate")
        db.execute(
            "INSERT INTO story_ingest_identity "
            "(document_id,source_sha256,fingerprint,story_id,at) VALUES(?,?,?,?,?)",
            (doc_id, source_sha, fingerprint, story_id, now()),
        )
        return story_id, False


    def extract(self, principal, request, idempotency_key):
        data = request.model_dump(mode="json")
        def authorize(db, actor):
            if isinstance(request, ExtractStart):
                if not self._document_allowed(db, actor, request.document_id):
                    fail("capability_denied")
                return None
            job = self._row(db, "story_jobs", request.job_id)
            if not job or job["owner_id"] != actor or not self._document_allowed(db, actor, job["document_id"]):
                fail("not_found_or_not_accessible")
            return None
        def apply(db, actor):
            if isinstance(request, ExtractStart):
                doc = self._document_allowed(db, actor, request.document_id)
                if doc.get("active_revision") != request.source_revision:
                    fail("source_changed")
                total = db.execute("SELECT count(*) FROM chunk_text WHERE document_id=? AND revision=?",
                                   (request.document_id, request.source_revision)).fetchone()[0]
                job_id = str(uuid4())
                db.execute("INSERT INTO story_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (job_id, actor, request.document_id, request.source_revision, 1,
                            "awaiting_agent", "awaiting_agent", request.batch_size, 0, total,
                            None, None, None, "{}", "[]", 0, None, now()))
                return {"resource_type": "job", "resource_ids": [job_id], "job_id": job_id,
                        "job_revision": 1, "executor_state": "awaiting_agent", "checkpoint": 0,
                        "commit_state": "queued", "change_summary": "Задание записано, исполнитель не запущен"}
            job = self._row(db, "story_jobs", request.job_id)
            if job["revision"] != request.expected_job_revision:
                fail("revision_conflict", "Current job revision " + str(job["revision"]))
            if isinstance(request, ExtractCancel):
                db.execute("""UPDATE story_jobs SET state='cancelled',executor_state='cancelled',
                              revision=revision+1,lease_token=NULL,lease_deadline=NULL,updated_at=?
                              WHERE id=?""", (now(), request.job_id))
                state, batch_id = "cancelled", None
            elif isinstance(request, ExtractClaim):
                if job["state"] in {"done", "cancelled"}:
                    fail("validation_failed", "Job is no longer claimable")
                if job["lease_token"] and job["lease_deadline"] and job["lease_deadline"] > time.time():
                    fail("busy_retryable", "Batch is already leased")
                if job["cursor_position"] >= job["total_chunks"]:
                    db.execute("UPDATE story_jobs SET state='done',executor_state='done',revision=revision+1 WHERE id=?",
                               (request.job_id,))
                    state, batch_id = "done", None
                else:
                    batch_id = str(uuid4())
                    lease = str(uuid4())
                    db.execute("""UPDATE story_jobs SET state='leased',executor_state='external_agent',
                              batch_id=?,lease_token=?,lease_deadline=?,attempts=attempts+1,
                              revision=revision+1,updated_at=? WHERE id=?""",
                               (batch_id, lease, time.time() + 600, now(), request.job_id))
                    state = "leased"
            elif isinstance(request, ExtractStage):
                if (job["state"] != "leased" or job["batch_id"] != request.batch_id or
                        job["lease_token"] != request.lease_token or
                        not job["lease_deadline"] or job["lease_deadline"] < time.time()):
                    fail("revision_conflict", "Lease expired or was replaced")
                if job["source_revision"] != request.source_revision:
                    fail("source_changed")
                doc = self._document_allowed(db, actor, job["document_id"])
                if doc.get("active_revision") != request.source_revision:
                    fail("source_changed")
                results = json.loads(job["result_ids"])
                for seed in request.candidates:
                    sid, _ = self._create_record(db, principal, seed.model_dump(mode="json"), {},
                                                 None, [{"kind": "document",
                                                         "source_id": job["document_id"],
                                                         "source_revision": job["source_revision"]}])
                    results.append(sid)
                grounded_new, grounded_reused = 0, 0
                if request.grounded_candidates:
                    # Same rowid ordering/cursor as corpus_read. At least one exact
                    # cited region must belong to this leased batch of source chunks.
                    scope = db.execute(
                        "SELECT chunk_id FROM chunk_text WHERE document_id=? AND revision=? "
                        "ORDER BY rowid LIMIT ? OFFSET ?",
                        (job["document_id"], job["source_revision"],
                         job["batch_size"], job["cursor_position"]),
                    ).fetchall()
                    allowed = set()
                    for item in scope:
                        chunk = self._corpus_row(db, "rkb_chunks", item["chunk_id"])
                        if chunk and chunk.get("document_id") == job["document_id"]:
                            allowed.update(str(v) for v in chunk.get("region_ids") or [])
                            allowed.update(str(v.get("region_id")) for v in chunk.get("source_spans") or []
                                           if v.get("region_id"))
                    for candidate in request.grounded_candidates:
                        sid, reused = self._accepted_story_candidate(
                            db, principal, job, doc, candidate, allowed,
                        )
                        results.append(sid)
                        grounded_reused += int(reused)
                        grounded_new += int(not reused)
                pos = min(job["cursor_position"] + job["batch_size"], job["total_chunks"])
                state = "done" if pos >= job["total_chunks"] else "awaiting_agent"
                db.execute("""UPDATE story_jobs SET state=?,executor_state=?,cursor_position=?,
                              revision=revision+1,lease_token=NULL,lease_deadline=NULL,
                              checkpoint=?,result_ids=?,updated_at=? WHERE id=?""",
                           (state, state, pos, canonical({"processed_chunks": pos, "skipped": request.skipped}),
                            canonical(results), now(), request.job_id))
                batch_id = request.batch_id
            else:
                fail("validation_failed")
            updated = self._row(db, "story_jobs", request.job_id)
            output = {"resource_type": "job", "resource_ids": [request.job_id],
                      "job_id": request.job_id, "job_revision": updated["revision"],
                      "executor_state": updated["executor_state"], "checkpoint": updated["cursor_position"],
                      "batch_id": batch_id, "commit_state": "queued" if state == "awaiting_agent" else "saved",
                      "change_summary": "Состояние задания сохранено"}
            if isinstance(request, ExtractClaim) and state == "leased":
                output.update(lease_token=lease, lease_deadline=updated["lease_deadline"],
                              source_revision=job["source_revision"],
                              start_cursor=job["cursor_position"],
                              batch_size=job["batch_size"], lease_active=True)
            if isinstance(request, ExtractStage):
                staged_count = len(request.candidates) + len(request.grounded_candidates)
                output["resource_ids"] = json.loads(updated["result_ids"])[-staged_count:] if staged_count else []
                output["grounded_candidates_created"] = grounded_new
                output["grounded_candidates_reused"] = grounded_reused
            return output
        result = self._mutation(principal, "story_extract", idempotency_key, data, authorize, apply)
        if isinstance(request, ExtractClaim) and "lease_deadline" in result:
            result = {**result, "lease_active": result["lease_deadline"] > time.time()}
        return result

    def job_get(self, principal, job_id):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            job = self._row(db, "story_jobs", job_id)
            if not job or job["owner_id"] != actor or not self._document_allowed(db, actor, job["document_id"]):
                fail("not_found_or_not_accessible")
            return {"job_id": job_id, "revision": job["revision"], "state": job["state"],
                    "executor_state": job["executor_state"], "document_id": job["document_id"],
                    "source_revision": job["source_revision"], "checkpoint": json.loads(job["checkpoint"]),
                    "processed_chunks": job["cursor_position"], "total_chunks": job["total_chunks"],
                    "attempts": job["attempts"], "last_error": job["last_error"],
                    "lease_active": bool(job["lease_token"] and job["lease_deadline"] > time.time()),
                    "result_ids": json.loads(job["result_ids"]),
                    "next_action": "claim" if job["state"] in {"awaiting_agent", "leased"} else None}
