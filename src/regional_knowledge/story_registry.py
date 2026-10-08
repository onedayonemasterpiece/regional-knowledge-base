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
    AddGap, AttachEvidence, ExtractCancel, ExtractClaim, ExtractStage, ExtractStart,
    LinkEntity, RecordAssessment, RecordInterest, ResolveGap, SetAngle,
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
    def __init__(self, corpus):
        if corpus is None:
            fail("dependency_unavailable", "SQLite authority is required")
        self.corpus = corpus
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
            CREATE TABLE IF NOT EXISTS story_schema_migrations(
              version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
            INSERT OR IGNORE INTO story_schema_migrations VALUES(1,datetime('now'));
            INSERT OR IGNORE INTO story_schema_migrations VALUES(2,datetime('now'));
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

    def get(self, principal, story_id, revision=None, view="compact"):
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

    def _invalidate(self, db, snap):
        for item in snap["variants"]:
            if item["state"] == "publish_ready":
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
            self._invalidate(db, snap)
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
            self._invalidate(db, snap)
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
            self._invalidate(db, snap)
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

    def _fingerprint(self, db, snap, variant):
        ids = [(a["assertion_id"], a["revision"], a.get("assessment_ids"), a.get("evidence_ids"))
               for a in snap["assertions"]]
        proof = []
        for aid, revision, assessments, evidence in ids:
            for eid in evidence:
                ev = self._row(db, "story_evidence", eid)
                # Check the current source identity at read/approval time. A new
                # index or rechunk on the *same original* does not revoke an
                # approval; changing the original does.
                doc_current = (self._corpus_row(db, "rkb_documents", ev["source_id"])
                               if ev["source_kind"] == "document" else None)
                current_sha = (doc_current.get("source_sha256") if doc_current else ev["source_sha256"])
                proof.append((eid, ev["source_sha256"], current_sha,
                              ev["source_revision"], ev["excerpt_sha256"]))
            for assessment in assessments:
                ar = self._row(db, "story_assessments", assessment)
                proof.append((assessment, json.loads(ar["payload"])))
        return digest({"variant": {k:v for k,v in variant.items() if k != "state"}, "assertions": ids, "proof": proof})

    def _validate(self, db, snap, variant, review=None):
        blockers, unknown = [], []
        if not variant:
            return ["variant_not_found"], []
        if not all(variant.get(k) for k in ("audience", "format", "language", "body")):
            blockers.append("missing_variant_fields")
        if not snap["assertions"]:
            blockers.append("no_reviewable_assertions")
        for a in snap["assertions"]:
            for evidence_id in a.get("evidence_ids") or []:
                evidence = self._row(db, "story_evidence", evidence_id)
                if not evidence:
                    blockers.append("missing_evidence:" + evidence_id)
                    continue
                if evidence["source_kind"] == "document":
                    latest_source = self._corpus_row(db, "rkb_documents", evidence["source_id"])
                    if (not latest_source or
                            latest_source.get("source_sha256") != evidence["source_sha256"]):
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
                    assessments = [self._row(db, "story_assessments", x) for x in a.get("assessment_ids", [])]
                    if not any(x and json.loads(x["payload"]).get("semantic_review") in
                               {"supported", "partially_supported", "contested"} for x in assessments):
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
                for a in snap["assertions"]:
                    if a["kind"] == "attributed_account":
                        kind = a.get("account_kind") or "рассказ"
                        attributed = a.get("attributed_to") or a.get("reported_by")
                        attribution.append((kind + (": " + attributed if attributed else ": происхождение не установлено"))[:160])
                    if len(attribution) == 3:
                        break
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
            take = max(1, min(int(limit), 5))
            rows = db.execute("""SELECT chunk_id,source_text,text_sha256 FROM chunk_text
              WHERE document_id=? AND revision=? ORDER BY rowid LIMIT ? OFFSET ?""",
                              (document_id, source_revision, take + 1, offset)).fetchall()
            return {"document_id": document_id, "source_revision": source_revision,
                    "source_sha256": d.get("source_sha256"),
                    "chunks": [{"chunk_id": r["chunk_id"], "text": r["source_text"][:6000],
                                "text_sha256": r["text_sha256"], "truncated": len(r["source_text"]) > 6000}
                               for r in rows[:take]],
                    "has_more": len(rows) > take, "next_cursor": str(offset + take) if len(rows) > take else None}

    def entity_list(self, principal, document_ids=None, kinds=None, query="", cursor=None, limit=20):
        with self.corpus.connect() as db:
            actor = self._actor(db, principal)
            # Fetch only scoped graph rows. Unknown scope does not become all actors' graph.
            docs = document_ids or [r["row_key"] for r in db.execute(
                "SELECT row_key FROM corpus_rows WHERE table_name='rkb_documents'")]
            docs = [d for d in docs[:100] if self._document_allowed(db, actor, d)]
            results = []
            for d in docs:
                for row in self._matching_corpus(db, "rkb_entities", document_id=d):
                    if kinds and row.get("kind") not in kinds:
                        continue
                    doc = self._document_allowed(db, actor, d)
                    if row.get("revision") != doc.get("active_revision"):
                        continue
                    if query and query.casefold() not in row.get("canonical_label", "").casefold():
                        continue
                    results.append({"entity_id": row["id"], "kind": row["kind"],
                                    "label": row["canonical_label"], "document_id": d,
                                    "revision": row["revision"]})
            results.sort(key=lambda x: (x["label"], x["entity_id"]))
            offset = max(0, int(cursor or 0))
            take = max(1, min(int(limit), 50))
            page = results[offset:offset + take]
            return {"items": page, "has_more": len(results) > offset + take,
                    "next_cursor": str(offset + take) if len(results) > offset + take else None,
                    "coverage": "accepted_mentions_not_complete_extraction"}

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
                output["resource_ids"] = json.loads(updated["result_ids"])[-len(request.candidates):] if request.candidates else []
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
