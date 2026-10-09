"""Evidence-grounded historical calendar over the existing Story Registry SQLite authority.

The calling model proposes dates; the backend validates source links and only
performs bounded, actor-scoped SQL lookup. No text embedding, timezone conversion,
Julian/Gregorian inference, duplicate event identity or separate calendar store.
"""
from __future__ import annotations

import base64
import json

from .sqlite_corpus import canonical
from .story_registry import StoryError, digest, fail


class StoryCalendar:
    def __init__(self, registry):
        self.registry = registry

    def _proofs(self, db, actor, row):
        proofs = []
        for evidence_id in json.loads(row["source_evidence_ids"]):
            evidence = self.registry._row(db, "story_evidence", evidence_id)
            if (not evidence or evidence["assertion_id"] != row["assertion_id"]
                    or evidence["assertion_revision"] != row["assertion_revision"]):
                fail("source_changed", "Calendar evidence no longer matches assertion")
            if evidence["source_kind"] == "document":
                if not self.registry._document_allowed(db, actor, evidence["source_id"]):
                    fail("not_found_or_not_accessible")
            elif evidence["source_kind"] == "external":
                source = db.execute("""SELECT owner_id FROM story_sources
                    WHERE id=? AND version=?""",
                    (evidence["source_id"], evidence["source_revision"])).fetchone()
                record = self.registry._row(db, "story_records", row["story_id"])
                if not source or (source["owner_id"] != actor and
                                  (not record or record["owner_id"] != actor)):
                    fail("not_found_or_not_accessible")
            else:
                fail("source_changed", "Unsupported calendar source")
            proofs.append({
                "evidence_id": evidence["id"],
                "source_kind": evidence["source_kind"],
                "source_id": evidence["source_id"],
                "source_revision": evidence["source_revision"],
                "original_excerpt": evidence["original_excerpt"][:500],
                "locator": json.loads(evidence["locator"]),
                "text_match": evidence["text_match"],
                "relation": evidence["relation"],
            })
        if not any(row["original_date_text"] in x["original_excerpt"] for x in proofs):
            fail("source_changed", "Date's original text is no longer in cited evidence")
        return proofs

    def _item(self, db, actor, row, *, canonical_story_id=None):
        return {
            "date_id": row["id"],
            "story_id": canonical_story_id or row["story_id"],
            "assertion_id": row["assertion_id"],
            "assertion_revision": row["assertion_revision"],
            "date_role": row["date_role"],
            "precision": row["precision"],
            "calendar": row["calendar"],
            "year": row["year"], "month": row["month"], "day": row["day"],
            "original_date_text": row["original_date_text"],
            "rationale": row["rationale"],
            "superseded_by": row["superseded_by"],
            "proofs": self._proofs(db, actor, row),
            "semantic_status": "source_attributed_not_historical_truth_certified",
            "recorded_by": row["actor_id"],
            "created_at": row["created_at"],
        }

    def page(self, db, actor, rec, snapshot, cursor, limit, assertion_id=None,
             *, include_history=False):
        """Revision-bound exact date readback, including after a reviewed merge."""
        take = max(1, min(int(limit), 10))
        after = ""
        if cursor:
            try:
                version, after = cursor.split(":", 1)
                if int(version) != int(rec["revision"]) or len(after) > 128:
                    raise ValueError("stale/oversized calendar cursor")
            except (ValueError, TypeError):
                fail("validation_failed", "Invalid or stale date cursor")
        current = {(a["assertion_id"], a["revision"])
                   for a in snapshot.get("assertions") or []}
        if assertion_id and not any(k[0] == assertion_id for k in current):
            fail("not_found_or_not_accessible")
        pairs = [
            {"assertion_id": ident, "revision": revision}
            for ident, revision in current
            if not assertion_id or ident == assertion_id
        ]
        if not pairs:
            return {
                "story_id": rec["id"], "story_revision": rec["revision"],
                "items": [], "has_more": False, "next_cursor": None,
                "coverage": "bounded_current_assertion_dates",
            }
        rows = db.execute("""WITH active AS (
            SELECT json_extract(value,'$.assertion_id') assertion_id,
                   json_extract(value,'$.revision') revision FROM json_each(?)
        )
        SELECT d.* FROM story_event_dates d JOIN active a
          ON a.assertion_id=d.assertion_id AND a.revision=d.assertion_revision
        WHERE d.id>? """ + ("" if include_history else "AND d.superseded_by IS NULL ") +
        """ORDER BY d.id LIMIT ?""",
            (canonical(pairs), after, take + 1)).fetchall()
        items = [self._item(db, actor, row, canonical_story_id=rec["id"])
                 for row in rows[:take]]
        more = len(rows) > take
        return {
            "story_id": rec["id"], "story_revision": rec["revision"],
            "section": "event_date_history_page" if include_history else "event_dates_page",
            "items": items, "has_more": more,
            "next_cursor": f"{rec['revision']}:{items[-1]['date_id']}" if more and items else None,
            "coverage": "bounded_current_assertion_dates",
        }

    @staticmethod
    def _encode_cursor(data):
        return base64.urlsafe_b64encode(canonical(data).encode()).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(token):
        try:
            if len(token) > 700:
                raise ValueError("oversized")
            data = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
            if data.get("v") != 1:
                raise ValueError("unknown calendar cursor")
            return data
        except (ValueError, UnicodeError, TypeError, KeyError, json.JSONDecodeError):
            fail("validation_failed", "Invalid calendar cursor")

    def query(self, principal, *, month, day, calendar="gregorian", limit=3, cursor=None):
        month, day = int(month), int(day)
        if not (1 <= month <= 12 and 1 <= day <= 31):
            fail("validation_failed", "Month must be 1..12 and day 1..31")
        if day > (29 if month == 2 else 30 if month in (4, 6, 9, 11) else 31):
            fail("validation_failed", "Invalid month/day pair")
        if calendar not in ("gregorian", "julian", "unspecified"):
            fail("validation_failed", "Explicit calendar type required")
        take = max(1, min(int(limit), 10))
        with self.registry.corpus.connect() as db:
            actor = self.registry._actor(db, principal)
            scope = digest({"actor":actor, "month":month, "day":day, "calendar":calendar})
            if cursor:
                state = self._decode_cursor(cursor)
                if state.get("scope") != scope or not isinstance(state.get("year"),int) \
                        or not isinstance(state.get("after"),str):
                    fail("validation_failed", "Calendar cursor belongs to another actor/filter")
                after_year, after_id = state["year"], state["after"]
            else:
                after_year, after_id = 0, ""
            # Calendar month/day/year are normalized for filtering; source excerpts
            # and calendar type remain authoritative and are not converted.
            visible = """(
                s.owner_id=? OR EXISTS(
                  SELECT 1 FROM story_grants g
                  WHERE g.story_id=s.id AND g.grantee_id=?
                ) OR EXISTS(
                  SELECT 1 FROM corpus_rows w
                  WHERE w.table_name='rkb_workspaces' AND w.row_key=s.workspace_id
                    AND json_extract(w.payload,'$.owner_user_id')=?
                ) OR EXISTS(
                  SELECT 1 FROM corpus_rows m
                  WHERE m.table_name='rkb_workspace_members'
                    AND json_extract(m.payload,'$.workspace_id')=s.workspace_id
                    AND json_extract(m.payload,'$.user_id')=?
                )
            )"""
            clause = """FROM story_event_dates d
                JOIN story_assertion_search a
                  ON a.assertion_id=d.assertion_id
                 AND a.revision=d.assertion_revision
                JOIN story_records s ON s.id=a.story_id
                WHERE d.date_role='event' AND d.precision='day'
                  AND d.calendar=? AND d.month=? AND d.day=?
                  AND d.superseded_by IS NULL AND s.archived=0
                  AND s.merged_into IS NULL
                  AND """ + visible + """
                  AND (d.year>? OR (d.year=? AND d.id>?))
                ORDER BY d.year,d.id LIMIT ?"""
            results = []
            scanned = 0
            scan_limit = 256
            row_exhausted = False
            while scanned < scan_limit and len(results) <= take:
                chunk = min(24, scan_limit - scanned)
                rows = db.execute("SELECT d.*,s.id canonical_story_id " + clause,
                                  (calendar, month, day, actor, actor, actor, actor,
                                   after_year, after_year, after_id, chunk)).fetchall()
                if not rows:
                    row_exhausted = True
                    break
                for row in rows:
                    scanned += 1
                    after_year, after_id = int(row["year"]), row["id"]
                    try:
                        rec, snapshot = self.registry._read_story(
                            db, actor, row["canonical_story_id"])
                        if (row["assertion_id"], row["assertion_revision"]) not in {
                                (a["assertion_id"], a["revision"])
                                for a in snapshot.get("assertions") or []}:
                            continue
                        result = self._item(db, actor, row,
                                            canonical_story_id=rec["id"])
                    except StoryError as error:
                        if error.code in {"not_found_or_not_accessible", "source_changed"}:
                            continue
                        raise
                    result["story_title"] = rec["title"]
                    results.append(result)
                    if len(results) > take:
                        break
                if len(rows) < chunk:
                    row_exhausted = True
                    break
            partial = scanned >= scan_limit and not row_exhausted
            # When a reader hits the ACL scan budget, return an explicit bounded
            # continuation rather than falsely claiming there were no matches.
            more = len(results) > take or partial
            items = results[:take]
            return {
                "items": items, "has_more": more,
                "next_cursor": self._encode_cursor({
                    "v":1,"scope":scope,"year":after_year,"after":after_id,
                }) if more else None,
                "month":month,"day":day,"calendar":calendar,
                "retrieval_mode":"exact_sql_source_authorized",
                "coverage":"partial_acl_scan_budget" if partial else "indexed_current_exact_day",
                "scope":"event_dates_only_no_publication_date",
                "calendar_conversion":"none",
            }
