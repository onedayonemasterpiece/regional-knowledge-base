"""Bounded observation dossier and exact metric search, no implicit unit arithmetic.

Amounts are source-attributed exact decimal STRINGS, not measurements blessed
as historical truth. Unit, measured dimension, period, subject label, method
and source root remain distinct. A matching display name is NOT entity identity.
"""
from __future__ import annotations

import base64
import json

from .sqlite_corpus import canonical
from .story_registry import StoryError,digest,fail


class StoryObservations:
    def __init__(self,registry):
        self.registry=registry

    def _item(self,db,actor,row,*,story_id=None):
        proofs=[]
        for evidence_id in json.loads(row["source_evidence_ids"]):
            evidence=self.registry._row(db,"story_evidence",evidence_id)
            if (not evidence or evidence["assertion_id"]!=row["assertion_id"]
                    or evidence["assertion_revision"]!=row["assertion_revision"]):
                fail("source_changed","Observation is no longer bound to the current assertion")
            if evidence["source_kind"]=="document":
                if not self.registry._document_allowed(db,actor,evidence["source_id"]):
                    fail("not_found_or_not_accessible")
            elif evidence["source_kind"]=="external":
                source=db.execute("""SELECT owner_id FROM story_sources
                    WHERE id=? AND version=?""",
                    (evidence["source_id"],evidence["source_revision"])).fetchone()
                origin=self.registry._row(db,"story_records",row["story_id"])
                if not source or (source["owner_id"]!=actor
                                  and (not origin or origin["owner_id"]!=actor)):
                    fail("not_found_or_not_accessible")
            else:
                fail("source_changed","Unsupported observation evidence kind")
            if self.registry._evidence_state(db,evidence)!="unchanged":
                fail("source_changed","Source changed since this numeric observation")
            proofs.append({
                "evidence_id":evidence["id"],
                "source_kind":evidence["source_kind"],
                "source_id":evidence["source_id"],
                "source_revision":evidence["source_revision"],
                "original_excerpt":evidence["original_excerpt"][:500],
                "locator":json.loads(evidence["locator"]),
                "text_match":evidence["text_match"],
                "relation":evidence["relation"],
            })
        if not any(row["original_value_text"] in p["original_excerpt"] for p in proofs):
            fail("source_changed","Original numeric wording no longer present")
        if row["period_text"]!="unknown" and not any(
                row["period_text"] in p["original_excerpt"] for p in proofs):
            fail("source_changed","Original period no longer present")
        return {
            "observation_id":row["id"],
            "story_id":story_id or row["story_id"],
            "assertion_id":row["assertion_id"],
            "assertion_revision":row["assertion_revision"],
            "original_value_text":row["original_value_text"],
            "value_decimal":row["value_decimal"],
            "metric_key":row["metric_key"],
            "unit_code":row["unit_code"],
            "subject_label":row["subject_label"],
            "subject_identity":"unverified_label_only",
            "period_text":row["period_text"],
            "period_start_year":row["period_start_year"],
            "period_end_year":row["period_end_year"],
            "method":row["method"],"method_note":row["method_note"],
            "precision":row["precision"],"rationale":row["rationale"],
            "superseded_by":row["superseded_by"],
            "proofs":proofs,
            "semantic_status":"sourced_numeric_account_not_verified_historical_truth",
            "recorded_by":row["actor_id"],"created_at":row["created_at"],
        }

    def page(self,db,actor,rec,snapshot,cursor,limit,assertion_id=None,*,history=False):
        take=max(1,min(int(limit),10))
        after=""
        if cursor:
            try:
                rev,sid,after=cursor.split(":",2)
                if (int(rev)!=int(rec["revision"]) or sid!=rec["id"]
                        or len(after)>128):
                    raise ValueError("stale or mismatched cursor")
            except (ValueError,TypeError):
                fail("validation_failed","Invalid or stale observation cursor")
        current={(a["assertion_id"],a["revision"])
                 for a in snapshot.get("assertions") or []}
        if assertion_id is not None and not any(
                aid==assertion_id for aid,_ in current):
            fail("not_found_or_not_accessible")
        ids=[{"id":aid,"revision":rev} for aid,rev in current
             if assertion_id is None or aid==assertion_id]
        if not ids:
            return {"story_id":rec["id"],"story_revision":rec["revision"],
                    "section":"observations_history_page" if history else "observations_page",
                    "items":[],"has_more":False,"next_cursor":None}
        rows=db.execute("""WITH active AS (
            SELECT json_extract(value,'$.id') aid,
                   json_extract(value,'$.revision') rev FROM json_each(?)
        )
        SELECT m.* FROM story_observations m
          JOIN active a ON m.assertion_id=a.aid AND m.assertion_revision=a.rev
        WHERE m.id>? """+("" if history else "AND m.superseded_by IS NULL ")+
        """ORDER BY m.id LIMIT ?""",
            (canonical(ids),after,take+1)).fetchall()
        items=[self._item(db,actor,row,story_id=rec["id"]) for row in rows[:take]]
        more=len(rows)>take
        return {
            "story_id":rec["id"],"story_revision":rec["revision"],
            "section":"observations_history_page" if history else "observations_page",
            "items":items,"has_more":more,
            "next_cursor":f"{rec['revision']}:{rec['id']}:{items[-1]['observation_id']}"
                          if more and items else None,
            "coverage":"current_assertion_source_attested_numbers_no_auto_aggregation",
        }

    @staticmethod
    def _encode(obj):
        return base64.urlsafe_b64encode(canonical(obj).encode()).decode().rstrip("=")

    @staticmethod
    def _decode(cursor):
        try:
            if len(cursor)>700:raise ValueError("Oversized cursor")
            o=json.loads(base64.urlsafe_b64decode(cursor+"="*(-len(cursor)%4)))
            if o.get("v")!=1 or not isinstance(o.get("after"),str):
                raise ValueError("Malformed cursor")
            return o
        except (ValueError,UnicodeError,TypeError,KeyError):
            fail("validation_failed","Invalid observation cursor")

    def search(self,principal,metric_key,unit_code=None,limit=3,cursor=None):
        import re
        code=r"[a-z][a-z0-9_]{1,63}"
        if (not re.fullmatch(code,metric_key)
                or (unit_code is not None and not re.fullmatch(code,unit_code))):
            fail("validation_failed","Exact metric/unit codes required")
        take=max(1,min(int(limit),10))
        with self.registry.corpus.connect() as db:
            actor=self.registry._actor(db,principal)
            scope=digest({"actor":actor,"metric":metric_key,"unit":unit_code})
            after=""
            if cursor:
                state=self._decode(cursor)
                if state.get("scope")!=scope:
                    fail("validation_failed","Observation cursor belongs to another scope")
                after=state["after"]
            # Scope BEFORE ranking, then re-check complete source/grant ACL
            # before any private text is returned. Never materialize the corpus.
            access="""(
                s.owner_id=? OR EXISTS(
                  SELECT 1 FROM story_grants g
                  WHERE g.story_id=s.id AND g.grantee_id=?
                ) OR EXISTS(
                  SELECT 1 FROM corpus_rows w WHERE w.table_name='rkb_workspaces'
                  AND w.row_key=s.workspace_id
                  AND json_extract(w.payload,'$.owner_user_id')=?
                ) OR EXISTS(
                  SELECT 1 FROM corpus_rows m
                  WHERE m.table_name='rkb_workspace_members'
                  AND json_extract(m.payload,'$.workspace_id')=s.workspace_id
                  AND json_extract(m.payload,'$.user_id')=?
                )
            )"""
            q="""SELECT n.*,s.id canonical_story_id
                FROM story_observations n
                JOIN story_assertion_search a
                  ON a.assertion_id=n.assertion_id
                 AND a.revision=n.assertion_revision
                JOIN story_records s ON s.id=a.story_id
                WHERE n.metric_key=?
                  AND (? IS NULL OR n.unit_code=?)
                  AND n.superseded_by IS NULL
                  AND s.archived=0 AND s.merged_into IS NULL
                  AND """+access+"""
                  AND n.id>? ORDER BY n.id LIMIT ?"""
            results=[]
            scanned=0;budget=256;exhausted=False
            while scanned<budget and len(results)<=take:
                batch=min(24,budget-scanned)
                rows=db.execute(q,(metric_key,unit_code,unit_code,
                                  actor,actor,actor,actor,after,batch)).fetchall()
                if not rows:
                    exhausted=True
                    break
                for row in rows:
                    scanned+=1
                    after=row["id"]
                    try:
                        record,snap=self.registry._read_story(
                            db,actor,row["canonical_story_id"])
                        if (row["assertion_id"],row["assertion_revision"]) not in {
                                (a["assertion_id"],a["revision"])
                                for a in snap.get("assertions") or []}:
                            continue
                        item=self._item(db,actor,row,story_id=record["id"])
                    except StoryError as exc:
                        if exc.code in {"not_found_or_not_accessible","source_changed"}:
                            continue
                        raise
                    item["story_title"]=record["title"]
                    results.append(item)
                    if len(results)>take:break
                if len(rows)<batch:
                    exhausted=True
                    break
            partial=scanned>=budget and not exhausted
            more=len(results)>take or partial
            items=results[:take]
            if len(results)>take:
                after=items[-1]["observation_id"] # do not skip the lookahead
            return {
                "items":items,"has_more":more,
                "next_cursor":self._encode({"v":1,"scope":scope,"after":after}) if more else None,
                "metric_key":metric_key,"unit_code":unit_code,
                "retrieval_mode":"exact_metric_source_authorized",
                "coverage":"partial_acl_scan_budget" if partial else "indexed_current_numeric_observations",
                "comparison_policy":"no_automatic_unit_or_entity_coercion",
            }

    def compare(self,principal,story_id,left_id,right_id):
        """Explain incompatibility, never compute a misleading historical trend."""
        if left_id==right_id:
            fail("validation_failed","Compare two distinct observations")
        with self.registry.corpus.connect() as db:
            actor=self.registry._actor(db,principal)
            rec,snap=self.registry._read_story(db,actor,story_id)
            rows=[self.registry._row(db,"story_observations",oid)
                  for oid in (left_id,right_id)]
            current={(a["assertion_id"],a["revision"])
                     for a in snap.get("assertions") or []}
            if any(r is None or r["superseded_by"] or
                   (r["assertion_id"],r["assertion_revision"]) not in current
                   for r in rows):
                fail("not_found_or_not_accessible")
            first,second=[self._item(db,actor,row,story_id=rec["id"]) for row in rows]
            if first["metric_key"]!=second["metric_key"]:
                verdict="different_metrics"
            elif first["unit_code"]!=second["unit_code"]:
                verdict="different_units"
            elif first["subject_label"]!=second["subject_label"]:
                verdict="unverified_subject_identity"
            elif first["method"]!=second["method"] or first["method"]=="unknown":
                verdict="incompatible_or_unknown_method"
            elif (first["period_start_year"] is None or second["period_start_year"] is None):
                verdict="period_not_confirmed"
            else:
                verdict="manual_identity_and_semantic_review_required"
            return {
                "story_id":rec["id"],"story_revision":rec["revision"],
                "left":first,"right":second,
                "comparable":False,"difference":None,"ratio":None,
                "verdict":verdict,
                "reason":"Exact same label is not canonical identity or matching methodology.",
            }
