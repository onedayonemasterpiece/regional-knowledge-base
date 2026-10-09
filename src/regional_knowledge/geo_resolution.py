"""Durable, bounded RKB-owned geo intents. Canonical place decisions stay with Street Story.

No map raster, raw book passage, bearer token, or atlas geometry enters this
queue. cartography.resolve/projection/decision v1 are compatibility envelopes,
NOT assertions that the external producer has deployed an accepted layer.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .entity_graph import normalize_alias
from .poi_reference import StreetStoryPoiResolver, canonical_poi_key

POLICY = "rkb-geo-resolution-v1"
CONTRACT = "cartography.resolve.v1"
KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}\Z")

GEO_SCHEMA = """
CREATE TABLE IF NOT EXISTS rkb_geo_intents(
 request_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL,
 document_id TEXT NOT NULL, source_revision INTEGER NOT NULL,
 mention_id TEXT NOT NULL, entity_id TEXT NOT NULL,
 source_sha256 TEXT NOT NULL, mention_sha256 TEXT NOT NULL,
 original_spelling TEXT NOT NULL, normalized_spelling TEXT NOT NULL,
 names_json TEXT NOT NULL, locator_json TEXT NOT NULL,
 period_json TEXT NOT NULL, scope_json TEXT NOT NULL,
 policy_version TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
 created_at REAL NOT NULL, updated_at REAL NOT NULL,
 UNIQUE(mention_id,policy_version)
);
CREATE INDEX IF NOT EXISTS geo_intent_actor ON rkb_geo_intents(actor_id,created_at);
CREATE INDEX IF NOT EXISTS geo_intent_entity ON rkb_geo_intents(entity_id,source_revision);
CREATE TABLE IF NOT EXISTS rkb_geo_aliases(
 request_id TEXT NOT NULL REFERENCES rkb_geo_intents(request_id),
 normalized_name TEXT NOT NULL, PRIMARY KEY(request_id,normalized_name)
);
CREATE INDEX IF NOT EXISTS geo_alias_exact ON rkb_geo_aliases(normalized_name,request_id);
CREATE TABLE IF NOT EXISTS rkb_geo_attempts(
 attempt_id TEXT PRIMARY KEY,
 request_id TEXT NOT NULL REFERENCES rkb_geo_intents(request_id),
 dependency_kind TEXT NOT NULL, dependency_ref TEXT NOT NULL,
 dependency_revision TEXT NOT NULL, input_hash TEXT NOT NULL,
 state TEXT NOT NULL, reason TEXT,
 priority INTEGER NOT NULL DEFAULT 0,
 lease_token TEXT, lease_fence INTEGER NOT NULL DEFAULT 0,
 lease_until REAL, attempts INTEGER NOT NULL DEFAULT 0,
 available_at REAL NOT NULL,
 proposal_json TEXT, proposal_hash TEXT,
 canonical_poi_ref TEXT, owner_identity_state TEXT,
 created_at REAL NOT NULL, updated_at REAL NOT NULL,
 UNIQUE(request_id,input_hash)
);
CREATE INDEX IF NOT EXISTS geo_attempt_claim
 ON rkb_geo_attempts(state,available_at,priority,created_at);
CREATE INDEX IF NOT EXISTS geo_attempt_by_request
 ON rkb_geo_attempts(request_id,created_at,attempt_id);
CREATE TABLE IF NOT EXISTS rkb_geo_receipts(
 command_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL, command TEXT NOT NULL,
 payload_hash TEXT NOT NULL, result_json TEXT NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS rkb_geo_watermarks(
 name TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at REAL NOT NULL
);
"""


class GeoProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["candidate","unresolved","ambiguous","outside_coverage","rejected"]
    reason: str = Field(min_length=10,max_length=500)
    canonical_poi_ref: str | None = None
    relation: Literal["same_site","within","nearby","occurred_at"] | None = None
    layer_ref: str | None = None
    geometry_ref: str | None = None

    @model_validator(mode="after")
    def check(self):
        if self.status=="candidate":
            if not self.canonical_poi_ref:
                raise ValueError("candidate canonical identity required")
            canonical_poi_key(self.canonical_poi_ref)
        elif self.canonical_poi_ref or self.relation:
            raise ValueError("unresolved proposal cannot assert POI identity or relation")
        return self


class GeoError(ValueError):
    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def ensure_geo_schema(db):
    db.executescript(GEO_SCHEMA)


def enqueue_mention(db, actor, mention, node, *, policy=POLICY):
    """The LocalContext graph-mention write calls this in the SAME SQLite tx."""
    if node.get("kind") != "poi_ref" or not mention.get("exact_source_spelling"):
        return None
    doc_id = str(mention["document_id"])
    source = db.execute(
        "SELECT payload FROM corpus_rows WHERE table_name='rkb_documents' AND row_key=?",
        (doc_id,)
    ).fetchone()
    if not source:
        raise GeoError("source_changed")
    doc = json.loads(source[0])
    if str(doc.get("owner_user_id")) != str(actor):
        raise GeoError("owner_scope_mismatch")
    evidence = mention.get("evidence") or {}
    if not evidence.get("chunk_id") or not evidence.get("region_id"):
        raise GeoError("invalid_evidence")
    metadata = node.get("metadata") or {}
    locator = metadata.get("poi_locator") or {}
    names = list(dict.fromkeys(str(n).strip() for n in [
        mention["exact_source_spelling"], node["canonical_label"],
        *(locator.get("names") or []),
    ] if isinstance(n, str) and n.strip()))[:20]
    if not names:
        raise GeoError("empty_place_names")
    refs = {str(k).lower():str(v) for k,v in (locator.get("external_ids") or {}).items()}
    # Coordinates in a legacy locator have neither surveyed accuracy nor
    # temporal validity. Preserve that *limitation* with any location hint.
    locator_safe = {
        "names": names, "external_ids": refs,
        "latitude": locator.get("latitude"),
        "longitude": locator.get("longitude"),
        "coordinate_method": "source_locator_unverified",
        "historical_geometry": "not_verified",
    }
    period = {"time_scope": None, "precision": "unknown",
              "calendar": "unspecified", "valid_year_from": None,
              "valid_year_to": None}
    source_hash = str(doc.get("source_sha256") or "")
    mention_hash = _hash({
        "doc":doc_id,"revision":mention["revision"],
        "mention_id":str(mention["id"]),
        "evidence":evidence,"name":mention["exact_source_spelling"],
    })
    request_id = "geor_" + _hash({
        "mention":str(mention["id"]),"revision":mention["revision"],
        "hash":mention_hash,"policy":policy,
    })[:32]
    now = time.time()
    scope = {
        "owner_id":str(actor),"visibility":"private",
        "source_visibility":doc.get("source_visibility","private"),
        "content_visibility":doc.get("content_visibility","private"),
        "distribution_authorized":False,
    }
    db.execute("""INSERT OR IGNORE INTO rkb_geo_intents(
        request_id,actor_id,document_id,source_revision,mention_id,
        entity_id,source_sha256,mention_sha256,original_spelling,
        normalized_spelling,names_json,locator_json,period_json,scope_json,
        policy_version,priority,created_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        request_id,str(actor),doc_id,mention["revision"],str(mention["id"]),
        str(mention["entity_id"]),source_hash,mention_hash,
        mention["exact_source_spelling"],normalize_alias(mention["exact_source_spelling"]),
        _json(names),_json(locator_safe),_json(period),_json(scope),policy,
        0,now,now,
    ))
    for name in names:
        normalized = normalize_alias(name)
        if normalized:
            db.execute("INSERT OR IGNORE INTO rkb_geo_aliases VALUES(?,?)",
                       (request_id, normalized))
    input_hash = _hash({"request":request_id, "dependency":"source",
                        "revision":mention["revision"],"hash":mention_hash})
    attempt_id = "geoa_" + input_hash[:32]
    db.execute("""INSERT OR IGNORE INTO rkb_geo_attempts(
        attempt_id,request_id,dependency_kind,dependency_ref,
        dependency_revision,input_hash,state,available_at,
        created_at,updated_at
    ) VALUES(?,?,?,?,?,?,'pending',?,?,?)""",
        (attempt_id,request_id,"source",doc_id,str(mention["revision"]),
         input_hash,now,now,now))
    return request_id


class GeoQueue:
    def __init__(self, corpus, resolver=None):
        self.corpus=corpus
        self.resolver=resolver or StreetStoryPoiResolver()

    @staticmethod
    def _actor(principal):
        try:
            return str(UUID(str(principal.subject)))
        except (ValueError, TypeError, AttributeError):
            raise GeoError("unauthorized") from None

    def _ctx(self, db, actor):
        from .sqlite_data import LocalContext
        ctx=LocalContext(self.corpus,db,actor)
        user=ctx.one("rkb_users",actor)
        if not user or user.get("status")!="active":
            raise GeoError("not_found_or_not_accessible")
        return ctx

    def _live(self, db, ctx, intent):
        """Recheck source, exact original evidence and owner on EVERY mutation."""
        if intent["actor_id"] != ctx.actor:
            raise GeoError("not_found_or_not_accessible")
        if not ctx.owned(intent["document_id"]) or not ctx.readable(intent["document_id"]):
            raise GeoError("not_found_or_not_accessible")
        doc=ctx.one("rkb_documents",intent["document_id"])
        if (not doc or doc.get("active_revision") != intent["source_revision"]
                or str(doc.get("source_sha256") or "") != intent["source_sha256"]):
            raise GeoError("stale_source")
        mention=ctx.one("rkb_entity_mentions",intent["mention_id"])
        entity=ctx.one("rkb_entities",intent["entity_id"])
        if not mention or not entity:
            raise GeoError("stale_source")
        if (mention.get("entity_id") != entity["id"]
                or mention.get("revision") != intent["source_revision"]
                or entity.get("kind")!="poi_ref"
                or entity.get("owner_user_id")!=ctx.actor
                or entity.get("document_id")!=intent["document_id"]
                or entity.get("revision")!=intent["source_revision"]):
            raise GeoError("stale_source")
        proof = mention.get("evidence") or {}
        actual = _hash({
            "doc":intent["document_id"],"revision":mention["revision"],
            "mention_id":mention["id"],"evidence":proof,
            "name":mention["exact_source_spelling"],
        })
        if actual != intent["mention_sha256"] or not ctx.check_evidence(
                intent["document_id"],intent["source_revision"],proof):
            raise GeoError("stale_source")
        return entity, mention

    @staticmethod
    def _receipt(db, actor, command, command_id, payload):
        if not KEY.fullmatch(command_id):
            raise GeoError("invalid_command_id")
        token=_hash({"command":command,"actor":actor,"payload":payload})
        row=db.execute("SELECT * FROM rkb_geo_receipts WHERE command_id=?",
                       (command_id,)).fetchone()
        if row:
            if row["actor_id"]!=actor or row["command"]!=command or row["payload_hash"]!=token:
                raise GeoError("idempotency_conflict")
            return json.loads(row["result_json"]),token
        return None,token

    @staticmethod
    def _save_receipt(db, actor, command, command_id, token, result):
        db.execute("INSERT INTO rkb_geo_receipts VALUES(?,?,?,?,?,?)",
                   (command_id,actor,command,token,_json(result),time.time()))
        return result

    def enqueue_existing(self, principal, entity_id, *, policy=POLICY):
        actor=self._actor(principal)
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            entity=ctx.one("rkb_entities",entity_id)
            if not entity or entity.get("kind")!="poi_ref" or entity.get("owner_user_id")!=actor:
                raise GeoError("not_found_or_not_accessible")
            if not ctx.owned(entity["document_id"]):
                raise GeoError("not_found_or_not_accessible")
            mentions=ctx.rows("rkb_entity_mentions",{"entity_id":entity_id})
            requests=[]
            for mention in mentions[:20]:
                if mention.get("document_id")!=entity["document_id"]:
                    continue
                if not ctx.check_evidence(mention["document_id"],mention["revision"],
                                          mention.get("evidence") or {}):
                    continue
                request=enqueue_mention(db,actor,mention,entity,policy=policy)
                if request:requests.append(request)
            return {"request_ids":list(dict.fromkeys(requests))[:20],
                    "count":len(requests),"enqueued_from":"accepted_graph_mentions"}

    def claim(self, principal, limit=3, lease_seconds=90):
        actor=self._actor(principal)
        limit=max(1,min(int(limit),5));lease_seconds=max(15,min(int(lease_seconds),180))
        now=time.time()
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            rows=db.execute("""SELECT a.*,i.* FROM rkb_geo_attempts a
                JOIN rkb_geo_intents i ON i.request_id=a.request_id
                WHERE i.actor_id=? AND a.attempts<5
                AND a.available_at<=?
                AND (a.state='pending' OR
                   (a.state IN ('leased','staged') AND a.lease_until<?))
                ORDER BY a.priority DESC,a.created_at,a.attempt_id LIMIT 25""",
                (actor,now,now)).fetchall()
            results=[];documents=set()
            for row in rows:
                if len(results)>=limit:break
                # Fairness: one mention from each document per bounded packet.
                if row["document_id"] in documents:continue
                documents.add(row["document_id"])
                try:self._live(db,ctx,row)
                except GeoError as exc:
                    db.execute("UPDATE rkb_geo_attempts SET state=?,reason=?,updated_at=? WHERE attempt_id=?",
                               ("stale_source" if exc.code=="stale_source" else "blocked_access",
                                exc.code,now,row["attempt_id"]))
                    continue
                token=secrets.token_hex(16)
                fence=int(row["lease_fence"])+1
                db.execute("""UPDATE rkb_geo_attempts SET state='leased',
                    lease_token=?,lease_fence=?,lease_until=?,attempts=attempts+1,
                    updated_at=? WHERE attempt_id=?""",
                    (token,fence,now+lease_seconds,now,row["attempt_id"]))
                results.append({
                    "request_id":row["request_id"],"attempt_id":row["attempt_id"],
                    "lease_token":token,"lease_fence":fence,
                    "expires_in_seconds":lease_seconds,
                    "source_ref":{
                        "document_id":row["document_id"],
                        "revision":row["source_revision"],
                        "mention_id":row["mention_id"],
                        "entity_id":row["entity_id"],
                        "mention_sha256":row["mention_sha256"],
                    },
                    "original_toponym":row["original_spelling"],
                    "variants":json.loads(row["names_json"]),
                    "locator":json.loads(row["locator_json"]),
                    "time":json.loads(row["period_json"]),
                    "scope":{"delivery":"owner_only","source_visibility":"private"},
                    "dependency":{
                        "kind":row["dependency_kind"],
                        "ref":row["dependency_ref"],
                        "revision":row["dependency_revision"],
                    },
                    "contract_version":CONTRACT,
                    "policy_version":row["policy_version"],
                })
            return {"items":results,"count":len(results),"next_action":"stage_or_retry",
                    "max_packet":5,"authoritative_owner":"Street Story",
                    "no_public_source_export":True}

    def stage(self,principal,attempt_id,lease_token,lease_fence,
              proposal,command_id):
        actor=self._actor(principal)
        proposed=(proposal.model_dump(exclude_none=True)
                  if isinstance(proposal,GeoProposal) else dict(proposal))
        if set(proposed)-set(GeoProposal.model_fields):
            raise GeoError("invalid_proposal_fields")
        state=proposed.get("status")
        if state not in ("candidate","unresolved","ambiguous","outside_coverage","rejected"):
            raise GeoError("invalid_proposal_status")
        if state=="candidate":
            ref=proposed.get("canonical_poi_ref")
            if not ref:raise GeoError("candidate_requires_canonical_ref")
            canonical_poi_key(ref)
            if proposed.get("layer_ref") or proposed.get("geometry_ref"):
                raise GeoError("cartography_layer_not_accepted")
        elif proposed.get("canonical_poi_ref"):
            raise GeoError("noncandidate_has_identity")
        if not isinstance(proposed.get("reason"),str) or not 10<=len(proposed["reason"])<=500:
            raise GeoError("rationale_required")
        if proposed.get("relation") not in (None,"same_site","within","nearby","occurred_at"):
            raise GeoError("invalid_spatial_relation")
        if state!="candidate" and proposed.get("relation"):
            raise GeoError("relation_requires_candidate")
        if len(_json(proposed))>3000:
            raise GeoError("proposal_too_large")
        payload={"attempt":attempt_id,"token":lease_token,"fence":lease_fence,
                 "proposal":proposed}
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay,token=self._receipt(db,actor,"stage",command_id,payload)
            if replay is not None:return replay
            ctx=self._ctx(db,actor)
            row=db.execute("""SELECT a.*,i.* FROM rkb_geo_attempts a
                JOIN rkb_geo_intents i ON i.request_id=a.request_id
                WHERE a.attempt_id=?""",(attempt_id,)).fetchone()
            if not row or row["actor_id"]!=actor:
                raise GeoError("not_found_or_not_accessible")
            if (row["state"]!="leased" or row["lease_token"]!=lease_token
                    or row["lease_fence"]!=lease_fence
                    or (row["lease_until"] or 0)<=time.time()):
                raise GeoError("stale_lease")
            self._live(db,ctx,row)
            db.execute("""UPDATE rkb_geo_attempts SET state='staged',
                proposal_json=?,proposal_hash=?,updated_at=?
                WHERE attempt_id=?""",
                (_json(proposed),_hash(proposed),time.time(),attempt_id))
            return self._save_receipt(db,actor,"stage",command_id,token,{
                "state":"staged","request_id":row["request_id"],
                "attempt_id":attempt_id,"proposal_hash":_hash(proposed),
                "owner_decision":"not_applied",
            })

    def _owner_match(self, intent, proposed):
        """External catalog read only. Nothing is auto-verified by this lookup."""
        from .contracts import PoiLocatorInput
        locator=json.loads(intent["locator_json"])
        input_locator=PoiLocatorInput.model_validate({
            "names":locator["names"],"external_ids":locator["external_ids"],
            "latitude":locator.get("latitude"),"longitude":locator.get("longitude")
        })
        match=self.resolver.resolve(input_locator)
        ref=proposed.get("canonical_poi_ref")
        if match.get("external_ref")!=ref:
            raise GeoError("owner_identity_unresolved_or_ambiguous")
        identity=self.resolver.version(ref)
        if not identity:
            raise GeoError("owner_identity_unavailable")
        return identity

    def apply(self,principal,attempt_id,lease_token,lease_fence,command_id):
        actor=self._actor(principal)
        payload={"attempt":attempt_id,"token":lease_token,"fence":lease_fence}
        # External read ONLY, before the short SQLite write lock.
        with self.corpus.connect() as db:
            replay,_=self._receipt(db,actor,"apply",command_id,payload)
            if replay is not None:
                return replay  # Lost network reply: never re-run owner lookup.
            row=db.execute("""SELECT a.*,i.* FROM rkb_geo_attempts a
                 JOIN rkb_geo_intents i ON i.request_id=a.request_id
                 WHERE a.attempt_id=?""",(attempt_id,)).fetchone()
            if row is None or row["actor_id"]!=actor:
                raise GeoError("not_found_or_not_accessible")
            proposed=json.loads(row["proposal_json"]) if row["proposal_json"] else None
        if proposed is None:
            raise GeoError("proposal_missing")
        identity=None
        if proposed["status"]=="candidate":
            identity=self._owner_match(row,proposed)
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay,token=self._receipt(db,actor,"apply",command_id,payload)
            if replay is not None:return replay
            ctx=self._ctx(db,actor)
            row=db.execute("""SELECT a.*,i.* FROM rkb_geo_attempts a
                 JOIN rkb_geo_intents i ON i.request_id=a.request_id
                 WHERE a.attempt_id=?""",(attempt_id,)).fetchone()
            if not row or row["actor_id"]!=actor:raise GeoError("not_found_or_not_accessible")
            if (row["state"]!="staged" or row["lease_token"]!=lease_token
                or row["lease_fence"]!=lease_fence
                or (row["lease_until"] or 0)<=time.time()):
                raise GeoError("stale_lease")
            if _hash(proposed)!=row["proposal_hash"]:
                raise GeoError("proposal_changed")
            try:entity,_=self._live(db,ctx,row)
            except GeoError as exc:
                # Do not silently turn stale/revoked source into successful POI.
                outcome="stale_source" if exc.code=="stale_source" else "blocked_access"
                db.execute("""UPDATE rkb_geo_attempts SET state=?,reason=?,lease_token=NULL,
                            lease_until=NULL,updated_at=? WHERE attempt_id=?""",
                           (outcome,exc.code,time.time(),attempt_id))
                result={"state":outcome,"request_id":row["request_id"],
                        "attempt_id":attempt_id,"error_code":exc.code}
                return self._save_receipt(db,actor,"apply",command_id,token,result)
            canonical=proposed.get("canonical_poi_ref")
            if proposed["status"]=="candidate":
                if canonical != identity.get("external_ref"):
                    raise GeoError("owner_revision_changed")
                old=entity.get("external_ref")
                if old and old!=canonical:
                    raise GeoError("canonical_identity_revision_conflict")
                metadata=dict(entity.get("metadata") or {})
                metadata["geo_queue"]={
                    "contract_version":CONTRACT,"attempt_id":attempt_id,
                    "resolution":"owner_alias_candidate",
                    "identity_state":identity.get("identity_state","candidate"),
                    "historical_geometry":"not_verified",
                }
                updated={**entity,"external_ref":canonical,
                         "state":"candidate" if entity["state"]=="unresolved" else entity["state"],
                         "metadata":metadata}
                # Same SQLite transaction as the durable resolution receipt.
                # No independent authority and no editorial story rewriting.
                self.corpus.put("rkb_entities",[updated],connection=db)
                result_state="linked_candidate"
                owner_state=identity.get("identity_state","candidate")
            else:
                result_state={"unresolved":"unresolved","ambiguous":"ambiguous",
                              "outside_coverage":"outside_coverage",
                              "rejected":"rejected"}[proposed["status"]]
                owner_state=None
            now=time.time()
            db.execute("""UPDATE rkb_geo_attempts SET
                state=?,reason=?,canonical_poi_ref=?,owner_identity_state=?,
                lease_token=NULL,lease_until=NULL,updated_at=?
                WHERE attempt_id=?""",
                (result_state,proposed["reason"],canonical,owner_state,now,attempt_id))
            result={
                "state":result_state,"request_id":row["request_id"],
                "attempt_id":attempt_id,"owner_identity_state":owner_state,
                "canonical_poi_ref":canonical,
                "historical_geometry":"not_verified",
                "story_revision_unchanged":True,
                "owner_write_performed":False,
                "receipt_id":command_id,
            }
            return self._save_receipt(db,actor,"apply",command_id,token,result)

    def status(self,principal,request_id=None,entity_id=None,limit=5):
        actor=self._actor(principal)
        limit=max(1,min(int(limit),10))
        with self.corpus.connect() as db:
            ctx=self._ctx(db,actor)
            sql="SELECT * FROM rkb_geo_intents WHERE actor_id=?";args=[actor]
            if request_id:sql+=" AND request_id=?";args.append(request_id)
            if entity_id:sql+=" AND entity_id=?";args.append(entity_id)
            sql+=" ORDER BY created_at DESC,request_id LIMIT ?";args.append(limit)
            items=[]
            for row in db.execute(sql,args):
                try:self._live(db,ctx,row)
                except GeoError:continue
                latest=db.execute("""SELECT attempt_id,state,reason,dependency_kind,
                         dependency_ref,dependency_revision,canonical_poi_ref,
                         owner_identity_state,attempts,lease_fence,created_at
                         FROM rkb_geo_attempts WHERE request_id=?
                         ORDER BY created_at DESC,attempt_id DESC LIMIT 8""",
                         (row["request_id"],)).fetchall()
                items.append({
                    "request_id":row["request_id"],
                    "entity_id":row["entity_id"],"mention_id":row["mention_id"],
                    "source_revision":row["source_revision"],
                    "original_toponym":row["original_spelling"],
                    "time":json.loads(row["period_json"]),
                    "attempts":[dict(x) for x in latest],
                    "source_access":"authorized_current",
                })
            return {"items":items,"count":len(items),
                    "accepted_layer_revision":None,
                    "cartography_capability":"awaiting_producer",
                    "paging":"bounded","scope":"actor_only"}

    def recheck(self,principal,name,dependency_kind,dependency_ref,
                dependency_revision,limit=20):
        """Incremental exact-name dependency event; never certify a map layer."""
        actor=self._actor(principal)
        if dependency_kind not in ("owner_poi","cartography_layer"):
            raise GeoError("unsupported_dependency")
        if not isinstance(dependency_revision,str) or not 1<=len(dependency_revision)<=128:
            raise GeoError("invalid_dependency_revision")
        if not isinstance(dependency_ref,str) or not 1<=len(dependency_ref)<=160:
            raise GeoError("invalid_dependency_ref")
        normalized=normalize_alias(name)
        if len(normalized)<3 or len(normalized)>200:
            raise GeoError("invalid_place_name")
        take=max(1,min(int(limit),30))
        now=time.time();created=[]
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            intents=db.execute("""SELECT i.* FROM rkb_geo_intents i
                JOIN rkb_geo_aliases a ON a.request_id=i.request_id
                WHERE i.actor_id=? AND a.normalized_name=?
                ORDER BY i.created_at,i.request_id LIMIT ?""",
                (actor,normalized,take)).fetchall()
            for row in intents:
                try:entity,_=self._live(db,ctx,row)
                except GeoError:continue
                if entity.get("external_ref") and dependency_kind=="owner_poi":
                    continue
                stamp=_hash({"request_id":row["request_id"],
                            "dependency_kind":dependency_kind,
                            "dependency_ref":dependency_ref,
                            "dependency_revision":dependency_revision,
                            "mention_sha":row["mention_sha256"],
                            "policy":row["policy_version"]})
                attempt_id="geoa_"+stamp[:32]
                count=db.execute("""INSERT OR IGNORE INTO rkb_geo_attempts(
                   attempt_id,request_id,dependency_kind,dependency_ref,
                   dependency_revision,input_hash,state,available_at,
                   created_at,updated_at)
                   VALUES(?,?,?,?,?,?,'pending',?,?,?)""",
                   (attempt_id,row["request_id"],dependency_kind,dependency_ref,
                    dependency_revision,stamp,now,now,now)).rowcount
                if count:created.append(attempt_id)
        return {"new_attempts":len(created),"attempt_ids":created,
                "scope":"exact_toponym_only","accepted_layer_revision":None,
                "cartography_decision":"not_inferred"}

    def lookup(self,principal,name,year=None,limit=3):
        actor=self._actor(principal)
        term=normalize_alias(name)
        if not term:return {"items":[],"coverage":"no_name"}
        take=max(1,min(int(limit),10))
        with self.corpus.connect() as db:
            ctx=self._ctx(db,actor)
            rows=db.execute("""SELECT DISTINCT i.* FROM rkb_geo_intents i
                JOIN rkb_geo_aliases a ON a.request_id=i.request_id
                WHERE i.actor_id=? AND a.normalized_name=?
                ORDER BY i.created_at,i.request_id LIMIT ?""",
                (actor,term,take+1)).fetchall()
            items=[]
            for row in rows[:take]:
                try:node,_=self._live(db,ctx,row)
                except GeoError:continue
                period=json.loads(row["period_json"])
                known_period=period.get("precision") not in (None,"unknown")
                if year is not None and known_period:
                    low,high=period.get("valid_year_from"),period.get("valid_year_to")
                    if low is not None and int(year)<low:continue
                    if high is not None and int(year)>high:continue
                ref=node.get("external_ref")
                items.append({
                    "request_id":row["request_id"],"entity_id":row["entity_id"],
                    "canonical_poi_ref":ref,
                    "identity_state":node["state"] if ref else "unresolved",
                    "original_toponym":row["original_spelling"],
                    "temporal_match":"unknown" if not known_period else "within_declared_scope",
                    "source_evidence_ref":"knowledge://chunks/"+
                          str(ctx.one("rkb_entity_mentions",row["mention_id"])["evidence"]["chunk_id"]),
                    "historical_geometry":"not_verified",
                })
        return {"items":items,"count":len(items),
                "coverage":"source_mentions_not_historical_map_coverage",
                "spatial_relation":"not_inferred",
                "time_query_year":year,"has_more":len(rows)>take}

    def worker_tick(self,limit=2):
        """Low-cost owner-local consumer, isolated from remote map/provider failures."""
        from .contracts import Principal
        with self.corpus.connect() as db:
            actors=[r[0] for r in db.execute("""SELECT DISTINCT i.actor_id
              FROM rkb_geo_intents i JOIN rkb_geo_attempts a
                ON a.request_id=i.request_id WHERE a.state='pending'
              AND a.available_at<=? ORDER BY i.actor_id LIMIT ?""",
              (time.time(),max(1,min(limit,5))))]
        results=[]
        for actor in actors:
            principal=Principal(subject=actor,client_id="geo-resolution-worker",
                                issuer="trusted-internal-rkb",
                                access_token="internal-rkb-non-delegated")
            try:
                packet=self.claim(principal,limit=1,lease_seconds=90)
                for job in packet["items"]:
                    from .contracts import PoiLocatorInput
                    location=job["locator"]
                    p=PoiLocatorInput.model_validate({
                        "names":location["names"],
                        "external_ids":location["external_ids"],
                        "latitude":location.get("latitude"),
                        "longitude":location.get("longitude"),
                    })
                    try:
                        match=self.resolver.resolve(p)
                    except (OSError,sqlite3.Error):
                        # Transient owner unavailable: retry after lease expiry;
                        # no false terminal "no match" caused by connectivity.
                        continue
                    ref=match.get("external_ref")
                    if ref:
                        proposal={"status":"candidate","canonical_poi_ref":ref,
                                  "reason":"Exact name or external-ID match in Street Story owner catalog; candidate identity only."}
                    else:
                        status="ambiguous" if match.get("state")=="ambiguous" else "unresolved"
                        proposal={"status":status,
                                  "reason":"Owner catalog has no unique matching identity; map coverage is not established."}
                    prefix=job["attempt_id"]+":"+str(job["lease_fence"])
                    self.stage(principal,job["attempt_id"],job["lease_token"],
                               job["lease_fence"],proposal,
                               "geo-worker-stage:"+_hash(prefix)[:32])
                    result=self.apply(principal,job["attempt_id"],job["lease_token"],
                                      job["lease_fence"],
                                      "geo-worker-apply:"+_hash(prefix)[:32])
                    results.append(result["state"])
            except (GeoError, ValueError, LookupError, sqlite3.Error):
                # Failed source/auth must not block normal indexing or reading.
                continue
        return {"processed":len(results),"states":results}
