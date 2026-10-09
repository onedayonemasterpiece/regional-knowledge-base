"""Durable, bounded RKB-owned geo intents. Canonical place decisions stay with Street Story.

No map raster, raw book passage, bearer token, or atlas geometry enters this
queue. rkb.geo_claim.v1 is an authenticated local MCP packet, not a portable
cartography.resolve/projection/decision transport or an assertion that an
external producer has deployed an accepted layer.
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
# Owner-only MCP lease packet, NOT a network cartography.resolve.v1 command.
# The cross-service contract additionally requires issuer/audience/resource
# binding and a checked canonical payload hash; no public relay exists yet.
CONTRACT = "rkb.geo_claim.v1"
CARTOGRAPHY_RESOLVE_CONTRACT = "cartography.resolve.v1"
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
-- Accepted source spellings are rare relative to vector discovery candidates.
-- This is a partial expression index on the existing graph mentions in the
-- SAME SQLite authority, not a new place, citation or identity table.
CREATE INDEX IF NOT EXISTS geo_source_mention_entity ON
 corpus_rows(json_extract(payload,'$.entity_id'),row_key)
 WHERE table_name='rkb_entity_mentions'
   AND coalesce(json_extract(payload,'$.exact_source_spelling'),'')<>'';

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
        # The original graph can cite a document via a scoped reader grant.
        # An unrelated owner's private proof is not silently exported to a
        # resolution worker; skip this optional side effect, not the book.
        return None
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
    # External owner aliases are exact namespace/value pairs; a name match
    # alone must never override a contradictory Wikidata/OSM ID.
    for namespace,value in refs.items():
        normalized=namespace+":"+normalize_alias(value)
        db.execute("INSERT OR IGNORE INTO rkb_geo_aliases VALUES(?,?)",
                   (request_id,normalized))
    input_hash = _hash({"request":request_id, "dependency":"source",
                        "revision":mention["revision"],"hash":mention_hash})
    attempt_id = "geoa_" + input_hash[:32]
    # Graph staging can precede book activation by hours/days while vector
    # publication is pending. A still-inactive revision is NOT stale. Keep its
    # intent durable but unclaimable until the source's acceptance transaction
    # wakes it (never through polling an unaccepted chapter).
    active=int(doc.get("active_revision") or 0)
    staged_rev=int(mention["revision"])
    initial=("awaiting_activation" if staged_rev>active else
             "pending" if staged_rev==active else "stale_source")
    db.execute("""INSERT OR IGNORE INTO rkb_geo_attempts(
        attempt_id,request_id,dependency_kind,dependency_ref,
        dependency_revision,input_hash,state,available_at,
        created_at,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (attempt_id,request_id,"source",doc_id,str(mention["revision"]),
         input_hash,initial,now,now,now))
    return request_id


def geo_revision_activated(db, document_id, revision):
    """Wake source-backed geo work in SAME SQLite tx as book activation.

    The accepted source revision, not the asynchronous map/POI owner, controls
    when a geographic mention first becomes eligible. Superseded source
    attempts lose their leases, while earlier receipts remain immutable.
    Never calls Street Story, Cartography, vectors or a model.
    """
    now=time.time()
    activated=db.execute("""UPDATE rkb_geo_attempts
        SET state='pending',reason=NULL,available_at=?,updated_at=?
        WHERE state='awaiting_activation'
        AND request_id IN (
          SELECT request_id FROM rkb_geo_intents
          WHERE document_id=? AND source_revision=?)""",
        (now,now,str(document_id),int(revision))).rowcount
    stale=db.execute("""UPDATE rkb_geo_attempts SET
        state='stale_source',reason='superseded_source_revision',
        lease_token=NULL,lease_until=NULL,proposal_json=NULL,
        proposal_hash=NULL,updated_at=?
        WHERE state IN ('awaiting_activation','pending','retry_wait',
                        'leased','staged')
        AND request_id IN (
          SELECT request_id FROM rkb_geo_intents
          WHERE document_id=? AND source_revision<?)""",
        (now,str(document_id),int(revision))).rowcount
    return {"newly_eligible":activated,"superseded_attempts":stale}


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
        if not ctx.readable(intent["document_id"]):
            raise GeoError("not_found_or_not_accessible")
        doc=ctx.one("rkb_documents",intent["document_id"])
        if not doc or str(doc.get("source_sha256") or "") != intent["source_sha256"]:
            raise GeoError("stale_source")
        active=int(doc.get("active_revision") or 0)
        requested=int(intent["source_revision"])
        if active<requested:
            raise GeoError("source_not_activated")
        if active>requested:
            raise GeoError("stale_source")
        mention=ctx.one("rkb_entity_mentions",intent["mention_id"])
        entity=ctx.one("rkb_entities",intent["entity_id"])
        if not mention or not entity:
            raise GeoError("stale_source")
        if (mention.get("entity_id") != entity["id"]
                or mention.get("document_id") != intent["document_id"]
                or mention.get("revision") != intent["source_revision"]
                or entity.get("kind")!="poi_ref"
                or entity.get("owner_user_id")!=ctx.actor):
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

    def receipt(self, principal, command_id):
        """Authenticated read after a lost MCP response; no repeat side effect."""
        actor=self._actor(principal)
        if not KEY.fullmatch(command_id):
            raise GeoError("invalid_command_id")
        with self.corpus.connect() as db:
            self._ctx(db,actor)
            row=db.execute(
                "SELECT result_json FROM rkb_geo_receipts WHERE command_id=? AND actor_id=?",
                (command_id,actor)).fetchone()
            if not row:
                raise GeoError("not_found_or_not_accessible")
            return json.loads(row[0])

    def enqueue_existing(self, principal, entity_id, cursor=None, limit=20,
                         *, policy=POLICY):
        """Backfill accepted exact spellings, not arbitrary discovery hits.

        The previous unbounded in-memory mentions[:20] could inspect twenty
        vector-only candidates with empty original spellings and entirely
        miss the only two source-quoted originals. Query the partial SQLite
        index first, then page through proven originals in stable row order.
        """
        actor=self._actor(principal)
        try:
            node_id=str(UUID(str(entity_id)))
            after=str(UUID(str(cursor))) if cursor else ""
        except (ValueError, TypeError, AttributeError):
            raise GeoError("invalid_source_cursor_or_entity") from None
        take=max(1,min(int(limit),20))
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            entity=ctx.one("rkb_entities",node_id)
            if (not entity or entity.get("kind")!="poi_ref" or
                    entity.get("owner_user_id")!=actor):
                raise GeoError("not_found_or_not_accessible")
            if not ctx.owned(entity["document_id"]):
                raise GeoError("not_found_or_not_accessible")
            rows=db.execute("""SELECT row_key,payload FROM corpus_rows
                WHERE table_name='rkb_entity_mentions'
                  AND json_extract(payload,'$.entity_id')=?
                  AND coalesce(json_extract(payload,'$.exact_source_spelling'),'')<>''
                  AND row_key>?
                ORDER BY row_key LIMIT ?""",
                (node_id,after,take+1)).fetchall()
            batch=rows[:take]
            requests=[]
            for raw in batch:
                mention=json.loads(raw["payload"])
                doc_id=mention.get("document_id")
                if not doc_id or not ctx.owned(doc_id):
                    # Cross-book owner ref is permitted, but a granted
                    # OTHER owner's private source cannot be exported here.
                    continue
                doc=ctx.one("rkb_documents",doc_id)
                if not doc or doc.get("active_revision")!=mention.get("revision"):
                    continue
                if not ctx.check_evidence(doc_id,mention["revision"],
                                          mention.get("evidence") or {}):
                    continue
                req=enqueue_mention(db,actor,mention,entity,policy=policy)
                if req:requests.append(req)
            request_ids=list(dict.fromkeys(requests))
            return {
                "request_ids":request_ids,
                "count":len(request_ids),
                "next_cursor":str(batch[-1]["row_key"]) if len(rows)>take else None,
                "complete":len(rows)<=take,
                "scanned":len(batch),
                "enqueued_from":"accepted_graph_source_spellings",
            }

    def claim(self, principal, limit=3, lease_seconds=90):
        actor=self._actor(principal)
        limit=max(1,min(int(limit),5));lease_seconds=max(15,min(int(lease_seconds),180))
        now=time.time()
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            # Explicit terminal state for leases exhausted during an owner outage:
            # otherwise they remain "leased" forever, invisible to operators.
            db.execute("""UPDATE rkb_geo_attempts SET
                    state='retry_exhausted',reason='lease_attempts_exhausted',
                    lease_token=NULL,lease_until=NULL,updated_at=?
                WHERE attempts>=5 AND state IN ('leased','staged')
                    AND lease_until<=? AND request_id IN
                    (SELECT request_id FROM rkb_geo_intents WHERE actor_id=?)""",
                    (now,now,actor))
            # Rank one eligible attempt PER DOCUMENT first. Applying LIMIT 25
            # before fairness starved a second book behind a large first book.
            rows=db.execute("""WITH ranked AS (
                SELECT a.attempt_id,
                    ROW_NUMBER() OVER (
                      PARTITION BY i.document_id
                      ORDER BY (a.priority +
                        MIN(5,CAST((? - a.created_at)/86400 AS INTEGER))) DESC,
                        a.created_at,a.attempt_id
                    ) AS doc_rank
                FROM rkb_geo_attempts a JOIN rkb_geo_intents i
                    ON i.request_id=a.request_id
                WHERE i.actor_id=? AND a.attempts<5
                    AND ((
                      a.state IN ('pending','retry_wait') AND a.available_at<=?
                    ) OR (
                      a.state IN ('leased','staged') AND a.lease_until<=?
                    ))
            ) SELECT a.*,i.* FROM ranked r
                JOIN rkb_geo_attempts a ON a.attempt_id=r.attempt_id
                JOIN rkb_geo_intents i ON i.request_id=a.request_id
                WHERE r.doc_rank=1
                ORDER BY (a.priority +
                   MIN(5,CAST((? - a.created_at)/86400 AS INTEGER))) DESC,
                   a.created_at,a.attempt_id LIMIT ?""",
                (now,actor,now,now,now,limit)).fetchall()
            results=[];documents=set()
            for row in rows:
                if len(results)>=limit:break
                # Fairness: one mention from each document per bounded packet.
                if row["document_id"] in documents:continue
                documents.add(row["document_id"])
                try:self._live(db,ctx,row)
                except GeoError as exc:
                    outcome=("awaiting_activation" if exc.code=="source_not_activated"
                             else "stale_source" if exc.code=="stale_source"
                             else "blocked_access")
                    db.execute("""UPDATE rkb_geo_attempts SET state=?,reason=?,
                        lease_token=NULL,lease_until=NULL,updated_at=?
                        WHERE attempt_id=?""",
                        (outcome,exc.code,now,row["attempt_id"]))
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

    def defer(self, principal, attempt_id, lease_token, lease_fence,
              *, reason="owner_unavailable"):
        """Release an attempted owner read without burning 90s per failure.

        A retry is bounded, delayed and guarded by the live source+fence.
        After five attempts, the same intent awaits an explicit relevant
        dependency revision, rather than retrying forever.
        """
        if reason not in ("owner_unavailable","owner_timeout"):
            raise GeoError("invalid_transient_reason")
        actor=self._actor(principal)
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ctx=self._ctx(db,actor)
            row=db.execute("""SELECT a.*,i.* FROM rkb_geo_attempts a
                JOIN rkb_geo_intents i ON i.request_id=a.request_id
                WHERE a.attempt_id=?""",(attempt_id,)).fetchone()
            if not row or row["actor_id"]!=actor:
                raise GeoError("not_found_or_not_accessible")
            if (row["state"] not in ("leased","staged") or
                  row["lease_token"]!=lease_token or
                  row["lease_fence"]!=lease_fence or
                  (row["lease_until"] or 0)<=time.time()):
                raise GeoError("stale_lease")
            try:self._live(db,ctx,row)
            except GeoError as exc:
                status=("stale_source" if exc.code=="stale_source"
                        else "blocked_access")
                db.execute("""UPDATE rkb_geo_attempts SET state=?,reason=?,
                    lease_token=NULL,lease_until=NULL,updated_at=?
                    WHERE attempt_id=?""",
                    (status,exc.code,time.time(),attempt_id))
                return {"state":status,"attempt_id":attempt_id}
            now=time.time()
            exhausted=row["attempts"]>=5
            status="dependency_unavailable" if exhausted else "retry_wait"
            # Exponential backoff, bounded to 10 minutes. A later accepted
            # alias/layer revision can create a NEW attempt even after exhaust.
            delay=min(600,15*2**min(row["attempts"]-1,6))
            db.execute("""UPDATE rkb_geo_attempts SET
                state=?,reason=?,available_at=?,lease_token=NULL,
                lease_until=NULL,proposal_json=NULL,proposal_hash=NULL,
                updated_at=? WHERE attempt_id=?""",
                (status,reason,now+delay,now,attempt_id))
            return {"state":status,"attempt_id":attempt_id,
                    "retry_after_seconds":None if exhausted else delay}

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
        if proposed.get("relation") is not None:
            # A catalog candidate has not undergone historical-spatial owner
            # acceptance. The precise relation is intentionally not supported
            # until the signed layer+geometry+owner decision contracts land.
            raise GeoError("spatial_relation_requires_owner_decision")
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
                source_access="authorized_current"
                try:self._live(db,ctx,row)
                except GeoError as exc:
                    if exc.code=="source_not_activated" and ctx.owned(row["document_id"]):
                        source_access="staged_not_accepted"
                    else:continue
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
                    "source_access":source_access,
                })
            summary=db.execute("""SELECT a.state,COUNT(*) n,
                MIN(a.created_at) oldest
                FROM rkb_geo_attempts a JOIN rkb_geo_intents i
                    ON i.request_id=a.request_id
                WHERE i.actor_id=? GROUP BY a.state""",
                (actor,)).fetchall()
            states={r["state"]:r["n"] for r in summary}
            # Count old pending intents, not global private corpus state.
            waiting=[r["oldest"] for r in summary
                     if r["state"] in ("pending","retry_wait")]
            lag=max(0,round(time.time()-min(waiting),1)) if waiting else 0
            return {"items":items,"count":len(items),
                    "state_counts":states,"oldest_pending_seconds":lag,
                    "accepted_layer_revision":None,
                    "cartography_capability":"awaiting_producer",
                    "owner_claim_contract":CONTRACT,
                    "interservice_resolve_contract":CARTOGRAPHY_RESOLVE_CONTRACT,
                    "interservice_transport_ready":False,
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

    def backfill_batch(self,limit=12):
        """One-time bounded catch-up for old, already accepted graph mentions.

        New book stages are always handled atomically by LocalContext.store.
        This does not inspect source text, extract new entities or reimport books.
        """
        take=max(1,min(int(limit),32))
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old=db.execute(
                "SELECT value_json FROM rkb_geo_watermarks WHERE name='graph_mentions_v1'"
            ).fetchone()
            state=json.loads(old[0]) if old else {"after":"","completed":False}
            if state.get("completed"):
                return {"scanned":0,"created":0,"completed":True}
            rows=db.execute("""SELECT row_key,payload FROM corpus_rows
                WHERE table_name='rkb_entity_mentions' AND row_key>?
                ORDER BY row_key LIMIT ?""",(state["after"],take)).fetchall()
            created=0
            from .sqlite_data import LocalContext
            for row in rows:
                mention=json.loads(row["payload"])
                entity_row=db.execute(
                    "SELECT payload FROM corpus_rows WHERE table_name='rkb_entities' AND row_key=?",
                    (mention.get("entity_id"),)).fetchone()
                if not entity_row:continue
                entity=json.loads(entity_row["payload"])
                if entity.get("kind")!="poi_ref":continue
                source_row=db.execute(
                    "SELECT payload FROM corpus_rows WHERE table_name='rkb_documents' AND row_key=?",
                    (mention.get("document_id"),)).fetchone()
                if not source_row:continue
                doc=json.loads(source_row["payload"])
                owner=str(doc.get("owner_user_id") or "")
                if (not owner or owner!=entity.get("owner_user_id")
                        or doc.get("active_revision")!=mention.get("revision")):
                    continue
                ctx=LocalContext(self.corpus,db,owner)
                user=ctx.one("rkb_users",owner)
                if not user or user.get("status")!="active" or not ctx.check_evidence(
                        doc["id"],mention["revision"],mention.get("evidence") or {}):
                    continue
                before=db.execute(
                    "SELECT COUNT(*) FROM rkb_geo_intents WHERE mention_id=? AND policy_version=?",
                    (mention["id"],POLICY)).fetchone()[0]
                key=enqueue_mention(db,owner,mention,entity)
                if key and not before:created+=1
            state={"after":rows[-1]["row_key"] if rows else state["after"],
                   "completed":len(rows)<take}
            db.execute("""INSERT INTO rkb_geo_watermarks VALUES('graph_mentions_v1',?,?)
                ON CONFLICT(name) DO UPDATE SET value_json=excluded.value_json,
                                             updated_at=excluded.updated_at""",
                (_json(state),time.time()))
            return {"scanned":len(rows),"created":created,
                    "completed":state["completed"]}

    def poll_owner_updates(self,limit=24):
        """Watch OWNER canonical alias additions, schedule only exact relevant intents.

        This is a local read-only projection poll. It does not read the user's
        private source out of RKB or write to Street Story. Alias created_at
        + SQLite rowid is a cursor, not a global atlas watermark.
        """
        take=max(1,min(int(limit),32))
        with self.corpus.connect() as db:
            old=db.execute(
                "SELECT value_json FROM rkb_geo_watermarks WHERE name='street_owner_alias_v1'"
            ).fetchone()
            cursor=json.loads(old[0]) if old else {"time":0.0,"rowid":0}
        try:
            with self.resolver._connect() as owner:
                owner.row_factory=sqlite3.Row
                aliases=owner.execute("""SELECT rowid,poi_id,namespace,value,created_at
                    FROM poi_aliases
                    WHERE created_at>? OR (created_at=? AND rowid>?)
                    ORDER BY created_at,rowid LIMIT ?""",
                    (cursor["time"],cursor["time"],cursor["rowid"],take)).fetchall()
                if not aliases:return {"seen":0,"scheduled":0}
                groups={}
                for row in aliases:
                    groups.setdefault(row["poi_id"],[]).append(row)
                owner_versions={}
                for poi_id,changes in groups.items():
                    row=owner.execute(
                        "SELECT id,status,canonical_name FROM pois WHERE id=?",
                        (poi_id,)).fetchone()
                    if not row or row["status"]=="merged":continue
                    values=owner.execute(
                        "SELECT namespace,value FROM poi_aliases WHERE poi_id=? ORDER BY namespace,normalized_value",
                        (poi_id,)).fetchall()
                    searchable={
                        normalize_alias(row["canonical_name"]),
                        *(normalize_alias(a["value"]) if a["namespace"]=="name"
                          else a["namespace"]+":"+normalize_alias(a["value"])
                          for a in values),
                    }
                    owner_versions[poi_id]={
                        "aliases":[v for v in searchable if v],
                        "revision":_hash({"owner":poi_id,"status":row["status"],
                                          "aliases":[(v["namespace"],v["value"])
                                                     for v in values]}),
                    }
        except (sqlite3.Error,RuntimeError,OSError):
            return {"seen":0,"scheduled":0,"owner":"unavailable"}
        created=0
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for poi_id,meta in owner_versions.items():
                for name in meta["aliases"][:40]:
                    matches=db.execute("""SELECT DISTINCT i.* FROM rkb_geo_intents i
                        JOIN rkb_geo_aliases a ON a.request_id=i.request_id
                        WHERE a.normalized_name=? LIMIT 30""",(name,)).fetchall()
                    for intent in matches:
                        try:
                            ctx=self._ctx(db,intent["actor_id"])
                            entity,_=self._live(db,ctx,intent)
                        except GeoError:
                            continue
                        if entity.get("external_ref"):
                            continue
                        states=[r[0] for r in db.execute("""SELECT state
                            FROM rkb_geo_attempts WHERE request_id=?
                            ORDER BY created_at DESC,attempt_id DESC LIMIT 3""",
                            (intent["request_id"],))]
                        if any(x in ("pending","leased","staged","linked_candidate")
                               for x in states):
                            continue
                        dep="streetstory://poi/"+poi_id
                        signature=_hash({
                            "request_id":intent["request_id"],
                            "dependency_kind":"owner_poi",
                            "dependency_ref":dep,
                            "dependency_revision":meta["revision"],
                            "mention_sha":intent["mention_sha256"],
                            "policy":intent["policy_version"],
                        })
                        now=time.time()
                        count=db.execute("""INSERT OR IGNORE INTO rkb_geo_attempts(
                            attempt_id,request_id,dependency_kind,dependency_ref,
                            dependency_revision,input_hash,state,available_at,
                            created_at,updated_at)
                            VALUES(?,?,?,?,?,?,'pending',?,?,?)""",(
                            "geoa_"+signature[:32],intent["request_id"],
                            "owner_poi",dep,meta["revision"],signature,now,now,now
                        )).rowcount
                        created+=int(bool(count))
            last=aliases[-1]
            new_cursor={"time":last["created_at"],"rowid":last["rowid"]}
            db.execute("""INSERT INTO rkb_geo_watermarks VALUES('street_owner_alias_v1',?,?)
                ON CONFLICT(name) DO UPDATE SET value_json=excluded.value_json,
                                             updated_at=excluded.updated_at""",
                (_json(new_cursor),time.time()))
        return {"seen":len(aliases),"scheduled":created,"scope":"relevant_aliases"}

    def recover_expired_leases(self,limit=32):
        """Reclaim crash-orphaned geo attempts without an external operator.

        Local SQLite BEGIN IMMEDIATE fences the reaper against fresh claims.
        An expired worker's token/proposal is invalidated before requeueing.
        After five attempts, preserve a terminal reason until a NEW relevant
        dependency revision creates a new attempt. Called only by the
        trusted existing graph-discovery daemon, not an exposed MCP action.
        """
        take=max(1,min(int(limit),64))
        now=time.time()
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            stale=db.execute("""SELECT attempt_id,lease_fence,lease_until,attempts
                FROM rkb_geo_attempts
                WHERE state IN ('leased','staged') AND lease_until<=?
                ORDER BY lease_until,attempt_id LIMIT ?""",
                (now,take)).fetchall()
            released=exhausted=0
            for row in stale:
                terminal=int(row["attempts"])>=5
                result="retry_exhausted" if terminal else "pending"
                reason="lease_attempts_exhausted" if terminal else "lease_expired_worker_restarted"
                updated=db.execute("""UPDATE rkb_geo_attempts
                    SET state=?,reason=?,lease_token=NULL,lease_until=NULL,
                        proposal_json=NULL,proposal_hash=NULL,
                        available_at=?,updated_at=?
                    WHERE attempt_id=? AND lease_fence=?
                      AND state IN ('leased','staged') AND lease_until<=?""",
                    (result,reason,now,now,row["attempt_id"],
                     row["lease_fence"],now)).rowcount
                if updated:
                    exhausted+=int(terminal)
                    released+=int(not terminal)
            return {"examined":len(stale),"requeued":released,
                    "retry_exhausted":exhausted,"max_batch":64}

    def worker_tick(self,limit=2):
        """Low-cost owner-local consumer, isolated from remote map/provider failures."""
        from .contracts import Principal
        # Worker crashes leave leased/staged rows that were previously ignored
        # by the actor preselection, even though claim() can re-lease them.
        # Requeue expired attempts in a separate tiny fenced transaction
        # BEFORE looking for eligible actors. Never call an owner/LLM inside.
        recovery=self.recover_expired_leases(limit=32)
        with self.corpus.connect() as db:
            actors=[r[0] for r in db.execute("""SELECT DISTINCT i.actor_id
              FROM rkb_geo_intents i JOIN rkb_geo_attempts a
                ON a.request_id=i.request_id
              JOIN corpus_rows u ON u.table_name='rkb_users'
                AND u.row_key=i.actor_id
                AND json_extract(u.payload,'$.status')='active'
              WHERE a.state IN ('pending','retry_wait')
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
                    except (OSError,sqlite3.Error,RuntimeError):
                        # A missing owner projection must NOT become a
                        # terminal no-match. Back off, bounded, with reason.
                        self.defer(principal,job["attempt_id"],job["lease_token"],
                                   job["lease_fence"],reason="owner_unavailable")
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
                    try:
                        result=self.apply(principal,job["attempt_id"],job["lease_token"],
                                          job["lease_fence"],
                                          "geo-worker-apply:"+_hash(prefix)[:32])
                    except (OSError,sqlite3.Error,RuntimeError):
                        self.defer(principal,job["attempt_id"],job["lease_token"],
                                   job["lease_fence"],reason="owner_unavailable")
                        continue
                    results.append(result["state"])
            except (GeoError, ValueError, LookupError, sqlite3.Error):
                # Failed source/auth must not block normal indexing or reading.
                continue
        return {"processed":len(results),"states":results,
                "recovered_expired_leases":recovery["requeued"],
                "recovered_retry_exhausted":recovery["retry_exhausted"]}
