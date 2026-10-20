"""Versioned participant policy over existing canonical rkb_users UUIDs.

Only trusted adapters construct RequestAuthority / observe_account. Neither is
a client payload or an identity provider. No tokens or personal OAuth invites
are stored here. Source ACL remains independently enforced by corpus readers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import re
from uuid import UUID, uuid4

from .participant_contracts import (
    AccountBegin, AccountConfirm, AccountUnlink, DomainWeight,
    ParticipantDisable, ParticipantGrant, ParticipantUpsert,
)
from .sqlite_corpus import canonical
from .story_registry import StoryRegistry


EXPERT_CAPABILITIES = frozenset({"public_search", "case_read", "case_contribute",
    "scan_proof_read", "personal_conclusion", "support_exact_revision", "discussion_request"})
CAPABILITIES = EXPERT_CAPABILITIES | {"maintain_shared_summary", "discussion_approve", "participants_manage", "import"}


class ParticipantError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def fail(code):
    raise ParticipantError(code)


@dataclass(frozen=True)
class RequestAuthority:
    """Internal server context; must never be parsed from tool arguments."""
    principal: object
    route: str
    link_id: str | None = None
    link_revision: int | None = None
    request_id: str | None = None
    request_generation: int | None = None
    executor_id: str | None = None
    execution_token: str | None = field(default=None,repr=False)
    public_only: bool = False


class Participants:
    def __init__(self, corpus, owner_subject, *, clock=None):
        self.corpus = corpus
        self.owner = str(UUID(str(owner_subject)))
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sources = StoryRegistry(corpus, migrate=False)
        with corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS rkb_participant_profiles(
              person_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
              disabled INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rkb_participant_grants(
              person_id TEXT NOT NULL, capability TEXT NOT NULL,
              expires_at TEXT, assigned_by TEXT NOT NULL, assigned_at TEXT NOT NULL,
              PRIMARY KEY(person_id,capability));
            CREATE TABLE IF NOT EXISTS rkb_participant_weights(
              person_id TEXT NOT NULL, domain TEXT NOT NULL, payload TEXT NOT NULL,
              revision INTEGER NOT NULL, PRIMARY KEY(person_id,domain));
            CREATE TABLE IF NOT EXISTS rkb_external_identities(
              id TEXT PRIMARY KEY, person_id TEXT NOT NULL, provider TEXT NOT NULL,
              tenant TEXT NOT NULL, provider_subject TEXT NOT NULL, state TEXT NOT NULL,
              revision INTEGER NOT NULL, method TEXT, confirmed_by TEXT, confirmed_at TEXT,
              attribution_review_required INTEGER NOT NULL DEFAULT 0);
            CREATE UNIQUE INDEX IF NOT EXISTS rkb_external_identity_live
              ON rkb_external_identities(provider,tenant,provider_subject)
              WHERE state IN ('pending','verified');
            CREATE INDEX IF NOT EXISTS rkb_external_identity_person ON rkb_external_identities(person_id);
            CREATE TABLE IF NOT EXISTS rkb_identity_observations(
              id TEXT PRIMARY KEY, provider TEXT NOT NULL, tenant TEXT NOT NULL,
              provider_subject TEXT NOT NULL, link_id TEXT NOT NULL,
              link_revision INTEGER NOT NULL, audience TEXT NOT NULL,
              expires_at TEXT NOT NULL, receipt_id TEXT NOT NULL UNIQUE, used_by TEXT);
            CREATE TABLE IF NOT EXISTS rkb_participant_events(
              person_id TEXT NOT NULL, revision INTEGER NOT NULL, actor_id TEXT NOT NULL,
              action TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(person_id,revision));
            CREATE TABLE IF NOT EXISTS rkb_participant_operations(
              actor_id TEXT NOT NULL, operation_key TEXT NOT NULL, person_id TEXT NOT NULL,
              request_hash TEXT NOT NULL, result TEXT NOT NULL,
              PRIMARY KEY(actor_id,operation_key));
            """)

    def _time(self):
        value = self.clock()
        if value.tzinfo is None:
            fail("invalid_server_time")
        return value.astimezone(timezone.utc)

    def _expired(self, value):
        return value is not None and datetime.fromisoformat(value) <= self._time()

    def _authority(self, db, context, *, admin=False):
        if not isinstance(context, RequestAuthority) or context.route not in {"owner", "personal", "expert", "delegated"}:
            fail("capability_denied")
        try:
            actor = str(UUID(str(context.principal.subject)))
        except (ValueError, AttributeError):
            fail("capability_denied")
        user = self.sources._corpus_row(db, "rkb_users", actor)
        if not user or user.get("status") != "active":
            fail("capability_denied")
        profile = db.execute("SELECT * FROM rkb_participant_profiles WHERE person_id=?", (actor,)).fetchone()
        # Legacy owner expiry must not permanently lock out the only repair
        # authority. Canonical user status and the disabled bit still apply.
        if profile and (profile["disabled"] or (actor != self.owner
                and self._expired(json.loads(profile["payload"]).get("expires_at")))):
            fail("capability_denied")
        if actor != self.owner and not profile:
            fail("capability_denied")
        if context.route == "expert":
            link = db.execute("SELECT * FROM rkb_external_identities WHERE id=?", (context.link_id,)).fetchone()
            if not link or link["person_id"] != actor or link["state"] != "verified" or link["revision"] != context.link_revision:
                fail("identity_unverified")
        elif context.link_id is not None or context.link_revision is not None:
            fail("capability_denied")
        if admin and (actor != self.owner or context.route != "owner"):
            fail("capability_denied")
        if context.request_id is not None:
            try:
                from .request_authority import execution_allowed
            except ImportError:
                fail("request_fence_denied")
            if context.route not in {"expert","delegated"} or not execution_allowed(db, context, self._time().timestamp()):
                fail("request_fence_denied")
        elif context.route == 'delegated':
            fail('request_fence_denied')
        elif any((context.request_generation, context.executor_id, context.execution_token, context.public_only)):
            fail("request_fence_denied")
        return actor

    def require(self, context, capability, *, connection=None):
        """Recheck inside the caller's transaction, including delayed delivery."""
        if capability not in CAPABILITIES:
            fail("capability_denied")
        if connection is None:
            with self.corpus.connect() as db:
                db.execute("BEGIN")
                return self.require(context, capability, connection=db)
        actor = self._authority(connection, context)
        # Group channels cannot inherit a personal projection. Case/public
        # exports must acquire their own audience-safe path before enabling it.
        if context.public_only and capability != "public_search":
            fail("public_projection_required")
        if context.route in {"expert","delegated"} and capability not in EXPERT_CAPABILITIES:
            fail("capability_denied")
        if actor == self.owner:
            if capability == "import" and context.route != "owner":
                fail("capability_denied")
            return actor
        if capability in {"import", "participants_manage"}:
            fail("capability_denied")
        grant = connection.execute("SELECT * FROM rkb_participant_grants WHERE person_id=? AND capability=?", (actor, capability)).fetchone()
        if not grant or self._expired(grant["expires_at"]):
            fail("capability_denied")
        return actor

    def _snapshot(self, db, person_id):
        row = db.execute("SELECT * FROM rkb_participant_profiles WHERE person_id=?", (person_id,)).fetchone()
        if not row:
            fail("not_found_or_not_accessible")
        return {"person_id": person_id, "revision": row["revision"], "disabled": bool(row["disabled"]),
                "profile": json.loads(row["payload"]),
                "grants": [dict(r) for r in db.execute("SELECT * FROM rkb_participant_grants WHERE person_id=? ORDER BY capability", (person_id,))],
                "domain_weights": [{**dict(r), "payload": json.loads(r["payload"])} for r in db.execute("SELECT * FROM rkb_participant_weights WHERE person_id=? ORDER BY domain", (person_id,))],
                "accounts": [dict(r) for r in db.execute("SELECT * FROM rkb_external_identities WHERE person_id=? ORDER BY id", (person_id,))]}

    def get(self, context, person_id):
        person_id = str(UUID(str(person_id)))
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            actor = self._authority(db, context)
            if context.public_only:
                fail("public_projection_required")
            if person_id != actor:
                self._authority(db, context, admin=True)
            return self._snapshot(db, person_id)

    def list(self, context, *, after_id="", limit=50):
        if not 1 <= limit <= 100:
            fail("invalid_limit")
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            self._authority(db, context, admin=True)
            rows = db.execute("SELECT person_id FROM rkb_participant_profiles WHERE person_id>? ORDER BY person_id LIMIT ?", (after_id, limit+1)).fetchall()
            return {"items": [self._snapshot(db, r[0]) for r in rows[:limit]], "has_more": len(rows)>limit,
                    "next_after_id": rows[limit-1][0] if len(rows)>limit else None}

    def audit(self, context, person_id, *, after_revision=0, limit=50):
        if after_revision < 0 or not 1 <= limit <= 100:
            fail("invalid_cursor")
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            self._authority(db, context, admin=True)
            rows = db.execute("SELECT * FROM rkb_participant_events WHERE person_id=? AND revision>? ORDER BY revision LIMIT ?", (str(UUID(str(person_id))), after_revision, limit+1)).fetchall()
            return {"items": [{**dict(r), "payload":json.loads(r["payload"])} for r in rows[:limit]],
                    "has_more":len(rows)>limit, "next_after_revision":rows[limit-1]["revision"] if len(rows)>limit else None}

    def operation_status(self, context, key):
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            actor = self._authority(db, context, admin=True)
            row = db.execute("SELECT result FROM rkb_participant_operations WHERE actor_id=? AND operation_key=?", (actor,key)).fetchone()
            return json.loads(row[0]) if row else {"state":"not_found"}

    def change(self, context, person_id, expected_revision, command, operation_key):
        if not isinstance(operation_key,str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", operation_key):
            fail("invalid_operation_key")
        if type(expected_revision) is not int or expected_revision < 0:
            fail("revision_conflict")
        person_id = str(UUID(str(person_id)))
        allowed=(ParticipantUpsert,ParticipantDisable,ParticipantGrant,DomainWeight,AccountBegin,AccountConfirm,AccountUnlink)
        if type(command) not in allowed:
            fail("invalid_command")
        payload=command.model_dump(mode="json")
        if isinstance(command,ParticipantUpsert):
            payload["categories"]=sorted(payload["categories"])
        action=type(command).__name__
        digest=hashlib.sha256(canonical([person_id,expected_revision,action,payload]).encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            actor=self._authority(db,context,admin=True)
            if isinstance(command,ParticipantUpsert) and person_id==self.owner and payload.get('expires_at') is not None:
                fail('cannot_expire_owner')
            saved=db.execute("SELECT * FROM rkb_participant_operations WHERE actor_id=? AND operation_key=?",(actor,operation_key)).fetchone()
            if saved:
                if saved["request_hash"]!=digest: fail("idempotency_conflict")
                return json.loads(saved["result"])
            current=db.execute("SELECT * FROM rkb_participant_profiles WHERE person_id=?",(person_id,)).fetchone()
            if (current["revision"] if current else 0)!=expected_revision: fail("revision_conflict")
            revision=expected_revision+1
            result={"person_id":person_id,"revision":revision,"operation_id":str(uuid4()),"state":"applied"}
            timestamp=self._time().isoformat()
            if isinstance(command,ParticipantUpsert):
                user=self.sources._corpus_row(db,"rkb_users",person_id)
                if not user:
                    self.corpus.put("rkb_users",[{"id":person_id,"status":"active","metadata":{},"created_at":timestamp,"updated_at":timestamp}],connection=db)
                # Updating a profile cannot silently re-enable a disabled identity.
                db.execute("INSERT INTO rkb_participant_profiles VALUES(?,?,0,?) ON CONFLICT(person_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload",(person_id,revision,canonical(payload)))
            else:
                if not current: fail("not_found_or_not_accessible")
                if isinstance(command,ParticipantDisable):
                    if person_id==self.owner: fail("cannot_disable_owner")
                    db.execute("UPDATE rkb_participant_profiles SET disabled=? WHERE person_id=?",(command.disabled,person_id))
                elif isinstance(command,ParticipantGrant):
                    if command.granted:
                        db.execute("INSERT INTO rkb_participant_grants VALUES(?,?,?,?,?) ON CONFLICT(person_id,capability) DO UPDATE SET expires_at=excluded.expires_at,assigned_by=excluded.assigned_by,assigned_at=excluded.assigned_at",(person_id,command.capability,payload["expires_at"],actor,timestamp))
                    else: db.execute("DELETE FROM rkb_participant_grants WHERE person_id=? AND capability=?",(person_id,command.capability))
                elif isinstance(command,DomainWeight):
                    weight={**payload,"assigned_by":actor,"assigned_at":timestamp}
                    db.execute("INSERT INTO rkb_participant_weights VALUES(?,?,?,?) ON CONFLICT(person_id,domain) DO UPDATE SET payload=excluded.payload,revision=excluded.revision",(person_id,command.domain,canonical(weight),revision))
                elif isinstance(command,AccountBegin):
                    self._validate_account(command)
                    conflict=db.execute("SELECT 1 FROM rkb_external_identities WHERE provider=? AND tenant=? AND provider_subject=? AND state IN ('pending','verified')",(command.provider,command.tenant,command.provider_subject)).fetchone()
                    if conflict: fail("identity_already_linked")
                    link_id=str(uuid4())
                    db.execute("INSERT INTO rkb_external_identities(id,person_id,provider,tenant,provider_subject,state,revision) VALUES(?,?,?,?,?,'pending',1)",(link_id,person_id,command.provider,command.tenant,command.provider_subject))
                    result.update(link_id=link_id,link_revision=1)
                else:
                    link=db.execute("SELECT * FROM rkb_external_identities WHERE id=? AND person_id=?",(str(command.link_id),person_id)).fetchone()
                    if not link or link["state"]=="revoked": fail("identity_unverified")
                    if link["revision"]!=command.expected_link_revision: fail("revision_conflict")
                    if isinstance(command,AccountConfirm):
                        if link["state"]!="pending": fail("identity_unverified")
                        self._confirm(db,link,command)
                        db.execute("UPDATE rkb_external_identities SET state='verified',revision=revision+1,method=?,confirmed_by=?,confirmed_at=? WHERE id=?",(command.method,actor,timestamp,link["id"]))
                    else:
                        db.execute("UPDATE rkb_external_identities SET state='revoked',revision=revision+1,attribution_review_required=? WHERE id=?",(command.incorrect_binding,link["id"]))
                    result.update(link_id=link["id"],link_revision=link["revision"]+1)
                db.execute("UPDATE rkb_participant_profiles SET revision=? WHERE person_id=?",(revision,person_id))
            db.execute("INSERT INTO rkb_participant_events VALUES(?,?,?,?,?,?)",(person_id,revision,actor,action,canonical({"request":payload,"result":result,"snapshot":self._snapshot(db,person_id)}),timestamp))
            db.execute("INSERT INTO rkb_participant_operations VALUES(?,?,?,?,?)",(actor,operation_key,person_id,digest,canonical(result)))
            return result

    @staticmethod
    def _validate_account(command):
        patterns={"slack":(r"T[A-Z0-9]+",r"[UW][A-Z0-9]+"),"vk":(r"[A-Za-z0-9._:-]+",r"[1-9][0-9]*"),"projects_hub":(r"[A-Za-z0-9._:/-]+",r"usr_[A-Za-z0-9_-]+"),"oidc":(r"https://[^\s]+",r"[^\s]+")}
        tenant,subject=patterns[command.provider]
        if not re.fullmatch(tenant,command.tenant) or not re.fullmatch(subject,command.provider_subject):
            fail("invalid_external_identity")

    def _confirm(self,db,link,command):
        if command.method=="owner_confirmed_slack":
            if link["provider"]!="slack" or command.observation_id is not None: fail("invalid_identity_proof")
            return
        observation=db.execute("SELECT * FROM rkb_identity_observations WHERE id=?",(str(command.observation_id),)).fetchone()
        if not observation or observation["used_by"] or self._expired(observation["expires_at"]): fail("invalid_identity_proof")
        for key in ("provider","tenant","provider_subject"):
            if observation[key]!=link[key]: fail("invalid_identity_proof")
        if observation["link_id"]!=link["id"] or observation["link_revision"]!=link["revision"] or observation["audience"]!="rkb-account-binding": fail("invalid_identity_proof")
        db.execute("UPDATE rkb_identity_observations SET used_by=? WHERE id=?",(link["id"],observation["id"]))

    def observe_account(self, *, link_id, link_revision, provider, tenant, provider_subject, audience, expires_at, receipt_id):
        """Trusted verifier hook, NOT MCP callable. Receipts contain no tokens."""
        if expires_at.tzinfo is None or expires_at <= self._time() or not receipt_id:
            fail("invalid_identity_proof")
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            link=db.execute("SELECT * FROM rkb_external_identities WHERE id=?",(str(link_id),)).fetchone()
            if not link or link["state"]!="pending" or link["revision"]!=link_revision or any(link[k]!=v for k,v in (("provider",provider),("tenant",tenant),("provider_subject",provider_subject))) or audience!="rkb-account-binding": fail("invalid_identity_proof")
            prior=db.execute("SELECT * FROM rkb_identity_observations WHERE receipt_id=?",(receipt_id,)).fetchone()
            if prior:
                if prior["link_id"]!=str(link_id) or prior["link_revision"]!=link_revision: fail("invalid_identity_proof")
                return prior["id"]
            identity=str(uuid4())
            db.execute("INSERT INTO rkb_identity_observations VALUES(?,?,?,?,?,?,?,?,?,NULL)",(identity,provider,tenant,provider_subject,str(link_id),link_revision,audience,expires_at.isoformat(),receipt_id))
            return identity

    def resolve_external(self, *, provider, tenant, provider_subject, principal_factory, connection=None):
        """Adapter calls only after verifying provider transport; never for a raw payload."""
        if connection is None:
            with self.corpus.connect() as db:
                db.execute("BEGIN")
                return self.resolve_external(provider=provider,tenant=tenant,provider_subject=provider_subject,principal_factory=principal_factory,connection=db)
        link=connection.execute("SELECT * FROM rkb_external_identities WHERE provider=? AND tenant=? AND provider_subject=? AND state='verified'",(provider,tenant,provider_subject)).fetchone()
        if not link: fail("identity_unverified")
        context=RequestAuthority(principal_factory(link["person_id"]),"expert",link["id"],link["revision"])
        self._authority(connection,context)
        return context

    def attribution(self, db, context):
        """Immutable assignment snapshot, not a scientific or human verdict."""
        actor = self._authority(db, context)
        row = db.execute("SELECT revision FROM rkb_participant_profiles WHERE person_id=?", (actor,)).fetchone()
        weights = []
        for weight in db.execute("SELECT * FROM rkb_participant_weights WHERE person_id=? ORDER BY domain", (actor,)):
            value=json.loads(weight["payload"])
            if not self._expired(value.get("expires_at")):
                weights.append({**value,"assigned_at_revision":weight["revision"]})
        return {"person_id":actor,"profile_revision":row[0] if row else None,
                "route":context.route,"external_link_id":context.link_id,
                "external_link_revision":context.link_revision,"domain_weights":weights,
                "request_id":context.request_id,"request_generation":context.request_generation,
                "executor_id":context.executor_id}
