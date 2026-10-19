"""Personal credentials in the existing RKB OAuth store, not a second IAM.

The current Participants policy owns people, expiry and rights. This module
only authenticates an existing person; it never creates a person or a grant.
Issuance/revocation is a trusted local owner administration operation, not a
public MCP tool or a client-supplied actor field.
"""
import hashlib
import hmac
import secrets
from uuid import UUID, uuid4

from .contracts import Principal
from .participants import ParticipantError, RequestAuthority


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def new_login_code():
    ident = str(uuid4())
    return ident, "rkbp_" + UUID(ident).hex + "." + secrets.token_urlsafe(32)


class PersonalPolicy:
    def __init__(self, participants):
        self.participants = participants

    def _context(self, subject, *, owner=False):
        return RequestAuthority(Principal(subject=str(UUID(str(subject))),
            client_id="rkb-personal-auth", issuer="rkb-local", access_token=""),
            "owner" if owner else "personal")

    def require_admin(self, context):
        with self.participants.corpus.connect() as db:
            return self.participants._authority(db, context, admin=True)

    def person(self, subject):
        if str(subject) == self.participants.owner:
            raise ValueError("personal credential cannot identify the owner")
        context = self._context(subject)
        with self.participants.corpus.connect() as db:
            self.participants._authority(db, context)
            row = db.execute("SELECT payload FROM rkb_participant_profiles WHERE person_id=?", (str(subject),)).fetchone()
        import json
        profile = json.loads(row[0])
        return {"subject": str(subject), "name": profile["name"]}


class PersonalOAuthMixin:
    """Used only by the existing RegionalOAuthProvider and its encrypted store."""
    personal_policy = None

    def _personal_person(self, subject):
        if self.personal_policy is None:
            raise ValueError("personal login unavailable")
        try:
            return self.personal_policy.person(subject)
        except (ParticipantError, ValueError, LookupError, TypeError):
            raise ValueError("personal login unavailable") from None

    def issue_personal_credential(self, context, subject, *, login_code, expires_at, reason):
        if self.personal_policy is None:
            raise ValueError("personal login unavailable")
        subject = str(UUID(str(subject)))
        actor = self.personal_policy.require_admin(context)
        self._personal_person(subject)
        ident = self._personal_code_id(login_code)
        if not ident or not reason or not self.clock() < expires_at <= self.clock() + 90 * 86400:
            raise ValueError("invalid personal credential")
        secret_hash = digest(login_code)
        def write(state):
            self.personal_policy.require_admin(context)
            self._personal_person(subject)
            existing = state["personal_credentials"].get(ident)
            if existing:
                if (existing["subject"] != str(subject) or existing["secret_hash"] != secret_hash
                    or existing["expires_at"] != expires_at or existing["created_by"] != actor
                    or existing["reason"] != reason):
                    raise ValueError("credential identity conflict")
                return self._credential_metadata(ident, existing)
            if len(state["personal_credentials"]) >= 4096:
                raise ValueError("personal credential capacity")
            row = {"subject": str(subject), "secret_hash": secret_hash, "enabled": True, "epoch": 1,
                "expires_at": expires_at, "created_at": self.clock(), "created_by": actor, "reason": reason}
            state["personal_credentials"][ident] = row
            state["personal_audit"][str(uuid4())] = {"action": "issued", "credential_id": ident,
                "subject": str(subject), "actor": actor, "epoch": 1, "at": self.clock(), "reason": reason}
            return self._credential_metadata(ident, row)
        return self.store.mutate(write)

    @staticmethod
    def _credential_metadata(ident, row):
        return {"credential_id": ident, "subject": row["subject"], "enabled": row["enabled"],
            "epoch": row["epoch"], "expires_at": row["expires_at"]}

    def personal_credentials(self, context, subject):
        if self.personal_policy is None:
            raise ValueError("personal login unavailable")
        self.personal_policy.require_admin(context)
        return self.store.read(lambda state: [self._credential_metadata(k, r)
            for k, r in state["personal_credentials"].items() if r["subject"] == str(subject)])

    def revoke_personal_credential(self, context, ident, *, expected_epoch, reason):
        if self.personal_policy is None:
            raise ValueError("personal login unavailable")
        actor = self.personal_policy.require_admin(context)
        if not reason:
            raise ValueError("reason required")
        def write(state):
            self.personal_policy.require_admin(context)
            row = state["personal_credentials"].get(str(ident))
            if row is None:
                raise ValueError("credential unavailable")
            if not row["enabled"] and row["epoch"] == expected_epoch + 1 and row.get("revoked_reason") == reason:
                return self._credential_metadata(str(ident), row)
            if row["epoch"] != expected_epoch:
                raise ValueError("credential revision conflict")
            row.update(enabled=False, epoch=row["epoch"] + 1, revoked_reason=reason)
            # Invalidate the complete set of sessions/code/access/refresh grants
            # created with this credential, including already-rotated families.
            for bucket in ("sessions", "codes", "access", "refresh"):
                for key, value in list(state[bucket].items()):
                    if value.get("personal_credential_id") == str(ident):
                        del state[bucket][key]
            state["personal_audit"][str(uuid4())] = {"action": "revoked", "credential_id": str(ident),
                "subject": row["subject"], "actor": actor, "epoch": row["epoch"],
                "at": self.clock(), "reason": reason}
            return self._credential_metadata(str(ident), row)
        return self.store.mutate(write)

    @staticmethod
    def _personal_code_id(code):
        if not isinstance(code, str) or not code.startswith("rkbp_") or not 75 <= len(code) <= 128:
            return None
        try:
            identity, secret = code[5:].split(".", 1)
            if len(identity) != 32 or len(secret) < 32:
                return None
            return str(UUID(hex=identity))
        except ValueError:
            return None

    def _record_allowed(self, record, *, state=None):
        if not record:
            return False
        if "expires_at" in record and record["expires_at"] <= self.clock():
            return False
        ident = record.get("personal_credential_id")
        if ident is None:
            return record.get("subject") == self.owner_subject
        row = (state["personal_credentials"].get(ident) if state is not None else
            self.store.read(lambda values: values["personal_credentials"].get(ident)))
        if (not row or not row["enabled"] or row["expires_at"] <= self.clock()
            or row["subject"] != record.get("subject") or row["epoch"] != record.get("personal_credential_epoch")):
            return False
        try:
            self._personal_person(row["subject"])
            return True
        except ValueError:
            return False

    def _new_personal_session(self, supplied):
        ident = self._personal_code_id(supplied)
        if ident is None:
            raise ValueError("login")
        raw = secrets.token_urlsafe(32)
        def write(state):
            row = state["personal_credentials"].get(ident)
            if not row or not hmac.compare_digest(row["secret_hash"], digest(supplied)):
                raise ValueError("login")
            record = {"subject": row["subject"], "personal_credential_id": ident,
                "personal_credential_epoch": row["epoch"], "expires_at": self.clock() + 900}
            if not self._record_allowed(record, state=state):
                raise ValueError("login")
            state["sessions"][digest(raw)] = record
        self.store.mutate(write)
        return raw

    def _session_record(self, raw):
        if not raw:
            return None
        record = self.store.read(lambda state: state["sessions"].get(digest(raw)))
        return record if self._record_allowed(record) else None

    def _login(self, supplied):
        return self._new_owner_session() if self.verify_owner_login(supplied) else self._new_personal_session(supplied)

    def _login_label(self, record):
        if record["subject"] == self.owner_subject:
            return "Владелец РКБ"
        return self._personal_person(record["subject"])["name"]
