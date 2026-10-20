"""LOCAL reviewed migration of existing owner OAuth families; no new issuance.

This is not a ready-read fallback, public route, token exchange or permission
administrator. The operator supplies the existing configured OAuth provider and
its existing authority SQLite path. State/key/token bytes never leave the store.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass,field
from datetime import datetime,timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import stat
from uuid import UUID

from mcp.shared.auth import OAuthClientInformationFull

from .oauth_provider import RegionalOAuthProvider,KNOWLEDGE_SCOPE,_digest
from .request_grants import reference


_LOG=logging.getLogger(__name__)


class ProjectionMigrationError(ValueError):pass


def _fail(code):raise ProjectionMigrationError(code)
def _canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def _sha(value):return hashlib.sha256(_canonical(value)).hexdigest()


def owner_projection_receipt_schema(db):
    """Fixed audit schema, also used by infrastructure authority export."""
    db.execute('''CREATE TABLE IF NOT EXISTS rkb_oauth_owner_projection_migrations(
        plan_sha256 TEXT PRIMARY KEY,receipt_json TEXT NOT NULL)''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS rkb_oauth_projection_receipt_immutable
        BEFORE UPDATE ON rkb_oauth_owner_projection_migrations
        BEGIN SELECT RAISE(ABORT,'immutable owner projection migration receipt'); END''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS rkb_oauth_projection_receipt_retained
        BEFORE DELETE ON rkb_oauth_owner_projection_migrations
        BEGIN SELECT RAISE(ABORT,'retained owner projection migration receipt'); END''')


@dataclass(frozen=True,eq=False)
class OwnerProjectionReview:
    plan_sha256:str
    eligible_families:int
    missing_projections:int
    existing_projections:int
    rejected:tuple[tuple[str,int],...]
    _owner:object=field(repr=False)

    def wire(self):
        return dict(codec='rkb.owner-issued-family-projection-review.v1',plan_sha256=self.plan_sha256,
            eligible_families=self.eligible_families,missing_projections=self.missing_projections,
            existing_projections=self.existing_projections,rejected=dict(self.rejected),
            issued_tokens=0,changed_grants=0,state_or_key_modified=False)


class OwnerFamilyProjectionMigration:
    """One local operator-owned review/apply over the already configured store.

    No caller actor/token or family selector exists. Only exact canonical owner
    families issued by this encrypted store can be selected. The operator must
    exclude legacy OAuth writers which do not update the same SQL projection.
    """
    def __init__(self,provider,*,corpus_path):
        if type(provider) is not RegionalOAuthProvider:_fail('existing_configured_provider_required')
        self.provider=provider;self.corpus=Path(corpus_path)
        self._review=None
        self._binding=self._configuration()

    def _configuration(self):
        p=self.provider
        paths=(Path(p.store.state_file),Path(p.store.key_file),self.corpus)
        for path in paths:
            if not path.is_absolute() or path.resolve(strict=True)!=path:_fail('existing_nonsymlink_private_paths_required')
            s=path.stat()
            if not stat.S_ISREG(s.st_mode) or s.st_uid!=os.geteuid() or s.st_mode&0o077:_fail('existing_owned_private_paths_required')
        if paths[0].stat().st_size>8*1024*1024:_fail('issued_state_byte_bound')
        if str(UUID(p.owner_subject))!=p.owner_subject:_fail('canonical_owner_required')
        policy=p.personal_policy
        if policy is not None and (policy.participants.owner!=p.owner_subject
                or policy.participants.corpus.path.resolve()!=self.corpus):_fail('existing_owner_authority_differs')
        return _sha(dict(state=str(paths[0]),key=str(paths[1]),authority=str(self.corpus),
            issuer=p.issuer,resource=p.resource,owner=p.owner_subject,client=p.client.client_id))

    def _connect(self):
        db=sqlite3.connect(self.corpus.as_uri()+'?mode=rw',uri=True,timeout=5,isolation_level=None)
        db.row_factory=sqlite3.Row
        return db

    def _owner_current(self,db):
        if self._configuration()!=self._binding:_fail('configured_owner_binding_changed')
        owner=self.provider.owner_subject
        row=db.execute("SELECT payload FROM corpus_rows WHERE table_name='rkb_users' AND row_key=?",(owner,)).fetchone()
        if row is None:_fail('active_canonical_owner_required')
        user=json.loads(row[0])
        profile=db.execute('SELECT disabled FROM rkb_participant_profiles WHERE person_id=?',(owner,)).fetchone()
        if user.get('id')!=owner or user.get('status')!='active' or profile and profile['disabled']:_fail('active_canonical_owner_required')
        fields={r[1] for r in db.execute('PRAGMA table_info(rkb_request_oauth_grants)')}
        if fields!={'id','person_id','client_id','resource','credential_id','credential_epoch','expires_at','enabled','issuer'}:
            _fail('existing_exact_projection_schema_required')

    def _client_current(self,state,client_id):
        p=self.provider
        if client_id==p.client.client_id:client=p.client
        else:
            raw=state['clients'].get(_digest(client_id))
            if type(raw) is not dict:return False
            try:client=OAuthClientInformationFull.model_validate(raw)
            except Exception:return False
        return bool(client.client_id==client_id and client.redirect_uris
            and all(p._redirect_allowed(str(uri)) for uri in client.redirect_uris)
            and client.token_endpoint_auth_method in {'none','client_secret_basic','client_secret_post'}
            and not set(client.grant_types)-{'authorization_code','refresh_token'}
            and set((client.scope or '').split())<={KNOWLEDGE_SCOPE}
            and (getattr(client,'issuer',None) is None or str(client.issuer)==p.issuer))

    def _desired(self,state):
        p=self.provider;now=p.clock();groups={};rejected=Counter()
        if (any(type(state.get(k)) is not dict or len(state[k])>4096 for k in ('access','refresh','used_refresh'))
                or type(state.get('clients')) is not dict or len(state['clients'])>256):_fail('issued_state_record_bound')
        for bucket in ('access','refresh'):
            for record in state[bucket].values():
                if type(record) is not dict:_fail('issued_state_record_invalid')
                expiry=record.get('expires_at')
                if type(expiry) not in (int,float) or not math.isfinite(expiry):_fail('issued_state_expiry_invalid')
                if expiry<=now:continue
                family=record.get('family')
                if type(family) is not str or not 1<=len(family)<=256:_fail('issued_state_family_invalid')
                groups.setdefault(family,[]).append(record)
        if len(groups)>256:_fail('issued_family_bound')
        rows=[]
        fields={'family','issuer','client_id','scopes','resource','subject','expires_at'}
        for family,records in groups.items():
            if any(r.get('subject')!=p.owner_subject or 'personal_credential_id' in r or 'personal_credential_epoch' in r for r in records):
                rejected['non_owner_family']+=1;continue
            if any(set(r)!=fields for r in records):
                rejected['unknown_issued_metadata']+=1;continue
            if any(r['issuer']!=p.issuer or r['resource']!=p.resource or r['scopes']!=[KNOWLEDGE_SCOPE]
                    or type(r['client_id']) is not str or not 1<=len(r['client_id'])<=256 for r in records):
                rejected['issued_scope_or_issuer_differs']+=1;continue
            if any(r['client_id']!=records[0]['client_id'] for r in records):
                rejected['mixed_family_metadata']+=1;continue
            if not self._client_current(state,records[0]['client_id']):
                rejected['registered_client_unknown']+=1;continue
            # These are the actual remaining access/refresh lifetimes. No fixed
            # refresh duration, mint, record copying or used_refresh authority.
            expiry=max(r['expires_at'] for r in records)
            rows.append(dict(id=reference(family),person_id=p.owner_subject,client_id=records[0]['client_id'],
                resource=p.resource,credential_id=None,credential_epoch=None,expires_at=expiry,enabled=1,issuer=p.issuer))
        rows.sort(key=lambda row:row['id'])
        return rows,tuple(sorted(rejected.items()))

    def _preflight(self,db,rows):
        missing=0
        for row in rows:
            old=db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?',(row['id'],)).fetchone()
            if old is None:missing+=1;continue
            if (not old['enabled'] or any(old[k]!=v for k,v in row.items() if k!='expires_at')
                    or type(old['expires_at']) not in (int,float) or not math.isfinite(old['expires_at'])
                    or old['expires_at']>row['expires_at']):_fail('existing_projection_revoked_or_conflicting')
            # Existing shorter projection lifetimes remain unchanged; they may
            # be renewed only by the ordinary authenticated refresh operation.
        return missing

    def _derive(self,state,db):
        self._owner_current(db);rows,rejected=self._desired(state)
        missing=self._preflight(db,rows)
        plan=_sha(dict(codec='rkb.owner-issued-family-projection-plan.v1',binding=self._binding,rows=rows,rejected=rejected))
        return rows,rejected,missing,plan

    def review(self):
        # Same canonical auth exclusion as mint/refresh/revoke, but no mutate()
        # or _write(): even pruning would alter evidence, so inspect raw state.
        with self.provider.store._lock,self.provider.store._file_lock(exclusive=True):
            db=self._connect()
            try:
                db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
                rows,rejected,missing,plan=self._derive(self.provider.store._read(),db)
            finally:db.rollback();db.close()
        review=OwnerProjectionReview(plan,len(rows),missing,len(rows)-missing,rejected,self)
        self._review=review
        _LOG.info('oauth_owner_projection state=reviewed plan_sha256=%s eligible=%s missing=%s rejected=%s',
            review.plan_sha256,review.eligible_families,review.missing_projections,sum(dict(review.rejected).values()))
        return review

    def apply(self,review):
        """Explicit LOCAL reviewed mutation; never called by token validation.

        Caller compares the fresh review.plan_sha256 with the independently
        reviewed operator plan before invoking. Only this retained review object
        is accepted; copied JSON grants nothing. All rows preflight before insert.
        """
        if type(review) is not OwnerProjectionReview or review is not self._review or review._owner is not self:
            _fail('retained_operator_review_required')
        with self.provider.store._lock,self.provider.store._file_lock(exclusive=True):
            db=self._connect()
            try:
                db.execute('BEGIN IMMEDIATE')
                rows,rejected,missing,plan=self._derive(self.provider.store._read(),db)
                if plan!=review.plan_sha256:_fail('issued_authorization_changed_since_review')
                if not rows:_fail('no_current_owner_families_to_project')
                owner_projection_receipt_schema(db)
                prior=db.execute('SELECT receipt_json FROM rkb_oauth_owner_projection_migrations WHERE plan_sha256=?',(plan,)).fetchone()
                if prior is not None:
                    if missing:_fail('migrated_projection_missing')
                    receipt=json.loads(prior[0]);db.rollback()
                    _LOG.info('oauth_owner_projection state=reconciled plan_sha256=%s',plan)
                    return receipt
                fields=('id','person_id','client_id','resource','credential_id','credential_epoch','expires_at','enabled','issuer')
                for row in rows:
                    if db.execute('SELECT 1 FROM rkb_request_oauth_grants WHERE id=?',(row['id'],)).fetchone() is None:
                        db.execute('INSERT INTO rkb_request_oauth_grants('+','.join(fields)+') VALUES('+','.join('?' for _ in fields)+')',tuple(row[k] for k in fields))
                if self._preflight(db,rows):_fail('projection_readback_failed')
                receipt=dict(codec='rkb.owner-issued-family-projection-receipt.v1',plan_sha256=plan,
                    applied_at=datetime.now(timezone.utc).isoformat(),projected_families=missing,
                    preserved_existing=len(rows)-missing,rejected=dict(rejected),issued_tokens=0,
                    changed_participant_grants=0,state_or_key_modified=False)
                db.execute('INSERT INTO rkb_oauth_owner_projection_migrations VALUES(?,?)',(plan,_canonical(receipt).decode()))
                db.commit()
                _LOG.info('oauth_owner_projection state=committed plan_sha256=%s projected=%s preserved=%s',plan,missing,len(rows)-missing)
                return receipt
            except BaseException:
                db.rollback();raise
            finally:db.close()
