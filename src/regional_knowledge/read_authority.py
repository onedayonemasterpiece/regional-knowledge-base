"""Short read authority from existing verified OAuth/participant identities.

Internal controller API only. Adapter identity and destination come from its
verified transport context, never tool arguments. Context references are bounded
RAM locators, not grants or durable request/executor authority.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass, field
import math
import secrets
import sqlite3
import time
import threading
from typing import Literal
from uuid import UUID

from .contracts import Principal
from .participants import Participants, ParticipantError, RequestAuthority, CAPABILITIES, EXPERT_CAPABILITIES
from .request_grants import allowed


class ReadAuthorityError(PermissionError):
    pass


def _bounded(value):
    if not isinstance(value, str) or not value or len(value) > 256 or len(value.encode()) > 256:
        raise ValueError('bounded trusted context identity required')


@dataclass(frozen=True)
class TrustedReadDestination:
    """Constructed only by the authenticated adapter's server-side routing."""
    transport: str
    audience: Literal['personal', 'group', 'public']
    destination_ref: str

    def __post_init__(self):
        _bounded(self.transport)
        _bounded(self.destination_ref)
        if self.audience not in {'personal', 'group', 'public'}:
            raise ValueError('unsupported trusted audience')
        _bounded(self.key)

    @property
    def key(self):
        return f'{self.transport}:{self.audience}:{self.destination_ref}'


@dataclass(frozen=True)
class VerifiedReadAuthority:
    """Only an identical object retained by its resolver can be rechecked."""
    context_ref: str
    person_id: str
    route: str
    auth_revision: int
    destination: TrustedReadDestination
    expires_at: float
    issuer: str
    resource: str
    client_id: str
    oauth_family_ref: str | None = field(repr=False)
    link_id: str | None = None
    link_revision: int | None = None

    @property
    def public_only(self):
        return self.destination.audience != 'personal'


@dataclass(frozen=True)
class _Current:
    revision: int
    capabilities: frozenset[str]
    categories: frozenset[str]


class ReadAuthority:
    def __init__(self, participants: Participants, *, oauth_provider, issuer: str,
                 resource: str, clock=time.time, ttl_seconds=120.0, max_contexts=256):
        if (oauth_provider.issuer != issuer or oauth_provider.resource != resource
                or oauth_provider.owner_subject != participants.owner):
            raise ValueError('configured OAuth authority must match exactly')
        if (not isinstance(ttl_seconds, (int, float)) or isinstance(ttl_seconds, bool)
                or not math.isfinite(ttl_seconds) or ttl_seconds <= 0
                or type(max_contexts) is not int or not 1 <= max_contexts <= 4096):
            raise ValueError('finite context lifetime and bounded RAM capacity required')
        if (oauth_provider.personal_policy is None
                or oauth_provider.personal_policy.participants is not participants):
            raise ValueError('explicit existing participant projection policy required')
        self.participants = participants
        self.provider = oauth_provider
        self.issuer, self.resource = issuer, resource
        self.clock, self.ttl_seconds = clock, float(ttl_seconds)
        self.max_contexts = max_contexts
        self._contexts = {}
        self._contexts_lock = threading.Lock()

    @contextmanager
    def _read(self):
        db = sqlite3.connect(self.participants.corpus.path.resolve().as_uri() + '?mode=ro',
                             uri=True, timeout=.1)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            yield db
        finally:
            db.close()

    def _state(self, principal, route, link_id=None, link_revision=None, family_ref=None):
        context = RequestAuthority(principal, route, link_id, link_revision)
        try:
            with self._read() as db:
                actor = self.participants.require(context, 'public_search', connection=db)
                if family_ref is not None and not allowed(
                        db, family_ref, actor, self.resource, self.clock(),
                        client_id=principal.client_id, issuer=self.issuer):
                    raise ReadAuthorityError('OAuth family is no longer current')
                profile = db.execute('SELECT revision,payload FROM rkb_participant_profiles WHERE person_id=?',
                                     (actor,)).fetchone()
                import json
                categories = frozenset(json.loads(profile['payload']).get('categories', ())) if profile else frozenset()
                if actor == self.participants.owner:
                    capabilities = CAPABILITIES
                    if route == 'expert':
                        capabilities = capabilities & EXPERT_CAPABILITIES
                else:
                    capabilities = frozenset(r['capability'] for r in db.execute(
                        'SELECT capability,expires_at FROM rkb_participant_grants WHERE person_id=?', (actor,))
                        if not self.participants._expired(r['expires_at']))
                return _Current(profile['revision'] if profile else 0, frozenset(capabilities), categories)
        except (ParticipantError, sqlite3.Error, ValueError, TypeError):
            raise ReadAuthorityError('current participant authority unavailable') from None

    def _retain(self, *, person_id, route, current, destination, expires_at,
                client_id, family_ref=None, link_id=None, link_revision=None):
        if not isinstance(destination, TrustedReadDestination):
            raise ReadAuthorityError('trusted destination required')
        now = self.clock()
        authority = VerifiedReadAuthority(secrets.token_urlsafe(24), person_id, route,
            current.revision, destination, min(expires_at, now + self.ttl_seconds),
            self.issuer, self.resource, client_id, family_ref, link_id, link_revision)
        with self._contexts_lock:
            for key, value in tuple(self._contexts.items()):
                if value.expires_at <= now:
                    del self._contexts[key]
            if len(self._contexts) >= self.max_contexts or expires_at <= now:
                raise ReadAuthorityError('read context unavailable or capacity exhausted')
            self._contexts[authority.context_ref] = authority
        return authority

    async def resolve_oauth(self, bearer: str, *, destination: TrustedReadDestination):
        """Verify actual existing opaque credentials; UUID/JWT projections do not authenticate."""
        if not isinstance(destination, TrustedReadDestination):
            raise ReadAuthorityError('trusted destination required')
        if not isinstance(bearer, str) or not 1 <= len(bearer) <= 8192:
            raise ReadAuthorityError('verified opaque OAuth credential required')
        token = await self.provider.load_access_token(bearer)
        if (token is None or token.claims.get('iss') != self.issuer
                or token.resource != self.resource or not token.client_id
                or token.expires_at is None or token.expires_at <= self.clock()):
            raise ReadAuthorityError('verified OAuth authority unavailable')
        family_ref = token.claims.get('rkb_request_grant')
        if not isinstance(family_ref, str) or len(family_ref) != 64:
            raise ReadAuthorityError('verified OAuth family projection required')
        try:
            actor = str(UUID(token.subject))
        except (TypeError, ValueError, AttributeError):
            raise ReadAuthorityError('canonical person required') from None
        principal = Principal(subject=actor, client_id=token.client_id, issuer=self.issuer, access_token='')
        route = 'owner' if actor == self.participants.owner else 'personal'
        current = await asyncio.to_thread(self._state, principal, route, family_ref=family_ref)
        return self._retain(person_id=actor, route=route, current=current, destination=destination,
                            expires_at=float(token.expires_at), client_id=token.client_id, family_ref=family_ref)

    async def resolve_adapter(self, *, provider: str, tenant: str, provider_subject: str,
                              destination: TrustedReadDestination):
        """Trusted adapter hook after provider transport verification; never an MCP tool."""
        if not isinstance(destination, TrustedReadDestination):
            raise ReadAuthorityError('trusted destination required')
        for value in (provider, tenant, provider_subject):
            _bounded(value)
        def resolve():
            with self._read() as db:
                context = self.participants.resolve_external(provider=provider, tenant=tenant,
                    provider_subject=provider_subject, principal_factory=lambda person: Principal(
                        subject=person, client_id=destination.transport, issuer=provider, access_token=''), connection=db)
                current = self._state(context.principal, context.route, context.link_id, context.link_revision)
                return context, current
        try:
            context, current = await asyncio.to_thread(resolve)
        except (ParticipantError, sqlite3.Error, ValueError, TypeError):
            raise ReadAuthorityError('verified adapter identity unavailable') from None
        return self._retain(person_id=context.principal.subject, route=context.route, current=current,
            destination=destination, expires_at=self.clock() + self.ttl_seconds,
            client_id=context.principal.client_id, link_id=context.link_id, link_revision=context.link_revision)

    async def recheck(self, authority: VerifiedReadAuthority):
        """Call immediately before delayed output; current grants and expiry are read again."""
        with self._contexts_lock:
            retained = (isinstance(authority, VerifiedReadAuthority)
                        and self._contexts.get(authority.context_ref) is authority)
        if not retained or authority.expires_at <= self.clock():
            raise ReadAuthorityError('read context expired or unknown')
        principal = Principal(subject=authority.person_id, client_id=authority.client_id,
                              issuer=authority.issuer, access_token='')
        current = await asyncio.to_thread(self._state, principal, authority.route,
            authority.link_id, authority.link_revision, authority.oauth_family_ref)
        with self._contexts_lock:
            retained = self._contexts.get(authority.context_ref) is authority
        if (not retained or current.revision != authority.auth_revision
                or authority.expires_at <= self.clock()):
            raise ReadAuthorityError('participant authority changed during read')
        return current

    async def require(self, authority: VerifiedReadAuthority, capability: str):
        current = await self.recheck(authority)
        if (capability not in {'public_search', 'case_read', 'scan_proof_read'}
                or capability not in current.capabilities
                or (authority.public_only and capability != 'public_search')):
            raise ReadAuthorityError('read capability denied')
        return authority.person_id

    async def artifact_eligibility(self, authority: VerifiedReadAuthority, artifact_class: str):
        """Capability eligibility only; source/purpose scope must be proved by the output gate."""
        current = await self.recheck(authority)
        if artifact_class in {'accepted_public_text', 'public_illustration'}:
            return True
        if artifact_class == 'scan_proof':
            if (not authority.public_only and 'scan_proof_read' in current.capabilities
                    and (authority.person_id == self.participants.owner
                         or current.categories & {'demo', 'expert', 'local_historian'})):
                return True
        # Full originals/private drafts require their independent resource policy.
        raise ReadAuthorityError('independent artifact authorization required')

    def release(self, authority: VerifiedReadAuthority):
        with self._contexts_lock:
            if self._contexts.get(authority.context_ref) is authority:
                del self._contexts[authority.context_ref]
