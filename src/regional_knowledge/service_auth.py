"""Technical credentials in the existing OAuth authority, never human identity."""
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import logging
import math
import re
import secrets
from uuid import UUID, uuid4


_LOG=logging.getLogger(__name__)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def service_scope(value):
    if (type(value) is not dict or set(value) != {'operation','provider','provider_tenant','workspace_ids'}
            or value['operation'] != 'knowledge_search' or value['provider'] != 'projects_hub'
            or type(value['provider_tenant']) is not str
            or not re.fullmatch(r'[A-Za-z0-9._:/-]{1,120}',value['provider_tenant'])
            or type(value['workspace_ids']) is not list or not 1 <= len(value['workspace_ids']) <= 32
            or any(type(item) is not str or not re.fullmatch(r'ws_[A-Za-z0-9_-]{1,250}',item) for item in value['workspace_ids'])
            or len(set(value['workspace_ids'])) != len(value['workspace_ids'])):
        raise ValueError('fixed bounded service scope required')
    return {**value,'workspace_ids':sorted(value['workspace_ids'])}


def new_service_token():
    ident = str(uuid4())
    return ident, 'rkbs_'+UUID(ident).hex+'.'+secrets.token_urlsafe(32)


def service_token_id(token):
    if type(token) is not str or not re.fullmatch(r'rkbs_[a-f0-9]{32}\.[A-Za-z0-9_-]{43}',token):
        return None
    return str(UUID(hex=token[5:37]))


@dataclass(frozen=True)
class VerifiedServiceCredential:
    credential_id: str
    epoch: int
    service_id: str
    scope_sha256: str
    expires_at: float
    token_sha256: str = field(repr=False)


class ServiceOAuthMixin:
    """Owner-only local administration; no token endpoint or new IAM/queue."""
    def _service_admin(self, context):
        if self.personal_policy is None:
            raise ValueError('existing participant administration required')
        return self.personal_policy.require_admin(context)

    @staticmethod
    def _service_metadata(ident, row):
        return dict(credential_id=ident,service_id=row['service_id'],epoch=row['epoch'],enabled=row['enabled'],
            scope=row['scope'],expires_at=row['expires_at'])

    def issue_service_credential(self, context, *, service_id, token, scope, expires_at, reason):
        actor = self._service_admin(context)
        ident = service_token_id(token)
        scope = service_scope(scope)
        if (ident is None or type(service_id) is not str or not re.fullmatch(r'[A-Za-z0-9._-]{1,80}',service_id)
                or type(reason) is not str or not 1 <= len(reason) <= 3000
                or type(expires_at) not in (int,float) or not math.isfinite(expires_at)
                or not self.clock() < expires_at <= self.clock()+366*86400):
            raise ValueError('bounded dedicated service credential required')
        expected = dict(service_id=service_id,secret_hash=hashlib.sha256(token.encode()).hexdigest(),scope=scope,
            expires_at=expires_at,created_by=actor,reason=reason)
        def write(state):
            self._service_admin(context)
            prior = state['service_credentials'].get(ident)
            if prior is not None:
                if any(prior.get(key) != value for key,value in expected.items()) or not prior['enabled']:
                    raise ValueError('service credential identity conflict')
                return self._service_metadata(ident,prior)
            if len(state['service_credentials']) >= 256:
                raise ValueError('service credential capacity')
            row = dict(expected,enabled=True,epoch=1,created_at=self.clock())
            state['service_credentials'][ident]=row
            state['service_audit'][ident+':1'] = dict(action='issued',credential_id=ident,service_id=service_id,
                epoch=1,actor=actor,at=self.clock(),reason=reason,scope_sha256=hashlib.sha256(canonical(scope)).hexdigest())
            return self._service_metadata(ident,row)
        result=self.store.mutate(write)
        # Same encrypted authority readback, never an inferred remote receipt.
        observed=self.store.read(lambda state:self._service_metadata(ident,state['service_credentials'][ident]))
        if observed != result:raise ValueError('service credential readback changed; reconcile same identity')
        _LOG.info('service credential issue committed',extra={'event':'service_credential_admin','action':'issue',
            'credential_id':ident,'epoch':result['epoch']})
        return result

    def service_credentials(self,context,*,service_id=None):
        self._service_admin(context)
        return self.store.read(lambda state:[self._service_metadata(ident,row) for ident,row in state['service_credentials'].items()
            if service_id is None or row['service_id']==service_id])

    def service_audit(self,context,*,credential_id):
        self._service_admin(context)
        ident=str(UUID(credential_id))
        return self.store.read(lambda state:[dict(row) for row in state['service_audit'].values() if row['credential_id']==ident])

    def revoke_service_credential(self,context,credential_id,*,expected_epoch,reason):
        actor=self._service_admin(context);ident=str(UUID(credential_id))
        if type(expected_epoch) is not int or expected_epoch<1 or type(reason) is not str or not 1<=len(reason)<=3000:
            raise ValueError('exact revocation precondition and reason required')
        def write(state):
            self._service_admin(context)
            row=state['service_credentials'].get(ident)
            if row is None:raise ValueError('service credential unavailable')
            if not row['enabled'] and row['epoch']==expected_epoch+1 and row.get('revoked_reason')==reason:
                return self._service_metadata(ident,row)
            if row['epoch']!=expected_epoch or not row['enabled']:
                raise ValueError('service credential epoch conflict')
            row.update(enabled=False,epoch=expected_epoch+1,revoked_reason=reason)
            state['service_audit'][ident+':'+str(row['epoch'])]=dict(action='revoked',credential_id=ident,
                service_id=row['service_id'],epoch=row['epoch'],actor=actor,at=self.clock(),reason=reason,
                scope_sha256=hashlib.sha256(canonical(row['scope'])).hexdigest())
            return self._service_metadata(ident,row)
        result=self.store.mutate(write)
        observed=self.store.read(lambda state:self._service_metadata(ident,state['service_credentials'][ident]))
        if observed != result:raise ValueError('service revoke readback changed; reconcile same epoch')
        _LOG.info('service credential revoke committed',extra={'event':'service_credential_admin','action':'revoke',
            'credential_id':ident,'epoch':result['epoch']})
        return result

    def verify_service_credential(self,token,*,service_id,credential_id,epoch,provider_tenant,workspace_id):
        if service_token_id(token)!=credential_id or type(epoch) is not int or epoch<1:return None
        secret_hash=hashlib.sha256(token.encode()).hexdigest()
        def read(state):
            row=state['service_credentials'].get(credential_id)
            if (row is None or not row['enabled'] or row['epoch']!=epoch or row['service_id']!=service_id
                    or row['expires_at']<=self.clock() or not hmac.compare_digest(row['secret_hash'],secret_hash)
                    or row['scope']['operation']!='knowledge_search' or row['scope']['provider']!='projects_hub'
                    or row['scope']['provider_tenant']!=provider_tenant or workspace_id not in row['scope']['workspace_ids']):
                return None
            return VerifiedServiceCredential(credential_id,epoch,service_id,
                hashlib.sha256(canonical(row['scope'])).hexdigest(),row['expires_at'],secret_hash)
        try:return self.store.try_read(read)
        except Exception:return None

    def service_credential_current(self,verified):
        if type(verified) is not VerifiedServiceCredential:return False
        def read(state):
            row=state['service_credentials'].get(verified.credential_id)
            return bool(row and row['enabled'] and row['epoch']==verified.epoch
                and row['service_id']==verified.service_id and row['expires_at']==verified.expires_at
                and row['expires_at']>self.clock() and hmac.compare_digest(row['secret_hash'],verified.token_sha256)
                and hashlib.sha256(canonical(row['scope'])).hexdigest()==verified.scope_sha256)
        try:return bool(self.store.try_read(read))
        except Exception:return False
