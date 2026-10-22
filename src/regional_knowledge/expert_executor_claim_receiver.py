"""Opt-in bounded trusted executor claim receiver over original Expert request control.

The caller is an already owner-provisioned SAME-HOST executor service.
This endpoint only grants the existing local lease/generation for a remotely
accepted expert request. It does not execute a model, query, send a Slack
message, mint credentials, persist provider effects or create another queue.
The source is deliberately inert until registered by the existing server owner.
"""
from __future__ import annotations

import asyncio
import json
from uuid import UUID

from starlette.responses import JSONResponse

from .expert_request_control import (
    ExecutorCredentials, RequestControlError, TargetExpertRequestControl,
)


PATH = '/internal/rkb/expert-executor-claim'
CODEC = 'rkb.expert-executor-claim.v1'
MAX_BODY = 2048
_MAX_LEASE_SECONDS = 300


def _json(value, code=200):
    return JSONResponse(value,status_code=code,
        headers={'Cache-Control':'no-store','Pragma':'no-cache'})


def _unique(pairs):
    output={}
    for key,value in pairs:
        if key in output:
            raise ValueError('duplicate JSON field')
        output[key]=value
    return output


def _request(raw):
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_BODY:
        raise ValueError('bounded claim input required')
    try:
        value=json.loads(raw,object_pairs_hook=_unique,
                         parse_constant=lambda _: (_ for _ in ()).throw(
                             ValueError('nonfinite claim input')))
    except (UnicodeError,TypeError,OverflowError,ValueError) as exc:
        raise ValueError('invalid claim input') from exc
    if (type(value) is not dict
            or set(value)!={'codec','request_id','lease_seconds'}
            or value['codec']!=CODEC):
        raise ValueError('exact claim envelope required')
    request_id=value['request_id']
    if request_id is not None:
        if (type(request_id) is not str or len(request_id)!=36):
            raise ValueError('canonical request id required')
        try:
            if str(UUID(request_id))!=request_id:
                raise ValueError('canonical request id required')
        except ValueError as exc:
            raise ValueError('canonical request id required') from exc
    lease=value['lease_seconds']
    if type(lease) is not int or not 1<=lease<=_MAX_LEASE_SECONDS:
        raise ValueError('bounded exact lease required')
    return request_id,lease


def _credentials(request):
    # Never trust X-Forwarded-For/Host from the HTTP caller. A service token
    # is a second, independent requirement enforced by _service in claim().
    client=request.scope.get('client')
    service_id=request.headers.get('x-rkb-expert-service','')
    bearer=request.headers.get('authorization','')
    token=bearer[7:] if bearer.startswith('Bearer ') else ''
    if (not client or client[0]!='127.0.0.1'
            or type(service_id) is not str or not 1<=len(service_id)<=160
            or not service_id.isascii()
            or type(token) is not str or not 8<=len(token)<=256
            or not token.isascii()):
        raise PermissionError('verified local executor required')
    return ExecutorCredentials(service_id,token)


def _lease_response(result):
    """Return only bounded service-authorized control fields, never request text."""
    if type(result) is not dict or type(result.get('state')) is not str:
        raise ValueError('exact claim result required')
    state=result['state']
    if state=='leased':
        if (type(result.get('request_id')) is not str
                or type(result.get('generation')) is not int
                or result['generation']<1
                or type(result.get('execution_token')) is not str
                or not 32<=len(result['execution_token'])<=256):
            raise ValueError('exact lease identity required')
        output=dict(codec=CODEC,state='leased',
            request_id=result['request_id'],generation=result['generation'],
            execution_token=result['execution_token'])
        if 'resume_state' in result:
            if result['resume_state'] not in (
                'result_unsealed','result_ready','send_intent_ready'):
                raise ValueError('unknown recovered claim state')
            output['resume_state']=result['resume_state']
        return output
    if state=='empty' and set(result)=={'state'}:
        return dict(codec=CODEC,state='empty')
    if state=='deferred' and set(result)<= {'state','reason'}:
        if 'reason' in result and result['reason']!='request_restore_pending':
            raise ValueError('unknown claim deferral')
        output=dict(codec=CODEC,state='deferred')
        if 'reason' in result:output['reason']=result['reason']
        return output
    if state=='blocked' and set(result)=={'state','request_id'}:
        return dict(codec=CODEC,state='blocked')
    raise ValueError('unknown claim disposition')


class ExpertExecutorClaimReceiver:
    """Transport for an already provisioned executor; no new service authority."""

    def __init__(self, control: TargetExpertRequestControl):
        if type(control) is not TargetExpertRequestControl:
            raise TypeError('existing exact expert request controller required')
        self.control=control

    async def __call__(self, request):
        try:
            credential=_credentials(request)
        except PermissionError:
            return _json({'error':'executor_authorization_required'},403)
        try:
            async with asyncio.timeout(2.5):
                raw=bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw)>MAX_BODY:
                        return _json({'error':'claim_request_bound'},413)
                request_id,lease=_request(bytes(raw))
                # claim() itself validates the existing service token, exact
                # remote intent/Dataset coverage, current grants and fair head.
                # A timeout can strand only a local lease; there is NO send.
                outcome=await asyncio.to_thread(
                    self.control.claim,credential,
                    request_id=request_id,lease_seconds=lease)
                return _json(_lease_response(outcome))
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return _json({'error':'claim_pending'},503)
        except RequestControlError as exc:
            return _json({'error':'executor_authorization_required'}
                if exc.code=='service_denied' else {'error':'claim_pending'},
                403 if exc.code=='service_denied' else 503)
        except ValueError:
            return _json({'error':'invalid_claim_request'},400)
        except Exception:
            # Unknown local claim outcome is never permission to invent a
            # second lease, replay a provider send, or reveal source contents.
            return _json({'error':'claim_pending'},503)
