"""Opt-in same-host delegated expert search over existing RKB target read gates.

Never accepts user/person/source text from the caller: query, actor, scope and
destination come from an already-verified, remotely accepted request. Reuses
the existing ReadAuthority + KaggleSourceBackend + ReadIssuanceMiddleware,
not a new evaluator, source/SQLite corpus, OAuth bearer or provider.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time

from mcp.server.mcpserver.exceptions import ToolError
from starlette.responses import JSONResponse

from .expert_request_control import ExecutorCredentials, TargetExpertRequestControl
from .kaggle_public_backend import KaggleSourceBackend
from .projects_hub_reader import (
    ReadContext, _connected_read, render as evidence_render, failure as evidence_failure,
)
from .read_authority import TrustedReadDestination
from .read_issuance import capture_read_output, bind_json_output

PATH = '/internal/rkb/expert-evidence'
CODEC = 'rkb.expert-evidence.v1'
MAX_REQUEST = 8192


def _render(value, context):
    envelope = evidence_render(value, context)
    return dict(envelope, codec=CODEC)


def _failure(context, reason='context_gap', status='unavailable'):
    return dict(evidence_failure(context, reason, status), codec=CODEC)


class _RequestFence:
    """Composable final gate: revoke the request and source independently."""
    def __init__(self, gate, current):
        self.gate, self.current = gate, current

    async def refresh(self, gated):
        if not self.current():
            raise PermissionError('expert request no longer current')
        return await self.gate.refresh(gated)

    def issuance_current(self, ticket):
        return self.current() and self.gate.issuance_current(ticket)

    def release(self, ticket):
        self.gate.release(ticket)


class ExpertEvidenceReceiver:
    """Explicitly provisioned internal route; inert until attached to server."""
    def __init__(self, *, control, backend):
        if (type(control) is not TargetExpertRequestControl
                or not isinstance(backend, KaggleSourceBackend)
                or control.spool.queue is not backend.worker_queue
                or control.people is not backend.read_authority.participants
                or not callable(getattr(backend,'search_adapter',None))):
            raise ValueError('existing canonical target queue/identity required')
        self.control, self.backend = control, backend
        self.authority = backend.read_authority

    def _input(self, payload):
        if (type(payload) is not dict or not {
                'codec','request_id','generation','execution_token',
                'min_data_seq','deadline_monotonic'}.issubset(payload)
                or set(payload)-{'codec','request_id','generation','execution_token',
                                 'min_data_seq','deadline_monotonic','top_k'}
                or payload['codec']!=CODEC):
            raise ValueError('exact delegated request required')
        if (type(payload['request_id']) is not str
                or not 1<=len(payload['request_id'])<=128
                or type(payload['generation']) is not int or payload['generation']<1
                or type(payload['execution_token']) is not str
                or not 32<=len(payload['execution_token'])<=256
                or type(payload['min_data_seq']) is not int
                or not 0<=payload['min_data_seq']<2**63
                or type(payload['deadline_monotonic']) not in (int,float)
                or not math.isfinite(payload['deadline_monotonic'])
                or payload['deadline_monotonic']>time.monotonic()+2.05
                or type(payload.get('top_k',3)) is not int or not 1<=payload.get('top_k',3)<=10):
            raise ValueError('invalid delegated request bounds')
        return ReadContext(payload['request_id'],payload['generation'],
                           payload['min_data_seq'],payload['deadline_monotonic'])

    @staticmethod
    def _destination(origin, person_id, request_id):
        # Stored verified Slack origin; not arbitrary caller-provided audience.
        audience='group' if origin['channel_type']=='group' else 'personal'
        key=json.dumps([origin['app_id'],origin['team_id'],origin['channel_id'],
                        origin['thread_ts'],person_id,request_id],separators=(',',':'))
        ref=hashlib.sha256(key.encode()).hexdigest()
        return TrustedReadDestination('slack',audience,ref)

    async def __call__(self, request):
        client=request.scope.get('client')
        token=request.headers.get('authorization','')
        token=token[7:] if token.startswith('Bearer ') else ''
        service_id=request.headers.get('x-rkb-expert-service','')
        if (not client or client[0]!='127.0.0.1'
                or not 1<=len(service_id)<=160 or not 8<=len(token)<=256):
            return JSONResponse({'error':'delegated_service_required'},status_code=403)
        credential=ExecutorCredentials(service_id,token)
        context=None
        authority=None
        try:
            raw=bytearray()
            async with asyncio.timeout(2):
                async for part in request.stream():
                    raw.extend(part)
                    if len(raw)>MAX_REQUEST:raise ValueError('bounded request required')
            value=json.loads(raw)
            context=self._input(value)
            if context.deadline_monotonic<=time.monotonic():
                return JSONResponse(_failure(context,'deadline_exceeded'))
            async with asyncio.timeout_at(context.deadline_monotonic):
                # Current service epoch, exact lease and Slack account are
                # checked by canonical Participants under the same control DB.
                claim=await asyncio.to_thread(self.control.claim_context,credential,
                    context.request_id,context.turn_generation,value['execution_token'])
                origin=claim['origin']
                destination=self._destination(origin,claim['person_id'],context.request_id)
                authority=await self.authority.resolve_adapter(
                    provider='slack',tenant=origin['team_id'],
                    provider_subject=origin['user_id'],destination=destination)
                row=await asyncio.to_thread(self.control._row,context.request_id)
                if (authority.person_id!=claim['person_id']
                        or authority.link_id!=row['link_id']
                        or authority.link_revision!=row['link_revision']):
                    raise PermissionError('verified link changed')
                assert context.request_id==claim['request_id']
                with capture_read_output() as calls:
                    transferred,authority=authority,None
                    result=await _connected_read(request,self.backend.search_adapter(
                        claim['input_text'],authority=transferred,
                        request_id=context.request_id,
                        turn_generation=context.turn_generation,
                        min_data_seq=context.min_data_seq,
                        deadline_monotonic=context.deadline_monotonic,
                        destination=destination,limit=value.get('top_k',3)))
                    if len(calls)!=1:
                        raise RuntimeError('retained source provenance required')
                    # The ASGI issuance middleware runs after the handler's
                    # timeout_at scope. The original absolute deadline must
                    # still be current when buffered evidence is emitted.
                    current=lambda:(time.monotonic()<context.deadline_monotonic
                        and self.control.issuance_current(
                            credential,context.request_id,context.turn_generation,
                            value['execution_token'],origin))
                    calls[0].gate=_RequestFence(calls[0].gate,current)
                    bind_json_output(request,calls,context.request_id,
                        lambda output:_render(output,context),
                        lambda:_failure(context))
                    return JSONResponse(_render(result,context),
                                        headers={'Cache-Control':'no-store'})
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return JSONResponse(_failure(context,'deadline_exceeded')
                                if context else {'error':'request_timeout'},
                                status_code=200 if context else 408)
        except PermissionError:
            return JSONResponse(_failure(context,'access_denied','denied')
                                if context else {'error':'delegation_required'},
                                status_code=200 if context else 403)
        except ToolError as exc:
            status,reason='unavailable','not_ready'
            try:
                message=json.loads(str(exc))
                if (message.get('status') in ('unavailable','cancelled','invalid_query','denied')
                        and type(message.get('reason')) is str
                        and message['reason'].replace('_','').islower()):
                    status,reason=message['status'],message['reason']
            except (ValueError,TypeError,AttributeError):
                pass
            return JSONResponse(_failure(context,reason,status)
                                if context else {'error':'unavailable'})
        except Exception:
            return JSONResponse(_failure(context) if context
                                else {'error':'invalid_delegated_request'},
                                status_code=200 if context else 422)
        finally:
            if authority is not None:
                self.authority.release(authority)
