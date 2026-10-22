"""An explicit existing-service Expert claim -> Dataset result/send-intent bridge.

This is not an executor, sender, scheduler, IAM or token issuer. It only
connects already provisioned ExecutorCredentials and the existing
TargetExpertDeliveryControl to the SAME signed ingress/Participants/spool.
Without an exact installed context, neither HTTP route is registered.
A single invocation advances one bounded checkpoint boundary and returns
pending until the original Dataset bytes are verifiably covered. No provider
send, fabricated NativeSlackReceipt, or local-only Saved is possible here.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hmac
import json
from uuid import UUID

from starlette.responses import JSONResponse

from .expert_executor_claim_receiver import (
    ExpertExecutorClaimReceiver, _credentials, _json as claim_json,
)
from .expert_request_control import (
    ExecutorCredentials, RequestControlError, TargetExpertRequestControl,
)
from .expert_delivery_control import TargetExpertDeliveryControl
from .expert_slack_ingress import ExpertSlackIngress
from .kaggle_service_startup import TargetServiceStartup


PATH = '/internal/rkb/expert-executor-result'
CODEC = 'rkb.expert-executor-result.v1'
MAX_BODY = 28672


@dataclass(frozen=True)
class ExpertExecutorContext:
    """Reference to owner-provisioned EXISTING executor and delivery authority."""
    credentials: ExecutorCredentials
    delivery: TargetExpertDeliveryControl

    def __post_init__(self):
        if (type(self.credentials) is not ExecutorCredentials
                or type(self.delivery) is not TargetExpertDeliveryControl):
            raise TypeError('existing exact executor and source-gated delivery required')


def _matches(request, configured):
    try:
        actual=_credentials(request)
    except PermissionError:
        return False
    return (hmac.compare_digest(actual.service_id,configured.service_id)
            and hmac.compare_digest(actual.token,configured.token))


class BoundExpertClaimReceiver(ExpertExecutorClaimReceiver):
    """Existing PR250 claim, scoped to the one configured service identity."""
    def __init__(self, control: TargetExpertRequestControl, credentials: ExecutorCredentials):
        super().__init__(control)
        if type(credentials) is not ExecutorCredentials:
            raise TypeError('existing provisioned executor required')
        self.credentials=credentials

    async def __call__(self, request):
        if not _matches(request,self.credentials):
            return claim_json({'error':'executor_authorization_required'},403)
        # Parent still verifies current stored service epoch/token, dialog head,
        # native Dataset coverage and current person/permission for each claim.
        return await super().__call__(request)


def _unique(pairs):
    value={}
    for key,item in pairs:
        if key in value:raise ValueError('duplicate field')
        value[key]=item
    return value


def _input(raw):
    if type(raw) is not bytes or not 1<=len(raw)<=MAX_BODY:
        raise ValueError('bounded result required')
    try:
        value=json.loads(raw,object_pairs_hook=_unique,
            parse_constant=lambda _:(lambda: (_ for _ in ()).throw(
                ValueError('nonfinite result')))())
    except (ValueError,UnicodeError,TypeError,OverflowError) as exc:
        raise ValueError('invalid result') from exc
    if (type(value) is not dict
            or set(value)!={'codec','request_id','generation','execution_token','text'}
            or value.get('codec')!=CODEC):
        raise ValueError('exact result request required')
    request_id=value['request_id']
    if type(request_id) is not str or len(request_id)!=36:
        raise ValueError('canonical request identity required')
    try:
        if str(UUID(request_id))!=request_id:raise ValueError('canonical request identity required')
    except (TypeError,ValueError) as exc:
        raise ValueError('canonical request identity required') from exc
    if (type(value['generation']) is not int or value['generation']<1
            or type(value['execution_token']) is not str
            or not 32<=len(value['execution_token'])<=256
            or type(value['text']) is not str
            or not 1<=len(value['text'])<=12000
            or len(value['text'].encode('utf-8'))>24000):
        raise ValueError('bounded execution/result required')
    return value


def _reply(state,*,accepted=False,code=202):
    return JSONResponse(dict(codec=CODEC,state=state,
        remote_accepted=accepted,saved=False,provider_sent=False),
        status_code=code,headers={'Cache-Control':'no-store'})


class ExpertExecutorResultReceiver:
    """One bounded attempt: prepare -> exact Dataset result -> exact send intent."""
    def __init__(self, context: ExpertExecutorContext):
        if type(context) is not ExpertExecutorContext:
            raise TypeError('existing executor context required')
        self.context=context
        self.control=context.delivery.control

    async def _advance(self,value):
        delivery=self.context.delivery
        ctl=self.control
        credentials=self.context.credentials
        rid=value['request_id']
        args=(credentials,rid,value['generation'],value['execution_token'])
        # Dataset is primary. Never save local-only result or a legacy journal
        # proof under a name suggesting remotely covered original child bytes.
        if delivery.dataset_effects is None:
            raise RequestControlError('dataset_effect_store_required')
        await asyncio.to_thread(ctl.claim_context,*args)
        row=await asyncio.to_thread(ctl._row,rid)
        if row['state'] in ('leased','result_unsealed'):
            await asyncio.to_thread(delivery.prepare_result,*args,text=value['text'])
        elif row['state'] in ('result_ready','send_intent_ready'):
            original=json.loads(row['result_json'] or '{}')
            if (original.get('text')!=value['text']
                    or original.get('request_id')!=rid):
                raise RequestControlError('result_conflict')
        else:
            raise RequestControlError('request_fence_denied')
        await delivery.stage_dataset_result(*args)
        result=await asyncio.to_thread(delivery.confirm_dataset_result,*args)
        if result.get('remote_accepted') is not True:
            return _reply('result_pending')
        await delivery.stage_dataset_send(*args)
        send=await asyncio.to_thread(delivery.confirm_dataset_send,*args)
        if send.get('remote_accepted') is not True:
            return _reply('send_intent_pending',accepted=True)
        # This is only proof of the ORIGINAL send intent in Dataset. No
        # claim_send_once, provider post, receipt, or false delivery ACK here.
        return _reply('send_intent_ready',accepted=True,code=200)

    async def __call__(self, request):
        if not _matches(request,self.context.credentials):
            return claim_json({'error':'executor_authorization_required'},403)
        try:
            async with asyncio.timeout(2.5):
                raw=bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw)>MAX_BODY:
                        return claim_json({'error':'result_bound'},413)
                return await self._advance(_input(bytes(raw)))
        except asyncio.CancelledError:raise
        except TimeoutError:
            return claim_json({'error':'result_persistence_pending'},503)
        except RequestControlError as exc:
            if exc.code in ('service_denied','identity_changed','slack_destination_revoked'):
                return claim_json({'error':'executor_authorization_required'},403)
            if exc.code in ('result_conflict','result_intent_conflict','send_intent_conflict'):
                return claim_json({'error':'result_conflict'},409)
            return claim_json({'error':'result_persistence_pending'},503)
        except ValueError:
            return claim_json({'error':'invalid_result'},400)
        except Exception:
            return claim_json({'error':'result_persistence_pending'},503)


def compose_bound_expert_executor(*,target_startup,expert_slack_ingress,context):
    """Fail closed for absent/mismatched existing authority/credential context."""
    if (type(target_startup) is not TargetServiceStartup
            or type(expert_slack_ingress) is not ExpertSlackIngress
            or type(context) is not ExpertExecutorContext):
        raise TypeError('existing target, signed ingress and executor context required')
    ctl=expert_slack_ingress.control
    cfg=target_startup.config
    if (context.delivery.control is not ctl
            or ctl.spool is not cfg.spool
            or ctl.people is not cfg.participants
            or cfg.queue is not ctl.spool.queue
            or target_startup.authority.participants is not ctl.people
            or target_startup.backend.read_authority is not target_startup.authority):
        raise ValueError('expert executor authority does not match signed ingress/target')
    # Exact already-owner-provisioned service; read-only, no credential creation.
    with ctl.corpus.connect() as db:
        ctl._service(db,context.credentials)
    return BoundExpertClaimReceiver(ctl,context.credentials),ExpertExecutorResultReceiver(context)
