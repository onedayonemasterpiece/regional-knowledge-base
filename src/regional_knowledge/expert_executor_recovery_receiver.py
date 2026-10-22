"""Bounded original expert Dataset delivery reconciliation for a provisioned executor.

No provider send/readback, new request claim, OAuth/IAM, credential mint or
scheduler. Existing TargetExpertDeliveryControl alone may reconstruct a
remotely COVERED result/send/native-receipt chain after local control loss.
UNKNOWN and uncovered receipts never create another send opportunity.
"""
from __future__ import annotations

import asyncio
import json
from uuid import UUID

from starlette.responses import JSONResponse

from .expert_executor_workflow import ExpertExecutorContext, _matches
from .expert_request_control import RequestControlError


PATH='/internal/rkb/expert-executor-recovery'
CODEC='rkb.expert-executor-recovery.v1'
MAX_BODY=1024
_STATES=frozenset({'recovery_review_required','recovered_result_ready',
                   'unknown','delivered','pending'})
_REASONS=frozenset({
    'intake_exact_coverage','current_verified_identity_or_capability',
    'accepted_original_result_record','original_send_disposition',
    'provider_native_disposition_or_covered_receipt',
    'original_receipt_remote_coverage',
})


def _response(value, code=200):
    return JSONResponse(value,status_code=code,
        headers={'Cache-Control':'no-store','Pragma':'no-cache'})


def _unique(pairs):
    d={}
    for key,value in pairs:
        if key in d:raise ValueError('duplicate field')
        d[key]=value
    return d


def _parse(raw):
    if type(raw) is not bytes or not 1<=len(raw)<=MAX_BODY:
        raise ValueError('bounded recovery identity required')
    try:
        value=json.loads(raw,object_pairs_hook=_unique,
                         parse_constant=lambda v:(_ for _ in ()).throw(
                             ValueError('nonfinite JSON')))
    except (ValueError,TypeError,UnicodeError,OverflowError) as exc:
        raise ValueError('invalid recovery input') from exc
    if (type(value) is not dict
            or set(value)!={'codec','request_id'}
            or value['codec']!=CODEC
            or type(value['request_id']) is not str
            or len(value['request_id'])!=36):
        raise ValueError('exact recovery input required')
    try:
        if str(UUID(value['request_id']))!=value['request_id']:
            raise ValueError('noncanonical request identity')
    except (ValueError,TypeError) as exc:
        raise ValueError('noncanonical request identity') from exc
    return value['request_id']


class ExpertExecutorRecoveryReceiver:
    """Current original executor, no extra permit or caller-chosen outcome."""
    def __init__(self,context: ExpertExecutorContext):
        if type(context) is not ExpertExecutorContext:
            raise TypeError('existing exact executor context required')
        self.context=context
        self.control=context.delivery.control

    async def __call__(self,request):
        if not _matches(request,self.context.credentials):
            return _response({'error':'executor_authorization_required'},403)
        try:
            async with asyncio.timeout(2.5):
                data=bytearray()
                async for chunk in request.stream():
                    data.extend(chunk)
                    if len(data)>MAX_BODY:
                        return _response({'error':'recovery_bound'},413)
                rid=_parse(bytes(data))
                if self.context.delivery.dataset_effects is None:
                    return _response({'error':'verified_dataset_required'},503)
                result=await asyncio.to_thread(
                    self.context.delivery.reconcile_restored_dataset_delivery,
                    self.context.credentials,rid)
                if type(result) is not dict or result.get('retry_send_allowed') is not False:
                    raise PermissionError('unsafe delivery recovery response')
                state=result.get('state')
                if state=='delivered' and result.get('remote_covered') is not True:
                    # A local old delivered flag without exact captured native
                    # receipt MUST NOT be returned as a verified delivery.
                    state='pending'
                    reason='original_receipt_remote_coverage'
                else:
                    reason=result.get('missing_contract')
                    if state not in _STATES:
                        state='pending'
                if reason not in _REASONS:
                    reason=None
                payload=dict(codec=CODEC,state=state,
                    retry_send_allowed=False,
                    native_receipt_covered=bool(
                        state=='delivered' and result.get('remote_covered') is True))
                if reason:payload['reason']=reason
                return _response(payload,200 if state=='delivered' else 202)
        except asyncio.CancelledError:raise
        except TimeoutError:
            return _response({'error':'recovery_pending'},503)
        except RequestControlError as exc:
            if exc.code=='service_denied':
                return _response({'error':'executor_authorization_required'},403)
            return _response({'error':'recovery_pending'},503)
        except ValueError:
            return _response({'error':'invalid_recovery_request'},400)
        except Exception:
            # The target's native bytes/outcome may be UNKNOWN. Never infer
            # "not sent" or issue a retry permit from a missing local record.
            return _response({'error':'recovery_pending'},503)
