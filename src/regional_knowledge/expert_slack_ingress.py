"""Optional native Slack Events API ingress for existing CAS-F10 request control.

No model, corpus, worker, provider send or journal writer is created here.
Slack's HTTP 200 is issued only after the original event was externally
accepted by the *existing* RecoveryJournal/ControlSpool writer; received-only
local state deliberately returns a retryable 503. Do not enable this route
without the existing accepted-intent reconciler.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time

from starlette.responses import JSONResponse, PlainTextResponse

from .expert_request_control import (
    RequestControlError, TargetExpertRequestControl,
    verified_slack_payload, _verified_slack_observation,
)
from .participants import ParticipantError

PATH='/internal/rkb/expert-slack-events'
MAX_BODY=262144
_MAX_INFLIGHT_STAGES=16  # Only ephemeral work under one HTTP owner.
_LOG=logging.getLogger(__name__)


class ExpertSlackIngress:
    def __init__(self, control: TargetExpertRequestControl, *, signing_secret: str, clock=None):
        if (type(control) is not TargetExpertRequestControl
                or type(signing_secret) is not str
                or not 16<=len(signing_secret)<=256
                or not signing_secret.isascii()
                or not callable(clock or time.time)):
            raise ValueError('exact existing control and private Slack signing secret required')
        self.control=control
        self._signing_secret=signing_secret
        self._clock=clock or time.time
        # Bounded, deduplicated pending CAS tasks. They retain the existing
        # writer fence if Slack's 2.5s HTTP timeout elapses during SQLite lock
        # contention. The next verified event retry reads the original bytes.
        self._staging_tasks={}
        self._reply = None

    def bind_search_reply(self, reply):
        """One opt-in same-owner responder; no automatic provider sender."""
        if (self._reply is not None or reply.control is not self.control
                or reply.ingress is not self or not callable(getattr(reply, "notify", None))):
            raise ValueError("exact existing Slack reply composition required")
        self._reply = reply

    async def __call__(self, request):
        try:
            async with asyncio.timeout(2.5):
                raw=bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw)>MAX_BODY:
                        return JSONResponse({'error':'request_bound'},status_code=413,
                                            headers={'Cache-Control':'no-store'})
                data=bytes(raw)
                payload=verified_slack_payload(data,dict(request.headers),
                    self._signing_secret,now=self._clock())
                if payload.get('type')=='url_verification':
                    # URL verification is signed with this installation's Slack
                    # secret; before a team installation Slack may not send team_id.
                    if (payload.get('api_app_id',self.control.app_id)!=self.control.app_id
                            or type(payload.get('challenge')) is not str
                            or not re.fullmatch(r'[A-Za-z0-9._-]{1,256}',payload['challenge'])):
                        return JSONResponse({'error':'slack_scope_denied'},status_code=403,
                                            headers={'Cache-Control':'no-store'})
                    return PlainTextResponse(payload['challenge'],
                                             headers={'Cache-Control':'no-store'})
                observation=_verified_slack_observation(payload,app_id=self.control.app_id,
                    team_id=self.control.team_id,bot_user_id=self.control.bot_user_id)
                if observation is None:
                    # Signed unsupported/self/bot event: no new request.
                    return JSONResponse({'ok':True},headers={'Cache-Control':'no-store'})
                outcome=await asyncio.to_thread(self.control.accept,observation)
                if self._reply is not None:
                    # Read-only search: existing event_id row is the at-most-once
                    # boundary; no Dataset checkpoint/agent job is necessary.
                    self._reply.notify(outcome['request_id'])
                    return JSONResponse({'ok':True},headers={'Cache-Control':'no-store'})
                if self.control.dataset_records is not None:
                    # A signed request is staged only by the existing fenced
                    # Dataset CAS. Local staging never earns Slack HTTP 200.
                    request_id=outcome['request_id']
                    task=self._staging_tasks.get(request_id)
                    if task is None:
                        if len(self._staging_tasks)>=_MAX_INFLIGHT_STAGES:
                            return JSONResponse({'error':'remote_intent_pending'},
                                status_code=503,headers={'Cache-Control':'no-store',
                                                         'Retry-After':'1'})
                        task=asyncio.create_task(
                            self.control.stage_received_dataset(request_id))
                        self._staging_tasks[request_id]=task
                        def done(finished, identity=request_id):
                            if self._staging_tasks.get(identity) is finished:
                                self._staging_tasks.pop(identity,None)
                            try:
                                finished.result()
                            except asyncio.CancelledError:
                                pass
                            except Exception as exc:
                                # Provider event content, ids and secrets MUST
                                # NOT appear in logs. The retry stays 503.
                                _LOG.warning("expert_dataset_stage_pending error_type=%s",
                                             type(exc).__name__)
                        task.add_done_callback(done)
                    # Do not cancel a CAS task while its worker thread still
                    # holds the existing writer fence. Slack HTTP may time out
                    # independently; never 200 until exact remote coverage.
                    await asyncio.shield(task)
                    covered=await asyncio.to_thread(
                        self.control.confirm_dataset_coverage,request_id)
                    outcome=dict(outcome,durable=covered)
                if outcome['durable'] is not True:
                    # Do not give Slack an irreversible 200 for a local-only
                    # received row that could vanish with the DevCoveer disk.
                    return JSONResponse({'error':'remote_intent_pending'},
                        status_code=503,headers={'Cache-Control':'no-store','Retry-After':'1'})
                return JSONResponse({'ok':True},headers={'Cache-Control':'no-store'})
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return JSONResponse({'error':'request_timeout'},status_code=503,
                                headers={'Cache-Control':'no-store','Retry-After':'1'})
        except RequestControlError as exc:
            code=exc.code
            if code=='request_restore_pending':
                return JSONResponse({'error':'request_pending'},status_code=503,
                    headers={'Cache-Control':'no-store','Retry-After':'1'})
            status=400 if code in ('invalid_signed_body','slack_event_invalid','invalid_request_text') else 403
            # No ids, raw event text, signatures or user identity in response.
            return JSONResponse({'error':'slack_request_invalid' if status==400
                                 else 'slack_auth_denied'},status_code=status,
                                headers={'Cache-Control':'no-store'})
        except (ParticipantError,PermissionError):
            return JSONResponse({'error':'slack_auth_denied'},status_code=403,
                                headers={'Cache-Control':'no-store'})
        except Exception:
            # An unknown control-write outcome must not be incorrectly ACKed.
            return JSONResponse({'error':'request_pending'},status_code=503,
                                headers={'Cache-Control':'no-store','Retry-After':'1'})
