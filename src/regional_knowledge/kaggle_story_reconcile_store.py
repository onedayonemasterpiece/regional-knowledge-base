"""Unregistered ReconcileStore over the EXISTING accepted source-command journal.

No PG connection/pool, extra journal, SQLite authoritative cursor, provider call,
or direct SQL mutation. The native SQL016 check/apply transaction is the only
progress writer. The caller injects current verified authority, the existing
ControlSpool+AcceptedSourceDispatch and a native actor-scoped read gate.
Pending or lost ACK never becomes a successful CAS from local state alone.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import secrets
from typing import Any, Callable, Awaitable

from .kaggle_story_reconcile import ReconcileIntent, ReconcileError, StoredIntent
from .kaggle_story_reconcile_native import (canonical, sha, intent_bytes,
    parse_intent_bytes, resolved_reconcile_command, validate_reconcile_command,
    progress_key, uuid, ROUTE)


class ReconcilePending(RuntimeError):
    """Not Saved. Return a typed pending/continuation, never an applied result."""


@dataclass(frozen=True)
class TrustedWriteScope:
    """Produced by the *existing* authority and writer-binding layer, not MCP args."""
    actor_id: str
    context: Any
    binding: Any
    current: Callable[[], Awaitable[None]]


class NativeAcceptedReconcileStore:
    """Implements the frozen ReconcileStore methods only when SQL016 is registered.

    The ControlSpool is just a bounded journal/intake, NOT the authoritative
    reconciliation state. On every successful accepted effect the exact native
    actor-scoped readback and remote DURABLE receipt must both agree.
    """
    def __init__(self, *, spool, dispatcher, writer_generation: str,
                 scope: Callable[..., Awaitable[TrustedWriteScope]],
                 native_read: Callable[[str,str], Awaitable[dict|None]]):
        if (spool is None or dispatcher is None or not callable(scope) or
                not callable(native_read) or type(writer_generation) is not str or
                not writer_generation or not callable(getattr(spool,'get',None)) or
                not callable(getattr(spool,'seal',None)) or
                getattr(dispatcher,'spool',None) is not spool):
            raise TypeError('existing exact spool/dispatch/current authority owners required')
        self.spool,self.dispatcher,self.writer_generation=spool,dispatcher,writer_generation
        self.scope,self.native_read=scope,native_read

    @staticmethod
    def _record(value, actor_id: str, parent: str) -> StoredIntent|None:
        if value is None:
            return None
        try:
            if type(value) is not dict or set(value)!={'operation_key','actor_id','intent_b64',
                    'request_sha256','intent_sha256','version','step_index','attempted',
                    'case_id','done','rejected','rejected_step'}:
                raise ValueError
            if value['operation_key']!=parent or value['actor_id']!=actor_id:
                raise ValueError
            raw=base64.b64decode(value['intent_b64'],validate=True)
            intent=parse_intent_bytes(raw)
            if (sha(raw)!=value['intent_sha256'] or intent.actor_id!=actor_id or
                    intent.operation_key!=parent or intent.request_sha256!=value['request_sha256']):
                raise ValueError
            version,step=value['version'],value['step_index']
            if type(version) is not int or version<1 or type(step) is not int or step<0:
                raise ValueError
            actions=intent.comparison.case_plan
            total=len(actions.evidence)+(1 if actions.action=='create' else 0)
            if step>total or any(type(value[key]) is not bool for key in
                                      ('attempted','done','rejected')):
                raise ValueError
            if ((value['done'] and (value['rejected'] or value['attempted'] or step!=total)) or
                    (value['rejected'] and (value['rejected_step']!=step or not value['attempted'])) or
                    (not value['rejected'] and value['rejected_step'] is not None)):
                raise ValueError
            if value['case_id'] is not None:
                uuid(value['case_id'])
            return StoredIntent(intent=intent,version=version,step_index=step,
                attempted=value['attempted'],case_id=value['case_id'],
                done=value['done'],rejected=value['rejected'],rejected_step=value['rejected_step'])
        except (ValueError,TypeError,KeyError,UnicodeError,ReconcileError):
            raise ReconcileError('untrusted_native_reconciliation_readback') from None

    @staticmethod
    def _last_effect_key(row: StoredIntent) -> str:
        # PG cursor is authoritative. Derive its LAST exact accepted command,
        # not a caller-selected receipt key. Version bumps once per transition.
        if row.version == 1:
            return row.intent.operation_key
        if row.rejected:
            action, step = 'reject', row.step_index
        elif row.done:
            action, step = 'finish', row.step_index
        elif row.attempted:
            action, step = 'begin', row.step_index
        elif row.step_index > 0:
            action, step = 'advance', row.step_index - 1
        else:
            raise ReconcileError('native_progress_receipt_ambiguous')
        return progress_key(row.intent, action, row.version - 1, step)

    async def _prove_remote(self, row: StoredIntent, database_generation: str) -> None:
        """Verify remote durable exact accepted command+PG outcome after raw GC.

        Uses the same preexisting RecoveryJournal prefix/objects/coverage as
        KaggleCasesBackend._recover_operation. Neither a PG-only row nor a
        local SQLite spool receipt is sufficient after an unknown ACK.
        """
        journal=self.spool.journal
        key=self._last_effect_key(row)
        remote=await journal.remote_outcome(key)
        if remote is None:
            raise ReconcilePending('native_reconciliation_outcome_not_remotely_durable')
        try:
            data=remote.data
            env=json.loads(data)
            if (canonical(env)!=data or set(env)!={'codec','stream','command_sha256','sequence',
                    'operation_key','database_generation','outcome_b64','outcome_sha256','outcome_length'}
                    or env['codec']!='rkb.outcome.v1' or env['stream']!=journal.stream
                    or env['operation_key']!=key or env['database_generation']!=database_generation):
                raise ValueError
            outcome=base64.b64decode(env['outcome_b64'],validate=True)
            if (type(env['outcome_length']) is not int or env['outcome_length']!=len(outcome)
                    or sha(outcome)!=env['outcome_sha256']):
                raise ValueError
            command_obj=await journal._read(journal.prefix+'/commands/'+sha(key.encode()),
                                            journal.max_segment_bytes)
            if command_obj is None or sha(command_obj.data)!=env['command_sha256']:
                raise ValueError
            raw=json.loads(command_obj.data)
            from .recovery_journal import ExactCommand
            command=ExactCommand(**{k:raw[k] for k in ('sequence','operation_key','request_id',
                  'person_id','route','logical_time','schema_revision','function_revision',
                  'executor_revision','ids','expected_revisions','kind')},
                  payload=base64.b64decode(raw['payload_b64'],validate=True))
            if (command.encode()!=command_obj.data or command.person_id!=row.intent.actor_id or
                    command.sequence!=env['sequence'] or command.operation_key!=key):
                raise ValueError
            validate_reconcile_command(command)
            envelope=json.loads(outcome)
            if (canonical(envelope)!=outcome or envelope.get('codec')!='rkb.applied-outcome.v1'
                    or envelope.get('operation_key')!=key or
                    envelope.get('command_sha256')!=env['command_sha256'] or
                    envelope.get('status')!='applied' or
                    envelope.get('person_id')!=row.intent.actor_id or
                    envelope.get('route')!=ROUTE):
                raise ValueError
            result_bytes=base64.b64decode(envelope['result_b64'],validate=True)
            if sha(result_bytes)!=envelope['result_sha256']:
                raise ValueError
            result=json.loads(result_bytes)
            expected=dict(parent_operation_key=row.intent.operation_key,
                actor_id=row.intent.actor_id,request_sha256=row.intent.request_sha256,
                progress_version=row.version,step_index=row.step_index,
                attempted=row.attempted,case_id=row.case_id,done=row.done,
                rejected=row.rejected,rejected_step=row.rejected_step)
            if result!=expected:
                raise ValueError
        except (ValueError,TypeError,KeyError,UnicodeError,ReconcileError):
            raise ReconcileError('reconciliation_remote_receipt_unproved') from None

    async def get(self, actor_id: str, operation_key: str) -> StoredIntent|None:
        actor_id=uuid(actor_id)
        verified=await self.scope(actor_id,source_ids=(),mutate=False)
        if type(verified) is not TrustedWriteScope or verified.actor_id!=actor_id:
            raise ReconcileError('reconciliation_authority_binding_changed')
        await verified.current()
        record=await self.native_read(actor_id,operation_key)
        await verified.current()
        row=self._record(record,actor_id,operation_key)
        if row is not None:
            await self._prove_remote(row, verified.binding.database_generation)
            await verified.current()
        return row

    async def _entry(self,key):
        try:
            return await self.spool.get(key)
        except Exception as error:
            if type(error).__name__=='SpoolError' and str(error)=='control operation not found':
                return None
            raise

    @staticmethod
    def _same_command(existing,expected):
        # request_id, sequence and logical_time are assigned once at intake.
        # A retry with same operation_key must compare ALL caller-controlled
        # command fields without inventing a second accepted journal record.
        validate_reconcile_command(existing)
        if (existing.operation_key!=expected.operation_key or
                existing.person_id!=expected.person_id or existing.route!=expected.route or
                existing.ids!=expected.ids or existing.expected_revisions!=expected.expected_revisions or
                existing.payload!=expected.payload):
            raise ReconcileError('reconciliation_operation_key_conflict')

    async def _execute(self, *, action: str, intent: ReconcileIntent,
                       version: int, step: int, case_id: str|None=None) -> StoredIntent:
        key=intent.operation_key if action=='stage' else progress_key(intent,action,version,step)
        sources=(intent.anchor_source.source_id,intent.peer_source.source_id)
        bound=await self.scope(intent.actor_id,source_ids=sources,mutate=True)
        if type(bound) is not TrustedWriteScope or bound.actor_id!=intent.actor_id:
            raise ReconcileError('reconciliation_authority_binding_changed')
        await bound.current()
        entry=await self._entry(key)
        # This is a preflight comparison only; the real/stored request_id,
        # sequence and timestamp remain those allocated by ControlSpool.
        preflight=resolved_reconcile_command(action=action,intent=intent,
            sequence=1,request_id='reconcile-immutable-preflight',
            logical_time='preflight',version=version,step=step,case_id=case_id)
        if entry is None:
            async with self.spool._lock:
                entry=await self._entry(key)
                if entry is None:
                    await bound.current()
                    state=await self.spool.scope_state()
                    if (state['pending'] or state['unknown'] or
                            state['sequence']!=state['accepted_sequence']):
                        raise ReconcilePending('prior_accepted_write_pending')
                    head=await self.spool.journal.head()
                    sequence=head.reference.last_sequence if head else 0
                    if sequence!=state['accepted_sequence']:
                        raise ReconcilePending('source_history_unavailable')
                    command=resolved_reconcile_command(action=action,intent=intent,
                        sequence=sequence+1,request_id='reconcile-'+secrets.token_hex(16),
                        logical_time=datetime.now(timezone.utc).isoformat(),
                        version=version,step=step,case_id=case_id)
                    frozen,data,handler=self.spool._validate(command,bound.binding.database_generation)
                    await bound.current()
                    facts=await self.spool.read_facts(None)
                    if (facts.pending or facts.unknown or
                        facts.scope_sequence>facts.covered_sequence or
                        not self.spool.facts_current(facts)):
                        raise ReconcilePending('source_history_unavailable')
                    entry=await self.spool._write(self.spool._stage,frozen,data,handler,
                                                   bound.binding.database_generation)
        self._same_command(entry.command,preflight)
        await bound.current()
        if entry.accepted is None:
            if entry.seal_state=='SENT_UNKNOWN':
                await self.spool.seal(bound.context,key,writer_generation=self.writer_generation)
            else:
                head=await self.spool.journal.head()
                if (head.reference.last_sequence if head else 0)!=entry.command.sequence-1:
                    raise ReconcilePending('prior_accepted_write_pending')
                await self.spool.seal(bound.context,key,writer_generation=self.writer_generation,
                                      predecessor=head)
            entry=await self.spool.get(key)
        await bound.current()
        if entry.outcome_state!='DURABLE':
            await self.dispatcher.enqueue(bound.context,key,writer_generation=self.writer_generation)
            entry=await self.spool.get(key)
        if entry.outcome_state!='DURABLE' or entry.apply_receipt is None:
            raise ReconcilePending('accepted_apply_or_remote_outcome_pending')
        if entry.apply_receipt.status!='applied':
            raise ReconcileError('native_reconciliation_precondition_rejected')
        await bound.current()
        observed=await self.native_read(intent.actor_id,intent.operation_key)
        await bound.current()
        row=self._record(observed,intent.actor_id,intent.operation_key)
        if row is None or row.intent.request_sha256!=intent.request_sha256 or \
                intent_bytes(row.intent)!=intent_bytes(intent):
            raise ReconcilePending('native_reconciliation_readback_unconfirmed')
        if (action=='stage' and row.version<1 or action!='stage' and row.version<version+1):
            raise ReconcilePending('native_reconciliation_progress_not_visible')
        await self._prove_remote(row,bound.binding.database_generation)
        await bound.current()
        return row

    async def stage(self,intent: ReconcileIntent) -> StoredIntent:
        # A previously committed native parent survives raw Dev spool GC;
        # no new command is created if the exact request already exists.
        current=await self.get(intent.actor_id,intent.operation_key)
        if current is not None:
            if intent_bytes(current.intent)!=intent_bytes(intent):
                raise ReconcileError('reconciliation_operation_key_conflict')
            return current
        return await self._execute(action='stage',intent=intent,version=0,step=0)

    async def _transition(self,actor_id: str,key: str,action: str,expected_version: int,
                          step: int|None=None,case_id: str|None=None) -> StoredIntent:
        current=await self.get(actor_id,key)
        if current is None:
            raise ReconcilePending('reconciliation_not_yet_applied')
        if step is None:
            step=current.step_index
        if type(expected_version) is not int or expected_version<1 or type(step) is not int:
            raise ReconcileError('invalid_reconciliation_cas')
        if current.version!=expected_version or current.step_index!=step:
            # A lost exact progress ACK may have advanced PG. Confirm via its
            # source authority and actual native readback, not a local counter.
            if current.version==expected_version+1 and (
                    (action=='advance' and current.step_index==step+1) or
                    (action=='begin' and current.step_index==step and current.attempted) or
                    (action=='reject' and current.step_index==step and current.rejected) or
                    (action=='finish' and current.step_index==step and current.done)):
                return current
            raise ReconcileError('stale_reconciliation_progress')
        if current.done or current.rejected:
            raise ReconcileError('reconciliation_already_terminal')
        if (action=='begin' and current.attempted or
                action in ('advance','reject') and not current.attempted or
                action=='finish' and current.attempted):
            raise ReconcileError('reconciliation_step_state_differs')
        return await self._execute(action=action,intent=current.intent,version=expected_version,
                                   step=step,case_id=case_id)

    async def begin(self,actor_id: str,operation_key: str,expected_version: int) -> StoredIntent:
        return await self._transition(actor_id,operation_key,'begin',expected_version)

    async def advance(self,actor_id: str,operation_key: str,expected_version: int,
                      step_index: int,case_id: str|None) -> StoredIntent:
        # Caller supplies NEXT step_index. PG/CAS command binds the previously
        # stored step, not the proposed new cursor value.
        if type(step_index) is not int or step_index<1:
            raise ReconcileError('invalid_next_step')
        return await self._transition(actor_id,operation_key,'advance',expected_version,
                                      step=step_index-1,case_id=case_id)

    async def reject(self,actor_id: str,operation_key: str,expected_version: int,
                     rejected_step: int) -> StoredIntent:
        return await self._transition(actor_id,operation_key,'reject',expected_version,step=rejected_step)

    async def finish(self,actor_id: str,operation_key: str,expected_version: int) -> StoredIntent:
        return await self._transition(actor_id,operation_key,'finish',expected_version)
