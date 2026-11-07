"""UNREGISTERED SQL016 native reconciliation command/handler leaf.

The only PG writes are versioned check/apply functions invoked by the existing
apply_accepted source journal. No local database, independent PG writer, or
Case CRUD is introduced here. Revisions are integration candidates, not pins
advertised in the live registry.
"""
from __future__ import annotations

import base64
from dataclasses import asdict
import hashlib
import json
import re
from uuid import UUID

from .kaggle_story_reconcile import (Comparison, Proof, ReconcileIntent,
                                     SourceSnapshot, StoredIntent, ReconcileError, _ensure_evidence)

ROUTE = 'story.reconcile.v1'
CODEC = 'rkb.story-reconcile-command.v1'
SCHEMA_REVISION = 'rkb-v4-sql001-016-v1'
FUNCTION_REVISION = 'rkb-v4-story-reconcile-v1'
EXECUTOR_REVISION = 'rkb-python-apply-v1'
REVISIONS = (SCHEMA_REVISION, FUNCTION_REVISION, EXECUTOR_REVISION)
PRECONDITIONS = frozenset({'progress_version', 'step_index'})
ACTIONS = frozenset({'stage', 'begin', 'advance', 'reject', 'finish'})
MAX_INTENT_BYTES = 192 * 1024  # transport cap for ONE selected source assertion pair
_HEX64 = re.compile(r'^[0-9a-f]{64}$')
_PARENT = re.compile(r'^story-reconcile:[0-9a-f]{64}$')
_TRANSITION = re.compile(r'^story-reconcile-progress:[0-9a-f]{64}$')


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def uuid(raw: str) -> str:
    try:
        if type(raw) is not str or str(UUID(raw)) != raw:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise ReconcileError('invalid_native_uuid') from None
    return raw


def _snapshot(snapshot: SourceSnapshot):
    return {**{k: getattr(snapshot, k) for k in (
        'source_id','source_revision','eligibility_revision','frontier_id',
        'accepted','withdrawn','source_pass_state','reconciliation_state','proofs_next_cursor')},
        'proofs': [asdict(proof) for proof in snapshot.proofs]}


def intent_document(intent: ReconcileIntent) -> dict:
    if type(intent) is not ReconcileIntent or type(intent.comparison) is not Comparison:
        raise ReconcileError('exact_model_intent_required')
    value = dict(actor_id=uuid(intent.actor_id), operation_key=intent.operation_key,
                 request_sha256=intent.request_sha256,
                 anchor_source=_snapshot(intent.anchor_source),
                 peer_source=_snapshot(intent.peer_source),
                 comparison=intent.comparison.model_dump(mode='json'))
    if not _PARENT.fullmatch(value['operation_key']) or not _HEX64.fullmatch(value['request_sha256']):
        raise ReconcileError('invalid_parent_operation_key')
    if sha(canonical(value['comparison'])) != intent.request_sha256:
        raise ReconcileError('immutable_comparison_hash_differs')
    anchor, peer = intent.comparison.anchor, intent.comparison.peer
    if (str(anchor.source_id) != intent.anchor_source.source_id or
            anchor.source_revision != intent.anchor_source.source_revision or
            anchor.eligibility_revision != intent.anchor_source.eligibility_revision or
            str(peer.source_id) != intent.peer_source.source_id or
            peer.source_revision != intent.peer_source.source_revision or
            peer.eligibility_revision != intent.peer_source.eligibility_revision):
        raise ReconcileError('intent_source_revision_differs')
    # Stable parent key from exact actor/frontier/model-supplied comparison key.
    expected = 'story-reconcile:' + sha(canonical([
        intent.actor_id,intent.anchor_source.frontier_id,intent.comparison.comparison_key]))
    if expected != intent.operation_key:
        raise ReconcileError('unstable_reconciliation_key')
    if any(len(snapshot.proofs) != 1 or snapshot.source_pass_state not in ('complete','pending')
           for snapshot in (intent.anchor_source,intent.peer_source)):
        raise ReconcileError('bounded_exact_assertion_pair_required')
    for link, snapshot in ((anchor,intent.anchor_source),(peer,intent.peer_source)):
        proof = snapshot.proofs[0]
        proof.validate()
        if str(link.assertion_id) != proof.assertion_id or not snapshot.accepted or snapshot.withdrawn:
            raise ReconcileError('source_proof_or_eligibility_changed')
    _ensure_evidence(intent.comparison, intent.anchor_source.proofs[0], intent.peer_source.proofs[0])
    return value


def intent_bytes(intent: ReconcileIntent) -> bytes:
    encoded = canonical(intent_document(intent))
    if not 1 <= len(encoded) <= MAX_INTENT_BYTES:
        raise ReconcileError('bounded_comparison_input_required')
    return encoded


def parse_intent_bytes(encoded: bytes) -> ReconcileIntent:
    if type(encoded) is not bytes or not 1 <= len(encoded) <= MAX_INTENT_BYTES:
        raise ReconcileError('bounded_comparison_input_required')
    try:
        value = json.loads(encoded)
        if type(value) is not dict or set(value) != {'actor_id','operation_key','request_sha256',
                'anchor_source','peer_source','comparison'} or canonical(value) != encoded:
            raise ValueError
        def read_snapshot(raw):
            if type(raw) is not dict or set(raw) != {'source_id','source_revision','eligibility_revision',
                    'frontier_id','accepted','withdrawn','source_pass_state','reconciliation_state',
                    'proofs','proofs_next_cursor'} or type(raw['proofs']) is not list:
                raise ValueError
            return SourceSnapshot(**{**raw, 'proofs': tuple(Proof(**p) for p in raw['proofs'])})
        intent = ReconcileIntent(uuid(value['actor_id']),value['operation_key'],
            value['request_sha256'],read_snapshot(value['anchor_source']),
            read_snapshot(value['peer_source']),Comparison.model_validate(value['comparison']))
        if intent_bytes(intent) != encoded:
            raise ValueError
        return intent
    except (ValueError,TypeError,KeyError,UnicodeError):
        raise ReconcileError('invalid_or_modified_native_intent') from None


def progress_key(intent: ReconcileIntent, action: str, version: int, step: int) -> str:
    if action not in ACTIONS - {'stage'} or type(version) is not int or version < 1 or \
            type(step) is not int or step < 0:
        raise ReconcileError('invalid_progress_precondition')
    return 'story-reconcile-progress:' + sha(canonical([
        intent.actor_id,intent.operation_key,action,version,step]))


def case_child_key(parent: str, step: int) -> str:
    # Exact same expression as _effect_key in frozen 9af253f parent leaf.
    if not _PARENT.fullmatch(parent) or type(step) is not int or step < 0:
        raise ReconcileError('invalid_case_child_index')
    return 'reconcile-case:' + sha(canonical([parent,step]))


def native_case_receipt_key(actor: str, parent: str, step: int) -> str:
    # Frozen KaggleCasesBackend._key applies actor-scoped SHA to child key.
    return 'case-op:' + uuid(actor) + ':' + sha(case_child_key(parent,step).encode())


def _steps(intent: ReconcileIntent) -> int:
    plan = intent.comparison.case_plan
    return len(plan.evidence) + (1 if plan.action == 'create' else 0)


def build_payload(action: str, intent: ReconcileIntent, *, version: int, step: int,
                  case_id: str | None = None) -> bytes:
    encoded = intent_bytes(intent)
    if action not in ACTIONS or type(version) is not int or type(step) is not int or step < 0:
        raise ReconcileError('invalid_reconciliation_transition')
    if action == 'stage':
        if version != 0 or step != 0 or case_id is not None:
            raise ReconcileError('invalid_new_reconciliation_precondition')
    else:
        if version < 1 or step > _steps(intent):
            raise ReconcileError('invalid_reconciliation_progress_version')
        if action in ('begin','advance','reject') and step >= _steps(intent):
            raise ReconcileError('progress_exceeds_selected_case_steps')
        if action == 'advance':
            uuid(case_id)
        elif case_id is not None:
            raise ReconcileError('unexpected_case_pointer')
    body = dict(codec=CODEC, action=action, parent_operation_key=intent.operation_key,
                request_sha256=intent.request_sha256, intent_sha256=sha(encoded),
                intent_b64=base64.b64encode(encoded).decode() if action == 'stage' else None,
                case_id=case_id, case_receipt_key=(native_case_receipt_key(
                    intent.actor_id,intent.operation_key,step) if action in ('advance','reject') else None))
    payload = canonical(body)
    if len(payload) > 256 * 1024:
        raise ReconcileError('native_comparison_payload_too_large')
    return payload


def decode_payload(data: bytes) -> dict:
    try:
        doc=json.loads(data)
        if (type(doc) is not dict or canonical(doc)!=data or set(doc)!={'codec','action',
                'parent_operation_key','request_sha256','intent_sha256','intent_b64',
                'case_id','case_receipt_key'} or doc['codec']!=CODEC or doc['action'] not in ACTIONS or
                not _PARENT.fullmatch(doc['parent_operation_key']) or
                not _HEX64.fullmatch(doc['request_sha256']) or
                not _HEX64.fullmatch(doc['intent_sha256'])):
            raise ValueError
        if doc['action']=='stage':
            encoded=base64.b64decode(doc['intent_b64'],validate=True)
            if base64.b64encode(encoded).decode()!=doc['intent_b64'] or sha(encoded)!=doc['intent_sha256']:
                raise ValueError
            intent=parse_intent_bytes(encoded)
            if intent.operation_key!=doc['parent_operation_key'] or intent.request_sha256!=doc['request_sha256']:
                raise ValueError
        elif doc['intent_b64'] is not None:
            raise ValueError
        if doc['action'] == 'advance':
            uuid(doc['case_id'])
        elif doc['case_id'] is not None:
            raise ValueError
        if doc['action'] in ('advance','reject'):
            if type(doc['case_receipt_key']) is not str or not doc['case_receipt_key'].startswith('case-op:'):
                raise ValueError
        elif doc['case_receipt_key'] is not None:
            raise ValueError
        return doc
    except (ValueError,TypeError,KeyError,UnicodeError,base64.binascii.Error,ReconcileError):
        raise ReconcileError('invalid_native_reconciliation_payload') from None


def validate_reconcile_command(command) -> dict:
    try:
        if (command.kind!='domain_command' or command.route!=ROUTE or
                tuple((command.schema_revision,command.function_revision,command.executor_revision))!=REVISIONS
                or set(command.ids)!={'actor_id','source_id','peer_source_id'} or
                set(command.expected_revisions)!=PRECONDITIONS or
                command.person_id!=command.ids['actor_id']):
            raise ValueError
        actor,source,peer=map(uuid,(command.person_id,command.ids['source_id'],command.ids['peer_source_id']))
        if source==peer:
            raise ValueError
        doc=decode_payload(command.payload)
        version=int(command.expected_revisions['progress_version'])
        step=int(command.expected_revisions['step_index'])
        if (str(version)!=command.expected_revisions['progress_version'] or
                str(step)!=command.expected_revisions['step_index'] or version<0 or step<0):
            raise ValueError
        if doc['action']=='stage':
            if command.operation_key!=doc['parent_operation_key'] or version!=0 or step!=0:
                raise ValueError
            intent=parse_intent_bytes(base64.b64decode(doc['intent_b64'],validate=True))
            if actor!=intent.actor_id or source!=intent.anchor_source.source_id or peer!=intent.peer_source.source_id:
                raise ValueError
        else:
            if not _TRANSITION.fullmatch(command.operation_key) or version==0:
                raise ValueError
            expected='story-reconcile-progress:'+sha(canonical([actor,doc['parent_operation_key'],doc['action'],version,step]))
            if command.operation_key!=expected:
                raise ValueError
            if doc['action'] in ('advance','reject') and doc['case_receipt_key'] != native_case_receipt_key(
                    actor,doc['parent_operation_key'],step):
                raise ValueError
        return doc
    except (ValueError,TypeError,KeyError,AttributeError,ReconcileError):
        raise ReconcileError('invalid_native_reconciliation_command') from None


def resolved_reconcile_command(*, action: str, intent: ReconcileIntent,
                               sequence: int, request_id: str, logical_time: str,
                               version: int, step: int, case_id: str | None = None):
    from .recovery_journal import ExactCommand
    if type(sequence) is not int or sequence<1:
        raise ReconcileError('invalid_native_sequence')
    operation_key=intent.operation_key if action=='stage' else progress_key(intent,action,version,step)
    command=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,
        person_id=intent.actor_id,logical_time=logical_time,route=ROUTE,
        schema_revision=SCHEMA_REVISION,function_revision=FUNCTION_REVISION,
        executor_revision=EXECUTOR_REVISION,kind='domain_command',
        ids=dict(actor_id=intent.actor_id,source_id=intent.anchor_source.source_id,
                 peer_source_id=intent.peer_source.source_id),
        expected_revisions=dict(progress_version=str(version),step_index=str(step)),
        payload=build_payload(action,intent,version=version,step=step,case_id=case_id))
    validate_reconcile_command(command)
    return command


def reconciliation_handler(*,check_sha256: str,apply_sha256: str):
    from .kaggle_apply import DependencySpec,HandlerSpec,PgFunction,Revisions
    # Only a registered exact installed SQL016 bundle can call this; the
    # leaf does NOT update the global registry or runtime migrations.
    return HandlerSpec('domain_command',ROUTE,Revisions(*REVISIONS),
        PgFunction('check_story_reconcile_v1',check_sha256),
        PgFunction('apply_story_reconcile_v1',apply_sha256),PRECONDITIONS,
        dependency=DependencySpec(ROUTE,'public_source',
            frozenset({'source_id','peer_source_id'}),'shared',False))
