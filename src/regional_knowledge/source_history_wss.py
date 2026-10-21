"""Dev exact journal verification over fixed bounded native maintenance phases."""
from __future__ import annotations

import asyncio
import logging
import base64
import json
import secrets

from .control_spool import SourceChange,SpoolConflict,_command,_database,_uuid
from .source_history_verifier import NativeSourceHistoryVerifier,_Coverage,_json,_hash
from .source_read_gate import binding_key
from .kaggle_recovery_dispatch import RecoveryPhaseDispatch


GEO_BOOTSTRAP_HISTORY_BYTES=6*1024*1024
_LOG=logging.getLogger(__name__)


class WssNativeHistoryProofAdapter:
    """Concrete startup adapter; existing queue and private native observations."""
    def __init__(self,*,database,expected_catalog_sha256,expected_acl_sha256,expected_observer_sha256,
                 coverage_seconds=30,timeout_seconds=10,max_commands=2048,max_history_bytes=4*1024*1024,
                 geo_bootstrap_operation_keys=(),geo_upgrade_pins=None):
        self.database=database;self.catalog=expected_catalog_sha256;self.acl=expected_acl_sha256
        self.observer=expected_observer_sha256;self.coverage_seconds=coverage_seconds
        self.timeout=timeout_seconds;self.max_commands=max_commands;self.max_bytes=max_history_bytes
        if type(self.observer) is not str or len(self.observer)!=64 or set(self.observer)-set('0123456789abcdef'):
            raise ValueError('independent native observer source pin required')
        self.geo_bootstrap_operation_keys=geo_bootstrap_operation_keys
        self.geo_upgrade_pins=geo_upgrade_pins
        self._bound=None

    def bind(self,*,spool,routes,binding_current,writer_generation):
        if self._bound is not None:raise ValueError('history adapter already bound')
        if writer_generation!=self.database.writer_generation:raise ValueError('native history writer differs')
        self._bound=WssSourceHistoryVerifier(spool,acquire=None,binding_current=binding_current,routes=dict(routes),
            expected_catalog_sha256=self.catalog,expected_acl_sha256=self.acl,writer_generation=writer_generation,
            timeout_seconds=self.timeout,coverage_seconds=self.coverage_seconds,max_commands=self.max_commands,
            max_history_bytes=self.max_bytes,database=self.database,observer_sha256=self.observer,
            geo_bootstrap_operation_keys=self.geo_bootstrap_operation_keys,geo_upgrade_pins=self.geo_upgrade_pins)
        return self._bound


class WssSourceHistoryVerifier(NativeSourceHistoryVerifier):
    def __init__(self,*args,database,observer_sha256,geo_bootstrap_operation_keys=(),cloud_prepare_seconds=60,geo_upgrade_pins=None,**kwargs):
        super().__init__(*args,**kwargs)
        if type(cloud_prepare_seconds) not in (int,float) or not 0<cloud_prepare_seconds<=60:
            raise ValueError('bounded cloud history preparation required')
        self.cloud_prepare_seconds=cloud_prepare_seconds
        self.database,self.observer_hash=database,observer_sha256
        self.geo_upgrade_pins=geo_upgrade_pins
        self.origin_closure=None
        if geo_upgrade_pins is not None:
            from .kaggle_geo_upgrade import GeoUpgradePins
            from .geo_manifest_commands import GEO_V2_REVISIONS
            if (type(geo_upgrade_pins) is not GeoUpgradePins or tuple(getattr(database,k) for k in
                    ('schema_revision','function_revision','executor_revision'))!=GEO_V2_REVISIONS.wire()):
                raise ValueError('fixed geographic closure transition required')
            self.origin_closure=geo_upgrade_pins.origin_closure(self.spool.registry,self.routes)
        if (type(geo_bootstrap_operation_keys) is not tuple or len(geo_bootstrap_operation_keys)>256
                or len(set(geo_bootstrap_operation_keys))!=len(geo_bootstrap_operation_keys)
                or any(type(key) is not str or not 1<=len(key.encode())<=256 for key in geo_bootstrap_operation_keys)):
            raise ValueError('bounded exact GEO bootstrap operation keys required')
        self.geo_bootstrap_operations=frozenset(geo_bootstrap_operation_keys)
        self._native_prefix=None
        # A retained native history transaction owns SHARE table locks between
        # phases. Ordinary writes must wait for its close/abort, including when
        # the control-side observation timed out or lost its open receipt.
        self.spool._native_history_unsettled=True

    def _closure_transition(self,known,observations,remote_outcomes,previous):
        if self.geo_upgrade_pins is None:return None
        from .geo_manifest_commands import GEO_REVISIONS,GEO_V2_REVISIONS,validate_geo_dependency_transition
        pins=self.geo_upgrade_pins
        closure_hash=_hash(_json(self.origin_closure))
        target_hash=_hash(_json(dict(catalog_sha256=self.catalog_hash,acl_sha256=self.acl_hash,
            registry_sha256=self.registry_hash,routes=sorted(self.routes))))
        found=[value for value in known.values() if value[0].route=='geo.dependencies.upgrade.v2']
        if not found:
            proof=previous.closure_transition if previous is not None and previous.verifier is self else None
            if (proof is None or len(proof)!=5 or proof[0]!=pins.artifact_sha256
                    or proof[3:]!=(closure_hash,target_hash) or previous.applied_sequence<proof[2]):
                raise SpoolConflict('ordered geographic closure transition unproved')
            return proof
        if len(found)!=1:raise SpoolConflict('ambiguous geographic closure transition')
        command,handler,_=found[0]
        validate_geo_dependency_transition(command,artifact_sha256=pins.artifact_sha256)
        observed=observations.get(command.sequence);remote=remote_outcomes.get(command.sequence)
        if observed is None or observed[0]!='applied':
            raise SpoolConflict('geographic closure transition not remotely durable')
        if remote is None:
            from .dataset_recovery_store import DatasetRecoveryStore
            store=self.spool.journal.store
            if type(store) is not DatasetRecoveryStore:
                raise SpoolConflict('geographic closure transition not remotely durable')
            outcome_key=self.spool.journal.prefix+'/outcomes/'+_hash(command.operation_key.encode())
            with store.queue.connect(write=False) as db:
                row=db.execute('SELECT data FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                    (store.storage_identity,outcome_key)).fetchone()
            if row is None:raise SpoolConflict('geographic transition pending receipt missing')
            envelope=json.loads(row['data'])
            body=base64.b64decode(envelope['outcome_b64'],validate=True)
            if ((envelope.get('operation_key'),envelope.get('command_sha256'),envelope.get('sequence'),
                    envelope.get('database_generation'),_hash(body),len(body))!=(command.operation_key,
                    _hash(command.encode()),command.sequence,self.database.database_generation,observed[1],observed[2])):
                raise SpoolConflict('geographic transition pending receipt differs from native')
            # This proves loaded closure for checkpoint capture only. The same
            # operation stays in coverage.pending until Dataset promotion.
            value=json.loads(body)
        else:value=json.loads(base64.b64decode(json.loads(remote.data)['outcome_b64'],validate=True))
        if (value.get('before')!=list(GEO_REVISIONS.wire()) or value.get('after')!=list(GEO_V2_REVISIONS.wire())
                or value.get('status')!='applied' or value.get('handler_sha256')!=handler.fingerprint
                or handler.transition is None or handler.transition.target!=GEO_V2_REVISIONS):
            raise SpoolConflict('geographic closure transition native outcome differs')
        return (pins.artifact_sha256,_hash(command.encode()),command.sequence,closure_hash,target_hash)

    async def _abort_session(self,binding,session):
        async def abort():
            async with asyncio.timeout(self.timeout_seconds):
                return await self._phase(binding,dict(action='abort',session=session))
        task=asyncio.create_task(abort())
        cancelled=False
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError:cancelled=True
            except Exception:break
        try:
            value=task.result()
            if value!={'aborted':True}:raise SpoolConflict('native history abort unproved')
            self.spool._native_history_unsettled=False
        except Exception as exc:
            # Keep the write fence. A later successful native open/close can
            # prove that the old bounded lease has ended; no guessed release.
            _LOG.warning('source_history_cleanup state=unsettled reason=%s',type(exc).__name__)
        if cancelled:raise asyncio.CancelledError

    def _charge_history(self,usage,command,size):
        # A measured, exact-key GEO bootstrap is a separate bounded window.
        # Ordinary source/Story/Case commands never borrow this allowance.
        geo=(command.route=='geo.manifest.install.v1' and
            command.schema_revision=='rkb-v4-sql001-011-v1' and
            command.operation_key in self.geo_bootstrap_operations)
        index=1 if geo else 0
        usage[index]+=size
        if usage[index]>(GEO_BOOTSTRAP_HISTORY_BYTES if geo else self.max_history_bytes):
            raise SpoolConflict('native source history budget exceeded')

    def _dispatch(self,binding):
        q=self.spool.queue
        if not self.binding_current(binding) or binding_key(binding)[3]!=self.database.database_generation:
            raise SpoolConflict('current native history binding required')
        with q.connect(write=False) as db:
            run=db.execute('select * from runs where id=?',(binding.run_id,)).fetchone()
            if run is None or run['runtime_incarnation']!=binding.runtime_incarnation:
                raise SpoolConflict('native history attachment differs')
            identity=(binding.run_id,run['launch_nonce'],binding.runtime_incarnation)
        bridge=getattr(q,'_recovery_phases',None)
        if bridge is None:
            bridge=RecoveryPhaseDispatch(q,database=self.database,identity=identity,binding_current=self.binding_current,
                timeout_seconds=self.timeout_seconds)
        elif (type(bridge) is not RecoveryPhaseDispatch or bridge.database!=self.database or bridge.identity!=identity):
            raise SpoolConflict('another retained maintenance owner requires reconciliation')
        return bridge

    async def _phase(self,binding,args):
        bridge=self._dispatch(binding)
        value=await bridge.request(binding,'source_history',args,operation_key=_hash(_json(args)))
        if not isinstance(value,dict) or 'error' in value:raise SpoolConflict('native source history observation unavailable')
        return value

    async def restore(self,binding):return await self._verify_remote(binding,mode='full')
    async def establish_genesis(self,binding):return await self._verify_remote(binding,mode='full',genesis=True)
    async def extend(self,binding):return await self._verify_remote(binding,mode='delta')
    async def renew(self,binding):return await self._verify_remote(binding,mode='renew')

    async def _verify_remote(self,binding,*,mode,genesis=False):
        s,j=self.spool,self.spool.journal;key=binding_key(binding)
        async with s._lock:
            previous=self._coverage
            if mode!='full' and (previous is None or s._source_anchor is not previous
                    or previous.binding is not binding or previous.key!=key or self._native_prefix is None):
                s.invalidate_source_history();self._coverage=None
                raise SpoolConflict('retained exact native source prefix unavailable')
            s.invalidate_source_history();self._coverage=None;version=s._source_version
            session=None;committing=False
            try:
                async with asyncio.timeout(self.cloud_prepare_seconds) as phase_timeout:
                    if not self.binding_current(binding) or not self._routes_current() or self._registry_hash()!=self.registry_hash:
                        raise SpoolConflict('source route or handler closure changed')
                    if mode!='full' and await self._head_unchanged(previous.head):head=previous.head
                    else:head=await j.head()
                    base=previous.head.reference if previous and previous.head else None
                    commands=[];ref=head.reference if head else None;usage=[0,0]
                    stop=None if mode=='full' else base
                    for _ in range(j.max_reconcile_hops):
                        if ref==stop:break
                        if ref is None:raise SpoolConflict('verified source prefix is not an exact ancestor')
                        older,raws=await j.read_segment(ref)
                        for raw in raws:
                            value=self._classify(raw,ref)
                            self._charge_history(usage,value[0],len(raw))
                            if len(commands)>=self.max_commands:
                                raise SpoolConflict('native source history budget exceeded')
                            commands.append(value)
                        ref=older
                    if ref!=stop:raise SpoolConflict('source genesis ancestry incomplete')
                    commands.sort(key=lambda x:x[0].sequence)
                    start=0 if mode=='full' else previous.accepted_sequence
                    if [c.sequence for c,_,_ in commands]!=list(range(start+1,start+len(commands)+1)):
                        raise SpoolConflict('accepted source history is not contiguous')
                    if mode=='renew' and commands:raise SpoolConflict('renewal source head changed')
                    accepted=start+len(commands)
                    known={c.sequence:(c,h,m) for c,h,m in commands}
                    if mode!='full':
                        for raw,hh,m in previous.pending:
                            c,h,m=self._classify(raw,m)
                            if h.fingerprint!=hh:raise SpoolConflict('pending source handler differs')
                            known[c.sequence]=(c,h,m);self._charge_history(usage,c,len(raw))
                    if len(known)>self.max_commands or len({c.operation_key for c,_,_ in known.values()})!=len(known):
                        raise SpoolConflict('source pending history bound or identity differs')
                    pending_earlier=sorted(seq for seq in known if mode!='full' and seq<=previous.applied_sequence)
                    # Cloud ancestry and outcome bytes are prepared BEFORE a
                    # native transaction opens. Keep at most four reads in flight
                    # and retain the original aggregate history byte limits.
                    checkpoint_key=self._checkpoint_key(key[3])
                    checkpoint=await j._read(checkpoint_key,8192)
                    remote_outcomes={};remote_bytes=0
                    retained=list(known.values())
                    for offset in range(0,len(retained),4):
                        batch=retained[offset:offset+4]
                        values=await asyncio.gather(*(j.remote_outcome(c.operation_key)
                            for c,_,_ in batch),return_exceptions=True)
                        for (c,_,_),remote in zip(batch,values):
                            if isinstance(remote,BaseException):raise remote
                            remote_bytes+=len(remote.data) if remote is not None else 0
                            if remote_bytes>2*(self.max_history_bytes+GEO_BOOTSTRAP_HISTORY_BYTES):
                                raise SpoolConflict('remote source outcome prefetch budget exceeded')
                            remote_outcomes[c.sequence]=remote
                    if not await self._head_unchanged(head) or s._writing or s._source_version is not version or not self.binding_current(binding):
                        raise SpoolConflict('source prefix changed during cloud preparation')
                    phase_timeout.reschedule(asyncio.get_running_loop().time()+self.timeout_seconds)
                    nonce=secrets.token_hex(24)
                    self.spool._native_history_unsettled=True
                    opened=await self._phase(binding,dict(action='open',mode=mode,nonce=nonce,
                        catalog_sha256=self.catalog_hash,acl_sha256=self.acl_hash,registry_sha256=self.registry_hash,
                        observer_sha256=self.observer_hash,routes=sorted(self.routes),prefix=None if mode=='full' else self._native_prefix,
                        pending_sequences=[] if mode=='full' else pending_earlier))
                    session=opened.get('session');applied=opened.get('applied_sequence')
                    if (type(session) is not str or not 1<=len(session)<=256 or opened.get('nonce')!=nonce
                            or type(applied) is not int or not 0<=applied<=accepted
                            or mode!='full' and applied<previous.applied_sequence
                            or mode=='renew' and applied!=previous.applied_sequence
                            or any(opened.get(k)!=v for k,v in {'database_generation':key[3],
                                'writer_generation':self.writer_generation,'catalog_sha256':self.catalog_hash,
                                'acl_sha256':self.acl_hash,'registry_sha256':self.registry_hash,
                                'observer_sha256':self.observer_hash}.items())):
                        raise SpoolConflict('native source observation binding differs')
                    remote=checkpoint;publish_genesis=False
                    if remote is None:
                        if self.origin_closure is not None:
                            raise SpoolConflict('ordered geographic upgrade requires original genesis')
                        # Only the actual pre-attachment empty-generation intent
                        # permits first genesis, alongside empty native+remote facts.
                        from .kaggle_target_launch import read_empty_intent
                        intent=read_empty_intent(s.queue,binding.run_id)
                        if (not genesis and intent is None or head is not None or applied!=0 or opened.get('empty') is not True
                                or (await _database(s._scope_state,None))['operation_count']):
                            raise SpoolConflict('exact generation genesis missing')
                        if not genesis and (intent.get('database',{}).get('database_generation')!=key[3]
                                or intent.get('database',{}).get('writer_generation')!=self.writer_generation):
                            raise SpoolConflict('empty generation intent differs')
                        publish_genesis=True
                    else:self._verify_genesis(remote.data,key[3],expected_closure=self.origin_closure)
                    expected=set(range(1,applied+1)) if mode=='full' else set(range(previous.applied_sequence+1,applied+1))|set(pending_earlier)
                    if mode=='renew':expected=set()
                    observations={};after=0
                    for _ in range(self.max_commands//16+2):
                        page=await self._phase(binding,dict(action='page',session=session,after=after))
                        rows=page.get('rows')
                        if type(rows) is not list or len(rows)>16:raise SpoolConflict('native receipt page shape differs')
                        for row in rows:
                            if not isinstance(row,(list,tuple)) or len(row)!=11:raise SpoolConflict('native receipt metadata shape differs')
                            seq,op,ch,hh,mh,status,oh,actual_oh,length,generation,actual_ch=row
                            if type(seq) is not int or seq not in expected or seq in observations or seq not in known:
                                raise SpoolConflict('native applied prefix is not exact')
                            c,h,m=known[seq]
                            if (seq<=after or op!=c.operation_key or ch!=_hash(c.encode()) or actual_ch!=ch
                                    or hh!=h.fingerprint or mh!=m.sha256 or generation!=key[3]
                                    or status not in ('applied','rejected_precondition') or oh!=actual_oh
                                    or type(length) is not int or not 1<=length<=j.max_outcome_bytes):
                                raise SpoolConflict('native exact applied metadata differs')
                            observations[seq]=(status,oh,length);after=seq
                        if page.get('after')!=after or type(page.get('done')) is not bool:
                            raise SpoolConflict('native receipt cursor differs')
                        if page['done']:break
                        if not rows:raise SpoolConflict('native receipt pagination stalled')
                    else:raise SpoolConflict('native receipt pagination budget exceeded')
                    if observations.keys()!=expected:raise SpoolConflict('native applied receipt prefix incomplete')
                    changes={};pending=[];origins=[]
                    for c,h,m in known.values():
                        outcome=observations.get(c.sequence);durable=False
                        if outcome is not None:
                            remote=remote_outcomes[c.sequence]
                            if remote is not None:
                                value=json.loads(remote.data)
                                try:body=base64.b64decode(value['outcome_b64'],validate=True)
                                except (KeyError,ValueError):raise SpoolConflict('remote source outcome bytes differ') from None
                                self._charge_history(usage,c,len(body))
                                expected_outcome={'codec':'rkb.outcome.v1','stream':j.stream,'command_sha256':_hash(c.encode()),
                                    'sequence':c.sequence,'operation_key':c.operation_key,'database_generation':key[3],
                                    'outcome_b64':base64.b64encode(body).decode(),'outcome_sha256':outcome[1],'outcome_length':outcome[2]}
                                if (remote.data!=_json(expected_outcome) or len(body)!=outcome[2] or _hash(body)!=outcome[1]):raise SpoolConflict('remote outcome differs from native exact prefix')
                                durable=True
                            if h.dependency.scope_class=='public_source' and outcome[0]=='applied':origins.append(c.ids['source_id'])
                        if not durable:pending.append((c.encode(),h.fingerprint,m))
                        if h.dependency.scope_class!='public_source':continue
                        source=c.ids['source_id'];old=changes.get(source)
                        changes[source]=SourceChange(source,c.sequence,_hash(c.encode()),bool(h.dependency.restriction or old and old.restriction),
                            bool(not durable or old and old.pending),False)
                    if mode=='renew':pending=list(previous.pending)
                    if s._writing or s._source_version is not version or not self.binding_current(binding):
                        raise SpoolConflict('source prefix changed during native verification')
                    closed=await self._phase(binding,dict(action='close',session=session,nonce=nonce,
                        origin_ids=sorted(set(origins))))
                    if (closed.get('nonce')!=nonce or closed.get('applied_sequence')!=applied
                            or type(closed.get('prefix')) is not str or not 1<=len(closed['prefix'])<=256
                            or s._writing or s._source_version is not version
                            or not self.binding_current(binding)):
                        raise SpoolConflict('native source completion changed')
                    session=None
                    self.spool._native_history_unsettled=False
                    phase_timeout.reschedule(asyncio.get_running_loop().time()+self.cloud_prepare_seconds)
                    committing=True
                    if publish_genesis:
                        await j._fenced_exact(checkpoint_key,self._checkpoint(key[3]),expected_token=None,
                            writer_generation=self.writer_generation)
                    if not await self._head_unchanged(head) or s._writing or s._source_version is not version or not self.binding_current(binding):
                        raise SpoolConflict('source prefix changed at native completion')
                    if mode=='full':await s._write(s._replace_source_heads,tuple(changes.values()))
                    elif changes:await s._write(self._merge_source_heads,tuple(changes.values()))
                    from .case_history import project_cases
                    case_state=(project_cases([v for v in known.values() if mode=='full' or v[0].sequence>previous.applied_sequence],
                        {seq:value[0] for seq,value in observations.items()},None if mode=='full' else previous.case_state)
                        if mode=='full' or previous.case_state is not None else None)
                    self._native_prefix=closed['prefix']
                    transition=self._closure_transition(known,observations,remote_outcomes,previous)
                    coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,nonce,head,accepted,applied,tuple(pending),case_state,transition)
                    self._coverage=coverage;s._source_anchor=coverage;s._source_head=head.reference if head else None
                    s._source_covered_sequence=accepted;s._source_version=object()
                    return coverage
            except (TimeoutError,SpoolConflict) as exc:
                # Missing new evidence (timeout, unavailable observation) does
                # not revoke an unexpired proof of an unchanged prefix; it still
                # expires at its own deadline. Contradictions keep closing reads.
                if (mode=='full' or committing or previous is None
                        or type(exc) is SpoolConflict and exc.args!=('native source history observation unavailable',)
                        or s._source_version is not version or s._source_anchor is not None or s._writing
                        or previous.deadline<=self.clock() or not self.binding_current(binding)):
                    raise
                self._coverage=previous;s._source_anchor=previous
                _LOG.warning('source_history_renewal_deferred mode=%s reason=%s',mode,type(exc).__name__)
                return previous
            finally:
                if session is not None:
                    await self._abort_session(binding,session)
