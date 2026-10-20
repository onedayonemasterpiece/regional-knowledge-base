"""Dev exact journal verification over fixed bounded native maintenance phases."""
from __future__ import annotations

import asyncio
import base64
import json
import secrets

from .control_spool import SourceChange,SpoolConflict,_command,_database,_uuid
from .source_history_verifier import NativeSourceHistoryVerifier,_Coverage,_json,_hash
from .source_read_gate import binding_key
from .kaggle_recovery_dispatch import RecoveryPhaseDispatch


class WssNativeHistoryProofAdapter:
    """Concrete startup adapter; existing queue and private native observations."""
    def __init__(self,*,database,expected_catalog_sha256,expected_acl_sha256,expected_observer_sha256,
                 coverage_seconds=30,timeout_seconds=10,max_commands=2048,max_history_bytes=4*1024*1024):
        self.database=database;self.catalog=expected_catalog_sha256;self.acl=expected_acl_sha256
        self.observer=expected_observer_sha256;self.coverage_seconds=coverage_seconds
        self.timeout=timeout_seconds;self.max_commands=max_commands;self.max_bytes=max_history_bytes
        if type(self.observer) is not str or len(self.observer)!=64 or set(self.observer)-set('0123456789abcdef'):
            raise ValueError('independent native observer source pin required')
        self._bound=None

    def bind(self,*,spool,routes,binding_current,writer_generation):
        if self._bound is not None:raise ValueError('history adapter already bound')
        if writer_generation!=self.database.writer_generation:raise ValueError('native history writer differs')
        self._bound=WssSourceHistoryVerifier(spool,acquire=None,binding_current=binding_current,routes=dict(routes),
            expected_catalog_sha256=self.catalog,expected_acl_sha256=self.acl,writer_generation=writer_generation,
            timeout_seconds=self.timeout,coverage_seconds=self.coverage_seconds,max_commands=self.max_commands,
            max_history_bytes=self.max_bytes,database=self.database,observer_sha256=self.observer)
        return self._bound


class WssSourceHistoryVerifier(NativeSourceHistoryVerifier):
    def __init__(self,*args,database,observer_sha256,**kwargs):
        super().__init__(*args,**kwargs)
        self.database,self.observer_hash=database,observer_sha256
        self._native_prefix=None

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

    def _classify(self,raw,ref):
        c=_command(raw);h=self.spool.registry.resolve(c);dep=h.dependency
        if (dep is None or dep.scope_class!='public_source' or dep.scope_owner!='shared'
                or dep.resource_keys!=frozenset({'source_id'}) or h.transition is not None
                or set(c.ids)!={'source_id'} or c.route not in ('source.import.v1','source.import.v2','source.withdraw.v1')
                or c.route in ('source.import.v1','source.import.v2') and 'source.import' not in self.routes):
            raise SpoolConflict('unclassified accepted source dependency')
        _uuid(c.ids['source_id'])
        return c,h,ref

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
            session=None
            async with asyncio.timeout(self.timeout_seconds):
                if not self.binding_current(binding) or not self._routes_current() or self._registry_hash()!=self.registry_hash:
                    raise SpoolConflict('source route or handler closure changed')
                if mode!='full' and await self._head_unchanged(previous.head):head=previous.head
                else:head=await j.head()
                base=previous.head.reference if previous and previous.head else None
                commands=[];ref=head.reference if head else None;used=0
                stop=None if mode=='full' else base
                for _ in range(j.max_reconcile_hops):
                    if ref==stop:break
                    if ref is None:raise SpoolConflict('verified source prefix is not an exact ancestor')
                    older,raws=await j.read_segment(ref)
                    for raw in raws:
                        used+=len(raw)
                        if used>self.max_history_bytes or len(commands)>=self.max_commands:
                            raise SpoolConflict('native source history budget exceeded')
                        commands.append(self._classify(raw,ref))
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
                        known[c.sequence]=(c,h,m);used+=len(raw)
                if len(known)>self.max_commands or used>self.max_history_bytes or len({c.operation_key for c,_,_ in known.values()})!=len(known):
                    raise SpoolConflict('source pending history bound or identity differs')
                pending_earlier=sorted(seq for seq in known if mode!='full' and seq<=previous.applied_sequence)
                nonce=secrets.token_hex(24)
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
                checkpoint_key=self._checkpoint_key(key[3]);remote=await j._read(checkpoint_key,8192)
                if remote is None:
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
                    await j._fenced_exact(checkpoint_key,self._checkpoint(key[3]),expected_token=None,
                        writer_generation=self.writer_generation)
                else:self._verify_genesis(remote.data,key[3])
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
                        remote=await j._read(j.prefix+'/outcomes/'+_hash(c.operation_key.encode()),j.max_outcome_bytes)
                        if remote is not None:
                            value=json.loads(remote.data)
                            try:body=base64.b64decode(value['outcome_b64'],validate=True)
                            except (KeyError,ValueError):raise SpoolConflict('remote source outcome bytes differ') from None
                            used+=len(body)
                            expected_outcome={'codec':'rkb.outcome.v1','stream':j.stream,'command_sha256':_hash(c.encode()),
                                'sequence':c.sequence,'operation_key':c.operation_key,'database_generation':key[3],
                                'outcome_b64':base64.b64encode(body).decode(),'outcome_sha256':outcome[1],'outcome_length':outcome[2]}
                            if (remote.data!=_json(expected_outcome) or len(body)!=outcome[2] or _hash(body)!=outcome[1]
                                    or used>self.max_history_bytes):raise SpoolConflict('remote outcome differs from native exact prefix')
                            durable=True
                        if c.route in ('source.import.v1','source.import.v2') and outcome[0]=='applied':origins.append(c.ids['source_id'])
                    if not durable:pending.append((c.encode(),h.fingerprint,m))
                    source=c.ids['source_id'];old=changes.get(source)
                    changes[source]=SourceChange(source,c.sequence,_hash(c.encode()),bool(h.dependency.restriction or old and old.restriction),
                        bool(not durable or old and old.pending),False)
                if mode=='renew':pending=list(previous.pending)
                if not await self._head_unchanged(head) or s._writing or s._source_version is not version or not self.binding_current(binding):
                    raise SpoolConflict('source prefix changed during native verification')
                closed=await self._phase(binding,dict(action='close',session=session,nonce=nonce,
                    origin_ids=sorted(set(origins))))
                session=None
                if (closed.get('nonce')!=nonce or closed.get('applied_sequence')!=applied
                        or type(closed.get('prefix')) is not str or not 1<=len(closed['prefix'])<=256
                        or not await self._head_unchanged(head) or s._writing or s._source_version is not version
                        or not self.binding_current(binding)):
                    raise SpoolConflict('native source completion changed')
                if mode=='full':await s._write(s._replace_source_heads,tuple(changes.values()))
                elif changes:await s._write(self._merge_source_heads,tuple(changes.values()))
                self._native_prefix=closed['prefix']
                coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,nonce,head,accepted,applied,tuple(pending))
                self._coverage=coverage;s._source_anchor=coverage;s._source_head=head.reference if head else None
                s._source_covered_sequence=accepted;s._source_version=object()
                return coverage
