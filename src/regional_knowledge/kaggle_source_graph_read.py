"""Fixed accepted-source graph reads with actual source visibility dependencies."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from typing import Annotated,Literal

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from pydantic import Field,model_validator

from .geo_migration import canonical,exact_uuid
from .kaggle_protocol import ReadIdentity,TrustedActorContext,Identifier,Nonnegative,EvidenceItem,EvidenceMetadata,ItemDependencies,SourceDependency
from .kaggle_query import ReadUnavailable,_frontier,_identity

SOURCE_GRAPH_READER_FUNCTIONS=('read_source_graph_v1(uuid,uuid,text,uuid,text,integer)',)



class SourceGraphReadRequest(ReadIdentity):
    type:Literal['SOURCE_GRAPH_READ']='SOURCE_GRAPH_READ'
    protocol_version:Literal[2]=2
    operation:Literal['source_graph_read']='source_graph_read'
    operation_version:Literal[1]=1
    action:Literal['entity','list','mentions','aliases','relations','poi_facts']='entity'
    entity_id:str|None=None
    source_id:str|None=None
    # Existing entity_list selectors; only a single accepted source is allowed.
    kinds:tuple[Literal['person','organization','event','historical_thread','poi_ref'],...]|None=Field(
        default=None,min_length=1,max_length=5)
    query:Annotated[str,Field(max_length=200)]=''
    cursor:Annotated[str,Field(max_length=1600)]|None=None
    limit:Annotated[int,Field(ge=1,le=3)]=3
    actor:TrustedActorContext
    min_data_seq:Nonnegative
    deadline_ms:Annotated[int,Field(gt=0)]
    compute_claim:Identifier|None=None

    @model_validator(mode='after')
    def exact_scope(self):
        for value in (self.entity_id,self.source_id):
            if value is not None:exact_uuid(value)
        if self.action in ('list','poi_facts'):
            if self.source_id is None or self.entity_id is not None:raise ValueError('exact accepted source scope required')
        elif self.entity_id is None:raise ValueError('exact source entity UUID required')
        if self.action!='list' and (self.kinds is not None or self.query):
            raise ValueError('entity selectors require a source-scoped list')
        if self.kinds is not None and len(set(self.kinds))!=len(self.kinds):
            raise ValueError('duplicate entity kinds')
        if self.query and (not self.query.strip() or len(self.query.encode('utf-8'))>512):
            raise ValueError('bounded nonblank entity query required')
        return self


class SourceGraphReadResult(ReadIdentity):
    type:Literal['SOURCE_GRAPH_READ_RESULT']='SOURCE_GRAPH_READ_RESULT'
    protocol_version:Literal[2]=2
    status:Literal['ok','unavailable','invalid_query','denied','cancelled']
    evidence:tuple[EvidenceItem,...]=Field(default=(),max_length=3)
    records:tuple[dict,...]=Field(default=(),max_length=3)
    next_cursor:Annotated[str,Field(max_length=1600)]|None=None
    observed_data_seq:Nonnegative|None
    observed_projection_seq:Nonnegative|None
    observed_visibility_epoch:Nonnegative|None=None
    observed_database_generation:Identifier|None=None
    reason:Literal['projection_lag','deadline_exceeded','not_ready','disconnected','caller_cancelled','turn_replaced','query_too_large',
        'invalid_input','access_denied','prior_write_pending','evidence_withdrawn','internal_error']|None=None
    prior_write_pending:bool=False

    @model_validator(mode='after')
    def source_dependencies(self):
        if self.status=='ok' and (self.reason is not None or self.prior_write_pending or any(value is None for value in
                (self.observed_data_seq,self.observed_projection_seq,self.observed_visibility_epoch,self.observed_database_generation))):
            raise ValueError('exact source graph frontier required')
        if self.status!='ok' and (self.records or self.evidence or self.next_cursor):raise ValueError('failed graph result cannot carry data')
        if len(self.records)!=len(self.evidence):raise ValueError('every graph record requires actual source dependencies')
        for record,item in zip(self.records,self.evidence):
            if (set(record)!={'id','source_id','source_revision','entity_id','action','data','semantic_status'} or record['id']!=item.id
                    or item.kind!=('geo' if record['action']=='poi_facts' else 'entity')
                    or record['semantic_status']!='model_authored_source_claim_not_verified_identity_or_geometry'
                    or item.dependencies.source_ids!=(record['source_id'],) or len(item.dependencies.source_revisions)!=1
                    or item.dependencies.source_revisions[0].content_revision!=record['source_revision']
                    or type(record['data']) is not dict or len(canonical(record['data']))>96*1024):raise ValueError('exact graph record/source pairing required')
            exact_uuid(record['source_id'])
            if record['entity_id'] is not None:exact_uuid(record['entity_id'])
            try:
                data=record['data'];action=record['action']
                if action in ('entity','list'):
                    key=data['key'];quote=data['mention']['evidence']['exact_quote']
                    if data['entity_id']!=record['entity_id'] or data['kind'] not in ('person','organization','event','historical_thread','poi_ref'):
                        raise ValueError('source entity identity differs')
                elif action in ('mentions','aliases','relations'):
                    field={'mentions':'mention_id','aliases':'alias_id','relations':'relation_id'}[action]
                    key=data[field];exact_uuid(key)
                    quote=data['evidence'][0]['exact_quote'] if action=='relations' else data['evidence']['exact_quote']
                elif action=='poi_facts':
                    key=data['candidate_id'];exact_uuid(key);quote=data['text']
                    if data['geometry_acceptance']!='source_hypothesis_not_owner_geometry':raise ValueError('source geometry hypothesis required')
                else:raise ValueError('fixed source graph action required')
                if record['id']!=record['source_id']+':'+key or item.text!=quote:
                    raise ValueError('source graph identity/evidence text differs')
            except (KeyError,TypeError,IndexError):
                raise ValueError('invalid source graph record material') from None
        if len(canonical(self.model_dump(mode='json')))>256*1024:raise ValueError('source graph result byte bound')
        return self


def _scope(request,observed):
    actor=request.actor.model_dump(mode='json')
    if request.action=='list':
        # context_ref is an ephemeral random authority handle minted afresh
        # for EACH OAuth read. Binding a paginated list cursor to that handle
        # makes the promised second MCP call impossible. Bind to the verified
        # person/audience/auth epoch instead; reauthorization and the current
        # source/grant checks still occur independently on every new read.
        actor.pop('context_ref',None)
    scope=dict(action=request.action,entity=request.entity_id,source=request.source_id,
        actor=actor,observed=observed)
    if request.kinds is not None or request.query:
        scope.update(kinds=request.kinds,query=request.query)
    return hashlib.sha256(canonical(scope)).hexdigest()


def _matches_list(record,request):
    data=record['data']
    if request.kinds is not None and data.get('kind') not in request.kinds:return False
    if not request.query:return True
    needle=request.query.strip().casefold()
    names=[data.get('canonical_label'),data['mention']['exact_source_spelling']]
    aliases=data.get('aliases',[])
    if type(aliases) is not list:raise ReadUnavailable('internal_error')
    names.extend(a.get('value') for a in aliases if type(a) is dict)
    return any(type(name) is str and needle in name.casefold() for name in names)


def _cursor(scope,source,key):
    return base64.urlsafe_b64encode(canonical(dict(v=1,scope=scope,source=source,key=key))).decode().rstrip('=')


def _after(raw,scope):
    value=json.loads(base64.b64decode(raw+'='*(-len(raw)%4),altchars=b'-_',validate=True))
    if (set(value)!={'v','scope','source','key'} or type(value['v']) is not int or value['v']!=1 or value['scope']!=scope
            or type(value['key']) is not str or not 1<=len(value['key'])<=100
            or _cursor(scope,value['source'],value['key'])!=raw):raise ValueError('invalid graph cursor')
    exact_uuid(value['source']);return value['source'],value['key']


def _record(row,request):
    source,revision,eligibility,key,data,text,bibliography=row;source=str(source);exact_uuid(source)
    if (revision!=1 or type(eligibility) is not int or eligibility<1 or type(key) is not str or not 1<=len(key)<=100
            or type(data) is not dict or len(canonical(data))>96*1024 or type(text) is not str or not 1<=len(text)<=2000):
        raise ReadUnavailable('internal_error')
    entity=data['entity_id'] if request.action in ('entity','list') else request.entity_id
    if entity is not None:exact_uuid(entity)
    if request.entity_id is not None and entity!=request.entity_id:raise ReadUnavailable('internal_error')
    if request.source_id is not None and source!=request.source_id:raise ReadUnavailable('internal_error')
    if request.action=='relations' and request.entity_id not in (data.get('source_id'),data.get('target_id')):raise ReadUnavailable('internal_error')
    ident=source+':'+key
    record=dict(id=ident,source_id=source,source_revision=revision,entity_id=entity,action=request.action,data=data,
        semantic_status='model_authored_source_claim_not_verified_identity_or_geometry')
    evidence=EvidenceItem(id=ident,kind='geo' if request.action=='poi_facts' else 'entity',text=text,
        metadata=EvidenceMetadata(citation=bibliography[:1024],locator='source-graph:'+key),
        dependencies=ItemDependencies(source_ids=(source,),source_revisions=(SourceDependency(source_id=source,
            content_revision=revision,eligibility_revision=eligibility),),variant_refs=(),grant_refs=()))
    return record,evidence,(source,key)


async def execute_source_graph_read(request:SourceGraphReadRequest,*,acquire):
    if type(request) is not SourceGraphReadRequest:raise TypeError('validated fixed source graph read required')
    loop=asyncio.get_running_loop();deadline=loop.time()+min(request.deadline_ms,2000)/1000;observed={}
    def failed(reason):
        return SourceGraphReadResult(**_identity(request),status='invalid_query' if reason=='invalid_input' else 'unavailable',
            observed_data_seq=observed.get('data'),observed_projection_seq=observed.get('projection'),
            observed_visibility_epoch=observed.get('visibility'),observed_database_generation=observed.get('database'),reason=reason)
    try:
        async with asyncio.timeout_at(deadline):
            async with acquire() as connection:
                if connection.closed or connection.info.transaction_status!=TransactionStatus.IDLE or connection.row_factory is not tuple_row:raise ReadUnavailable('internal_error')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        async def execute(statement,params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",(str(max(1,int((deadline-loop.time())*1000))),))
                            await cursor.execute(statement,params)
                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        await execute('SELECT max(version) FROM rkb_v4.schema_migrations')
                        installed=await cursor.fetchone()
                        if not installed or type(installed[0]) is not int or installed[0]<11:
                            raise ReadUnavailable('not_ready')
                        scope=_scope(request,observed);after=(None,None)
                        if request.cursor:
                            try:after=_after(request.cursor,scope)
                            except (ValueError,TypeError,KeyError):raise ReadUnavailable('invalid_input') from None
                        records=[];evidence=[];positions=[];used=0;next_cursor=None
                        filtered=request.action=='list' and (request.kinds is not None or bool(request.query))
                        # SQL022 applies both selectors before LIMIT, so a
                        # sparse hit takes one source-scoped PG page. Pre-022
                        # workers retain the original bounded keyset fallback:
                        # a new handler may be staged before its DDL is applied.
                        sql_prefilter=filtered and installed[0]>=22
                        after_page=after
                        for page in range(1 if sql_prefilter else 8 if filtered else 1):
                            if sql_prefilter:
                                await execute('SELECT * FROM rkb_v4.read_source_graph_filtered_v1(%s::uuid,%s::uuid,%s,%s::text[],%s,%s)',
                                    (request.source_id,*after_page,
                                     list(request.kinds) if request.kinds is not None else None,
                                     request.query.strip().casefold() if request.query else None,
                                     request.limit+1))
                            else:
                                await execute('SELECT * FROM rkb_v4.read_source_graph_v1(%s::uuid,%s::uuid,%s,%s::uuid,%s,%s)',
                                    (request.entity_id,request.source_id,request.action,*after_page,request.limit+1))
                            rows=await cursor.fetchall()
                            if len(rows)>request.limit+1:raise ReadUnavailable('internal_error')
                            for offset,row in enumerate(rows[:request.limit] if not filtered else rows):
                                record,item,position=_record(row,request)
                                if filtered and not sql_prefilter and not _matches_list(record,request):
                                    after_page=position
                                    continue
                                size=len(canonical(record))+len(canonical(item.model_dump(mode='json')))
                                if used+size>240*1024:
                                    if not records:raise ReadUnavailable('internal_error')
                                    next_cursor=_cursor(scope,*after_page)
                                    break
                                records.append(record);evidence.append(item);positions.append(position);used+=size
                                after_page=position
                                if len(records)==request.limit:
                                    if offset+1<len(rows) or len(rows)==request.limit+1:
                                        next_cursor=_cursor(scope,*after_page)
                                    break
                            if len(records)==request.limit or next_cursor is not None:break
                            if len(rows)<request.limit+1:break
                            if page==7:next_cursor=_cursor(scope,*after_page)
                        # Do not change the no-filter branch or its lookahead.
                        if not filtered and len(rows)>len(records):
                            next_cursor=_cursor(scope,*positions[-1]) if positions else None
                        return SourceGraphReadResult(**_identity(request),status='ok',records=tuple(records),evidence=tuple(evidence),
                            next_cursor=next_cursor,
                            observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
    except ReadUnavailable as exc:return failed(exc.reason)
    except TimeoutError:return failed('deadline_exceeded')
    except asyncio.CancelledError:raise
    except psycopg.errors.QueryCanceled:return failed('deadline_exceeded' if loop.time()>=deadline-.001 else 'internal_error')
    except (psycopg.Error,ValueError,TypeError,KeyError):return failed('internal_error')
