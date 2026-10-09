"""Local corpus authority and narrow SQLite SQL adapter for existing consumers.

Temporary filtered relational views expose only actor-authorized corpus rows.
Writes go through local ownership/provenance guards in the same short SQLite
transaction. Remote services are never consulted for corpus authorization.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
json_module=json
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime,timezone
from uuid import UUID,uuid4
from urllib.parse import urlparse
from .sqlite_corpus import COLUMNS,TABLES,canonical,row_key
from .postgres_backend import _DbResponse
from .rights import assert_visibility_allowed
from .contracts import Visibility,RightsStatus

BOOL_FIELDS={c['column_name'] for cols in COLUMNS.values() for c in cols if c['data_type']=='boolean'}
JSON_FIELDS={c['column_name'] for cols in COLUMNS.values() for c in cols if c['data_type'] in ('json','jsonb','ARRAY')}

def scalar(value):
    if hasattr(value,'obj'):value=value.obj
    if isinstance(value,(dict,list,tuple)):return canonical(value)
    if isinstance(value,(UUID,datetime)):return str(value)
    return value


def defaults(table):
    row={}
    for c in COLUMNS[table]:
        raw=c.get('column_default');name=c['column_name']
        if raw is None:row[name]=None
        elif raw=='now()':row[name]=str(datetime.now(timezone.utc))
        elif 'gen_random_uuid' in raw:row[name]=str(uuid4())
        elif raw in ('true','false'):row[name]=raw=='true'
        elif raw.startswith("'"):
            value=raw[1:raw.rfind("'")].replace("''", "'")
            if c['data_type']=='ARRAY':value=[] if value=='{}' else json.loads(value)
            elif c['data_type'] in ('json','jsonb'):value=json.loads(value)
            row[name]=value
        elif re.fullmatch(r'\d+',raw):row[name]=int(raw)
        else:raise ValueError('unsupported local default: '+name)
    return row


class LocalContext:
    def __init__(self,corpus,db,actor):self.corpus=corpus;self.db=db;self.actor=actor;self.activation=False
    def rows(self,table,filters=None):
        result=[];where=['table_name=?'];args=[table]
        for key,value in (filters or {}).items():
            if not re.fullmatch(r'\w+',key):raise ValueError('invalid local column')
            expression=key if key in ('document_id','revision') else "json_extract(payload,'$."+key+"')"
            if isinstance(value,(list,tuple)):
                where.append(expression+' in(select value from json_each(?))');args.append(canonical(value))
            else:where.append(expression+'=?');args.append(value)
        for r in self.db.execute('select payload from corpus_rows where '+' and '.join(where),args):
            value=json.loads(r[0])
            if table=='rkb_chunks':
                t=self.db.execute('select source_text,search_material from chunk_text where chunk_id=?',(value['id'],)).fetchone()
                if t:
                    value['source_text']=t['source_text']
                    if 'search_material' not in value:value['search_material']=t['search_material']
            result.append(value)
        return result
    def one(self,table,ident):
        r=self.db.execute('select payload from corpus_rows where table_name=? and row_key=?',(table,str(ident))).fetchone()
        if not r:return None
        value=json.loads(r[0])
        if table=='rkb_chunks':
            t=self.db.execute('select source_text,search_material from chunk_text where chunk_id=?',(str(ident),)).fetchone()
            if t:
                value['source_text']=t['source_text']
                if 'search_material' not in value:value['search_material']=t['search_material']
        return value
    def owned(self,doc):
        d=self.one('rkb_documents',doc);return bool(d and d['owner_user_id']==self.actor)
    def readable(self,doc):
        if self.actor is None:return True
        d=self.one('rkb_documents',doc)
        if not d:return False
        if d['owner_user_id']==self.actor:return True
        evidence=d.get('rights_evidence') or {}
        if d.get('content_visibility')=='public' and evidence.get('public_distribution') is True and d.get('rights_policy_version') and d.get('rights_status') in ('licensed','permission_granted','public_domain_verified','statutory_access_verified'):return True
        if any(g['document_id']==doc and g['grantee_user_id']==self.actor for g in self.rows('rkb_document_grants')):return True
        return bool(d.get('workspace_id') and d.get('content_visibility') in ('workspace','public') and any(m['workspace_id']==d['workspace_id'] and m['user_id']==self.actor for m in self.rows('rkb_workspace_members')))
    def asset(self,doc,visibility):
        if self.actor is None or self.owned(doc):return True
        d=self.one('rkb_documents',doc)
        if not d:return False
        if visibility=='public':return d.get('content_visibility')=='public' and self.readable(doc)
        if visibility=='workspace':return bool(d.get('workspace_id') and any(m['workspace_id']==d['workspace_id'] and m['user_id']==self.actor for m in self.rows('rkb_workspace_members')))
        return visibility=='private' and any(g['document_id']==doc and g['grantee_user_id']==self.actor for g in self.rows('rkb_document_grants'))
    def document(self,table,row):
        if table=='rkb_documents':return row['id']
        if row.get('document_id'):return row['document_id']
        if table=='rkb_regions':
            p=self.one('rkb_pages',row.get('page_id'));return p.get('document_id') if p else None
        if table=='rkb_region_relations':
            r=self.one('rkb_regions',row.get('source_region_id'));return self.document('rkb_regions',r) if r else None
        if table.startswith('rkb_chunk_embeddings'):
            c=self.one('rkb_chunks',row.get('chunk_id'));return c.get('document_id') if c else None
        return None
    def visible(self,table,payload):
        if self.actor is None:return True
        r=json.loads(payload)
        if table=='rkb_users':return r['id']==self.actor
        if table in ('rkb_ingestion_jobs','rkb_integration_outbox'):return r.get('owner_user_id')==self.actor
        if table=='rkb_graph_discovery_jobs':return r.get('actor_id')==self.actor
        if table=='rkb_author_profiles':return r.get('verification_state')=='verified'
        if table=='rkb_author_authority':return r.get('is_active') is True
        if table=='rkb_workspaces':return r.get('owner_user_id')==self.actor or any(m['workspace_id']==r['id'] and m['user_id']==self.actor for m in self.rows('rkb_workspace_members'))
        if table=='rkb_workspace_members':return r.get('user_id')==self.actor or (self.one('rkb_workspaces',r['workspace_id']) or {}).get('owner_user_id')==self.actor
        doc=self.document(table,r)
        if not doc:return False
        if table=='rkb_objects':return self.asset(doc,r.get('access_class','private'))
        if table=='rkb_illustrations':return self.asset(doc,r.get('visibility','private'))
        if table=='rkb_document_grants':return self.owned(doc) or r['grantee_user_id']==self.actor
        return self.readable(doc)
    def check_evidence(self,doc,revision,e):
        if isinstance(e,str):e=json.loads(e)
        c=self.one('rkb_chunks',e.get('chunk_id'));p=self.one('rkb_pages',e.get('page_id'));r=self.one('rkb_regions',e.get('region_id'))
        return bool(c and p and r and self.readable(doc) and c['document_id']==doc and c['revision']==revision and p['document_id']==doc and p['revision']==revision and r['page_id']==p['id'] and p['id'] in c.get('page_ids',[]) and r['id'] in c.get('region_ids',[]) and 1<=len(e.get('exact_quote',''))<=2000 and e['exact_quote'] in (r.get('source_text') or '') and e['exact_quote'] in c['source_text'])
    def check_write(self,table,old,new):
        if self.actor is None:return
        row=new or old
        if table in ('rkb_author_profiles','rkb_author_authority','rkb_users','rkb_workspace_members','rkb_workspaces'):
            raise PermissionError('internal state writes require service identity')
        if table=='rkb_integration_outbox':raise PermissionError('outbox writes require activation/service identity')
        if table=='rkb_objects':raise PermissionError('object registration requires service identity')
        if table=='rkb_graph_discovery_jobs':
            if row['actor_id']!=self.actor:raise PermissionError('job actor mismatch')
            return
        doc=self.document(table,row)
        if table in ('rkb_entity_mentions','rkb_entity_aliases'):
            node=self.one('rkb_entities',row['entity_id'])
            if not self.readable(doc) or not node or node['owner_user_id']!=self.actor:raise PermissionError('entity ownership required')
        elif table=='rkb_entities':
            if row.get('owner_user_id')!=self.actor or not self.readable(doc):raise PermissionError('entity ownership required')
        elif not (table=='rkb_documents' and old is None and new.get('owner_user_id')==self.actor) and not self.owned(doc):raise PermissionError('document ownership required')
        if old and self.document(table,old)!=doc:raise PermissionError('cannot change source identity')
        if table=='rkb_documents':
            if old and any(row.get(k)!=old.get(k) for k in ('id','owner_user_id','source_sha256')):raise PermissionError('immutable source identity')
            if row.get('owner_user_id')!=self.actor or row.get('source_visibility')!='private':raise PermissionError('private source ownership required')
            if old and not self.activation and row.get('active_revision')!=old.get('active_revision'):raise PermissionError('activation RPC required')
            if row.get('content_visibility')=='public':assert_visibility_allowed(Visibility.PUBLIC,RightsStatus(row['rights_status']),evidence=row.get('rights_evidence') or {},policy_version=row.get('rights_policy_version'))
        if table in ('rkb_chunks','rkb_pages','rkb_regions','rkb_region_relations','rkb_illustrations'):
            revision=row.get('revision')
            if revision is None:
                p=self.one('rkb_pages',row.get('page_id'))
                if not p and table=='rkb_region_relations':
                    r=self.one('rkb_regions',row.get('source_region_id'));p=self.one('rkb_pages',r.get('page_id')) if r else None
                revision=p.get('revision') if p else None
            d=self.one('rkb_documents',doc)
            if revision is None or revision<=d.get('active_revision',0):raise PermissionError('selected/old revision is immutable')
        if table=='rkb_region_relations' and new:
            source=self.one('rkb_regions',row['source_region_id']);target=self.one('rkb_regions',row['target_region_id'])
            sp=self.one('rkb_pages',source['page_id']) if source else None;tp=self.one('rkb_pages',target['page_id']) if target else None
            if not sp or not tp or (sp['document_id'],sp['revision'])!=(tp['document_id'],tp['revision']):raise ValueError('relation escapes revision')
        if table=='rkb_entity_relations' and new:
            source=self.one('rkb_entities',row['source_id'])
            target=self.one('rkb_entities',row['target_id'])
            if not source or not target or any(
                    entity['owner_user_id']!=self.actor for entity in (source,target)):
                raise PermissionError('graph relation identity ownership required')
            # Postgres guard and typed graph bundles enforce the same directed
            # endpoint contract. Do not allow direct SQL to bypass the model
            # validator or confuse organization, physical place and person.
            from .entity_graph import valid_relation_shape
            if not valid_relation_shape(
                    row['kind'],source['kind'],target['kind'],
                    same_entity=row['source_id']==row['target_id']):
                raise ValueError('invalid graph relation shape')
        if table=='rkb_illustrations' and row.get('visibility')=='public':assert_visibility_allowed(Visibility.PUBLIC,RightsStatus(row['rights_status']),evidence=row.get('rights_evidence') or {},policy_version=row.get('rights_policy_version'))
        if table.startswith('rkb_entity_') and new:
            evidence=row.get('evidence') or {};items=evidence if isinstance(evidence,list) else [evidence]
            if not all(self.check_evidence(doc,row['revision'],e) for e in items):raise ValueError('invalid graph evidence')
        if table=='rkb_entities' and row.get('owner_user_id')!=self.actor:raise PermissionError('entity actor mismatch')
    def store(self,table,payload,mode='update'):
        row=json.loads(payload) if isinstance(payload,str) else payload
        if mode=='insert':
            base=defaults(table);base.update({k:v for k,v in row.items() if v is not None});row=base
        key=row_key(table,row);old=self.one(table,key)
        if old and mode=='update':row={**old,**row}
        self.check_write(table,old,row)
        if mode=='insert' and old:return 1
        self.corpus.put(table,[row],connection=self.db)
        if table=='rkb_entity_mentions' and row:
            # SAME transaction as accepted graph mention. Never wait for
            # Street Story, cartography, geocoding or a model under this lock.
            from .geo_resolution import enqueue_mention
            node=self.one('rkb_entities',row['entity_id'])
            if node and node.get('kind')=='poi_ref':
                enqueue_mention(self.db,self.actor or node['owner_user_id'],
                                row,node)
        return 1
    def remove(self,table,key):
        old=self.one(table,key)
        if not old:return 0
        self.check_write(table,old,None)
        if table=='rkb_pages':
            for r in self.rows('rkb_regions'):
                if r['page_id']==key:self.remove('rkb_regions',r['id'])
            for r in self.rows('rkb_illustrations'):
                if r['page_id']==key:self.remove('rkb_illustrations',r['id'])
        if table=='rkb_regions':
            for r in self.rows('rkb_region_relations'):
                if key in (r['source_region_id'],r['target_region_id']):self.remove('rkb_region_relations',row_key('rkb_region_relations',r))
        if table=='rkb_chunks':self.db.execute('delete from chunk_text where chunk_id=?',(key,))
        self.db.execute('delete from corpus_rows where table_name=? and row_key=?',(table,key));return 1
    def prepare(self):
        self.db.create_function('rkb_current_actor_id',0,lambda:self.actor)
        self.db.create_function('rkb_can_read_document',1,self.readable)
        self.db.create_function('rkb_graph_active',2,lambda doc,rev:bool(self.readable(doc) and (self.one('rkb_documents',doc) or {}).get('active_revision')==rev))
        self.db.create_function('rkb_graph_owned',1,self.owned)
        self.db.create_function('rkb_graph_check_evidence',3,self.check_evidence)
        self.db.create_function('local_visible',2,self.visible)
        self.db.create_function('local_store',3,self.store)
        self.db.create_function('local_remove',2,self.remove)
        for table,columns in COLUMNS.items():
            names=[c['column_name'] for c in columns]
            fields=[]
            for name in names:
                if table=='rkb_chunks' and name=='source_text':expr='t.source_text'
                elif table=='rkb_chunks' and name=='search_material':expr="case when json_type(r.payload,'$.search_material')='null' then null else t.search_material end"
                elif name=='id':expr='r.row_key'
                elif name=='document_id' and table not in ('rkb_regions','rkb_region_relations'):expr='r.document_id'
                elif name=='revision':expr='r.revision'
                else:expr=f"json_extract(r.payload,'$.{name}')"
                fields.append(expr+' as "'+name+'"')
            join=' left join chunk_text t on t.chunk_id=r.row_key' if table=='rkb_chunks' else ''
            self.db.execute(f'create temp view "{table}" as select '+','.join(fields)+f",r.row_key as _local_key from corpus_rows r{join} where r.table_name='{table}' and local_visible('{table}',r.payload)")
            pairs=[]
            for c in columns:
                n=c['column_name'];expr=f'new."{n}"'
                if c['data_type'] in ('ARRAY','json','jsonb'):expr='json('+expr+')'
                elif c['data_type']=='boolean':expr="json(case when "+expr+" then 'true' else 'false' end)"
                pairs.extend(["'"+n+"'",expr])
            payload='json_object('+','.join(pairs)+')'
            self.db.execute(f"create temp trigger '{table}_insert' instead of insert on '{table}' begin select local_store('{table}',{payload},'insert');end")
            self.db.execute(f"create temp trigger '{table}_update' instead of update on '{table}' begin select local_store('{table}',{payload},'update');end")
            self.db.execute(f"create temp trigger '{table}_delete' instead of delete on '{table}' begin select local_remove('{table}',old._local_key);end")


def translate(statement):
    s=statement.replace('public.','')
    s=re.sub(r'\b([\w.]+)\s*=\s*any\((%s|[\w.]+)(?:::[\w]+\[\])?\)',r'\1 in (select value from json_each(\2))',s,flags=re.I)
    s=re.sub(r'::(?:uuid\[\]|text\[\]|[a-zA-Z_][\w]*)(?:\(\d+\))?','',s)
    s=re.sub(r'now\(\)\s*([+-])\s*interval\s*\'([^\']+)\'',lambda m:"datetime('now','"+m[1]+m[2]+"')",s,flags=re.I)
    s=re.sub(r'now\(\)',"datetime('now')",s,flags=re.I)
    s=re.sub(r'\s+for\s+(?:update|share)(?:\s+of\s+\w+)?(?:\s+skip\s+locked)?','',s,flags=re.I)
    s=re.sub(r'\boffset\s+(%s|\d+)\s+limit\s+(%s|\d+)',r'limit \2 offset \1',s,flags=re.I) if '%s' not in s else s
    # Bound offset parameters retain their order through LIMIT -1 OFFSET.
    s=re.sub(r'\boffset\s+%s\s+limit\s+(\d+)',r'limit \1 offset %s',s,flags=re.I)
    s=re.sub(r'\s+on conflict\s*\([^)]*\)\s*do nothing\s*$', '',s,flags=re.I)
    return s.replace('%s','?')

class LocalCursor:
    def __init__(self,cursor):self.cursor=cursor;self.rowcount=cursor.rowcount if cursor else 0
    def decode(self,row):
        if row is None:return None
        result={}
        for key,value in dict(row).items():
            if key=='_local_key':continue
            if key in BOOL_FIELDS and value is not None:value=bool(value)
            elif key in JSON_FIELDS and isinstance(value,str):
                try:value=json.loads(value)
                except ValueError:pass
            elif (key=='id' or key.endswith('_id')) and isinstance(value,str):
                try:value=UUID(value)
                except ValueError:pass
            result[key]=value
        return result
    async def fetchone(self):return self.decode(self.cursor.fetchone())
    async def fetchall(self):return [self.decode(r) for r in self.cursor.fetchall()]

class LocalConnection:
    def __init__(self,context):self.context=context;self.db=context.db
    async def execute(self,statement,values=()):
        if not isinstance(statement,str):statement=statement.as_string(None)
        if 'rkb_index_counts(' in statement:
            from .e5_contract import SPACE as es
            from .bge_contract import SPACE as bs,REVISION
            query="""select count(*) active_chunks,
              coalesce(sum(json_extract(e.payload,'$.embedding_space')=? and json_extract(e.payload,'$.revision')=t.revision and json_extract(e.payload,'$.text_sha256')=t.text_sha256 and json_extract(e.payload,'$.search_material_sha256')=t.search_material_sha256),0) e5_ready,
              coalesce(sum(json_extract(b.payload,'$.embedding_space')=? and json_extract(b.payload,'$.model_revision')=? and json_extract(b.payload,'$.revision')=t.revision and json_extract(b.payload,'$.text_sha256')=t.text_sha256 and json_extract(b.payload,'$.search_material_sha256')=t.search_material_sha256),0) bge_ready
              from corpus_rows d join chunk_text t on t.document_id=d.row_key and t.revision=json_extract(d.payload,'$.active_revision')
              left join corpus_rows e on e.table_name='rkb_chunk_embeddings_e5' and e.row_key=t.chunk_id
              left join corpus_rows b on b.table_name='rkb_chunk_embeddings_bge' and b.row_key=t.chunk_id
              where d.table_name='rkb_documents' and local_visible('rkb_documents',d.payload)
                and (? is not null or coalesce(json_extract(d.payload,'$.catalog.searchable'),1)<>0)
                and (? is null or d.row_key=?)"""
            target=str(values[0]) if values and values[0] else None
            return LocalCursor(await asyncio.to_thread(self.db.execute,query,(es,bs,REVISION,target,target,target)))
        if 'pg_advisory' in statement:return LocalCursor(self.db.execute('select 1 as locked'))
        if 'string_agg' in statement:
            text=','.join(str(r['id'])+str(r['active_revision']) for r in sorted(self.context.rows('rkb_documents'),key=lambda r:r['id']) if r['active_revision']>0 and self.context.readable(r['id']))
            return LocalCursor(self.db.execute('select ? as version',(hashlib.md5(text.encode()).hexdigest(),)))
        query=translate(statement);args=tuple(scalar(v) for v in values)
        def execute():
            if re.match(r'\s*(insert|update|delete|replace)',query,re.I) and not self.db.in_transaction:self.db.execute('begin immediate')
            return self.db.execute(query,args)
        return LocalCursor(await asyncio.to_thread(execute))

class SQLiteDataClient:
    def __init__(self,corpus,backend):self.corpus=corpus;self.backend=backend
    @staticmethod
    def actor(headers):
        if (headers or {}).get('x-rkb-actor'):return str(UUID(headers['x-rkb-actor']))
        if (headers or {}).get('x-rkb-service')=='1':return None
        raise PermissionError('application actor context required')
    @asynccontextmanager
    async def _connection(self,headers,*,write=False):
        actor=self.actor(headers)
        with self.corpus.connect() as db:
            if write:await asyncio.to_thread(db.execute,'begin immediate')
            ctx=LocalContext(self.corpus,db,actor)
            if actor:
                user=ctx.one('rkb_users',actor)
                if user and user['status']!='active':raise PermissionError('regional knowledge account is disabled')
                if not user:
                    if not db.in_transaction:await asyncio.to_thread(db.execute,'begin immediate')
                    user=ctx.one('rkb_users',actor)
                    if not user:self.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':actor}],connection=db)
                    elif user['status']!='active':raise PermissionError('regional knowledge account is disabled')
                    if not write:await asyncio.to_thread(db.commit)
            ctx.prepare();yield LocalConnection(ctx)
    async def _ensure_open(self):pass
    async def aclose(self):pass
    @staticmethod
    def route(url):
        path=urlparse(url).path.split('/rest/v1/',1)[1].strip('/')
        if path.startswith('rpc/'):return path[4:],True
        if path not in TABLES:raise ValueError('unsupported local corpus table')
        return path,False
    async def get(self,url,*,headers=None,params=None):
        table,rpc=self.route(url)
        if rpc:raise ValueError('GET RPC unsupported')
        args=dict(params or {});select=args.pop('select','*');limit=int(args.pop('limit',1000));offset=int(args.pop('offset',0));order=args.pop('order',None)
        if not 1<=limit<=1000 or offset<0:raise ValueError('invalid pagination')
        if select!='*' and not all(re.fullmatch(r'\w+',x) for x in select.split(',')):raise ValueError('invalid columns')
        values=[];where=[]
        for k,v in args.items():
            if not re.fullmatch(r'\w+',k) or not v.startswith('eq.'):raise ValueError('exact filters required')
            where.append('"'+k+'"=?');values.append(v[3:])
        statement='select '+select+' from '+table+(' where '+' and '.join(where) if where else '')
        if order:
            n,_,d=order.partition('.')
            if not re.fullmatch(r'\w+',n) or d not in ('asc','desc'):raise ValueError('invalid order')
            statement+=' order by '+n+' '+d
        async with self._connection(headers) as db:
            rows=await(await db.execute(statement+' limit ? offset ?',[*values,limit,offset])).fetchall()
        return _DbResponse(rows)
    async def post(self,url,*,headers=None,json=None):
        table,rpc=self.route(url)
        if rpc:return await self.backend.local_rpc(table,json or {},headers)
        rows=json if isinstance(json,list) else [json]
        async with self._connection(headers,write=True) as db:
            for row in rows:
                old=db.context.one(table,row_key(table,row)) if row.get('id') else None
                new={**defaults(table),**row}
                if table=='rkb_document_grants':
                    old=db.context.one(table,row_key(table,new))
                db.context.check_write(table,old,new)
                self.corpus.put(table,[new],connection=db.db)
        return _DbResponse([])
    async def patch(self,url,*,headers=None,params=None,json=None):
        table,rpc=self.route(url)
        if rpc or not params:raise ValueError('scoped table patch required')
        async with self._connection(headers,write=True) as db:
            rows=await self._selected(db,table,params);updated=[]
            for old in rows:
                new={**old,**(json or {})};db.context.check_write(table,old,new)
                self.corpus.put(table,[new],connection=db.db);updated.append(new)
        return _DbResponse(updated if 'return=representation' in (headers or {}).get('Prefer','') else [])
    async def _selected(self,db,table,params):
        conditions=[];values=[]
        for k,v in params.items():
            if not re.fullmatch(r'\w+',k) or not v.startswith('eq.'):raise ValueError('exact scoped filters required')
            conditions.append(k+'=?');values.append(v[3:])
        raw=db.db.execute('select * from '+table+' where '+' and '.join(conditions),values).fetchall()
        return [json_module.loads(canonical(LocalCursor(None).decode(r))) for r in raw]
    async def delete(self,url,*,headers=None,params=None):
        table,rpc=self.route(url)
        if rpc or not params:raise ValueError('scoped delete required')
        async with self._connection(headers,write=True) as db:
            rows=await self._selected(db,table,params)
            for row in rows:db.context.remove(table,row_key(table,{k:str(v) if isinstance(v,UUID) else v for k,v in row.items()}))
        return _DbResponse([])
