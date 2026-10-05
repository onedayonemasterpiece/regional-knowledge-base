"""Persistent exact corpus, external-content FTS and page fragments.

Only the server opens this database. UUID actor checks precede every public read.
The JSON preserves accepted fields verbatim; it is never sent to the vector plane.
"""
from __future__ import annotations
import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

COLUMNS=json.loads((Path(__file__).with_name('sqlite_columns.json')).read_text())
COLUMNS.pop('rkb_schema_migrations',None)
COLUMNS['rkb_documents'].append({'column_name':'catalog','data_type':'jsonb','udt_name':'jsonb','column_default':"'{}'::jsonb"})
TABLES=frozenset(COLUMNS)



def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str)


def row_key(table,row):
    if 'id' in row:return str(row['id'])
    if table.startswith('rkb_chunk_embeddings_'):return str(row['chunk_id'])
    fields={'rkb_document_grants':('document_id','grantee_user_id'),
      'rkb_workspace_members':('workspace_id','user_id'),
      'rkb_region_relations':('source_region_id','target_region_id','kind'),
      'rkb_author_authority':('author_id','geography','subject','policy_version'),
      'rkb_chunk_embeddings_e5':('chunk_id',),'rkb_chunk_embeddings_bge':('chunk_id',)}[table]
    return canonical([row[k] for k in fields])


class SQLiteCorpus:
    def __init__(self,path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with self.connect() as db:
            db.execute('pragma journal_mode=wal')
            db.executescript('''
            create table if not exists corpus_rows(
              table_name text not null,row_key text not null,document_id text,
              revision integer,payload text not null,primary key(table_name,row_key));
            create index if not exists region_page on corpus_rows(json_extract(payload,'$.page_id')) where table_name='rkb_regions';
            create index if not exists corpus_doc on corpus_rows(table_name,document_id,revision);
            create table if not exists chunk_text(
              rowid integer primary key,chunk_id text unique not null,
              document_id text not null,revision integer not null,
              source_text text not null,search_material text not null,
              text_sha256 text not null,search_material_sha256 text not null);
            create index if not exists chunk_text_doc_revision on chunk_text(document_id,revision,chunk_id);
            create virtual table if not exists chunk_fts using fts5(
              search_material,content='chunk_text',content_rowid='rowid',tokenize='unicode61');
            create trigger if not exists chunk_ai after insert on chunk_text begin
              insert into chunk_fts(rowid,search_material) values(new.rowid,new.search_material);end;
            create trigger if not exists chunk_ad after delete on chunk_text begin
              insert into chunk_fts(chunk_fts,rowid,search_material) values('delete',old.rowid,old.search_material);end;
            create trigger if not exists chunk_au after update on chunk_text begin
              insert into chunk_fts(chunk_fts,rowid,search_material) values('delete',old.rowid,old.search_material);
              insert into chunk_fts(rowid,search_material) values(new.rowid,new.search_material);end;
            create table if not exists chunk_fragments(
              chunk_id text not null references chunk_text(chunk_id) on delete cascade,
              ordinal integer not null,page_id text not null,physical_page_index integer not null,
              region_id text not null,text_start integer not null,text_end integer not null,
              source_sha256 text not null,revision integer not null,
              primary key(chunk_id,ordinal));
            create table if not exists chunk_positions(chunk_id text primary key references chunk_text(chunk_id) on delete cascade,document_id text not null,revision integer not null,start_position integer not null,end_position integer not null,article_id text);
            create index if not exists chunk_position_start on chunk_positions(document_id,revision,article_id,start_position,chunk_id);
            create index if not exists chunk_position_end on chunk_positions(document_id,revision,article_id,end_position,chunk_id);
            create table if not exists component_chunks(chunk_id text primary key references chunk_text(chunk_id) on delete cascade,component_id text not null);
            create table if not exists catalog_components(id text primary key,parent_id text not null,revision integer not null,payload text not null);
            create table if not exists migration_state(key text primary key,value text not null);
            create table if not exists revision_publication(document_id text not null,revision integer not null,ingestion_id text not null,source_sha256 text not null,manifest text not null,poi_events text not null,state text not null default 'pending',primary key(document_id,revision));
            create table if not exists vector_sync(
              chunk_id text primary key,revision integer not null,text_sha256 text not null,
              search_material_sha256 text not null,state text not null default 'pending');
            ''')
        self.path.chmod(0o600)

    @contextmanager
    def connect(self):
        db=sqlite3.connect(self.path,timeout=15,check_same_thread=False)
        db.row_factory=sqlite3.Row
        db.execute('pragma busy_timeout=15000')
        db.execute('pragma foreign_keys=on')
        try:
            with db:yield db
        finally:db.close()

    def put(self,table,rows,*,connection=None):
        if table not in TABLES:raise ValueError('unsupported corpus table')
        from contextlib import nullcontext
        with (self.connect() if connection is None else nullcontext(connection)) as db:
            for row in rows:
                row=json.loads(canonical(row));key=row_key(table,row)
                doc=row.get('document_id') or (key if table=='rkb_documents' else None)
                db.execute('insert into corpus_rows values(?,?,?,?,?) on conflict(table_name,row_key) do update set document_id=excluded.document_id,revision=excluded.revision,payload=excluded.payload',
                  (table,key,doc,row.get('revision'),canonical({k:v for k,v in row.items() if table!='rkb_chunks' or (k!='source_text' and (k!='search_material' or v is None))})))
                if table=='rkb_chunks':
                    text=row.get('source_text');material=row.get('search_material')
                    if text is None:raise ValueError('exact source text required before migration')
                    material=text if material is None else material
                    sha=hashlib.sha256(text.encode()).hexdigest();msha=hashlib.sha256(material.encode()).hexdigest()
                    if sha!=row['text_sha256'] or msha!=row.get('search_material_sha256',sha):raise ValueError('accepted text/hash mismatch')
                    db.execute('''insert into chunk_text(chunk_id,document_id,revision,source_text,search_material,text_sha256,search_material_sha256)
                      values(?,?,?,?,?,?,?) on conflict(chunk_id) do update set document_id=excluded.document_id,revision=excluded.revision,
                      source_text=excluded.source_text,search_material=excluded.search_material,text_sha256=excluded.text_sha256,search_material_sha256=excluded.search_material_sha256''',
                      (key,doc,row['revision'],text,material,sha,msha))

    def rows(self,table,filters=None):
        if table not in TABLES:raise ValueError('unsupported corpus table')
        filters=filters or {}
        with self.connect() as db:
            sql='select payload from corpus_rows where table_name=?';args=[table]
            # Exact identity/document queries use indexes and do not scan the corpus.
            for key in ('id','document_id','revision'):
                if key in filters:
                    sql+=' and '+('row_key' if key=='id' else key)+'=?';args.append(filters[key])
            values=[json.loads(r[0]) for r in db.execute(sql,args)]
            if table=='rkb_chunks':
                for value in values:
                    text=db.execute('select source_text,search_material from chunk_text where chunk_id=?',(value['id'],)).fetchone()
                    if text:
                        value['source_text']=text['source_text']
                        if 'search_material' not in value:value['search_material']=text['search_material']
        return [r for r in values if all(str(r.get(k))==str(v) for k,v in filters.items())]

    def one(self,table,ident):
        if table not in TABLES:raise ValueError('unsupported corpus table')
        with self.connect() as db:
            row=db.execute('select payload from corpus_rows where table_name=? and row_key=?',(table,str(ident))).fetchone()
            if not row:return None
            value=json.loads(row[0])
            if table=='rkb_chunks':
                text=db.execute('select source_text,search_material from chunk_text where chunk_id=?',(str(ident),)).fetchone()
                if text:
                    value['source_text']=text['source_text']
                    if 'search_material' not in value:value['search_material']=text['search_material']
            return value

    def authorize(self,actor,document_id,*,owner=False):
        actor=str(UUID(str(actor)));doc=self.one('rkb_documents',document_id)
        user=self.one('rkb_users',actor)
        if not doc or not user or user.get('status')!='active':raise LookupError('evidence_not_found')
        if str(doc['owner_user_id'])==actor:return doc
        if owner:raise PermissionError('private_source_owner_required')
        if doc.get('content_visibility')=='public' and doc.get('rights_status') in ('licensed','permission_granted','public_domain_verified','statutory_access_verified') and (doc.get('rights_evidence') or {}).get('public_distribution') is True and doc.get('rights_policy_version'):return doc
        if self.rows('rkb_document_grants',{'document_id':document_id,'grantee_user_id':actor}):return doc
        if doc.get('workspace_id') and doc.get('content_visibility') in ('workspace','public') and self.rows('rkb_workspace_members',{'workspace_id':doc['workspace_id'],'user_id':actor}):return doc
        raise LookupError('evidence_not_found')

    def visible_documents(self,actor):
        result=[]
        for doc in self.rows('rkb_documents'):
            try:self.authorize(actor,doc['id'])
            except (LookupError,PermissionError):continue
            result.append(doc)
        return result

    def lexical(self,actor,query,*,depth=100,phrase=False,allowed=None):
        # No SQL or FTS syntax from user input is executed. Literal terms only.
        terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)[:64]
        if not terms:return []
        expression='"'+' '.join(terms)+'"' if phrase else ' AND '.join('"'+t+'"' for t in terms)
        docs=allowed if allowed is not None else {d['id']:d['active_revision'] for d in self.visible_documents(actor)}
        if not docs:return []
        with self.connect() as db:
            db.execute('create temp table allowed(document_id text primary key,revision integer)')
            db.executemany('insert into allowed values(?,?)',docs.items())
            rows=db.execute('''select t.chunk_id,t.revision,t.text_sha256,t.search_material_sha256
              from chunk_fts join chunk_text t on t.rowid=chunk_fts.rowid
              join allowed a on a.document_id=t.document_id and a.revision=t.revision
              where chunk_fts match ? order by bm25(chunk_fts),t.chunk_id limit ?''',
              (expression,max(1,min(depth,100)))).fetchall()
        return [{'chunk_id':r['chunk_id'],'branch':'lexical','rank':i+1,**dict(r)} for i,r in enumerate(rows)]

    def catalog(self,actor,query='',*,offset=0,limit=20,kind=None,allowed=None):
        needle=query.casefold();result=[]
        for doc in (self.visible_documents(actor) if allowed is None else [d for d in self.rows('rkb_documents') if d['id'] in allowed]):
            meta=doc.get('catalog') or {};k=meta.get('kind','book')
            if kind and k!=kind:continue
            if needle and needle not in (str(doc.get('title',''))+' '+canonical(doc.get('authors') or [])+' '+canonical(meta)).casefold():continue
            result.append({**doc,'catalog':{'kind':k,**meta}})
        result.sort(key=lambda r:(str(r.get('title','')).casefold(),r['id']))
        return {'items':result[offset:offset+limit],'next_cursor':str(offset+limit) if len(result)>offset+limit else None}

    def build_fragments(self,document_id=None,revision=None):
        missing=[];count=0
        for chunk in self.rows('rkb_chunks',{'document_id':document_id,'revision':revision} if document_id is not None and revision is not None else None):
            doc=self.one('rkb_documents',chunk['document_id']);text=chunk['source_text'];cursor=0;parts=[]
            for rid in [*(chunk.get('region_ids') or []),*(chunk.get('footnote_region_ids') or [])]:
                region=self.one('rkb_regions',rid)
                if not region:missing.append((chunk['id'],'region_missing'));continue
                rtext=(region.get('source_text') or '').strip()
                if not rtext:continue
                start=text.find(rtext,cursor)
                if start<0:missing.append((chunk['id'],'region_text_not_exact'));continue
                page=self.one('rkb_pages',region['page_id'])
                if not page or page['document_id']!=chunk['document_id'] or page['revision']!=chunk['revision']:raise ValueError('fragment provenance mismatch')
                gap=text[cursor:start].strip()
                if gap and gap!='[Footnote]':missing.append((chunk['id'],'unmapped_text_gap'))
                end=start+len(rtext);parts.append((chunk['id'],len(parts),page['id'],page['physical_page_index'],rid,start,end,doc['source_sha256'],chunk['revision']));cursor=end
            if text[cursor:].strip():missing.append((chunk['id'],'unmapped_text'))
            with self.connect() as db:
                db.execute('delete from chunk_fragments where chunk_id=?',(chunk['id'],))
                db.executemany('insert into chunk_fragments values(?,?,?,?,?,?,?,?,?)',parts)
                positions=[p[3]*100000+self.one('rkb_regions',p[4])['reading_order'] for p in parts]
                if positions:db.execute('insert into chunk_positions values(?,?,?,?,?,?) on conflict(chunk_id) do update set start_position=excluded.start_position,end_position=excluded.end_position,article_id=excluded.article_id',(chunk['id'],chunk['document_id'],chunk['revision'],min(positions),max(positions),(chunk.get('metadata') or {}).get('article_id')))
            count+=len(parts)
        return {'fragments':count,'unmapped':len(missing)}

    def fragments(self,chunk_id):
        with self.connect() as db:return [dict(r) for r in db.execute('select * from chunk_fragments where chunk_id=? order by ordinal',(chunk_id,))]

    def candidate_metadata(self,allowed):
        """IDs/hashes only. Indexed document/revision joins never hydrate text."""
        with self.connect() as db:
            db.execute('create temp table selected_docs(document_id text primary key,revision integer)')
            db.executemany('insert into selected_docs values(?,?)',allowed.items())
            return [dict(r) for r in db.execute('select t.chunk_id,t.document_id,t.revision,t.text_sha256,t.search_material_sha256 from selected_docs a cross join chunk_text t on t.document_id=a.document_id and t.revision=a.revision')]

    def neighbors(self,actor,selected):
        output=[]
        with self.connect() as db:
            for ident in selected:
                current=self.one('rkb_chunks',str(ident))
                if not current:continue
                doc=self.authorize(actor,current['document_id'])
                if current['revision']!=doc['active_revision']:continue
                position=db.execute('select * from chunk_positions where chunk_id=?',(str(ident),)).fetchone()
                if not position:continue
                component=db.execute('select component_id from component_chunks where chunk_id=?',(str(ident),)).fetchone()
                params=[doc['id'],doc['active_revision'],position['article_id']]
                clause=''
                if component:
                    clause=' and chunk_id in(select chunk_id from component_chunks where component_id=?)';params.append(component['component_id'])
                else:clause=' and not exists(select 1 from component_chunks cc where cc.chunk_id=chunk_positions.chunk_id)'
                previous=db.execute('select chunk_id from chunk_positions where document_id=? and revision=? and article_id is ?'+clause+' and end_position<? order by end_position desc,chunk_id desc limit 1',(*params,position['start_position'])).fetchone()
                following=db.execute('select chunk_id from chunk_positions where document_id=? and revision=? and article_id is ?'+clause+' and start_position>? order by start_position,chunk_id limit 1',(*params,position['end_position'])).fetchone()
                result=dict(current)
                for prefix,pair in (('prev',previous),('next',following)):
                    if pair:
                        value=self.one('rkb_chunks',pair[0])
                        result.update({prefix+'_id':value['id'],prefix+'_title':value['title'],prefix+'_text':value['source_text'],prefix+'_illustrations':value.get('illustration_ids')})
                output.append(result)
        return output

    def backup(self,target):
        target=Path(target)
        if target.resolve()==self.path.resolve():raise ValueError('backup must have a different path')
        with self.connect() as db:
            with sqlite3.connect(target) as out:db.backup(out)
        target.chmod(0o600)
        with sqlite3.connect(target) as restored:
            if restored.execute('pragma integrity_check').fetchone()[0]!='ok':raise ValueError('backup restore integrity failed')
        return target

    def digest(self):
        with self.connect() as db:
            digest=hashlib.sha256();n=0
            for row in db.execute('select table_name,row_key,payload from corpus_rows order by table_name,row_key'):
                table,key,payload=row
                value=json.loads(payload)
                if table=='rkb_chunks':
                    text=db.execute('select source_text,search_material from chunk_text where chunk_id=?',(key,)).fetchone()
                    value['source_text']=text['source_text']
                    if 'search_material' not in value:value['search_material']=text['search_material']
                digest.update(canonical([table,key,canonical(value)]).encode());n+=1
            return {'rows':n,'sha256':digest.hexdigest()}
