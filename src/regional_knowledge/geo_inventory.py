"""Fixed read-only legacy GEO inventory; private evidence is hashed in RAM only."""
from pathlib import Path
import json
import sqlite3
import threading

from .geo_migration import InventoryCounts,sha
from .geo_whitelist import build_geo_whitelist


def geo_watermark(manifest):
    """Only the required GEO subset; unrelated books/Story/exclusions do not drift."""
    return sha(dict(codec='rkb.geo-source-watermark.v1',
        items=[item.wire() for item in manifest.items if item.payload.kind!='excluded_non_geo'],
        missing_dependencies=list(manifest.missing_dependencies),
        geo_counts={key:getattr(manifest.inventory_counts,key) for key in
            ('geo_places','geo_intents','accepted_geo_metadata')}))


def _rows(db,table,fields,ids=None):
    args=[table]
    projection=','.join("'"+field+"',json_extract(payload,'$."+field+"')" for field in fields)
    statement='SELECT json_object('+projection+') FROM corpus_rows WHERE table_name=?'
    if ids is not None:
        if not ids:return []
        if len(ids)>4096:raise ValueError('geo_metadata_identity_bound')
        statement+=' AND row_key IN ('+','.join('?' for _ in ids)+')';args+=sorted(ids)
    statement+=' ORDER BY row_key LIMIT 4097'
    result=[]
    for raw, in db.execute(statement,args):
        value=json.loads(raw);result.append({key:value[key] for key in fields if key in value})
    if len(result)>4096:raise ValueError('geo_metadata_row_bound')
    return result


def collect_geo_inventory(db):
    entities=_rows(db,'rkb_entities',('id','owner_user_id','document_id','revision','kind','canonical_label','state','external_ref','metadata'))
    mentions=_rows(db,'rkb_entity_mentions',('id','entity_id','document_id','revision','exact_source_spelling','evidence'))
    aliases=_rows(db,'rkb_entity_aliases',('id','entity_id','document_id','revision','value','language','alias_type','time_scope','evidence'))
    relations=_rows(db,'rkb_entity_relations',('id','source_id','target_id','kind','document_id','revision','evidence','time_scope','state'))
    proofs=[row.get('evidence',{}) for row in (*mentions,*aliases)]
    proofs += [proof for row in relations for proof in row.get('evidence',[])]
    documents=_rows(db,'rkb_documents',('id','owner_user_id','title','authors','publication_year','source_sha256','active_revision','source_document_id','catalog'))
    pages=_rows(db,'rkb_pages',('id','document_id','revision','physical_page_index','printed_page_number'),
        {v['page_id'] for v in proofs if v.get('page_id')})
    regions=_rows(db,'rkb_regions',('id','page_id'),{v['region_id'] for v in proofs if v.get('region_id')})
    chunks=_rows(db,'rkb_chunks',('id','document_id','revision','page_ids','region_ids'),
        {v['chunk_id'] for v in proofs if v.get('chunk_id')})
    fields=('request_id','actor_id','document_id','source_revision','mention_id','entity_id','source_sha256',
        'mention_sha256','original_spelling','names_json','scope_json','period_json','locator_json')
    intents=[dict(zip(fields,row)) for row in db.execute('SELECT '+','.join(fields)+
        ' FROM rkb_geo_intents ORDER BY request_id LIMIT 4097')]
    if len(intents)>4096:raise ValueError('geo_intent_bound')
    data=dict(entities=entities,mentions=mentions,aliases=aliases,relations=relations,source_metadata=documents,
        page_metadata=pages,region_metadata=regions,chunk_metadata=chunks,geo_intents=intents)
    places=[n for n in entities if n.get('kind')=='poi_ref']
    counts=InventoryCounts(entities=len(entities),geo_places=len(places),mentions=len(mentions),aliases=len(aliases),
        relations=len(relations),geo_intents=len(intents),accepted_geo_metadata=sum(bool((n.get('metadata')or{}).get('geo_queue')) for n in places))
    return build_geo_whitelist(source_inventory_sha256=sha(data),expected_counts=counts,**data).require_ready()


class GeoInventoryObservation:
    """Retains one RO connection/version; unrelated commits require only recheck."""
    def __init__(self,corpus_path):
        path=Path(corpus_path)
        if not path.is_file():raise ValueError('existing canonical legacy inventory required')
        self.path=path;self.lock=threading.RLock();self.db=None;self.version=None;self.watermark=None

    def refresh(self,expected):
        with self.lock:
            if self.db is None:
                self.db=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,check_same_thread=False)
                self.db.execute('PRAGMA query_only=on');self.db.execute('PRAGMA busy_timeout=250')
            self.version=None;self.watermark=None
            before=self.db.execute('PRAGMA data_version').fetchone()[0]
            self.db.execute('BEGIN')
            try:manifest=collect_geo_inventory(self.db)
            finally:self.db.rollback()
            after=self.db.execute('PRAGMA data_version').fetchone()[0]
            if before!=after or geo_watermark(manifest)!=expected:raise ValueError('geo_source_watermark_changed')
            self.version=after;self.watermark=expected
            return expected

    def current(self,expected):
        if not self.lock.acquire(blocking=False):return False
        try:
            return bool(self.db is not None and self.watermark==expected and self.version is not None
                and self.db.execute('PRAGMA data_version').fetchone()[0]==self.version)
        except sqlite3.Error:return False
        finally:self.lock.release()

    def close(self):
        with self.lock:
            if self.db is not None:self.db.close();self.db=None
            self.version=None;self.watermark=None
