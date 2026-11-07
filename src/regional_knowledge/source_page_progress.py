"""Published physical pages from exact source.stage -> TEXT APPLIED journal receipts.

The existing ordered ControlSpool is authoritative; stage APPLIED alone is not
publication, and compact native counters are not page-acceptance receipts.
"""
import asyncio
import base64
import hashlib
import json


def compact_progress(key, command_bytes, outcome_bytes=None):
    """Derived page receipt facts, without book text or a second content archive."""
    if not key.startswith(('source-stage:', 'source-packet:')): return None
    raw = base64.b64decode(json.loads(command_bytes)['payload_b64'], validate=True)
    packet = json.loads(raw)
    result = {k:packet.get(k) for k in ('ingestion_id','batch_index','component','last_batch_sha256')}
    if key.startswith('source-stage:'):
        result['stage_sha256'] = hashlib.sha256(raw).hexdigest()
        result['model_payload'] = {'pages':[{'physical_page_index':p['physical_page_index']}
                                            for p in packet.get('model_payload',{}).get('pages',[])]}
    return progress_outcome(json.dumps(result), outcome_bytes)


def progress_outcome(progress_json, outcome_bytes):
    if progress_json is None: return None
    packet = json.loads(progress_json)
    if packet.get('component') == 'semantic' and outcome_bytes:
        raw = base64.b64decode(json.loads(outcome_bytes)['result_b64'], validate=True)
        body = json.loads(raw) if raw else {}
        packet['rejection'] = {k:body.get(k) for k in ('reason','error_type','sqlstate')}
    return json.dumps(packet, separators=(',',':'))


def initialize_progress_receipts(db):
    # Column presence is the migration boundary; a warm reopen never scans history.
    if 'progress_json' in {r[1] for r in db.execute('PRAGMA table_info(rkb_control_operations)')}: return
    db.execute('ALTER TABLE rkb_control_operations ADD COLUMN progress_json TEXT')
    for row in db.execute("SELECT operation_key,command_bytes,outcome_bytes FROM rkb_control_operations "
                          "WHERE operation_key LIKE 'source-stage:%' OR operation_key LIKE 'source-packet:%'"):
        db.execute('UPDATE rkb_control_operations SET progress_json=? WHERE operation_key=?',
                   (compact_progress(row['operation_key'],row['command_bytes'],row['outcome_bytes']),row['operation_key']))


def progress_columns(*, outcome=False):
    # Legacy/rolled-back writers leave NULL; their original decode path stays compatible.
    columns = 'progress_json,CASE WHEN progress_json IS NULL THEN command_bytes END AS command_bytes'
    return columns + (',CASE WHEN progress_json IS NULL THEN outcome_bytes END AS outcome_bytes' if outcome else '')


def progress_packet(item):
    if item['progress_json'] is not None:
        packet = json.loads(item['progress_json'])
        return packet, packet.get('stage_sha256')
    raw = base64.b64decode(json.loads(item['command_bytes'])['payload_b64'],validate=True)
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def read_publication_progress(queue, row, component="text"):
    """Open SQLite in the calling worker; connections never cross threads."""
    with queue.connect(write=False) as db:
        return publication_progress(db, row) if component=="text" else publication_progress(db, row, component)


def publication_progress(db, row, component="text"):
    ingestion=row['ingestion_id']
    stages={}
    published=set()
    text_batches=set()
    stage_prefix='source-stage:'+ingestion+':'
    text_prefix='source-packet:'+ingestion+':'
    # Same bounded source-scoped journal read as the existing activity watcher.
    entries=db.execute(
        "SELECT operation_key,"+progress_columns()+" FROM rkb_control_operations "
        "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' AND apply_state='APPLIED' "
        "AND (operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
        (row['source_id'],row['person_id'],stage_prefix+'%',text_prefix+'%:'+component))
    for item in entries:
        doc, stage_sha = progress_packet(item)
        if doc.get('ingestion_id')!=ingestion:continue
        index=doc.get('batch_index')
        if type(index) is not int:continue
        if item['operation_key'].startswith(stage_prefix):
            pages=doc.get('model_payload',{}).get('pages',[])
            stages[index,stage_sha]={
                page['physical_page_index'] for page in pages}
        elif (item['operation_key'].startswith(text_prefix)
              and doc.get('component')==component):
            pages=stages.get((index,doc.get('last_batch_sha256')))
            if pages is not None:
                published.update(pages)
                text_batches.add(index)
    return published,text_batches


def published_pages(db,row):
    return publication_progress(db,row)[0]


def next_unaccepted(cursor, published):
    if type(cursor) is not str or not cursor.isdecimal():return cursor
    index=int(cursor)
    if index not in published:return cursor  # Authored reverse/gap cursor stays intact.
    while index in published:index+=1
    return str(index)


def enrichment_gaps(db, row):
    """Ordinary continuation excludes the latest unresolved SQL rollback."""
    published = set(); stages = {}; semantic = set(); latest = {}; retried = set()
    for item in db.execute(
            "SELECT operation_key,apply_state,"+progress_columns(outcome=True)+" FROM rkb_control_operations "
            "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' "
            "AND apply_state IN ('APPLIED','REJECTED_PRECONDITION') "
            "AND (((operation_key LIKE ? OR operation_key LIKE ?) AND apply_state='APPLIED') "
            "OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
            (row['source_id'],row['person_id'],'source-stage:'+row['ingestion_id']+':%',
             'source-packet:'+row['ingestion_id']+':%:text','source-packet:'+row['ingestion_id']+':%:semantic')):
        packet, stage_sha = progress_packet(item)
        if packet.get('ingestion_id') != row['ingestion_id']: continue
        if item['operation_key'].startswith('source-stage:'):
            if item['apply_state']=='APPLIED':
                stages[packet['batch_index'],stage_sha] = {
                    page['physical_page_index'] for page in packet['model_payload']['pages']}
        elif packet.get('component')=='text':
            if item['apply_state']=='APPLIED':
                published.update(stages.get((packet['batch_index'],packet['last_batch_sha256']),set()))
        elif packet.get('component')=='semantic':
            pages = stages.get((packet['batch_index'],packet['last_batch_sha256']),set())
            reason = None
            if item['apply_state']=='APPLIED': semantic.update(pages)
            elif item['progress_json'] is not None or item['outcome_bytes']:
                if item['progress_json'] is not None:
                    body = packet.get('rejection',{})
                else:
                    outcome = json.loads(item['outcome_bytes'])
                    result = base64.b64decode(outcome['result_b64'],validate=True)
                    body = json.loads(result) if result else {}
                reason = body.get('reason')
                # Before 798 a fixed model rejection was retained as a SQL rollback with a
                # Python error type and no SQLSTATE. Such a page gets one ordinary retry.
                if (reason=='native_source_sql_rolled_back' and body.get('sqlstate') is None
                        and body.get('error_type') in ('ValueError','SourceMaterializationRejected')):
                    if not pages & retried: reason = 'masked_model_rejection'
                    retried.update(pages)
            latest.update((page,reason) for page in pages)
    return published - semantic - {page for page,reason in latest.items()
                                  if reason=='native_source_sql_rolled_back'}


def _accepted_model_key(db, row, physical_index):
    """Newest stage bound to TEXT APPLIED; rejected TEXT never replaces it."""
    published = set()
    for item in db.execute(
            "SELECT operation_key,"+progress_columns()+" FROM rkb_control_operations "
            "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' AND apply_state='APPLIED' "
            "AND (operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence DESC LIMIT 10000",
            (row['source_id'],row['person_id'],'source-stage:'+row['ingestion_id']+':%',
             'source-packet:'+row['ingestion_id']+':%:text')):
        packet, stage_sha = progress_packet(item)
        if packet.get('ingestion_id') != row['ingestion_id']: continue
        if not item['operation_key'].startswith('source-stage:'):
            published.add((packet.get('batch_index'), packet.get('last_batch_sha256')))
            continue
        if (packet.get('batch_index'), stage_sha) not in published: continue
        if any(p.get('physical_page_index') == physical_index for p in packet.get('model_payload',{}).get('pages',[])):
            return item['operation_key']
    return None


def _model_page(payload, physical_index):
    from .source_batch_context import chunk_pages
    model = payload.get('model_payload') or {}
    for page in model.get('pages', []):
        if page.get('physical_page_index') == physical_index:
            return dict(page=page, chunks=[c for c in model.get('chunks', [])
                                           if page.get('page_id') in chunk_pages(c, text_only=True)])
    return None


def accepted_model_page(db, row, physical_index):
    key = _accepted_model_key(db, row, physical_index)
    if key is None: return None
    original = db.execute('SELECT command_bytes FROM rkb_control_operations WHERE operation_key=?', (key,)).fetchone()[0]
    return _model_page(json.loads(base64.b64decode(json.loads(original)['payload_b64'],validate=True)), physical_index)


async def read_accepted_model_page(queue, row, physical_index, spool):
    # SEM APPLIED can still leave source tails; retired originals use the existing journal.
    def key():
        with queue.connect(write=False) as db:
            return _accepted_model_key(db, row, physical_index)
    operation_key = await asyncio.to_thread(key)
    if operation_key is None: return None
    entry = await spool.get(operation_key)
    return _model_page(json.loads(entry.command.payload), physical_index)


def read_enrichment_gaps(queue, row):
    with queue.connect(write=False) as db:
        return enrichment_gaps(db, row)
