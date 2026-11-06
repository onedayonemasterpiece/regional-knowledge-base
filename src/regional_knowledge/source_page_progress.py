"""Published physical pages from exact source.stage -> TEXT APPLIED journal receipts.

The existing ordered ControlSpool is authoritative; stage APPLIED alone is not
publication, and compact native counters are not page-acceptance receipts.
"""
import base64
import hashlib
import json


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
        "SELECT operation_key,command_bytes FROM rkb_control_operations "
        "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' AND apply_state='APPLIED' "
        "AND (operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
        (row['source_id'],row['person_id'],stage_prefix+'%',text_prefix+'%:'+component))
    for item in entries:
        payload=base64.b64decode(json.loads(item['command_bytes'])['payload_b64'],validate=True)
        doc=json.loads(payload)
        if doc.get('ingestion_id')!=ingestion:continue
        index=doc.get('batch_index')
        if type(index) is not int:continue
        if item['operation_key'].startswith(stage_prefix):
            pages=doc.get('model_payload',{}).get('pages',[])
            stages[index,hashlib.sha256(payload).hexdigest()]={
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
    published, _ = publication_progress(db, row)
    stages = {}; semantic = set(); latest = {}; retried = set()
    for item in db.execute(
            "SELECT operation_key,command_bytes,apply_state,outcome_bytes FROM rkb_control_operations "
            "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' "
            "AND apply_state IN ('APPLIED','REJECTED_PRECONDITION') "
            "AND (operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
            (row['source_id'],row['person_id'],'source-stage:'+row['ingestion_id']+':%',
             'source-packet:'+row['ingestion_id']+':%:semantic')):
        raw = base64.b64decode(json.loads(item['command_bytes'])['payload_b64'], validate=True)
        packet = json.loads(raw)
        if packet.get('ingestion_id') != row['ingestion_id']: continue
        if item['operation_key'].startswith('source-stage:'):
            if item['apply_state']=='APPLIED':
                stages[packet['batch_index'],hashlib.sha256(raw).hexdigest()] = {
                    page['physical_page_index'] for page in packet['model_payload']['pages']}
        elif packet.get('component')=='semantic':
            pages = stages.get((packet['batch_index'],packet['last_batch_sha256']),set())
            reason = None
            if item['apply_state']=='APPLIED': semantic.update(pages)
            elif item['outcome_bytes']:
                outcome = json.loads(item['outcome_bytes'])
                body = json.loads(base64.b64decode(outcome['result_b64'],validate=True))
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


def accepted_model_page(db, row, physical_index):
    """The latest APPLIED model page and its chunks: what a semantic retry must restage verbatim."""
    from .source_batch_context import chunk_pages
    found = None
    for item in db.execute(
            "SELECT command_bytes FROM rkb_control_operations "
            "WHERE source_id=? AND person_id=? AND seal_state='ACCEPTED' AND apply_state='APPLIED' "
            "AND operation_key LIKE ? ORDER BY sequence LIMIT 10000",
            (row['source_id'],row['person_id'],'source-stage:'+row['ingestion_id']+':%')):
        packet = json.loads(base64.b64decode(json.loads(item['command_bytes'])['payload_b64'], validate=True))
        if packet.get('ingestion_id') != row['ingestion_id']: continue
        model = packet.get('model_payload') or {}
        for page in model.get('pages', []):
            if page.get('physical_page_index') == physical_index:
                found = dict(page=page, chunks=[c for c in model.get('chunks', [])
                                                if page.get('page_id') in chunk_pages(c, text_only=True)])
    return found


def read_accepted_model_page(queue, row, physical_index):
    with queue.connect(write=False) as db:
        return accepted_model_page(db, row, physical_index)


def read_enrichment_gaps(queue, row):
    with queue.connect(write=False) as db:
        return enrichment_gaps(db, row)
