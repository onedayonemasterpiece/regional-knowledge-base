"""Published physical pages from exact source.stage -> TEXT APPLIED journal receipts.

The existing ordered ControlSpool is authoritative; stage APPLIED alone is not
publication, and compact native counters are not page-acceptance receipts.
"""
import base64
import hashlib
import json


def publication_progress(db, row):
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
        (row['source_id'],row['person_id'],stage_prefix+'%',text_prefix+'%:text'))
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
              and doc.get('component')=='text'):
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
