"""Telegram captions are a projection of existing started-import receipts."""
import base64
import hashlib
import json


def started_statuses(queue, actor, *, synced=None, preferred=None, limit=None):
    with queue.connect(write=False) as db:
        rows=db.execute(
            "SELECT c.*,n.phase AS native_phase,n.pending_operation_key,n.batch_count,"
            "n.published_batch_count,n.enriched_batch_count,n.rejected_batch_index,o.receipt_json "
            "FROM rkb_source_intake_control c "
            "JOIN rkb_native_source_intake_state n USING(ingestion_id) "
            "JOIN rkb_original_intakes o ON o.ingestion_id=c.ingestion_id AND o.actor=c.person_id "
            "WHERE c.person_id=? AND o.receipt_json IS NOT NULL ORDER BY c.created",
            (actor,)).fetchall()
        result=[]
        rows=sorted(rows,key=lambda row:row['ingestion_id']!=preferred)
        for row in rows:
            stamp=tuple(row[key] for key in ('metadata_json','phase','native_phase',
                'rejected_batch_index','receipt_json'))
            if synced and synced.get(row['ingestion_id'],(None,))[0]==stamp:continue
            stages={}; text=set(); semantic=set(); errors=False
            for op in db.execute(
                "SELECT operation_key,command_bytes,apply_state FROM rkb_control_operations "
                "WHERE source_id=? AND person_id=? AND "
                "(operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
                (row['source_id'],actor,'source-stage:'+row['ingestion_id']+':%',
                 'source-packet:'+row['ingestion_id']+':%')):
                if op['apply_state']=='REJECTED_PRECONDITION':errors=True
                if op['apply_state']!='APPLIED':continue
                payload=base64.b64decode(json.loads(op['command_bytes'])['payload_b64'],validate=True)
                packet=json.loads(payload)
                if packet.get('ingestion_id')!=row['ingestion_id']:continue
                index=packet.get('batch_index')
                if op['operation_key'].startswith('source-stage:'):
                    pages=packet.get('model_payload',{}).get('pages',[])
                    stages[index,hashlib.sha256(payload).hexdigest()]={
                        page['physical_page_index'] for page in pages}
                else:
                    pages=stages.get((index,packet.get('last_batch_sha256')),set())
                    if packet.get('component')=='text':text.update(pages)
                    elif packet.get('component')=='semantic':semantic.update(pages)
            finished=row['native_phase']=='finalized' or row['phase']=='finalized'
            if row['native_phase']=='failed' or row['phase']=='failed':
                status='❌ Ошибка импорта'
            elif finished:
                expected=set(range(json.loads(row['receipt_json'])['page_count']))
                complete=expected<=text and expected<=semantic and not row['pending_operation_key']
                status=('✅ Импорт завершён' if complete and not errors else
                        '✅⚠️ Импорт завершён; были ошибки' if complete else
                        '🏁⚠️ Обработка закончена; есть пропуски или ошибки')
            else:
                status='⏳⚠️ Импортируется; были ошибки' if errors else '⏳ Импортируется'
            result.append(({**dict(row),'caption_stamp':stamp},status))
            if limit and len(result)>=limit:break
        return result
