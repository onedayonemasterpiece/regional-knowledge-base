"""Telegram captions are a projection of existing started-import receipts."""
import json

from .source_page_progress import progress_columns, progress_packet


def started_statuses(queue, actor, *, synced=None, preferred=None, limit=None):
    with queue.connect(write=False) as db:
        rows=db.execute(
            "SELECT c.*,n.phase AS native_phase,n.pending_operation_key,n.batch_count,"
            "n.published_batch_count,n.enriched_batch_count,n.rejected_batch_index,o.receipt_json,o.telegram_locator_json "
            "FROM rkb_source_intake_control c "
            "JOIN rkb_native_source_intake_state n USING(ingestion_id) "
            "JOIN rkb_original_intakes o ON o.ingestion_id=c.ingestion_id AND o.actor=c.person_id "
            "WHERE c.person_id=? AND o.receipt_json IS NOT NULL ORDER BY c.created DESC,c.rowid DESC",
            (actor,)).fetchall()
        result=[];current={}
        for row in rows:
            locator=json.loads(row['telegram_locator_json']) if row['telegram_locator_json'] else None
            target=(locator['chat_id'],locator['message_id']) if locator else row['ingestion_id']
            current.setdefault(target,dict(row))
        rows=sorted(current.values(),key=lambda row:row['ingestion_id']!=preferred)
        for row in rows:
            stamp=tuple(row[key] for key in ('metadata_json','phase','native_phase',
                'rejected_batch_index','receipt_json'))
            # A caption marks lifecycle/error milestones, not each packet counter.
            stamp+=(bool(row['pending_operation_key']) if 'finalized' in (row['phase'],row['native_phase']) else False,)
            if synced and synced.get(row['ingestion_id'],(None,))[0]==stamp:continue
            stages={}; text=set(); semantic=set()
            for op in db.execute(
                "SELECT operation_key,apply_state,"+progress_columns()+" FROM rkb_control_operations "
                "WHERE source_id=? AND person_id=? AND "
                "(operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence LIMIT 10000",
                (row['source_id'],actor,'source-stage:'+row['ingestion_id']+':%',
                 'source-packet:'+row['ingestion_id']+':%')):
                if op['apply_state']!='APPLIED':continue
                packet,stage_sha=progress_packet(op)
                if packet.get('ingestion_id')!=row['ingestion_id']:continue
                index=packet.get('batch_index')
                if op['operation_key'].startswith('source-stage:'):
                    pages=packet.get('model_payload',{}).get('pages',[])
                    stages[index,stage_sha]={
                        page['physical_page_index'] for page in pages}
                else:
                    pages=stages.get((index,packet.get('last_batch_sha256')),set())
                    if packet.get('component')=='text':text.update(pages)
                    elif packet.get('component')=='semantic':semantic.update(pages)
            expected=set(range(json.loads(row['receipt_json'])['page_count']))
            completed=expected & text & semantic
            finished=row['native_phase']=='finalized' or row['phase']=='finalized'
            missing=sorted(expected-text)
            if not finished:
                last_text=max(expected & text,default=-1)
                missing=[page for page in missing if page<last_text]
            text_only=(expected & text)-semantic
            if (not finished and row['rejected_batch_index'] is None
                    and row['enriched_batch_count']<row['batch_count']):
                text_only-=set().union(*(pages for (index,_),pages in stages.items()
                    if index==row['batch_count']-1))
            problems=[]
            if missing:
                pages=', '.join(str(page+1) for page in missing[:5])
                if len(missing)>5:pages+=f' и ещё {len(missing)-5}'
                problems.append(f'не импортированы стр. {pages}')
            if text_only:problems.append(f'не разобраны {len(text_only)} стр.')
            if row['rejected_batch_index'] is not None:
                problems.append('пакет отклонён, ждёт переотправки')
            if finished and row['pending_operation_key']:
                problems.append('сохранение не завершено')
            if row['native_phase']=='failed' or row['phase']=='failed':
                status='❌ Ошибка импорта'
            elif finished and not problems:
                status='✅ Импорт завершён'
            else:
                status=('🏁⚠️ Обработка закончена' if finished else
                        '⏳⚠️ Импортируется' if problems else '⏳ Импортируется')
                status+=f' · страниц {len(completed)}/{len(expected)}'
                status+=''.join(f' · {problem}' for problem in problems)
            result.append(({**dict(row),'caption_stamp':stamp},status))
            if limit and len(result)>=limit:break
        return result
