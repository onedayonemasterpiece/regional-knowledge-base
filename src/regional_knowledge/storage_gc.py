"""Guarded cache GC. Immutable metadata is retained; deleted_at tracks bytes only."""
from uuid import UUID
import json,logging

# Candidates are derived exclusively from known database-owned objects. No bucket wipe.
CANDIDATES="""select o.* from rkb_objects o join rkb_documents d on d.id=o.document_id
 where o.deleted_at is null and (
  (o.kind='text_projection' and not exists(select 1 from rkb_chunks c where c.text_object_id=o.id and c.source_text is null))
  or (o.kind='page_render' and d.source_archive_status='verified')
  or (o.kind='illustration_crop' and exists(select 1 from rkb_illustrations i where i.crop_object_id=o.id and i.vibepublish_entry_ref is not null))
  or (o.kind='source_pdf' and d.source_archive_status='verified' and d.source_archive_ref is not null and o.sha256=d.source_sha256
      and not exists(select 1 from rkb_ingestion_jobs j where j.document_id=d.id and j.state not in ('finalized','failed')))
  or (o.kind='document_graph' and not exists(select 1 from rkb_ingestion_jobs j where j.staged_graph_object_id=o.id and j.state not in ('finalized','failed'))
      and not exists(select 1 from rkb_regions r join rkb_pages p on p.id=r.page_id where p.document_id=d.id and r.text_sha256 is not null and r.source_text is null))
 ) and o.created_at < now()-interval '1 hour' order by o.created_at,o.id limit 100"""


async def collect(backend, *, apply=False):
    async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
        objects=await(await db.execute(CANDIDATES)).fetchall()
    result={'apply':apply,'candidate_objects':len(objects),'candidate_bytes':sum(o['size_bytes'] for o in objects),'deleted_objects':0,'deleted_bytes':0}
    if not apply:return result
    for obj in objects:
        async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
            # Freeze lifecycle state against stage/finalize while rechecking the exact candidate.
            await db.execute('select id from rkb_documents where id=%s for update',(obj['document_id'],))
            await db.execute('select id from rkb_ingestion_jobs where document_id=%s for update',(obj['document_id'],))
            candidates=await(await db.execute(CANDIDATES)).fetchall()
            if obj['id'] not in {r['id'] for r in candidates}:continue
            # Detach superseded binary pointers only after their replacements exist.
            if obj['kind']=='text_projection':
                await db.execute('update rkb_chunks set text_object_id=null where text_object_id=%s and source_text is not null',(obj['id'],))
            elif obj['kind']=='page_render':
                await db.execute('update rkb_pages set page_object_id=null where page_object_id=%s',(obj['id'],))
            elif obj['kind']=='illustration_crop':
                await db.execute('update rkb_illustrations set crop_object_id=null where crop_object_id=%s and vibepublish_entry_ref is not null',(obj['id'],))
            elif obj['kind']=='document_graph':
                await db.execute("update rkb_ingestion_jobs set staged_graph_object_id=null where staged_graph_object_id=%s and state in ('finalized','failed')",(obj['id'],))
            await backend.object_store.delete(obj['object_key'])
            await db.execute('update rkb_objects set deleted_at=now() where id=%s',(obj['id'],))
            result['deleted_objects']+=1;result['deleted_bytes']+=obj['size_bytes']
    logging.getLogger(__name__).info(json.dumps({'event':'storage_gc',**result}))
    return result


async def reserve_source(backend, principal, document_id, key, downloaded, mime):
    """Reserve capacity before upload; concurrent starts cannot overcommit staging."""
    import os
    from uuid import uuid5
    if not hasattr(backend,'data_client'):return
    maximum=int(os.environ.get('RKB_STAGING_MAX_BYTES',str(800*1024*1024)))
    if not 1<=maximum<=1024*1024*1024:raise ValueError('staging_capacity_config_invalid')
    ident=uuid5(UUID(document_id),'source:'+downloaded.sha256)
    async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
        await db.execute("select pg_advisory_xact_lock(hashtext('rkb-staging-capacity'))")
        await db.execute('select id from rkb_documents where id=%s for update',(UUID(document_id),))
        existing=await(await db.execute('select id,deleted_at from rkb_objects where id=%s',(ident,))).fetchone()
        if existing and existing['deleted_at'] is None:return
        used=await(await db.execute('select coalesce(sum(size_bytes),0)::bigint bytes from rkb_objects where deleted_at is null')).fetchone()
        if used['bytes']+downloaded.size_bytes>maximum:raise RuntimeError('source_staging_capacity_exceeded')
        if existing:
            # A deliberate revision after source-cache GC reuses the same identity,
            # but must reserve capacity again and track the new temporary bytes.
            await db.execute('update rkb_objects set deleted_at=null,created_at=now() where id=%s',(ident,))
            return
        await db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes,access_class) values(%s,%s,'source_pdf',%s,%s,%s,%s,'private')",
            (ident,UUID(document_id),key,downloaded.sha256,mime,downloaded.size_bytes))
