"""Exact-source archive on the existing maintenance owner and Vibe provider queue."""
import asyncio, hashlib, json, logging, os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID
from .file_ingress import sha256_file
from .contracts import Principal
from .illustration_mirror import VibePublishClient
log=logging.getLogger(__name__)


def _source_display_metadata(document, uri=None):
    source_format=(document.get('source_format') or ('djvu' if document.get('mime_type')=='image/vnd.djvu' else 'pdf')).lower()
    extension='.djvu' if source_format=='djvu' else '.pdf'
    raw_name=(document.get('source_filename') or '').strip()
    if raw_name:
        filename=Path(raw_name).name
    else:
        title=(document.get('title') or 'regional-knowledge-source').strip()
        stem=''.join(ch if ch.isalnum() or ch in ' ._()-' else '_' for ch in title).strip(' ._')
        filename=(stem[:120] or 'regional-knowledge-source')+extension
    authors=document.get('authors') or []
    if isinstance(authors,str):
        authors=[authors]
    lines=[str(document.get('title') or filename).strip()]
    if authors:
        lines.append('Автор: '+', '.join(str(value).strip() for value in authors if str(value).strip())[:300])
    if document.get('publication_year'):
        lines.append('Год: '+str(document['publication_year']))
    lines.append('Файл: '+filename)
    if uri:
        lines.append('Источник: '+uri)
    return filename, '\n'.join(lines)[:1000]


async def archive_payload(client, entry_ref):
    read=await client.call('vibepublish_media_store',{'command':{'kind':'get','entry_ref':entry_ref}})
    for _ in range(60):
        receipt=await client.receipt(read['operation_id'])
        if receipt['operation_complete']:break
        await asyncio.sleep(.5)
    if receipt['state']!='verified':raise RuntimeError('archive_source_unavailable')
    assets=[m for item in receipt.get('items',[]) for m in item.get('media',[]) if m.get('source',{}).get('kind')=='asset']
    evidence=[e for item in receipt.get('items',[]) for e in item.get('media_evidence',[]) if e.get('media_kind')=='document']
    if len(assets)!=1 or len(evidence)!=1:raise RuntimeError('archive_download_binding_missing')
    response=await client.request('GET',client.issuer+'/v1/assets/'+assets[0]['source']['id'])
    data=response.content
    actual_sha=hashlib.sha256(data).hexdigest()
    provider_sha=evidence[0].get('sha256')
    if provider_sha and provider_sha!=actual_sha:raise RuntimeError('archive_provider_digest_mismatch')
    return data,provider_sha or actual_sha


async def archive_bytes(client, entry_ref):
    return (await archive_payload(client,entry_ref))[0]


async def download_source(backend, principal, document_id, obj, path, *, require_archive=False):
    # Caller already resolves ingestion/illustration under RLS. Recheck source privacy:
    # public parsed text does not confer access to the private original scan.
    response=await backend.client.get(backend.config.url.rstrip('/')+'/rest/v1/rkb_documents',
        headers=backend._headers(principal),params={'id':'eq.'+str(document_id),'limit':'1'})
    response.raise_for_status();rows=response.json()
    if not rows or str(rows[0]['owner_user_id'])!=principal.subject:raise PermissionError('private_source_owner_required')
    doc=rows[0]
    if not require_archive and not obj.get('deleted_at'):
        try:
            await backend.object_store.download_file(obj['object_key'],str(path))
            if (await asyncio.to_thread(sha256_file,path))[0]!=obj['sha256']:raise ValueError('source_integrity_mismatch')
            return
        except Exception:
            if doc.get('source_archive_status')!='verified':raise
    if doc.get('source_archive_status')!='verified':raise RuntimeError('source_archive_pending')
    client=VibePublishClient(os.environ['RKB_VIBEPUBLISH_GRANT_FILE'])
    try:
        if client.owner!=principal.subject:raise PermissionError('source_grant_owner_mismatch')
        data=await archive_bytes(client,doc['source_archive_ref'])
        if hashlib.sha256(data).hexdigest()!=obj['sha256']:raise ValueError('archived_source_integrity_mismatch')
        path.write_bytes(data)
        log.info(json.dumps({'event':'source_archive_download_verified',
            'document_id':str(document_id),'source_archive_ref':doc['source_archive_ref'],
            'sha256':obj['sha256'],'size_bytes':len(data),'require_archive':require_archive}))
    finally:await client.close()


class SourceArchive:
    def __init__(self,backend,client):self.backend=backend;self.client=client
    async def update(self,document,**values):
        from psycopg import sql
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            query=sql.SQL('update rkb_documents set {} where id=%s and owner_user_id=%s and source_sha256=%s').format(
                sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in values))
            await db.execute(query,(*values.values(),document['id'],UUID(self.client.owner),document['source_sha256']))
    async def tick(self):
        actor=Principal(subject=self.client.owner,client_id='source-archive',issuer='internal-actor-bridge',access_token='internal-actor-bridge')
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            docs=await(await db.execute("""select d.*,o.object_key,o.id object_id,o.mime_type from rkb_documents d
             join rkb_objects o on o.document_id=d.id and o.kind='source_pdf' and o.sha256=d.source_sha256 and o.deleted_at is null
             where d.owner_user_id=%s and d.source_archive_status='pending' and not exists(select 1 from rkb_documents other where other.owner_user_id=d.owner_user_id and other.source_sha256=d.source_sha256 and other.id<d.id)
             and (d.source_archive_attempt_at is null or d.source_archive_error is null or d.source_archive_attempt_at<now()-interval '30 seconds')
             order by d.source_archive_attempt_at nulls first,d.id limit 1""",(UUID(self.client.owner),))).fetchall()
        count=0
        for doc in docs:
            try:
                previous_attempt = doc.get('source_archive_attempt_at') is not None
                # System selection contains metadata only; source bytes require actor authorization.
                async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
                    authorized=await(await db.execute('select id from rkb_documents where id=%s and owner_user_id=rkb_current_actor_id()', (doc['id'],))).fetchone()
                    if not authorized:continue
                await self.update(doc,source_archive_attempt_at=datetime.now(timezone.utc))
                if not doc.get('source_format'):
                    await self.update(doc,source_format='djvu' if doc['mime_type']=='image/vnd.djvu' else 'pdf')
                uri='knowledge://documents/'+str(doc['id'])+'/source'
                origin={'system':'regional_knowledge','ref':uri,'sha256':doc['source_sha256']}
                operation=doc['source_archive_operation_id']
                if not operation:
                    data=await self.backend.object_store.get_bytes(doc['object_key'])
                    if hashlib.sha256(data).hexdigest()!=doc['source_sha256']:raise ValueError('source_integrity_mismatch')
                    key="rkb:source:"+hashlib.sha256(f"{self.client.owner}:{doc['id']}:{doc['source_sha256']}".encode()).hexdigest()
                    upload=await self.client.request('POST',self.client.issuer+'/v1/assets',
                        headers={'Content-Type':doc['mime_type'],'Idempotency-Key':key+':asset'},content=data)
                    filename=doc.get('source_archive_filename')
                    caption=doc.get('source_archive_caption')
                    if not filename or not caption:
                        if previous_attempt:
                            source_format=(doc.get('source_format') or ('djvu' if doc.get('mime_type')=='image/vnd.djvu' else 'pdf')).lower()
                            filename=doc.get('source_filename') or 'source.'+source_format
                            caption='Regional Knowledge source '+uri
                        else:
                            filename,caption=_source_display_metadata(doc,uri)
                        await self.update(doc,source_archive_filename=filename,source_archive_caption=caption)
                    receipt=await self.client.call('vibepublish_media_store',{'request_key':key,'command':{
                        'kind':'put','to':self.client.grant['destination_alias'],'thread_ref':self.client.grant['source_thread_ref'],
                        'content':{'text':caption},'origin':origin,
                        'media':[{'source':{'kind':'asset','id':upload.json()['asset_id']},'role':'document',
                                  'alt_text':filename}]}})
                    operation=receipt['operation_id'];await self.update(doc,source_archive_operation_id=operation)
                receipt=await self.client.receipt(operation)
                if not receipt['operation_complete']:continue
                if receipt['state'] in ('blocked','failed'):
                    # Vibe owns the dispatch proof. Its existing recovery refuses
                    # dispatched/uncertain effects; preserve the original operation.
                    await self.client.call('vibepublish_publication_update',{
                        'publication_id':receipt['resource_id'],'expected_revision':receipt['revision'],
                        'request_key':'rkb:source:retry:'+operation,
                        'change':{'kind':'retry_failed','destinations':[self.client.grant['destination_alias']]}})
                    await self.update(doc,source_archive_error='source_retry_admitted')
                    log.info(json.dumps({'event':'source_archive_safe_retry','document_id':str(doc['id']),'operation_id':operation}))
                    continue
                if receipt['state']!='verified':raise RuntimeError('source_delivery_not_verified')
                read_id=doc['source_archive_read_id']
                if not read_id:
                    listed=await self.client.call('vibepublish_media_store',{'command':{'kind':'search','text':uri},'limit':25})
                    matches=[r for r in listed['media_store_items'] if r.get('origin')==origin]
                    if len(matches)!=1:raise RuntimeError('source_origin_readback_missing')
                    read=await self.client.call('vibepublish_media_store',{'command':{'kind':'get','entry_ref':matches[0]['entry_ref']}})
                    read_id=read['operation_id'];await self.update(doc,source_archive_read_id=read_id)
                read=await self.client.receipt(read_id)
                if not read['operation_complete']:continue
                entries=read.get('media_store_items') or []
                evidence=[e for item in read.get('items',[]) for e in item.get('media_evidence',[])]
                if read['state']!='verified' or len(entries)!=1 or entries[0].get('origin')!=origin or entries[0]['thread_ref']!=self.client.grant['source_thread_ref'] or not evidence or any(e['media_kind']!='document' or e.get('sha256')!=doc['source_sha256'] for e in evidence):
                    raise RuntimeError('source_native_readback_mismatch')
                await self.update(doc,source_archive_ref=entries[0]['entry_ref'],source_archive_status='verified',source_archive_error=None)
                async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
                    # Shared exact bytes do not merge historical logical roots.
                    await db.execute("update rkb_documents set source_archive_ref=%s,source_archive_origin_ref=%s,source_archive_status='verified',source_archive_error=null where owner_user_id=%s and source_sha256=%s",
                        (entries[0]['entry_ref'],uri,UUID(self.client.owner),doc['source_sha256']))
                count+=1;log.info(json.dumps({'event':'source_archive_verified','document_id':str(doc['id'])}))
            except Exception as error:
                await self.update(doc,source_archive_error=type(error).__name__)
                log.warning(json.dumps({'event':'source_archive_retry','document_id':str(doc['id']),'error_type':type(error).__name__}))
        return count
