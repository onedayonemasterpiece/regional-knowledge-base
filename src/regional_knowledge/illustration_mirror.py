"""Bounded private mirror reconciliation through VibePublish's own provider lane."""
import json
import os
import time
from pathlib import Path
from uuid import UUID
import httpx
from .illustrations import fetch_crop


class VibePublishClient:
    def __init__(self, grant_path):
        self.path = Path(grant_path)
        if self.path.stat().st_mode & 0o077:
            raise PermissionError('VibePublish grant must be private')
        self.grant = json.loads(self.path.read_text())
        self.owner = str(UUID(self.grant['rkb_owner_id']))
        self.issuer = self.grant['issuer'].rstrip('/')
        if not self.issuer.startswith('https://'):
            raise ValueError('VibePublish grant must use HTTPS')
        self.http = httpx.AsyncClient(timeout=120, trust_env=False)
        self.sequence = 0

    async def close(self):
        await self.http.aclose()

    async def refresh(self):
        latest=json.loads(self.path.read_text())
        if latest['client_id']!=self.grant['client_id'] or latest['rkb_owner_id']!=self.owner or latest['issuer'].rstrip('/')!=self.issuer:
            raise PermissionError('mirror grant binding changed; restart required')
        self.grant=latest
        response = await self.http.post(self.issuer+'/token', data={
            'grant_type':'refresh_token', 'refresh_token':self.grant['refresh_token'],
            'client_id':self.grant['client_id'], 'scope':'vibepublish',
            'resource':self.issuer+'/mcp/'})
        response.raise_for_status()
        token = response.json()
        self.grant.update(access_token=token['access_token'], refresh_token=token['refresh_token'],
                          expires_at=time.time()+token['expires_in'])
        temp = self.path.with_name(self.path.name+'.new')
        temp.write_text(json.dumps(self.grant));temp.chmod(0o600);temp.replace(self.path)

    async def request(self, method, url, **kwargs):
        if self.grant['expires_at'] < time.time()+60:
            await self.refresh()
        headers=kwargs.pop('headers', {})
        response = await self.http.request(method, url, headers={'Authorization':'Bearer '+self.grant['access_token'],
                                           **headers}, **kwargs)
        if response.status_code == 401:
            await self.refresh()
            response = await self.http.request(method, url, headers={'Authorization':'Bearer '+self.grant['access_token'], **headers}, **kwargs)
        response.raise_for_status()
        return response

    async def call(self, name, arguments):
        self.sequence += 1
        response = await self.request('POST', self.issuer+'/mcp/', headers={'Accept':'application/json, text/event-stream'},
                                     json={'jsonrpc':'2.0','id':self.sequence,'method':'tools/call',
                                           'params':{'name':name,'arguments':arguments}})
        body = json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith('data: '))) if response.headers.get('content-type','').startswith('text/event-stream') else response.json()
        if 'error' in body:
            raise RuntimeError('VibePublish RPC error')
        result = body['result']
        data = result.get('structuredContent') or json.loads(result['content'][0]['text'])
        if result.get('isError') or 'error' in data:
            code=data.get('error',{}).get('code','tool_failed')
            import logging
            logging.getLogger(__name__).warning(json.dumps({'event':'vibepublish_tool_failed','tool':name,'code':code}))
            raise RuntimeError('VibePublish tool error: '+code)
        return data

    async def receipt(self, operation):
        return (await self.call('vibepublish_status', {'ids':[operation]}))['receipts'][0]

    async def put(self, key, metadata, crop, mime):
        response = await self.request('POST', self.issuer+'/v1/assets',
                                      headers={'Content-Type':mime,'Idempotency-Key':key+':asset'}, content=crop)
        asset = response.json()['asset_id']
        return await self.call('vibepublish_media_store', {'request_key':key,'command':{
            'kind':'put','to':self.grant['destination_alias'],'thread_ref':self.grant['thread_ref'],
            'content':{'text':'Regional Knowledge illustration '+metadata['uri']},
            'media':[{'source':{'kind':'asset','id':asset},'role':'document'}],
            'origin':{'system':'regional_knowledge','ref':metadata['uri']}}})


class IllustrationMirror:
    def __init__(self, backend, client):
        self.backend = backend
        self.client = client

    async def update(self, item, **values):
        # Frozen owner/active revision guard also fences results after revocation.
        from psycopg import sql
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            query=sql.SQL('update rkb_illustrations i set {} from rkb_pages p,rkb_documents d,rkb_users u where i.id=%s and p.id=i.page_id and d.id=i.document_id and p.revision=d.active_revision and d.owner_user_id=%s and u.id=d.owner_user_id and u.status=\'active\' and i.visibility=\'private\'').format(sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in values))
            await db.execute(query, (*values.values(),item['id'],UUID(self.client.owner)))

    async def tick(self):
        from .contracts import Principal
        actor=Principal(subject=self.client.owner,client_id='private-mirror',issuer='internal-actor-bridge',access_token='internal-actor-bridge')
        async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
            items=await(await db.execute("""select i.id,i.document_id,p.revision,i.mirror_operation_id,i.mirror_read_operation_id,i.display_crop_sha256,i.source_crop_sha256,i.provider_crop_sha256 from rkb_illustrations i
             join rkb_pages p on p.id=i.page_id join rkb_documents d on d.id=i.document_id
             where d.owner_user_id=%s and p.revision=d.active_revision and i.visibility='private'
              and i.vibepublish_entry_ref is null and i.crop_object_id is not null order by i.mirror_attempt_at nulls first,i.id limit 4""",(UUID(self.client.owner),))).fetchall()
        verified=0
        for item in items:
            try:
                from datetime import datetime,timezone
                await self.update(item,mirror_attempt_at=datetime.now(timezone.utc))
                operation=item['mirror_operation_id']
                if not operation:
                    metadata,crop,mime=await fetch_crop(self.backend,actor,str(item['id']))
                    key=f"rkb:mirror:{self.client.owner}:{item['document_id']}:{item['revision']}:{item['id']}"
                    receipt=await self.client.put(key,metadata,crop,mime)
                    operation=receipt['operation_id']
                    await self.update(item,mirror_operation_id=operation,mirror_error_type=None)
                receipt=await self.client.receipt(operation)
                if not receipt['operation_complete']:
                    continue
                if receipt['state']!='verified':
                    raise RuntimeError('mirror outcome not verified')
                read_operation=item['mirror_read_operation_id']
                if not read_operation:
                    listed=await self.client.call('vibepublish_media_store',{'command':{'kind':'search','text':'knowledge://illustrations/'+str(item['id'])},'limit':25})
                    matches=[r for r in listed['media_store_items'] if r.get('origin')=={'system':'regional_knowledge','ref':'knowledge://illustrations/'+str(item['id'])}]
                    if len(matches)!=1:
                        raise RuntimeError('mirror resource readback missing')
                    read=await self.client.call('vibepublish_media_store',{'command':{'kind':'get','entry_ref':matches[0]['entry_ref']}})
                    read_operation=read['operation_id'];await self.update(item,mirror_read_operation_id=read_operation)
                read=await self.client.receipt(read_operation)
                if not read['operation_complete']:
                    continue
                entries=read.get('media_store_items') or []
                evidence=[e for r in read.get('items',[]) for e in r.get('media_evidence',[]) if e.get('media_kind')=='document']
                provider_shas={e.get('sha256') for e in evidence if e.get('sha256')}
                if read['state']!='verified' or len(entries)!=1 or entries[0]['thread_ref']!=self.client.grant['thread_ref'] or entries[0].get('origin')!={'system':'regional_knowledge','ref':'knowledge://illustrations/'+str(item['id'])} or len(evidence)!=1 or len(provider_shas)!=1:
                    raise RuntimeError('mirror native document/topic readback mismatch')
                provider_sha=next(iter(provider_shas))
                persisted=item.get('provider_crop_sha256')
                if persisted and persisted!=provider_sha:
                    raise RuntimeError('mirror provider digest changed')
                await self.update(item,vibepublish_entry_ref=entries[0]['entry_ref'],provider_crop_sha256=provider_sha,mirror_error_type=None)
                verified+=1
            except Exception as error:
                await self.update(item,mirror_error_type=type(error).__name__)
                import logging
                logging.getLogger(__name__).warning(json.dumps({'event':'illustration_mirror_retry','illustration_id':str(item['id']),'error_type':type(error).__name__}))
        return verified
