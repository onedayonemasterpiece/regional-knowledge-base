"""Narrow authorized crop read. Resource identity never conveys access."""
import hashlib
import json
import logging
from uuid import UUID


async def row(backend, principal, illustration_id, document_id=None):
    ident = str(UUID(illustration_id.removeprefix('knowledge://illustrations/')))
    params = {'id': 'eq.'+ident, 'limit': '1'}
    if document_id:
        params['document_id'] = 'eq.'+str(document_id)
    response = await backend.client.get(backend.config.url.rstrip('/')+'/rest/v1/rkb_illustrations',
                                        headers=backend._headers(principal), params=params)
    response.raise_for_status()
    rows = response.json()
    if not rows:
        raise LookupError('illustration_not_found')
    return rows[0]


async def descriptor(backend, principal, illustration_id, document_id=None):
    item = await row(backend, principal, illustration_id, document_id)
    async def related(table, ident, select):
        response = await backend.client.get(backend.config.url.rstrip('/')+'/rest/v1/'+table,
                                            headers=backend._headers(principal),
                                            params={'id': 'eq.'+str(ident), 'select': select, 'limit':'1'})
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError('illustration_evidence_not_found')
        return rows[0]
    page = await related('rkb_pages', item['page_id'], 'id,document_id,physical_page_index,revision')
    region = await related('rkb_regions', item['source_region_id'], 'id,bbox,page_id,kind')
    if region['kind'] != 'figure' or str(region['page_id']) != str(page['id']) or str(page['document_id'])!=str(item['document_id']):
        raise ValueError('illustration source binding mismatch')
    output = {k:item.get(k) for k in ('kind','caption_text','caption_region_ids','visual_description',
              'visual_description_provenance','visual_description_language','source_crop_sha256',
              'visibility','rights_status','vibepublish_entry_ref')}
    # psycopg returns UUID[] as native UUID objects. The descriptor is also used
    # by the direct ImageContent tool, outside Pydantic's fetch serialization.
    output['caption_region_ids']=[str(value) for value in item.get('caption_region_ids') or []]
    output.update(illustration_id=str(item['id']), uri='knowledge://illustrations/'+str(item['id']),
                  page_id=str(page['id']), physical_page_index=page['physical_page_index'],
                  bbox=region['bbox'], revision=page['revision'])
    return output


async def fetch_crop(backend, principal, illustration_id):
    item = await row(backend, principal, illustration_id)
    # Scope a service-only locator to the exact actor-authorized document/object.
    obj = await backend._server_object(document_id=str(item['document_id']),
                                       object_id=str(item['crop_object_id']), kind='illustration_crop')
    if not obj:
        raise LookupError('illustration_crop_missing')
    data = await backend.object_store.get_bytes(obj['object_key'])
    if hashlib.sha256(data).hexdigest() != obj['sha256']:
        raise ValueError('stored crop integrity mismatch')
    logging.getLogger(__name__).info(json.dumps({'event':'illustration_crop_read','illustration_id':str(item['id']),'bytes':len(data),'mime_type':obj['mime_type']}))
    return await descriptor(backend, principal, str(item['id'])), data, obj['mime_type']
