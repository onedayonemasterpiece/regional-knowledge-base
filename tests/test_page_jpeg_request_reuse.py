"""Current fictional old-pin fragments through real controller GET and MCP issuance."""
import copy
import json
from io import BytesIO
from types import MethodType, SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.original_intake import canonical, issued_image_receipt, encode_page_cursor
from regional_knowledge.original_intake_controller import OriginalIntakeController
from test_current_original_full_page import setup_page, issue, native_intake, target, auth, people


async def jpeg_case(s,monkeypatch,*,fault=None):
    f=await setup_page(s,monkeypatch);originals=s.originals;receipt=f.original_receipt
    record=dict(handle='fictional-retained-original',sha256=receipt.sha256,
        spec=dict(source_id=receipt.source_id,ingestion_id=receipt.ingestion_id))
    originals._row=lambda actor,ingestion:dict(acquisition_json=json.dumps(record))
    originals.current_binding=lambda:current_binding(s.t.queue)
    originals.binding_current=s.importer._binding_current
    originals.pages=MethodType(OriginalIntakeController.pages,originals)
    issued=issued_image_receipt(receipt,f.parts[0]);stats=dict(get=0,head=0,phase=0,if_matches=[])
    with s.t.queue.connect() as db:
        db.execute('CREATE TABLE IF NOT EXISTS rkb_original_image_issuance('
            'ingestion_id TEXT NOT NULL,physical_page_index INTEGER NOT NULL,receipt_json TEXT NOT NULL,'
            'PRIMARY KEY(ingestion_id,physical_page_index))')
        db.execute('INSERT INTO rkb_original_image_issuance VALUES(?,?,?)',(receipt.ingestion_id,0,canonical(issued).decode()))
    async def phase(binding,name,args,private=None,*,page_read=None):
        assert name=='original_pages' and args['original_sha256']==receipt.sha256
        assert args['handle']==record['handle']
        stats['phase']+=1
        part=copy.deepcopy(next(p for p in f.parts if p['native_cursor']==args['cursor']))
        if stats['phase']==2:
            if fault=='page':part['page_id']='00000000-0000-0000-0000-000000000000'
            elif fault in ('image','receipt'):
                changed=dict(issued)
                changed['image_sha256' if fault=='image' else 'source_image_receipt_sha256']='f'*64
                with s.t.queue.connect() as db:db.execute('UPDATE rkb_original_image_issuance SET receipt_json=? WHERE ingestion_id=?',
                    (canonical(changed).decode(),receipt.ingestion_id))
            elif fault=='binding':
                with s.t.queue.connect() as db:db.execute('UPDATE runs SET channel_active=0 WHERE id=?',(s.t.run,))
            elif fault=='withdrawal':
                await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
        return part
    originals._phase=phase
    def head_object(**kwargs):
        stats['head']+=1
        return dict(ETag='fictional-image-version',ContentLength=len(b'synthetic-jpeg'))
    def get_object(**kwargs):
        stats['get']+=1;stats['if_matches'].append(kwargs['IfMatch'])
        assert kwargs['IfMatch']=='fictional-image-version'
        return dict(ContentLength=len(b'synthetic-jpeg'),Body=BytesIO(b'synthetic-jpeg'))
    originals.store=SimpleNamespace(bucket='fictional-private',client=SimpleNamespace(
        head_object=head_object,get_object=get_object))
    f.observed_fragments={}
    assemble=originals._assemble_available
    def observe_assembly(ingestion,index,chunks):
        f.observed_fragments.update(chunks)
        return assemble(ingestion,index,chunks)
    originals._assemble_available=observe_assembly
    f.stats=stats
    return f


@pytest.mark.asyncio
async def test_old_126_block_page_uses_one_head_and_get_with_all_native_fragment_checks(native_intake,monkeypatch):
    s=native_intake;f=await jpeg_case(s,monkeypatch)
    result,_=await issue(f);manifest=json.loads(result['result']['content'][0]['text']);page=manifest['pages'][0]
    assert f.stats['phase']==11 and f.stats['head']==1 and f.stats['get']==1
    assert f.stats['if_matches']==['fictional-image-version']
    assert len(page['native_blocks'])==126 and page['native_text']==f.native and page['native_text_info']['complete']
    assert len(result['result']['content'])==2 and result['result']['content'][1]['data']=='c3ludGhldGljLWpwZWc='
    assert f.immutable==[canonical(p) for p in f.parts]
    assert f.observed_fragments=={canonical(p['native_cursor']).decode():canonical(p) for p in f.parts}
    assert s.originals.receipts[f.book.ingestion_id]==f.original_receipt
    # An explicit fragment request keeps the previous GET/SHA path.
    result,_=await issue(f,cursor=encode_page_cursor(0,f.parts[0]['next_native']))
    assert f.stats['get']==2 and f.stats['head']==2 and f.stats['phase']==12
    assert not json.loads(result['result']['content'][0]['text'])['pages'][0]['native_text_info']['complete']


@pytest.mark.asyncio
async def test_initial_page_still_requires_available_derivative(native_intake,monkeypatch):
    s=native_intake;f=await jpeg_case(s,monkeypatch)
    async def missing(key,version):
        f.stats['head']+=1
        return None
    s.originals._head=missing
    with pytest.raises(ToolError):await issue(f)
    assert f.stats['head']==1 and f.stats['get']==0 and f.stats['phase']==1


@pytest.mark.asyncio
async def test_internal_reuse_rejects_wrong_image_type_before_accessing_bytes(native_intake,monkeypatch):
    s=native_intake;f=await jpeg_case(s,monkeypatch);pages=s.originals.pages
    async def wrong_image(*args,**kwargs):
        if kwargs.get('_page_image') is not None:kwargs['_page_image']=object()
        return await pages(*args,**kwargs)
    s.originals.pages=wrong_image
    # Importer deliberately hides the internal provenance label in its envelope.
    with pytest.raises(ToolError,match='native_original_page_unavailable'):await issue(f)
    assert f.stats['head']==1 and f.stats['get']==1 and f.stats['phase']==2


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['page','image','receipt','binding','withdrawal'])
async def test_request_local_image_cannot_survive_changed_page_image_receipt_binding_or_owner(native_intake,monkeypatch,fault):
    s=native_intake;f=await jpeg_case(s,monkeypatch,fault=fault)
    with pytest.raises(ToolError):await issue(f)
    assert f.stats['get']==1 and f.stats['phase']==2
    assert f.immutable==[canonical(p) for p in f.parts]
