"""Real DB races, crop/source evidence and changed-input indexing acceptance."""
import asyncio
import hashlib
import json
from pathlib import Path
from uuid import UUID,uuid4,uuid5
import httpx
import pytest
from test_graph_postgres import graph_db
from test_indexing import complete_documents,V384
from regional_knowledge.contracts import Principal,ChatFile,StagePageInput
from regional_knowledge.file_ingress import DownloadedFile
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.local_e5 import LocalE5Embedder
from regional_knowledge.e5_contract import SPACE
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.indexing import IndexReconciler
from regional_knowledge.index_readiness import counts
from regional_knowledge.illustrations import fetch_crop
from regional_knowledge.stage_service import finalize_ingestion


class Objects:
    def __init__(self):self.data={}
    async def put_bytes(self,key,data,mime):self.data[key]=bytes(data)
    async def put_file(self,key,path,mime):self.data[key]=Path(path).read_bytes()
    async def get_bytes(self,key):return self.data[key]
    async def get_range(self,key,start,end):return self.data[key][start:end]
    async def download_file(self,key,path):Path(path).write_bytes(self.data[key])


def pdf():
    fitz=pytest.importorskip('fitz');book=fitz.open()
    book.new_page().insert_text((50,70),'Synthetic source. Not historical evidence.')
    page=book.new_page();page.draw_rect(fitz.Rect(80,100,480,500),color=(0,0,1));page.insert_text((80,530),'Abb. 1: Der blaue Leuchtturm.')
    page=book.new_page();page.draw_circle(fitz.Point(280,300),120,color=(1,0,0),fill=(1,0,0))
    data=book.tobytes();book.close();return data


@pytest.mark.asyncio
async def test_source_race_visual_crop_new_revision_and_changed_index(graph_db,tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_REQUIRED_VECTOR_SPACES','e5,bge')
    monkeypatch.setenv('RKB_WORK_DIR',str(tmp_path));data=pdf()
    class Downloader:
        async def download(self,url,directory):
            path=Path(directory)/'fixture.pdf';path.write_bytes(data)
            return DownloadedFile(path=path,sha256=hashlib.sha256(data).hexdigest(),size_bytes=len(data))
    b=PostgresBackend(graph_db,embedder=LexicalOnlyEmbedder(),object_store=Objects());b.file_downloader=Downloader()
    actor=lambda:Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='no-token')
    owner,other=actor(),actor()
    async def start(who,policy='reuse'):
        return await b.book_ingest(command='start',principal=who,file=ChatFile(download_url='https://example.com/owned.pdf',file_id=str(uuid4())),ingestion_id=None,cursor=None,payload={'title':'Owned synthetic','duplicate_policy':policy})
    try:
        first,second=await asyncio.gather(start(owner),start(owner));assert first.document_id==second.document_id and first.ingestion_id==second.ingestion_id
        again=await start(owner);assert again.document_id==first.document_id
        foreign=await start(other);assert foreign.document_id!=first.document_id
        doc=UUID(first.document_id);pages=[];chunks=[]
        for i in range(3):
            pid=str(uuid5(doc,f'page:1:{i}'));box={'left':0,'top':0,'right':1000,'bottom':1000}
            page={'page_id':pid,'physical_page_index':i,'source_material':'visual_reviewed','source_review_note':'Synthetic fixture visually checked','regions':[],'illustrations':[]}
            if i==0:
                page['regions']=[{'region_key':'body','kind':'body','bbox':box,'reading_order':0,'source_text':'Synthetic source. Not historical evidence.'}]
                chunks.append({'chunk_key':'text','title':'Text','region_refs':[{'page_id':pid,'region_key':'body'}]})
            else:
                page['regions']=[{'region_key':'figure','kind':'figure','bbox':box,'reading_order':0}]
                figure={'illustration_key':'figure','source_region_key':'figure','kind':'drawing','visual_description':'A blue lighthouse' if i==1 else 'A red circle','visual_description_language':'en'}
                if i==1:
                    page['regions'].append({'region_key':'caption','kind':'caption','bbox':{'left':0,'top':800,'right':1000,'bottom':1000},'reading_order':1,'source_text':'Abb. 1: Der blaue Leuchtturm.'})
                    figure['caption_region_keys']=['caption']
                page['illustrations']=[figure]
                chunks.append({'chunk_key':f'figure{i}','title':f'Figure {i}','region_refs':[{'page_id':pid,'region_key':'caption' if i==1 else 'figure'}],'illustration_refs':[{'page_id':pid,'illustration_key':'figure'}]})
            pages.append(page)
        await b.book_ingest(command='stage',principal=owner,file=None,ingestion_id=first.ingestion_id,cursor=None,payload={'pages':pages,'chunks':chunks})
        ready=await b.book_ingest(command='validate',principal=owner,file=None,ingestion_id=first.ingestion_id,cursor=None,payload=None);assert ready.state=='ready',ready
        finalized=await finalize_ingestion(b,principal=owner,ingestion_id=first.ingestion_id);assert finalized.state=='finalized'
        async with b.data_client._connection(b._headers(owner)) as db:
            rows=await(await db.execute('select * from rkb_chunks where document_id=%s order by title',(doc,))).fetchall()
        images=[r for r in rows if r['illustration_ids']];assert len(images)==2
        from regional_knowledge import server as server_module
        monkeypatch.setenv('RKB_DEV_NOAUTH','1')
        monkeypatch.setattr(server_module,'_principal',lambda:owner)
        server=server_module.build_server(b)
        for row in images:
            content=await server._tool_manager.call_tool('illustration_fetch',{'id':str(row['illustration_ids'][0])},None)
            assert content[1].type=='image' and content[1].mime_type=='image/png'
            metadata=json.loads(content[0].text)
            assert all(isinstance(value,str) for value in metadata['caption_region_ids'])
        assert metadata['visual_description_provenance']=='model_observation'
        image_only=next(r for r in images if r['text_start']==r['text_end'])
        fetched=await b.fetch(str(image_only['id']),owner);assert fetched.text==''
        description=fetched.metadata['illustrations'][0];assert description['visual_description_provenance']=='model_observation'
        descriptor,crop,mime=await fetch_crop(b,owner,description['uri']);assert mime=='image/png' and hashlib.sha256(crop).hexdigest()==descriptor['source_crop_sha256']
        with pytest.raises(LookupError):await fetch_crop(b,other,description['uri'])
        caption=await b.search('Leuchtturm',owner);assert any(r.id==str(images[0]['id']) for r in caption.results)
        assert (await start(owner)).document_id==str(doc)
        revision=await start(owner,'new_revision');assert revision.document_id==str(doc) and revision.ingestion_id!=first.ingestion_id
        async with b.data_client._connection(b._headers(owner)) as db:
            job=await(await db.execute('select staged_revision from rkb_ingestion_jobs where id=%s',(UUID(revision.ingestion_id),))).fetchone();assert job['staged_revision']==2
            assert (await(await db.execute('select count(*) n from rkb_documents where owner_user_id=%s',(UUID(owner.subject),))).fetchone())['n']==1
        calls=[]
        async def encode(request):
            calls.append(json.loads(request.content)['texts'])
            return httpx.Response(200,json={'space':SPACE,'vectors':[V384 for _ in calls[-1]],'timings':{}})
        b.embedder=LocalE5Embedder();await b.embedder.client.aclose()
        b.embedder.client=httpx.AsyncClient(base_url='http://127.0.0.1:8767',transport=httpx.MockTransport(encode))
        q=BgeQueue(tmp_path/'queue.sqlite');worker=IndexReconciler(b,q)
        initial=await worker.tick();assert initial['e5_written']==3 and initial['bge_submitted']==3
        complete_documents(q,owner);assert (await worker.tick())['bge_written']==3
        assert (await worker.tick())['e5_written']==0
        assert any('A red circle' in text for call in calls for text in call)
        changed=image_only['search_material']+'\n\n[Model observation; not printed source text]\nA misleading date 1777.'
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('update rkb_chunks set search_material=%s,search_material_sha256=%s where id=%s',(changed,hashlib.sha256(changed.encode()).hexdigest(),image_only['id']))
        assert (await counts(b,owner))['e5_ready']==2
        stale=await worker.install_bge(owner,image_only,{'space':q.lookup(owner.subject, __import__('regional_knowledge.indexing',fromlist=['bge_key']).bge_key(image_only))['result']['space'],'vectors':[[1.0]+[0.0]*1023]});assert stale==0
        delta=await worker.tick();assert delta['e5_written']==1 and delta['bge_submitted']==1 and len(calls[-1])==1
        complete_documents(q,owner);assert (await worker.tick())['bge_written']==1
        assert (await b.fetch(str(image_only['id']),owner)).text=='' # misleading description never becomes a source quote.
    finally:await b.aclose()


def test_unrepresented_figure_and_forged_description_provenance():
    from regional_knowledge.stage_graph import StagedGraph,compile_model_stage,merge_stage,validate_graph
    doc=uuid4();pid=str(uuid5(doc,'page:1:0'))
    page={'page_id':pid,'physical_page_index':0,'source_material':'visual_reviewed','source_review_note':'checked','regions':[{'region_key':'figure','kind':'figure','reading_order':0,'bbox':{'left':0,'top':0,'right':1000,'bottom':1000}}]}
    graph=StagedGraph(revision=1);parsed=StagePageInput.model_validate(page)
    merged=merge_stage(graph,compile_model_stage(graph,document_id=str(doc),revision=1,pages=[parsed],chunks=[]))
    assert any('figure must be staged' in e for e in validate_graph(merged,expected_page_count=1).errors)
    page['illustrations']=[{'illustration_key':'i','source_region_key':'figure','kind':'other','visual_description':'invented','visual_description_provenance':'printed_source'}]
    with pytest.raises(ValueError):StagePageInput.model_validate(page)


@pytest.mark.asyncio
async def test_archived_crop_bytes_use_provider_rendition_digest(monkeypatch):
    import regional_knowledge.illustrations as illustrations_module
    import regional_knowledge.source_archive as source_archive_module
    import regional_knowledge.illustration_mirror as mirror_module

    provider_bytes=b"provider-sanitized-rendition"
    provider_sha=hashlib.sha256(provider_bytes).hexdigest()
    canonical_sha=hashlib.sha256(b"canonical-display-rendition").hexdigest()
    state={"provider_sha":provider_sha}

    async def fake_row(*args,**kwargs):
        return {
            "id": uuid4(), "document_id": uuid4(), "crop_object_id": None,
            "vibepublish_entry_ref": "entry", "display_crop_sha256": canonical_sha,
            "provider_crop_sha256": state["provider_sha"], "source_crop_sha256": "a"*64,
        }
    async def fake_descriptor(*args,**kwargs):
        return {"illustration_id":"synthetic","display_crop_sha256":canonical_sha,
                "provider_crop_sha256":state["provider_sha"]}
    class Backend:
        async def _server_object(self,**kwargs): return None
    class Client:
        def __init__(self,*args,**kwargs): pass
        async def close(self): pass

    monkeypatch.setattr(illustrations_module,"row",fake_row)
    monkeypatch.setattr(illustrations_module,"descriptor",fake_descriptor)
    monkeypatch.setattr(mirror_module,"VibePublishClient",Client)
    monkeypatch.setenv("RKB_VIBEPUBLISH_GRANT_FILE","/unused")

    async def archived(*args,**kwargs): return provider_bytes,provider_sha
    monkeypatch.setattr(source_archive_module,"archive_payload",archived)
    principal=Principal(subject=str(uuid4()),client_id="t",issuer="t",access_token="x")

    metadata,data,mime=await illustrations_module.fetch_crop(
        Backend(),principal,"00000000-0000-0000-0000-000000000001"
    )
    assert data==provider_bytes and mime=="image/png"
    assert metadata["delivered_crop_sha256"]==provider_sha
    assert metadata["delivered_crop_sha256"]!=canonical_sha
    assert metadata["delivered_crop_origin"]=="telegram_archive"

    state["provider_sha"]="b"*64
    with pytest.raises(ValueError,match="provider crop integrity mismatch"):
        await illustrations_module.fetch_crop(
            Backend(),principal,"00000000-0000-0000-0000-000000000001"
        )
