import hashlib
from uuid import uuid4
import pytest
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.contracts import Principal
from regional_knowledge.graph_service import GraphService
from regional_knowledge.graph_discovery import GraphDiscoveryWorker

@pytest.mark.asyncio
async def test_graph_stage_read_discovery_and_revocation(tmp_path):
 b=SQLiteBackend(corpus_path=tmp_path/'db',embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
 ids=[str(uuid4()) for _ in range(5)];owner,doc,page,region,chunk=ids;text='Exact historical town.';sha=hashlib.sha256(text.encode()).hexdigest()
 b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
 b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'title':'Book','source_sha256':'a'*64,'active_revision':1,'page_count':1}])
 b.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
 b.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,'page_id':page,'reading_order':0,'kind':'body','source_text':text,'text_sha256':sha}])
 b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha,'region_ids':[region],'page_ids':[page],'title':'Exact'}])
 actor=Principal(subject=owner,client_id='test',issuer='test',access_token='test');g=GraphService(b)
 e={'chunk_id':chunk,'page_id':page,'region_id':region,'exact_quote':text}
 result=await g.stage(actor,doc,1,{'entities':[{'key':'town','kind':'event','canonical_label':'Historical town','exact_source_spelling':'town','evidence':e,'aliases':[{'value':'town','evidence':e}]}]})
 node=result['entities']['town'];assert (await g.read(actor,node,20))['entity']['canonical_label']=='Historical town'
 async with b.data_client._connection({'x-rkb-service':'1'}) as db:
  await db.execute("update rkb_graph_discovery_jobs set available_at=now()-interval '10 seconds'")
 worker=GraphDiscoveryWorker(b);job=await worker.claim();assert job and job['state']=='pending'
 await worker.entity(job,actor)
 await worker.finish(job)
 assert any(m["signals"].get("ranking") for m in b.corpus.rows("rkb_entity_mentions"))
 b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner,'status':'disabled'}])
 with pytest.raises(PermissionError):await g.read(actor,node,20)
