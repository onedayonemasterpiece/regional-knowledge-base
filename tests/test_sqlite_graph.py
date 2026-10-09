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


@pytest.mark.asyncio
async def test_historical_organizations_are_sourced_not_merged_and_acl_scoped(tmp_path):
    """Organization, predecessor and place are separate identities in SQLite."""
    b=SQLiteBackend(corpus_path=tmp_path/'org.sqlite3',
                    embedder=LexicalOnlyEmbedder(),
                    object_store=UnavailableObjectStore())
    owner,doc,page,region,chunk=[str(uuid4()) for _ in range(5)]
    original=(
        "В 1810 году Купеческая палата участвовала в торговой реформе. "
        "Мария Шульц состояла в Купеческой палате. "
        "Торговое общество сменило Купеческую палату. "
        "Купеческая палата работала в Старой ратуше."
    )
    sha=hashlib.sha256(original.encode()).hexdigest()
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
    b.corpus.put('rkb_documents',[{
        **defaults('rkb_documents'),'id':doc,'owner_user_id':owner,
        'title':'Synthetic guild chronicle', 'source_sha256':'b'*64,
        'active_revision':1,'page_count':1,
    }])
    b.corpus.put('rkb_pages',[{
        **defaults('rkb_pages'),'id':page,'document_id':doc,
        'revision':1,'physical_page_index':0,
    }])
    b.corpus.put('rkb_regions',[{
        **defaults('rkb_regions'),'id':region,'page_id':page,
        'kind':'body','source_text':original,'text_sha256':sha,
    }])
    b.corpus.put('rkb_chunks',[{
        **defaults('rkb_chunks'),'id':chunk,'document_id':doc,
        'revision':1,'source_text':original,'search_material':original,
        'text_sha256':sha,'search_material_sha256':sha,
        'region_ids':[region],'page_ids':[page],'title':'Guild source',
    }])
    actor=Principal(subject=owner,client_id='source-test',issuer='synthetic',
                    access_token='not-a-production-token')
    proof={'chunk_id':chunk,'page_id':page,'region_id':region,
           'exact_quote':original}
    def entity(key,kind,label,**kwargs):
        return {
            'key':key,'kind':kind,'canonical_label':label,
            'exact_source_spelling':label,'evidence':proof,**kwargs,
        }
    bundle={'entities':[
        entity('guild','organization','Купеческая палата',
               aliases=[{'value':'Купеческая палата','alias_type':'historical',
                         'time_scope':'1810','language':'ru','evidence':proof}]),
        entity('successor','organization','Торговое общество'),
        entity('person','person','Мария Шульц'),
        entity('reform','event','торговой реформе'),
        entity('place','poi_ref','Старой ратуше',
               poi_locator={'names':['Старая ратуша']}),
    ],'relations':[
        {'source_key':'guild','target_key':'successor',
         'kind':'predecessor_of','time_scope':'после 1810',
         'evidence':[proof]},
        {'source_key':'person','target_key':'guild','kind':'affiliated_with',
         'time_scope':'1810','evidence':[proof]},
        {'source_key':'guild','target_key':'reform','kind':'participated_in',
         'time_scope':'1810','evidence':[proof]},
        {'source_key':'guild','target_key':'place','kind':'operated_at',
         'time_scope':'1810','evidence':[proof]},
    ]}
    g=GraphService(b)
    saved=await g.stage(actor,doc,1,bundle)
    assert len(saved['entities'])==5 and saved['relations']==4
    assert saved['unresolved_pois']==1
    guild=await g.read(actor,saved['entities']['guild'],limit=20)
    assert guild['entity']['kind']=='organization'
    assert guild['entity']['canonical_label']=='Купеческая палата'
    assert guild['aliases'][0]['time_scope']=='1810'
    by_kind={n['kind']:n for n in guild['neighbors']}
    assert set(by_kind)=={'predecessor_of','affiliated_with','participated_in','operated_at'}
    predecessor=by_kind['predecessor_of']
    assert str(predecessor['source_id'])==saved['entities']['guild']
    assert str(predecessor['target_id'])==saved['entities']['successor']
    assert predecessor['evidence'][0]['exact_quote']==original
    assert by_kind['operated_at']['neighbor_kind']=='poi_ref'
    successor=await g.read(actor,saved['entities']['successor'],limit=20)
    assert successor['neighbors'][0]['kind']=='predecessor_of'
    assert str(successor['neighbors'][0]['source_id'])==saved['entities']['guild']
    assert saved['entities']['guild']!=saved['entities']['successor']
    again=await g.stage(actor,doc,1,bundle)
    assert again['entities']==saved['entities']
    assert len((await g.read(actor,saved['entities']['guild']))['neighbors'])==4

    # The model contract rejects reversed roles; the SQLite writer also
    # independently verifies shapes for direct SQL entry points.
    from pydantic import ValidationError
    from regional_knowledge.entity_graph import GraphBundle
    reversed_edge={**bundle,'relations':[{
        'source_key':'place','target_key':'guild',
        'kind':'operated_at','evidence':[proof],
    }]}
    with pytest.raises(ValidationError):
        GraphBundle.model_validate(reversed_edge)

    # The actual SQLite authority independently checks source/target kinds
    # for direct SQL clients, not merely GraphBundle/Pydantic inputs.
    from regional_knowledge.sqlite_data import LocalContext
    with b.corpus.connect() as db:
        context=LocalContext(b.corpus,db,owner)
        accepted=next(row for row in b.corpus.rows('rkb_entity_relations')
                      if row['kind']=='predecessor_of')
        context.check_write('rkb_entity_relations',None,accepted)
        forged={
            **defaults('rkb_entity_relations'),'id':str(uuid4()),
            'source_id':saved['entities']['person'],
            'target_id':saved['entities']['guild'],
            'kind':'operated_at','document_id':doc,'revision':1,
            'evidence':[proof],'state':'candidate',
        }
        with pytest.raises(ValueError,match='invalid graph relation shape'):
            context.check_write('rkb_entity_relations',None,forged)

    stranger=str(uuid4())
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':stranger}])
    visitor=Principal(subject=stranger,client_id='source-test',issuer='synthetic',
                      access_token='not-a-production-token')
    with pytest.raises(LookupError):
        await g.read(visitor,saved['entities']['guild'])


@pytest.mark.asyncio
async def test_graph_links_street_story_real_opaque_poi_without_local_identity_copy(tmp_path):
    import sqlite3
    from regional_knowledge.poi_reference import StreetStoryPoiResolver
    src=tmp_path/'street-owner.sqlite3'
    opaque='poi_ss_3a81064258bae2c9b8c41f44'
    with sqlite3.connect(src) as db:
        db.executescript("""
        CREATE TABLE pois(id TEXT PRIMARY KEY,status TEXT,canonical_name TEXT,
                          latitude REAL,longitude REAL);
        CREATE TABLE poi_aliases(poi_id TEXT,namespace TEXT,value TEXT,normalized_value TEXT);
        """)
    backend=SQLiteBackend(
        corpus_path=tmp_path/'regional.sqlite3',
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    owner,document,page,region,chunk=[str(uuid4()) for _ in range(5)]
    text='21 мая 1896 года Кёнигсбергский зоопарк был открыт.'
    sha=hashlib.sha256(text.encode()).hexdigest()
    backend.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
    backend.corpus.put('rkb_documents',[{
        **defaults('rkb_documents'),'id':document,'owner_user_id':owner,
        'title':'Source', 'source_sha256':'f'*64,'active_revision':1,'page_count':1,
    }])
    backend.corpus.put('rkb_pages',[{
        **defaults('rkb_pages'),'id':page,'document_id':document,
        'revision':1,'physical_page_index':0,
    }])
    backend.corpus.put('rkb_regions',[{
        **defaults('rkb_regions'),'id':region,'page_id':page,
        'reading_order':0,'kind':'body','source_text':text,'text_sha256':sha,
    }])
    backend.corpus.put('rkb_chunks',[{
        **defaults('rkb_chunks'),'id':chunk,'document_id':document,'revision':1,
        'source_text':text,'text_sha256':sha,
        'search_material':text,'search_material_sha256':sha,
        'region_ids':[region],'page_ids':[page],'title':'Zoo opening',
    }])
    proof={'chunk_id':chunk,'page_id':page,'region_id':region,'exact_quote':text}
    actor=Principal(subject=owner,client_id='test',issuer='test',access_token='local')
    graph=GraphService(backend,StreetStoryPoiResolver(src))
    first=await graph.stage(actor,document,1,{'entities':[{
        'key':'zoo-site','kind':'poi_ref',
        'canonical_label':'Калининградский зоопарк',
        'exact_source_spelling':'Кёнигсбергский зоопарк',
        'poi_locator':{
            'names':['Калининградский зоопарк','Кёнигсбергский зоопарк'],
            'external_ids':{'wikidata':'Q1193386'},
        },
        'evidence':proof,
    }]})
    assert first['unresolved_pois']==1
    original=await graph.read(actor,first['entities']['zoo-site'])
    assert original['entity']['external_ref'] is None
    assert original['entity']['state']=='unresolved'

    # Street Story owner imports a verified external binding in its OWN SQLite
    # after RKB has already staged this exact private book evidence.
    with sqlite3.connect(src) as db:
        db.execute("INSERT INTO pois VALUES(?,?,?,?,?)",
                   (opaque,'candidate','Калининградский зоопарк',54.72044,20.48737))
        db.execute("INSERT INTO poi_aliases VALUES(?,?,?,?)",
                   (opaque,'wikidata','Q1193386','q1193386'))
    payload={'entities':[{
        'key':'zoo-site','kind':'poi_ref',
        'canonical_label':'Калининградский зоопарк',
        'exact_source_spelling':'Кёнигсбергский зоопарк',
        'poi_locator':{
            'names':['Калининградский зоопарк','Кёнигсбергский зоопарк'],
            'external_ids':{'wikidata':'Q1193386'},
        },
        'evidence':proof,
    }]}
    saved=await graph.stage(actor,document,1,payload)
    assert saved['unresolved_pois']==0
    assert saved['entities']==first['entities']
    assert await graph.stage(actor,document,1,payload)==saved
    node=await graph.read(actor,saved['entities']['zoo-site'])
    assert node['entity']['external_ref']=='streetstory://poi/'+opaque
    assert node['entity']['external_identity_state']=='candidate'
    discovery=await graph.discover_poi(actor,'streetstory://poi/'+opaque)
    assert discovery['identity_state']=='candidate'
    assert discovery['historical_geometry']=='not_verified'
    with pytest.raises(ValueError):
        await graph.discover_poi(actor,'streetstory://poi/poi_ss_unsafe')
