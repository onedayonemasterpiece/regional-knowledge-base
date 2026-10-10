"""Synthetic source tests, not historical assertions about Kneiphof."""
import hashlib
from copy import deepcopy
from uuid import uuid4
import pytest
from regional_knowledge.contracts import Principal
from regional_knowledge.entity_graph import GraphBundle, validate_staged_bundle
from regional_knowledge.graph_service import GraphService
from regional_knowledge.story_registry import StoryRegistry
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore


def source(backend, actor, text):
    doc,page,region,chunk=[str(uuid4()) for _ in range(4)]
    sha=hashlib.sha256(text.encode()).hexdigest()
    backend.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,
        'owner_user_id':actor.subject,'title':'Synthetic source','source_sha256':sha,
        'active_revision':1,'page_count':1}])
    backend.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,
        'document_id':doc,'revision':1,'physical_page_index':0}])
    backend.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,
        'page_id':page,'kind':'body','source_text':text,'text_sha256':sha}])
    backend.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,
        'document_id':doc,'revision':1,'source_text':text,'text_sha256':sha,
        'search_material':text,'search_material_sha256':sha,
        'region_ids':[region],'page_ids':[page],'title':'Synthetic passage'}])
    return doc,{'chunk_id':chunk,'page_id':page,'region_id':region,'exact_quote':text}


class NoExternalCalls:
    def resolve(self,*args):raise AssertionError('A name lookup must not decide identity')
    def version(self,*args):raise AssertionError('A map/POI is not required to import a location')


@pytest.mark.asyncio
async def test_locations_reuse_alias_hierarchy_cross_book_and_later_map(tmp_path):
    b=SQLiteBackend(corpus_path=tmp_path/'locations.sqlite',
        embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    owner=str(uuid4())
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
    actor=Principal(subject=owner,client_id='test',issuer='test',access_token='test')
    g=GraphService(b,NoExternalCalls());catalog=StoryRegistry(b.corpus)
    doc,e=source(b,actor,'На острове Кнайпхоф находились собор и улица Ланггассе.')
    def place(key,name,kind,**extras):
        return {'key':key,'kind':'poi_ref','canonical_label':name,
            'exact_source_spelling':name,'place_kind':kind,'evidence':e,**extras}
    research={'url':'https://example.org/synthetic-island',
              'note':'Synthetic reference identifying an alternative name; not the book quote.'}
    first={'entities':[
        place('island','Кнайпхоф','island',place_context='Остров, не городская организация.',
            aliases=[{'value':'Остров Канта','alias_type':'current','evidence':e,
                      'research_sources':[research]}]),
        place('street','Ланггассе','street'),
        place('cathedral','собор','building'),
    ],'relations':[
        {'source_key':child,'target_key':'island','kind':'located_in','evidence':[e]}
        for child in ('street','cathedral')
    ]}
    saved=await g.stage(actor,doc,1,first)
    assert len(saved['entities'])==3
    assert await g.stage(actor,doc,1,first)==saved
    found=catalog.entity_list(actor,kinds=['place'],query='ОСТРОВ КАНТА')
    assert len(found['items'])==1
    assert found['items'][0]['entity_id']==saved['entities']['island']
    assert found['items'][0]['place_kind']=='island'
    island=await g.read(actor,saved['entities']['island'])
    assert len(island['neighbors'])==2
    # Old broad discovery suggestions must not appear as model-linked mentions
    # or become aliases in the registry (for example another island).
    from uuid import uuid5,UUID
    automatic=deepcopy(b.corpus.rows('rkb_entity_mentions')[0])
    automatic.update(id=str(uuid5(UUID(saved['entities']['island']),'old-discovery')),
        entity_id=saved['entities']['island'],exact_source_spelling='Other island',
        signals={'identity_unresolved':True,'retrieval_mode':'vector_only'})
    b.corpus.put('rkb_entity_mentions',[automatic])
    assert len((await g.read(actor,saved['entities']['island']))['mentions'])==1
    assert not catalog.entity_list(actor,kinds=['place'],query='Other island')['items']
    # Alias synchronization is not permission to select an unlinked identity.
    from regional_knowledge.graph_discovery import GraphDiscoveryWorker
    worker=GraphDiscoveryWorker(b);calls=[]
    class Tracker:
        def resolve(self,*args):calls.append('resolve');return {'external_ref':None}
    worker.graph.resolver=Tracker()
    await worker.sync_pois()
    assert calls==[]
    assert island['entity']['external_ref'] is None
    assert island['aliases'][0]['evidence']['alias_basis']=='external_research'

    # A different book reuses the SAME location despite a different spelling.
    doc2,e2=source(b,actor,'На острове Канта был двор.')
    second={'entities':[
        {'key':'same-island','entity_id':saved['entities']['island'],'kind':'poi_ref',
         'canonical_label':'Остров Канта','exact_source_spelling':'острове Канта',
         'evidence':e2},
        {'key':'court','kind':'poi_ref','canonical_label':'двор',
         'exact_source_spelling':'двор','place_kind':'courtyard','evidence':e2},
    ],'relations':[{'source_key':'court','target_key':'same-island',
                   'kind':'located_in','evidence':[e2]}]}
    again=await g.stage(actor,doc2,1,second)
    assert again['entities']['same-island']==saved['entities']['island']
    assert len(b.corpus.rows('rkb_entities'))==4
    assert len((await g.read(actor,saved['entities']['island']))['mentions'])==2
    assert catalog.entity_list(actor,kinds=['poi_ref'],query='ОСТРОВЕ КАНТА')['items'][0]['entity_id']==saved['entities']['island']

    # Reference-only hierarchy endpoint adds no fabricated source mention.
    edge={'entity_refs':{'court':again['entities']['court'],'island':saved['entities']['island']},
          'relations':[{'source_key':'court','target_key':'island','kind':'located_in',
                        'time_scope':'source period','evidence':[e2]}]}
    mentions=len(b.corpus.rows('rkb_entity_mentions'))
    await g.stage(actor,doc2,1,edge)
    assert len(b.corpus.rows('rkb_entity_mentions'))==mentions
    enriched=deepcopy(second)
    enriched['entities']=enriched['entities'][:1];enriched['relations']=[]
    enriched['entities'][0]['map_refs']=['cartography://place/synthetic-island']
    await g.stage(actor,doc2,1,enriched)
    assert len(b.corpus.rows('rkb_entities'))==4
    assert (await g.read(actor,saved['entities']['island']))['entity']['metadata']['map_refs']==['cartography://place/synthetic-island']
    assert b.corpus.one('rkb_documents',doc)['active_revision']==1
    assert b.corpus.one('rkb_documents',doc2)['active_revision']==1

    # Matching spelling alone never creates a false identity merge.
    homonym=deepcopy(first);homonym['entities']=homonym['entities'][:1];homonym['relations']=[]
    homonym['entities'][0]['key']='distinct-site';homonym['entities'][0]['aliases']=[]
    other=await g.stage(actor,doc,1,homonym)
    assert other['entities']['distinct-site']!=saved['entities']['island']

    # Another account cannot search private aliases or use the referenced parent.
    outsider=str(uuid4());b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':outsider}])
    visitor=Principal(subject=outsider,client_id='test',issuer='test',access_token='test')
    assert not catalog.entity_list(visitor,query='Канта')['items']
    vdoc,ve=source(b,visitor,'Улица находится на острове.')
    bad={'entities':[{'key':'road','kind':'poi_ref','canonical_label':'Улица',
        'exact_source_spelling':'Улица','evidence':ve}],
        'entity_refs':{'parent':saved['entities']['island']},
        'relations':[{'source_key':'road','target_key':'parent','kind':'located_in','evidence':[ve]}]}
    with pytest.raises(LookupError):await g.stage(visitor,vdoc,1,bad)


def test_relation_only_stage_validates_real_evidence():
    from regional_knowledge.stage_graph import StagedGraph,StagedChunk
    chunk,page,region=[str(uuid4()) for _ in range(3)]
    e={'chunk_id':chunk,'page_id':page,'region_id':region,'exact_quote':'A is on B.'}
    graph=StagedGraph(revision=1,chunks=[StagedChunk(chunk_key='c',chunk_id=chunk,
        title='Synthetic',region_ids=[region],page_ids=[page],text='A is on B.',normalized_text='A is on B.')])
    bundle=GraphBundle(entity_refs={'a':str(uuid4()),'b':str(uuid4())},relations=[{
        'source_key':'a','target_key':'b','kind':'located_in','evidence':[e]}])
    validate_staged_bundle(bundle,graph)
    bundle.relations[0].evidence[0].exact_quote='Invented'
    with pytest.raises(ValueError):validate_staged_bundle(bundle,graph)
