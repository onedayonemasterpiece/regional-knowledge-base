from uuid import uuid4
import sqlite3
import pytest
from pydantic import ValidationError
from regional_knowledge.entity_graph import GraphBundle,GraphEvidence,normalize_alias,entity_id,validate_staged_bundle
from regional_knowledge.poi_reference import StreetStoryPoiResolver
from regional_knowledge.contracts import PoiLocatorInput

def evidence():return {'chunk_id':str(uuid4()),'page_id':str(uuid4()),'region_id':str(uuid4()),'exact_quote':'Ada attended'}
def node(key='ada',kind='person',**kw):return {'key':key,'kind':kind,'canonical_label':'Ada','exact_source_spelling':'Ada','evidence':evidence(),**kw}

def test_alias_normalization_is_not_identity_merge():
    assert normalize_alias('  KÖNIGSBERG\t')=='königsberg'
    assert normalize_alias('Koenigsberg')!='königsberg'
    doc=uuid4();assert entity_id(doc,'ada-1')!=entity_id(doc,'ada-2')
    GraphBundle(entities=[node('ada-1'),node('ada-2')])

@pytest.mark.parametrize('kind,pair',[('participated_in',('person','event')),('occurred_at',('event','poi_ref')),('member_of',('person','historical_thread'))])
def test_relation_shapes(kind,pair):
    a=node('a',pair[0]);b=node('b',pair[1]);
    if pair[1]=='poi_ref':b['poi_locator']={'names':['Place']}
    GraphBundle(entities=[a,b],relations=[{'source_key':'a','target_key':'b','kind':kind,'evidence':[evidence()]}])
    with pytest.raises(ValidationError):GraphBundle(entities=[a,b],relations=[{'source_key':'b','target_key':'a','kind':kind,'evidence':[evidence()]}])

@pytest.mark.parametrize('changes',[{'kind':'poi_ref'},{'state':'reviewed'},{'canonical_label':' '},{'metadata':{'arbitrary':True}}])
def test_invalid_candidates_fail_closed(changes):
    with pytest.raises(ValidationError):GraphBundle(entities=[node(**changes)])

def test_exact_staged_references_not_page_ids_alone():
    from regional_knowledge.stage_graph import StagedGraph,StagedChunk
    e=evidence();c=StagedChunk(chunk_key='c',chunk_id=e['chunk_id'],title='Source',region_ids=[e['region_id']],page_ids=[e['page_id']],text='Ada attended',normalized_text='Ada attended')
    g=StagedGraph(revision=1,chunks=[c]);b=GraphBundle(entities=[node(evidence=e)])
    validate_staged_bundle(b,g)
    changed=b.model_copy(deep=True);changed.entities[0].evidence.exact_quote='invented'
    with pytest.raises(ValueError):validate_staged_bundle(changed,g)
    changed=b.model_copy(deep=True);changed.entities[0].evidence.region_id=uuid4()
    with pytest.raises(ValueError):validate_staged_bundle(changed,g)

def test_poi_read_only_resolution_preserves_ambiguity(tmp_path):
    path=tmp_path/'canonical.db'
    with sqlite3.connect(path) as db:
        db.executescript('create table pois(id text,canonical_name text,status text);create table poi_aliases(poi_id text,namespace text,value text,normalized_value text);')
        first,second=str(uuid4()),str(uuid4());db.executemany('insert into pois values(?,?,?)',[(first,'Modern','candidate'),(second,'Same','candidate')]);db.executemany('insert into poi_aliases values(?,?,?,?)',[(first,'name','Old German','old german'),(first,'name','Same','same')])
    r=StreetStoryPoiResolver(path)
    assert r.resolve(PoiLocatorInput(names=['Old German']))['external_ref']=='streetstory://poi/'+first
    assert r.resolve(PoiLocatorInput(names=['Same']))=={'state':'ambiguous','external_ref':None}
    assert r.resolve(PoiLocatorInput(names=['Missing']))['state']=='unresolved'
    before=path.read_bytes();r.version('streetstory://poi/'+first);assert path.read_bytes()==before

def test_bounded_bundle():
    with pytest.raises(ValidationError):GraphBundle(entities=[node(str(i)) for i in range(33)])

@pytest.mark.asyncio
async def test_related_reuses_backend_and_canonical_alias_contract():
    from regional_knowledge.graph_service import GraphService
    from regional_knowledge.contracts import SearchOutput
    class Backend:
        data_client=None
        async def search(self,query,principal,**kw):self.called=(query,kw);return SearchOutput(results=[],mode='lexical_degraded')
    class Resolver:
        def version(self,ref):return {'names':['Modern','Old German'],'version':'1'}
    b=Backend();g=GraphService(b,Resolver())
    async def read(*args):return {'entity':{'external_ref':'streetstory://poi/'+str(uuid4()),'canonical_label':'Modern'},'aliases':[]}
    g.read=read;await g.related(None,str(uuid4()),'Current name query',100)
    assert b.called==('Current name query',{'match_count':20,'aliases':[{'name':'Modern','kind':'historical'},{'name':'Old German','kind':'historical'}]})
