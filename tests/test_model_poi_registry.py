"""RKB model-selected Street Story POI bridge; no second POI catalogue."""
import hashlib
from uuid import uuid4
import httpx
import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.graph_service import GraphService
from regional_knowledge.poi_registry_bridge import PoiRegistry, PoiRegistryError
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore


class NoOwnerLookups:
    def resolve(self,*_):raise AssertionError("No automatic POI selection")
    def version(self,*_):raise AssertionError("Local location never requires owner")


def fixture(tmp_path):
    b=SQLiteBackend(corpus_path=tmp_path/'db.sqlite',
        embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    owner,doc,page,region,chunk=[str(uuid4()) for _ in range(5)]
    text="Собор находился на острове и существовал в этом месте."
    sha=hashlib.sha256(text.encode()).hexdigest()
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
    b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,
                                  'title':'Test source','source_sha256':'b'*64,
                                  'active_revision':1,'page_count':1}])
    b.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,
                              'document_id':doc,'revision':1,'physical_page_index':0}])
    b.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,
                                'page_id':page,'kind':'body','source_text':text,'text_sha256':sha}])
    b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,
                               'document_id':doc,'revision':1,'source_text':text,
                               'text_sha256':sha,'search_material':text,
                               'search_material_sha256':sha,'region_ids':[region],
                               'page_ids':[page],'title':'Test source'}])
    return b,Principal(subject=owner,issuer='test',client_id='test',access_token='test'),doc,\
        dict(chunk_id=chunk,page_id=page,region_id=region,exact_quote=text)


class Owner:
    def __init__(self,poi_id):
        self.poi_id=poi_id
        self.requests=[]
    async def post(self,url,json,headers,timeout):
        self.requests.append(json)
        assert headers["Authorization"]=="Bearer testing-owner-secret"
        assert url=="http://owner.test/v1/internal/rkb-poi"
        if json["action"]=="search":
            value={"state":"candidates","identity_selected":False,"candidates":[{
                "poi_id":self.poi_id,"canonical_name":"Собор"}]}
        else:
            value={"state":"linked","poi":{"poi_ref":"streetstory://poi/"+self.poi_id,
                "status":"candidate"},"replayed":len(self.requests)>2}
        return httpx.Response(200,request=httpx.Request("POST",url),json=value)


@pytest.mark.asyncio
async def test_model_explicitly_selects_one_owner_poi_and_reads_history(tmp_path):
    b,actor,doc,e=fixture(tmp_path)
    g=GraphService(b,NoOwnerLookups())
    created=await g.stage(actor,doc,1,{"entities":[{
        "key":"cathedral","kind":"poi_ref","canonical_label":"Собор",
        "place_kind":"building","exact_source_spelling":"Собор","evidence":e}]})
    eid=created["entities"]["cathedral"]
    poi_id="poi_ss_3a81064258bae2c9b8c41f44"
    fake=Owner(poi_id)
    bridge=PoiRegistry(b,owner_url="http://owner.test",owner_token="testing-owner-secret",client=fake)
    candidates=await bridge.act(actor,"search",query="Собор")
    assert candidates["identity_selected"] is False
    assert (await g.read(actor,eid))["entity"]["external_ref"] is None
    chosen=await bridge.act(actor,"select",entity_id=eid,poi_id=poi_id,
        model_reason="This is the same physical cathedral and same site.")
    assert chosen["poi_ref"]=="streetstory://poi/"+poi_id
    assert (await g.read(actor,eid))["entity"]["external_ref"]==chosen["poi_ref"]
    assert (await bridge.context(actor,chosen["poi_ref"]))["locations"][0]["entity_id"]==eid
    again=await bridge.act(actor,"select",entity_id=eid,poi_id=poi_id,
        model_reason="This is the same physical cathedral and same site.")
    assert again["poi_ref"]==chosen["poi_ref"]
    assert b.corpus.one('rkb_documents',doc)['active_revision']==1
    assert len(b.corpus.rows('rkb_entities'))==1
    assert fake.requests[1]["source_ref"]=="knowledge://chunks/"+e["chunk_id"]
    assert "exact_quote" not in str(fake.requests)


@pytest.mark.asyncio
async def test_model_can_create_new_candidate_but_not_without_evidence(tmp_path):
    b,actor,doc,e=fixture(tmp_path)
    g=GraphService(b,NoOwnerLookups())
    new=await g.stage(actor,doc,1,{"entities":[{
        "key":"lost","kind":"poi_ref","canonical_label":"Утраченный дом",
        "place_kind":"building","exact_source_spelling":"Собор","evidence":e}]})
    eid=new["entities"]["lost"]
    poi_id="poi_ss_ecc9f5dfb17d2dc5ee8e2ea8"
    fake=Owner(poi_id)
    bridge=PoiRegistry(b,owner_url="http://owner.test",owner_token="testing-owner-secret",client=fake)
    with pytest.raises(ValueError):
        await bridge.act(actor,"create",entity_id=eid,site_state="lost_site",
                         model_reason="Model chose a physical site.")
    saved=await bridge.act(actor,"create",entity_id=eid,site_state="lost_site",
                           site_context="A known former address where a building once stood",
                           model_reason="Model distinguishes a lost physical address from the surrounding district.")
    assert saved["poi_ref"]=="streetstory://poi/"+poi_id
    assert fake.requests[-1]["site_state"]=="lost_site"
    wrong=Principal(subject=str(uuid4()),issuer='test',client_id='test',access_token='test')
    with pytest.raises((LookupError,PermissionError)):
        await bridge.act(wrong,"create",entity_id=eid,site_state="lost_site",
                         site_context="Address",model_reason="The original documented site.")
    assert len(fake.requests)==1



@pytest.mark.asyncio
async def test_poi_context_projects_place_hierarchy_and_unverified_map_refs_without_geometry(tmp_path):
    """Street Story sees source-side geography, not fake accepted cartography.

    The owner connector only needs one authenticated read-only POI context
    endpoint. No cartography or Street Story DB writes occur on context reads.
    """
    b,actor,doc,e=fixture(tmp_path)
    g=GraphService(b,NoOwnerLookups())
    cart_ref="cartography://place/place_0123456789abcdef01234567"
    staged=await g.stage(actor,doc,1,{"entities":[
        {"key":"site","kind":"poi_ref","canonical_label":"Собор",
         "place_kind":"building","place_context":"Каменный собор острова; не старый деревянный",
         "exact_source_spelling":"Собор","evidence":e,
         "map_refs":[cart_ref]},
        {"key":"island","kind":"poi_ref","canonical_label":"Остров",
         "place_kind":"island","exact_source_spelling":"острове","evidence":e},
    ],"relations":[
        {"source_key":"site","target_key":"island","kind":"located_in",
         "time_scope":"Период, описанный в синтетическом источнике",
         "evidence":[e]},
    ]})
    entity=staged["entities"]["site"]
    poi_id="poi_ss_3a81064258bae2c9b8c41f44"
    bridge=PoiRegistry(b,owner_url="http://owner.test",
                       owner_token="testing-owner-secret",client=Owner(poi_id))
    receipt=await bridge.act(
        actor,"select",entity_id=entity,poi_id=poi_id,
        model_reason="An explicitly chosen physical building, not an address-only guess.")
    ref=receipt["poi_ref"]
    response=await bridge.context(actor,ref,limit=1)
    assert response["poi_ref"]==ref
    assert response["source_scope"]=="actor_authorized"
    assert response["cartography_acceptance_claimed"] is False
    assert response["complete_extraction_claimed"] is False
    assert response["has_more"] is False # exactly one, not a phantom next page
    assert len(response["locations"])==1
    item=response["locations"][0]
    assert item["place_kind"]=="building"
    assert item["place_context"].startswith("Каменный собор")
    assert item["map_refs"]==[cart_ref]
    assert item["map_ref_verification"]=="not_verified"
    assert item["historical_geometry"]=="not_verified"
    assert item["evidence_refs"][0]["exact_quote"]==e["exact_quote"]
    assert item["relations"]==[{
        "kind":"located_in","neighbor_kind":"poi_ref",
        "neighbor_label":"Остров","neighbor_id":staged["entities"]["island"],
        "neighbor_poi_ref":None,"source_relation_state":"candidate",
        "source_time_scope":"Период, описанный в синтетическом источнике",
    }]
    # The evidence-bearing entity and the owner canonical POI did not fork.
    assert len(b.corpus.rows("rkb_entities"))==2
    assert (await g.read(actor,entity))["entity"]["external_ref"]==ref
    assert b.corpus.one("rkb_documents",doc)["active_revision"]==1
    # Another principal cannot read this private source even by guessing POI ID.
    another=Principal(subject=str(uuid4()),issuer="test",
                      client_id="test",access_token="test")
    restricted=await bridge.context(another,ref)
    assert restricted["locations"]==[] and restricted["has_more"] is False
    assert "Собор" not in str(restricted)
    await b.aclose()


@pytest.mark.asyncio
async def test_poi_context_more_flag_means_actual_extra_authorized_location(tmp_path):
    """At most limit locations are returned, with one extra ACL-scoped probe."""
    b,actor,doc,e=fixture(tmp_path)
    g=GraphService(b,NoOwnerLookups())
    saved=await g.stage(actor,doc,1,{"entities":[
        {"key":key,"kind":"poi_ref","canonical_label":label,
         "exact_source_spelling":"Собор","evidence":e}
        for key,label in (("physical","Собор"),("related-site","Собор: участок башни"))]})
    poi_id="poi_ss_3a81064258bae2c9b8c41f44"
    bridge=PoiRegistry(b,owner_url="http://owner.test",
                       owner_token="testing-owner-secret",client=Owner(poi_id))
    # Two distinct source-side records can temporarily refer to one reviewed
    # physical site pending later editorial deduplication.
    for eid in saved["entities"].values():
        await bridge.act(actor,"select",entity_id=eid,poi_id=poi_id,
                         model_reason="Same physical building site, explicitly verified by model.")
    ref="streetstory://poi/"+poi_id
    page=await bridge.context(actor,ref,limit=1)
    assert len(page["locations"])==1
    assert page["has_more"] is True
    full=await bridge.context(actor,ref,limit=2)
    assert len(full["locations"])==2
    assert full["has_more"] is False
    assert sorted(x["entity_id"] for x in full["locations"])==sorted(saved["entities"].values())
    await b.aclose()
