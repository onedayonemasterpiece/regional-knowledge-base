"""RKB geographic intent is on the same SQLite transaction as accepted mention.

These tests have NO external PostGIS or network dependency and never claim
historical polygon acceptance from a local source/POI identity candidate.
"""
import hashlib
import json
import sqlite3
import time
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.geo_resolution import GeoQueue,GeoError
from regional_knowledge.graph_service import GraphService
from regional_knowledge.poi_reference import StreetStoryPoiResolver
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore


def prepare(tmp_path):
    backend=SQLiteBackend(
        corpus_path=tmp_path/"regional.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore()
    )
    owner,doc,page,region,chunk=[str(uuid4()) for _ in range(5)]
    text=("В 1896 году рядом с Кёнигсбергским зоопарком существовал "
          "исторический зоосад на территории Хуфена.")
    digest=hashlib.sha256(text.encode()).hexdigest()
    backend.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":owner}])
    backend.corpus.put("rkb_documents",[{
        **defaults("rkb_documents"),"id":doc,"owner_user_id":owner,
        "title":"Private synthetic regional source",
        "source_sha256":"a"*64,"active_revision":1,"page_count":1
    }])
    backend.corpus.put("rkb_pages",[{
        **defaults("rkb_pages"),"id":page,"document_id":doc,
        "revision":1,"physical_page_index":0,
    }])
    backend.corpus.put("rkb_regions",[{
        **defaults("rkb_regions"),"id":region,"page_id":page,
        "kind":"body","reading_order":0,"source_text":text,"text_sha256":digest
    }])
    backend.corpus.put("rkb_chunks",[{
        **defaults("rkb_chunks"),"id":chunk,"document_id":doc,"revision":1,
        "source_text":text,"text_sha256":digest,"search_material":text,
        "search_material_sha256":digest,"page_ids":[page],
        "region_ids":[region],"title":"Private graph source"
    }])
    street=tmp_path/"street-owner.sqlite3"
    with sqlite3.connect(street) as db:
        db.executescript("""
          CREATE TABLE pois(id TEXT PRIMARY KEY,status TEXT,canonical_name TEXT,
                            latitude REAL,longitude REAL);
          CREATE TABLE poi_aliases(poi_id TEXT,namespace TEXT,value TEXT,
                                   normalized_value TEXT,created_at REAL DEFAULT 0);
        """)
    actor=Principal(subject=owner,client_id="synthetic-geo",issuer="unit-test",
                    access_token="test-only-non-production")
    resolver=StreetStoryPoiResolver(street)
    graph=GraphService(backend,resolver)
    queue=GeoQueue(backend.corpus,resolver)
    proof={"chunk_id":chunk,"page_id":page,"region_id":region,
           "exact_quote":text}
    bundle={"entities":[{
        "key":"historical-zoo-site","kind":"poi_ref",
        "canonical_label":"Историческое место зоопарка",
        "exact_source_spelling":"Кёнигсбергским зоопарком",
        "poi_locator":{"names":["Калининградский зоопарк",
                                  "Кёнигсбергский зоопарк"],
                       "external_ids":{"wikidata":"Q1193386"}},
        "evidence":proof,
    }]}
    return backend,actor,graph,queue,street,doc,bundle,text


@pytest.mark.asyncio
async def test_geo_intent_auto_atomic_queue_late_owner_binding_and_durable_replay(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    saved=await graph.stage(actor,doc,1,bundle)
    node=saved["entities"]["historical-zoo-site"]
    assert saved["unresolved_pois"]==1
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==1
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_attempts").fetchone()[0]==1
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_aliases").fetchone()[0]>=2
    request=geo.status(actor,entity_id=node)["items"][0]["request_id"]
    claim=geo.claim(actor,limit=1)["items"][0]
    assert claim["request_id"]==request
    assert claim["contract_version"]=="rkb.geo_claim.v1"
    assert claim["source_ref"]["entity_id"]==node
    assert text not in json.dumps(claim,ensure_ascii=False)
    assert claim["time"]["precision"]=="unknown"
    stage=geo.stage(actor,claim["attempt_id"],claim["lease_token"],
                    claim["lease_fence"],
                    {"status":"unresolved",
                     "reason":"No canonical owner POI in this version of Street Story."},
                    "geo-unit-stage-l1")
    assert stage["state"]=="staged"
    initial=geo.apply(actor,claim["attempt_id"],claim["lease_token"],
                      claim["lease_fence"],"geo-unit-apply-l1")
    assert initial["state"]=="unresolved"
    assert geo.apply(actor,claim["attempt_id"],claim["lease_token"],
                     claim["lease_fence"],"geo-unit-apply-l1")==initial

    # The actual Street Story *owner* gains a stable canonical candidate later.
    owner_ref="poi_ss_2d0ab75849ea3099ea17193a"
    with sqlite3.connect(street) as db:
        db.execute("INSERT INTO pois VALUES(?,?,?,?,?)",
                   (owner_ref,"candidate","Калининградский зоопарк",54.72044,20.48737))
        db.execute("INSERT INTO poi_aliases(poi_id,namespace,value,normalized_value) VALUES(?,?,?,?)",
                   (owner_ref,"wikidata","Q1193386","q1193386"))
    irrelevant=geo.recheck(actor,"Непохожая на зоопарк улица","owner_poi",
                           "streetstory://poi/"+owner_ref,"new-owner-revision")
    assert irrelevant["new_attempts"]==0
    changed=geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                        "streetstory://poi/"+owner_ref,"new-owner-revision")
    assert changed["new_attempts"]==1
    assert geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                       "streetstory://poi/"+owner_ref,"new-owner-revision")["new_attempts"]==0

    new=geo.claim(actor)["items"][0]
    assert new["attempt_id"]!=claim["attempt_id"]
    proposal={"status":"candidate",
              "canonical_poi_ref":"streetstory://poi/"+owner_ref,
              "reason":"Exact Wikidata ID in the Street Story owner aliases, no historical geometry."}
    staged=geo.stage(actor,new["attempt_id"],new["lease_token"],
                     new["lease_fence"],proposal,"geo-unit-stage-l2")
    assert geo.stage(actor,new["attempt_id"],new["lease_token"],
                     new["lease_fence"],proposal,"geo-unit-stage-l2")==staged
    with pytest.raises(GeoError,match="idempotency_conflict"):
        geo.stage(actor,new["attempt_id"],new["lease_token"],
                  new["lease_fence"],{"status":"unresolved",
                  "reason":"Different message reusing the same idempotency key."},"geo-unit-stage-l2")
    applied=geo.apply(actor,new["attempt_id"],new["lease_token"],
                      new["lease_fence"],"geo-unit-apply-l2")
    assert applied["state"]=="linked_candidate"
    assert applied["historical_geometry"]=="not_verified"
    assert applied["owner_identity_state"]=="candidate"
    assert applied["story_revision_unchanged"]
    node_after=await graph.read(actor,node)
    assert node_after["entity"]["external_ref"]=="streetstory://poi/"+owner_ref
    assert node_after["entity"]["external_identity_state"]=="candidate"
    assert len(b.corpus.rows("rkb_entities"))==1
    assert len(b.corpus.rows("rkb_entity_mentions"))==1
    assert geo.lookup(actor,"Кёнигсбергский зоопарк",year=1896)["items"][0][
        "temporal_match"]=="unknown"
    assert geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                       "streetstory://poi/"+owner_ref,"future-alias")["new_attempts"]==0
    with sqlite3.connect(street) as db:
        db.execute("DELETE FROM poi_aliases")
        db.execute("DELETE FROM pois")
    # Network response lost after commit: persisted receipt is sufficient,
    # even if the owner projection has since gone away.
    assert geo.apply(actor,new["attempt_id"],new["lease_token"],
                     new["lease_fence"],"geo-unit-apply-l2")==applied
    assert geo.status(actor,request_id=request)["items"][0]["attempts"][0]["state"]=="linked_candidate"


@pytest.mark.asyncio
async def test_geo_fence_expiry_and_source_revisions_guard_mutations(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    first=await graph.stage(actor,doc,1,bundle)
    original=geo.claim(actor,limit=1,lease_seconds=15)["items"][0]
    with b.corpus.connect() as db:
        db.execute("UPDATE rkb_geo_attempts SET lease_until=0 WHERE attempt_id=?",
                   (original["attempt_id"],))
    fresh=geo.claim(actor,limit=1)["items"][0]
    assert fresh["attempt_id"]==original["attempt_id"]
    assert fresh["lease_fence"]==original["lease_fence"]+1
    with pytest.raises(GeoError,match="stale_lease"):
        geo.stage(actor,original["attempt_id"],original["lease_token"],
                  original["lease_fence"],
                  {"status":"unresolved","reason":"Expired prior agent lease."},
                  "geo-stale-lease")
    stage=geo.stage(actor,fresh["attempt_id"],fresh["lease_token"],
                    fresh["lease_fence"],
                    {"status":"unresolved","reason":"No owner projection yet."},
                    "geo-new-lease")
    assert stage["state"]=="staged"
    before=len(b.corpus.rows("rkb_entities"))
    changed=b.corpus.one("rkb_documents",doc)
    b.corpus.put("rkb_documents",[{**changed,"active_revision":2}])
    result=geo.apply(actor,fresh["attempt_id"],fresh["lease_token"],
                     fresh["lease_fence"],"geo-after-revision")
    assert result["state"]=="stale_source"
    assert len(b.corpus.rows("rkb_entities"))==before
    assert geo.claim(actor,limit=1)["items"]==[]
    # Actor revocation blocks every read/write despite a prior lease.
    b.corpus.put("rkb_users",[{**b.corpus.one("rkb_users",actor.subject),
                              "status":"disabled"}])
    with pytest.raises(GeoError,match="not_found_or_not_accessible"):
        geo.status(actor)


@pytest.mark.asyncio
async def test_geo_existing_enqueue_duplicate_and_separate_acl(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    first=await graph.stage(actor,doc,1,bundle)
    eid=first["entities"]["historical-zoo-site"]
    saved=geo.enqueue_existing(actor,eid)
    assert saved["count"]>=1
    assert geo.enqueue_existing(actor,eid)["request_ids"]==saved["request_ids"]
    assert len(b.corpus.rows("rkb_entities"))==1
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==1
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_attempts").fetchone()[0]==1
    other=Principal(subject=str(uuid4()),client_id="other",issuer="unit-test",
                    access_token="test-only")
    b.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":other.subject}])
    assert geo.status(other)["items"]==[]
    assert geo.lookup(other,"Кёнигсбергский зоопарк")["items"]==[]
    with pytest.raises(GeoError,match="not_found_or_not_accessible"):
        geo.enqueue_existing(other,eid)
    assert geo.recheck(actor,"Кёнигсбергский зоопарк",
                       "cartography_layer","kneiphof-1928","L1")["new_attempts"]==1
    assert geo.recheck(actor,"Кёнигсбергский зоопарк",
                       "cartography_layer","kneiphof-1928","L1")["new_attempts"]==0
    assert geo.recheck(other,"Кёнигсбергский зоопарк",
                       "cartography_layer","kneiphof-1928","L2")["new_attempts"]==0
    request=geo.status(actor,entity_id=eid)["items"][0]
    assert len(request["attempts"])==2


@pytest.mark.asyncio
async def test_graph_transaction_rolls_back_intent_with_invalid_source_quote(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    bundle["entities"][0]["evidence"]["exact_quote"]="nonexistent synthetic quote"
    with pytest.raises((ValueError,LookupError)):
        await graph.stage(actor,doc,1,bundle)
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==0
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_attempts").fetchone()[0]==0


@pytest.mark.asyncio
async def test_automatic_owner_alias_watermark_and_backfill_on_old_source(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    initial=await graph.stage(actor,doc,1,bundle)
    node=initial["entities"]["historical-zoo-site"]
    first=geo.claim(actor,limit=1)["items"][0]
    geo.stage(actor,first["attempt_id"],first["lease_token"],first["lease_fence"],
              {"status":"unresolved",
               "reason":"Street Story has no candidate yet; final under this dependency."},
              "owner-delta-stage-l1")
    geo.apply(actor,first["attempt_id"],first["lease_token"],
              first["lease_fence"],"owner-delta-apply-l1")
    with sqlite3.connect(street) as owner:
        owner.execute("INSERT INTO pois VALUES(?,?,?,?,?)",
                      ("poi_ss_2d0ab75849ea3099ea17193a","candidate",
                       "Калининградский зоопарк",54.7204,20.4874))
        owner.execute("INSERT INTO poi_aliases VALUES(?,?,?,?,?)",
                      ("poi_ss_2d0ab75849ea3099ea17193a","wikidata",
                       "Q1193386","q1193386",time.time()))
    changed=geo.poll_owner_updates(limit=8)
    assert changed["seen"]==1 and changed["scheduled"]==1
    assert geo.poll_owner_updates(limit=8)["scheduled"]==0
    handled=geo.worker_tick(limit=1)
    assert handled["processed"]==1 and handled["states"]==["linked_candidate"]
    fetched=await graph.read(actor,node)
    assert fetched["entity"]["external_ref"]=="streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a"
    assert len(b.corpus.rows("rkb_entities"))==1
    assert geo.recheck(actor,"Далёкий адрес","owner_poi",
                       "another-owner","revision-L3")["new_attempts"]==0

    # Simulate upgrading an older installation which had accepted graph rows
    # before this queue existed: bounded source-safe backfill, no source import.
    with b.corpus.connect() as db:
        db.execute("DELETE FROM rkb_geo_receipts")
        db.execute("DELETE FROM rkb_geo_attempts")
        db.execute("DELETE FROM rkb_geo_aliases")
        db.execute("DELETE FROM rkb_geo_intents")
        db.execute("DELETE FROM rkb_geo_watermarks")
    restored=geo.backfill_batch(5)
    assert restored["created"]==1 and restored["completed"]
    assert geo.backfill_batch(5)["created"]==0
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==1
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_attempts").fetchone()[0]==1



@pytest.mark.asyncio
async def test_transient_owner_failure_is_bounded_then_relevant_revision_recovers(tmp_path):
    """Five owner outages never produce a false 'no POI' resolution."""
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    saved=await graph.stage(actor,doc,1,bundle)
    geo.resolver.path=str(tmp_path/"missing-owner-database.sqlite")
    for n in range(5):
        batch=geo.worker_tick(1)
        assert batch["processed"]==0
        with b.corpus.connect() as db:
            status,reason,attempts,available=db.execute(
                "SELECT state,reason,attempts,available_at FROM rkb_geo_attempts"
            ).fetchone()
            assert reason=="owner_unavailable"
            assert attempts==n+1
            assert status==("dependency_unavailable" if n==4 else "retry_wait")
            if n<4:
                assert available>time.time()
                assert geo.claim(actor)["items"]==[]
                db.execute("UPDATE rkb_geo_attempts SET available_at=0")
    assert geo.status(actor)["state_counts"]["dependency_unavailable"]==1
    with sqlite3.connect(street) as owner:
        pid="poi_ss_2d0ab75849ea3099ea17193a"
        owner.execute("INSERT INTO pois VALUES(?,?,?,?,?)",
                      (pid,"candidate","Калининградский зоопарк",54.7204,20.4874))
        owner.execute("""INSERT INTO poi_aliases(poi_id,namespace,value,normalized_value,created_at)
            VALUES(?,?,?,?,?)""",(pid,"wikidata","Q1193386","q1193386",time.time()))
    geo.resolver.path=str(street)
    assert geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                       "streetstory://poi/"+pid,"owner-new-generation")["new_attempts"]==1
    resolved=geo.worker_tick(1)
    assert resolved["states"]==["linked_candidate"]
    fetched=await graph.read(actor,saved["entities"]["historical-zoo-site"])
    assert fetched["entity"]["external_ref"]=="streetstory://poi/"+pid
    assert geo.status(actor)["state_counts"]["dependency_unavailable"]==1


@pytest.mark.asyncio
async def test_claim_fairness_across_books_beyond_old_25_result_cap(tmp_path):
    """Twenty-six claims from one source cannot hide a second book."""
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    for n in range(26):
        another=json.loads(json.dumps(bundle))
        another["entities"][0]["key"]="big-book-place-"+str(n)
        await graph.stage(actor,doc,1,another)

    old_page,old_region,old_chunk=(
        bundle["entities"][0]["evidence"][key] for key in (
            "page_id","region_id","chunk_id")
    )
    doc2,page2,region2,chunk2=[str(uuid4()) for _ in range(4)]
    source=b.corpus.one("rkb_documents",doc)
    b.corpus.put("rkb_documents",[{**source,"id":doc2,
                                   "title":"Second synthetic source"}])
    b.corpus.put("rkb_pages",[{**b.corpus.one("rkb_pages",old_page),
                              "id":page2,"document_id":doc2}])
    b.corpus.put("rkb_regions",[{**b.corpus.one("rkb_regions",old_region),
                                "id":region2,"page_id":page2}])
    b.corpus.put("rkb_chunks",[{**b.corpus.one("rkb_chunks",old_chunk),
                               "id":chunk2,"document_id":doc2,
                               "page_ids":[page2],"region_ids":[region2]}])
    small=json.loads(json.dumps(bundle))
    small["entities"][0]["key"]="small-book-place"
    small["entities"][0]["evidence"].update(
        chunk_id=chunk2,page_id=page2,region_id=region2
    )
    await graph.stage(actor,doc2,1,small)
    claimed=geo.claim(actor,limit=2)["items"]
    assert len(claimed)==2
    assert set(x["source_ref"]["document_id"] for x in claimed)=={doc,doc2}
    assert len({x["attempt_id"] for x in claimed})==2


@pytest.mark.asyncio
async def test_unaccepted_cartography_cannot_assert_same_site_or_footprint(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    await graph.stage(actor,doc,1,bundle)
    claim=geo.claim(actor,limit=1)["items"][0]
    with pytest.raises(GeoError,match="spatial_relation_requires_owner_decision"):
        geo.stage(actor,claim["attempt_id"],claim["lease_token"],
                  claim["lease_fence"],
                  {"status":"candidate",
                   "canonical_poi_ref":"streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a",
                   "relation":"same_site",
                   "reason":"Same-name modern POI alone cannot prove historical same_site."},
                  "geo-unsupported-samesite")
    with pytest.raises(GeoError,match="cartography_layer_not_accepted"):
        geo.stage(actor,claim["attempt_id"],claim["lease_token"],
                  claim["lease_fence"],
                  {"status":"candidate",
                   "canonical_poi_ref":"streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a",
                   "layer_ref":"fake-kneiphof-layer-revision",
                   "reason":"An unaccepted map must not be treated as owner evidence."},
                  "geo-fake-layer")
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_receipts").fetchone()[0]==0



@pytest.mark.asyncio
async def test_geo_request_finds_original_among_80_vector_only_mentions_and_pages(tmp_path):
    """Real zoo-like case: 81 mentions, only 1 original spelling and proof."""
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    accepted=await graph.stage(actor,doc,1,bundle)
    entity=accepted["entities"]["historical-zoo-site"]
    original=b.corpus.rows("rkb_entity_mentions")[0]
    vector_only=[]
    from uuid import UUID
    for i in range(80):
        vector_only.append({**original,
            "id":str(UUID(f"00000000-0000-4000-8000-{i:012x}")),
            "exact_source_spelling":"","state":"candidate",
            "signals":{"retrieval_mode":"vector_only","identity_unresolved":True}})
    b.corpus.put("rkb_entity_mentions",vector_only)
    assert len(b.corpus.rows("rkb_entity_mentions"))==81
    result=geo.enqueue_existing(actor,entity)
    assert result["count"]==1 and result["scanned"]==1
    assert result["next_cursor"] is None
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==1

    # More original mentions than one bounded packet, with stable cursor.
    sourced=[]
    for i in range(24):
        sourced.append({**original,
            "id":str(UUID(f"11111111-1111-4111-8111-{i:012x}")),
            "exact_source_spelling":"Кёнигсбергским зоопарком"})
    b.corpus.put("rkb_entity_mentions",sourced)
    seen=set()
    cursor=None
    for index in range(5):
        page=geo.enqueue_existing(actor,entity,cursor=cursor,limit=7)
        assert page["scanned"]<=7
        seen.update(page["request_ids"])
        if page["next_cursor"] is None:
            assert page["complete"] is True
            break
        assert page["complete"] is False
        cursor=page["next_cursor"]
    assert len(seen)==25
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==25
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_attempts").fetchone()[0]==25
    # Replay one cursor page: stable intents and no new attempts.
    geo.enqueue_existing(actor,entity,cursor=cursor,limit=7)
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==25
    with pytest.raises(GeoError,match="invalid_source_cursor_or_entity"):
        geo.enqueue_existing(actor,entity,cursor="not a UUID")


@pytest.mark.asyncio
async def test_geo_request_can_backfill_owned_cross_book_mentions_of_same_node(tmp_path):
    """A later authorized book may cite an existing owner graph identity."""
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    first=await graph.stage(actor,doc,1,bundle)
    entity_id=first["entities"]["historical-zoo-site"]
    oldpage,oldregion,oldchunk=[bundle["entities"][0]["evidence"][key]
                                for key in ("page_id","region_id","chunk_id")]
    doc2,page2,region2,chunk2=[str(uuid4()) for _ in range(4)]
    b.corpus.put("rkb_documents",[{**b.corpus.one("rkb_documents",doc),
                                    "id":doc2,"title":"Second private source"}])
    b.corpus.put("rkb_pages",[{**b.corpus.one("rkb_pages",oldpage),
                               "id":page2,"document_id":doc2}])
    b.corpus.put("rkb_regions",[{**b.corpus.one("rkb_regions",oldregion),
                                 "id":region2,"page_id":page2}])
    b.corpus.put("rkb_chunks",[{**b.corpus.one("rkb_chunks",oldchunk),
                                "id":chunk2,"document_id":doc2,
                                "page_ids":[page2],"region_ids":[region2]}])
    second=json.loads(json.dumps(bundle))
    second["entities"][0]["key"]="same-place-later-source"
    second["entities"][0]["entity_id"]=entity_id
    second["entities"][0]["evidence"].update(
        chunk_id=chunk2,page_id=page2,region_id=region2)
    result=await graph.stage(actor,doc2,1,second)
    assert result["entities"]["same-place-later-source"]==entity_id
    assert len(b.corpus.rows("rkb_entities"))==1
    geo_existing=geo.enqueue_existing(actor,entity_id,limit=20)
    assert geo_existing["count"]==2
    assert geo_existing["complete"]
    with b.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_geo_intents").fetchone()[0]==2
    assert len(b.corpus.rows("rkb_entity_mentions"))==2



@pytest.mark.asyncio
async def test_geo_worker_autoreclaims_expired_staged_lease_after_restart(tmp_path):
    """The service, not a human geo_claim, resumes a killed external worker."""
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    saved=await graph.stage(actor,doc,1,bundle)
    eid=saved["entities"]["historical-zoo-site"]
    old=geo.claim(actor,limit=1,lease_seconds=15)["items"][0]
    geo.stage(actor,old["attempt_id"],old["lease_token"],old["lease_fence"],
              {"status":"unresolved",
               "reason":"Before restart the owner POI had not yet been found."},
              "crash-stage-first")
    with sqlite3.connect(street) as owner:
        pid="poi_ss_2d0ab75849ea3099ea17193a"
        owner.execute("INSERT INTO pois VALUES(?,?,?,?,?)",
                      (pid,"candidate","Калининградский зоопарк",54.7204,20.4874))
        owner.execute("""INSERT INTO poi_aliases(poi_id,namespace,value,normalized_value,created_at)
            VALUES(?,?,?,?,?)""",(pid,"wikidata","Q1193386","q1193386",time.time()))
    # Simulate abrupt worker death between stage and apply. The old
    # proposal must never survive a new lease.
    with b.corpus.connect() as db:
        db.execute("UPDATE rkb_geo_attempts SET lease_until=0 WHERE attempt_id=?",
                   (old["attempt_id"],))
    from regional_knowledge.geo_resolution import GeoQueue
    restarted=GeoQueue(b.corpus,geo.resolver)
    outcome=restarted.worker_tick(limit=1)
    assert outcome["recovered_expired_leases"]==1
    assert outcome["processed"]==1
    assert outcome["states"]==["linked_candidate"]
    with b.corpus.connect() as db:
        row=db.execute("""SELECT state,attempts,lease_fence,lease_token,
                       proposal_json,canonical_poi_ref
                       FROM rkb_geo_attempts WHERE attempt_id=?""",
                       (old["attempt_id"],)).fetchone()
    assert row["state"]=="linked_candidate" and row["attempts"]==2
    assert row["lease_fence"]==old["lease_fence"]+1
    assert row["lease_token"] is None
    assert row["canonical_poi_ref"]=="streetstory://poi/"+pid
    assert json.loads(row["proposal_json"])["status"]=="candidate"
    assert (await graph.read(actor,eid))["entity"]["external_ref"]=="streetstory://poi/"+pid
    assert len(b.corpus.rows("rkb_entities"))==1
    assert len(b.corpus.rows("rkb_entity_mentions"))==1
    with pytest.raises(GeoError,match="stale_lease"):
        restarted.stage(actor,old["attempt_id"],old["lease_token"],old["lease_fence"],
                        {"status":"unresolved","reason":"Stale worker attempted an overwrite."},
                        "old-worker-after-restart")


@pytest.mark.asyncio
async def test_geo_worker_exhausted_crash_is_terminal_and_revision_reopens(tmp_path):
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    saved=await graph.stage(actor,doc,1,bundle)
    previous=geo.claim(actor,limit=1)["items"][0]
    with b.corpus.connect() as db:
        db.execute("""UPDATE rkb_geo_attempts SET attempts=5,
                   state='leased',lease_until=0 WHERE attempt_id=?""",
                   (previous["attempt_id"],))
    recovered=geo.worker_tick(limit=1)
    assert recovered["recovered_retry_exhausted"]==1
    assert recovered["processed"]==0
    assert geo.worker_tick(limit=1)["recovered_retry_exhausted"]==0
    with b.corpus.connect() as db:
        row=db.execute("SELECT state,reason,lease_token FROM rkb_geo_attempts").fetchone()
    assert row["state"]=="retry_exhausted"
    assert row["reason"]=="lease_attempts_exhausted"
    assert row["lease_token"] is None
    assert geo.claim(actor,limit=1)["items"]==[]
    new=geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                    "streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a",
                    "legitimate-owner-alias-rev-2")
    assert new["new_attempts"]==1
    assert geo.recheck(actor,"Кёнигсбергский зоопарк","owner_poi",
                       "streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a",
                       "legitimate-owner-alias-rev-2")["new_attempts"]==0
    fresh=geo.claim(actor,limit=1)["items"][0]
    assert fresh["attempt_id"]!=previous["attempt_id"]


@pytest.mark.asyncio
async def test_geo_concurrent_reapers_are_idempotent_and_disabled_users_not_polled(tmp_path):
    import asyncio
    from regional_knowledge.geo_resolution import GeoQueue
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    await graph.stage(actor,doc,1,bundle)
    c=geo.claim(actor,limit=1)["items"][0]
    with b.corpus.connect() as db:
        db.execute("UPDATE rkb_geo_attempts SET lease_until=0 WHERE attempt_id=?",
                   (c["attempt_id"],))
    pair=await asyncio.gather(
        asyncio.to_thread(GeoQueue(b.corpus,geo.resolver).recover_expired_leases,2),
        asyncio.to_thread(GeoQueue(b.corpus,geo.resolver).recover_expired_leases,2))
    assert sum(x["requeued"] for x in pair)==1
    assert sum(x["retry_exhausted"] for x in pair)==0
    with b.corpus.connect() as db:
        row=db.execute("SELECT state,lease_token,attempts FROM rkb_geo_attempts").fetchone()
    assert row["state"]=="pending" and row["lease_token"] is None
    assert row["attempts"]==1
    b.corpus.put("rkb_users",[{**b.corpus.one("rkb_users",actor.subject),
                              "status":"disabled"}])
    result=geo.worker_tick(limit=1)
    assert result["processed"]==0
    with b.corpus.connect() as db:
        row=db.execute("SELECT state,attempts FROM rkb_geo_attempts").fetchone()
    assert row["state"]=="pending" and row["attempts"]==1



@pytest.mark.asyncio
async def test_staged_book_geo_intent_is_dormant_until_actual_vector_activation(tmp_path):
    """Full normal SQLite book activation, not a mocked status flip.

    A source-backed POI graph mention is staged while BGE publication is
    incomplete. The geo daemon must NOT mark that future accepted revision
    stale, nor read it into Street Story before finalization. The normal
    rkb_activate_revision transaction wakes exactly its original intent.
    """
    from regional_knowledge.e5_contract import SPACE as ES
    from regional_knowledge.bge_contract import SPACE as BS,REVISION
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    old_page=bundle["entities"][0]["evidence"]["page_id"]
    old_region=bundle["entities"][0]["evidence"]["region_id"]
    original=bundle["entities"][0]["evidence"]["chunk_id"]
    page2,region2,chunk2,job=[str(uuid4()) for _ in range(4)]
    head={"x-rkb-actor":actor.subject}
    source=b.corpus.one("rkb_documents",doc)
    assert source["active_revision"]==1
    initial=await b.local_rpc("rkb_start_ingestion",{
        "p_ingestion_id":job,"p_document_id":doc,
        "p_title":source["title"],"p_authors":[],
        "p_source_sha256":source["source_sha256"],
        "p_source_file_id":"staged-book-revision-2",
        "p_page_count":1,"p_duplicate_policy":"new_revision"
    },head)
    assert initial.json()[0]["document_id"]==doc
    await b.client.patch(b.config.url+"/rest/v1/rkb_ingestion_jobs",
                         headers=head,params={"id":"eq."+job},json={"state":"ready"})
    await b.client.post(b.config.url+"/rest/v1/rkb_pages",headers=head,json=[{
        "id":page2,"document_id":doc,"revision":2,"physical_page_index":0}])
    sha=hashlib.sha256(text.encode()).hexdigest()
    await b.client.post(b.config.url+"/rest/v1/rkb_regions",headers=head,json=[{
        "id":region2,"page_id":page2,"kind":"body","reading_order":0,
        "source_text":text,"text_sha256":sha}])
    await b.local_rpc("rkb_insert_chunks",{
        "p_document_id":doc,"p_ingestion_id":job,"p_revision":2,
        "p_chunks":[{"id":chunk2,"source_text":text,"search_material":text,
                     "text_sha256":sha,"search_material_sha256":sha,
                     "region_ids":[region2],"footnote_region_ids":[],
                     "page_ids":[page2],"illustration_ids":[],
                     "title":"Exact place evidence",
                     "text_start":0,"text_end":len(text)}]
    },head)
    staged=json.loads(json.dumps(bundle))
    staged["entities"][0]["key"]="future-book-zoo"
    staged["entities"][0]["evidence"].update(
        chunk_id=chunk2,page_id=page2,region_id=region2
    )
    accepted=await graph.stage(actor,doc,2,staged,staged_texts={chunk2:text})
    node=accepted["entities"]["future-book-zoo"]
    with b.corpus.connect() as db:
        rows=db.execute("""SELECT a.state,a.attempts FROM rkb_geo_attempts a
            JOIN rkb_geo_intents i ON i.request_id=a.request_id
            WHERE i.document_id=? AND i.source_revision=2""",
            (doc,)).fetchall()
        assert len(rows)==1
        assert rows[0]["state"]=="awaiting_activation"
        assert rows[0]["attempts"]==0
    before=geo.status(actor,entity_id=node)["items"][0]
    assert before["source_access"]=="staged_not_accepted"
    assert before["attempts"][0]["state"]=="awaiting_activation"
    # A pending source is not an independently available place assertion.
    assert geo.lookup(actor,"Кёнигсбергский зоопарк")["items"]==[]
    assert geo.claim(actor,limit=5)["items"]==[]
    assert geo.worker_tick(limit=1)["processed"]==0

    activation={"p_document_id":doc,"p_ingestion_id":job,"p_revision":2}
    pending=(await b.local_rpc("rkb_activate_revision",activation,head)).json()[0]
    assert pending["pending_vectors"]
    assert b.corpus.one("rkb_documents",doc)["active_revision"]==1
    assert geo.status(actor,entity_id=node)["items"][0]["attempts"][0][
        "state"]=="awaiting_activation"
    for table,space in (("rkb_chunk_embeddings_e5",ES),
                        ("rkb_chunk_embeddings_bge",BS)):
        b.corpus.put(table,[{**defaults(table),
            "chunk_id":chunk2,"embedding_space":space,"revision":2,
            "text_sha256":sha,"search_material_sha256":sha,
            "model_revision":REVISION}])
    await b.activate_pending()
    assert b.corpus.one("rkb_documents",doc)["active_revision"]==2
    with b.corpus.connect() as db:
        row=db.execute("""SELECT a.state,a.attempts FROM rkb_geo_attempts a
          JOIN rkb_geo_intents i ON i.request_id=a.request_id
          WHERE i.document_id=? AND i.source_revision=2""",(doc,)).fetchone()
        assert row["state"]=="pending" and row["attempts"]==0
    found=geo.status(actor,entity_id=node)["items"][0]
    assert found["source_access"]=="authorized_current"
    assert found["attempts"][0]["state"]=="pending"
    assert geo.lookup(actor,"Кёнигсбергский зоопарк")["items"][0]["entity_id"]==node
    # The first resolver call may only run AFTER the source was accepted.
    completed=geo.worker_tick(1)
    assert completed["processed"]==1 and completed["states"]==["unresolved"]
    assert len(b.corpus.rows("rkb_entities"))==1
    await b.aclose()


@pytest.mark.asyncio
async def test_geo_activation_supersedes_old_inflight_leases_without_moving_proofs(tmp_path):
    from regional_knowledge.geo_resolution import geo_revision_activated
    b,actor,graph,geo,street,doc,bundle,text=prepare(tmp_path)
    accepted=await graph.stage(actor,doc,1,bundle)
    lease=geo.claim(actor,limit=1)["items"][0]
    eid=accepted["entities"]["historical-zoo-site"]
    with b.corpus.connect() as db:
        # Simulated normal service-owned activation transaction on an
        # already proof-backed source; no old story/graph node is erased.
        db.execute("BEGIN IMMEDIATE")
        old=b.corpus.one("rkb_documents",doc)
        b.corpus.put("rkb_documents",[{**old,"active_revision":2}],
                     connection=db)
        first=geo_revision_activated(db,doc,2)
        second=geo_revision_activated(db,doc,2)
        assert first["superseded_attempts"]==1
        assert second["superseded_attempts"]==0
    with b.corpus.connect() as db:
        state=db.execute("SELECT state,reason,lease_token FROM rkb_geo_attempts").fetchone()
    assert state["state"]=="stale_source"
    assert state["reason"]=="superseded_source_revision"
    assert state["lease_token"] is None
    with pytest.raises(GeoError,match="stale_lease"):
        geo.stage(actor,lease["attempt_id"],lease["lease_token"],
                  lease["lease_fence"],
                  {"status":"unresolved","reason":"Old revision can no longer apply."},
                  "geo-old-revision-stage")
    assert len(b.corpus.rows("rkb_entity_mentions"))==1
    assert len(b.corpus.rows("rkb_entities"))==1
