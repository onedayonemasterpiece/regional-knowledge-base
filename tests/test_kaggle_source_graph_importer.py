"""Owner stage→same DOCUMENT queue→exact sealed current11 graph import."""
import asyncio
import hashlib
import json
from uuid import UUID

import pytest

from regional_knowledge.stage_graph import StagedGraph
from regional_knowledge.source_graph import validate_graph_import_payload
from test_kaggle_source_importer import intake,staged,options,finish_document,TEXT
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


@pytest.mark.asyncio
@pytest.mark.parametrize('intake',[11],indirect=True)
async def test_actual_intake_preserves_graph_entities_links_and_poi_hypotheses_without_extra_model_jobs(intake):
    s=intake;begun=await staged(s)
    row=s.importer._row(s.principal.subject,begun.ingestion_id)
    graph=StagedGraph.model_validate_json(row['stage_json']);region=graph.pages[0].regions[0];chunk=graph.chunks[0]
    proof=dict(chunk_id=str(chunk.chunk_id),page_id=str(region.page_id),region_id=str(region.region_id),exact_quote=TEXT)
    bundle=dict(entities=[dict(key=key,kind=kind,canonical_label='Exact synthetic source identity',
        exact_source_spelling='Exact',evidence=proof,aliases=[dict(value='synthetic source alias',evidence=proof)])
        for key,kind in [('person','person'),('organization','organization')]],
        relations=[dict(source_key='person',target_key='organization',kind='affiliated_with',evidence=[proof])])
    candidate=dict(candidate_key='independent-finding',title='Source attribution',summary='Independent source-backed fact.',proposition=TEXT,narrative_outcome='facts_only',
        entity_keys=['person','organization'],evidence_refs=[dict(page_id=str(region.page_id),region_key=region.region_key,original_excerpt=TEXT)])
    fact=dict(candidate_key='source-location-hypothesis',poi_locator=dict(names=['Exact source place']),kind='location',text=TEXT,
        evidence_refs=[dict(page_id=str(region.page_id),region_key=region.region_key)])
    await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=begun.ingestion_id,
        payload=dict(entity_candidates=bundle,story_candidates=[candidate],poi_facts=[fact]),
        source_import=options(research=True,ingestion_id=begun.ingestion_id))
    await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=begun.ingestion_id)
    pending=await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=begun.ingestion_id)
    assert pending.state=='processing'
    for _ in range(10):await finish_document(s)
    await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=begun.ingestion_id)
    entry=await s.spool.get('source-import:'+begun.ingestion_id)
    assert entry.accepted is not None and entry.outcome_state!='DURABLE' # no fabricated Saved/native apply
    command=entry.command;value=validate_graph_import_payload(command.payload,source_id=begun.document_id,person_id=s.principal.subject)
    resolved=value['source_graph']
    assert {e['kind'] for e in resolved['entities']}=={'person','organization'}
    assert len({e['entity_id'] for e in resolved['entities']})==2 # no same-spelling merge
    assert resolved['relations'][0]['kind']=='affiliated_with'
    assert value['semantic']['findings'][0]['entity_keys']==['person','organization']
    assert len(resolved['finding_entities'][0]['entity_ids'])==2 and value['semantic']['stories']==[]
    assert resolved['poi_facts'][0]['geometry_acceptance']=='source_hypothesis_not_owner_geometry'
    assert await s.spool.journal.verify_accepted(entry.accepted,command)==hashlib.sha256(command.encode()).hexdigest()
    with s.t.queue.connect(write=False) as db:
        jobs=list(db.execute('SELECT kind FROM jobs'))
        assert sum(job[0]=='document' for job in jobs)==10 and sum(job[0]=='source_command' for job in jobs)==1
    original=command.encode()
    await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=begun.ingestion_id)
    assert (await s.spool.get(command.operation_key)).command.encode()==original
    assert len(s.originals.calls)==1
