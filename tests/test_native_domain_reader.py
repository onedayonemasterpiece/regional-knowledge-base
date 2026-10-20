"""Owned synthetic PG fixture: production writer/reader grants, no BGE inference."""
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import tuple_row
import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_domain_bindings import (current_handler,resolved_current_import_v4_command,
    resolved_current_case_command,DOMAIN_REVISIONS)
from regional_knowledge.kaggle_owned_pg import configure_native_roles,DOMAIN_SQL_ARTIFACTS
from regional_knowledge.kaggle_database_probe import native_probe_for_schema
from regional_knowledge.kaggle_source_acl import inspect_acl
from regional_knowledge.kaggle_source_read import SourceReadRequest,execute_source_read
from regional_knowledge.kaggle_editorial_contracts import EditorialReadRequest
from regional_knowledge.kaggle_editorial_read import execute_editorial_read
from regional_knowledge.kaggle_case_contracts import CaseReadRequest
from regional_knowledge.kaggle_case_read import execute_case_read
from regional_knowledge.kaggle_geo_contracts import GeoReadRequest
from regional_knowledge.kaggle_geo_read import execute_geo_read
from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest,execute_source_graph_read
from regional_knowledge.source_graph import resolve_source_graph
from regional_knowledge.kaggle_protocol import Query,PassageReadRequest,PassageContinuation
from regional_knowledge.kaggle_query import execute_query,execute_passage_read,PinnedTokenizer
from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
from test_geo_manifest_native import native,commands
from test_geo_migration import uid
from test_kaggle_apply import pinned
from test_source_story_native import apply
from test_source_graph import payload as graph_payload
from test_source_import import SOURCE
from test_source_import_v2 import STORY
from test_disagreement_native import ACTOR,CASE,create,attribution

pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),reason='explicit owned native fixture only')


@pytest.mark.asyncio
async def test_current11_production_roles_source_case_editorial_geo_and_hybrid_reads(native):
    admin=native['connection'];options=admin.info.get_parameters();handlers=[]
    for route in ('source.import.v4','disagreement.edit.v1','geo.manifest.install.v1'):
        prototype=current_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
        check=await pinned(admin,prototype.check.name);effect=await pinned(admin,prototype.apply.name)
        handlers.append(current_handler(route,check_sha256=check.definition_sha256,apply_sha256=effect.definition_sha256))
    native['registry']=HandlerRegistry(handlers)
    async with admin.transaction():
        await configure_native_roles(admin,selected_sql=DOMAIN_SQL_ARTIFACTS,dbname='rkb')
    writer=await psycopg.AsyncConnection.connect(**dict(options,user='rkb_writer'),row_factory=tuple_row)
    reader=await psycopg.AsyncConnection.connect(**dict(options,user='rkb_reader'),row_factory=tuple_row)
    @asynccontextmanager
    async def acquire():yield reader
    try:
        native['connection']=writer;payload,graph=graph_payload()
        payload['source_graph']=resolve_source_graph(graph,source_id=SOURCE,person_id=ACTOR,semantic=payload['semantic'])
        source=resolved_current_import_v4_command(sequence=1,operation_key='native-role-source',request_id='native-role-1',
            person_id=ACTOR,logical_time='2026-10-20T19:00:00Z',source_id=SOURCE,payload=payload)
        assert (await apply(native,source)).status=='applied'
        case=resolved_current_case_command(sequence=2,operation_key='native-role-case',request_id='native-role-2',
            person_id=ACTOR,logical_time='2026-10-20T19:00:00Z',case_id=CASE,object_id=CASE,
            action='create',command=create(),source_dependencies=[],attribution=attribution())
        assert (await apply(native,case)).status=='applied'
        manifest,_=commands()
        from regional_knowledge.geo_manifest_commands import plan_geo_manifest
        geo=plan_geo_manifest(manifest).commands(first_sequence=3,operation_key='native-role-geo',request_id='native-role-3',
            person_id=ACTOR,logical_time='2026-10-20T19:00:00Z')
        for command in geo:assert (await apply(native,command)).status=='applied'
        sequence=geo[-1].sequence
        identity=dict(request_id='native-reader',connection_generation=1,turn_generation=0,min_data_seq=sequence,
            deadline_ms=2000,actor=dict(person_id=ACTOR,audience='synthetic-personal',auth_epoch=1,visibility_epoch=0))
        catalogue=await execute_source_read(SourceReadRequest(query='Exact title',**identity),acquire=acquire)
        assert catalogue.status=='ok' and catalogue.records[0]['title']==payload['source_manifest']['metadata']['title']
        editorial=await execute_editorial_read(EditorialReadRequest(story_id=STORY,**identity),acquire=acquire)
        assert editorial.status=='ok' and editorial.record['body']==payload['semantic']['stories'][0]['default_variant']['body']
        case_read=await execute_case_read(CaseReadRequest(case_id=CASE,**identity),acquire=acquire)
        assert case_read.status=='ok',case_read.reason
        geo_read=await execute_geo_read(GeoReadRequest(action='identity',entity_id=uid(10),**identity),acquire=acquire)
        assert geo_read.status=='ok' and len(geo_read.records)==1
        graph_read=await execute_source_graph_read(SourceGraphReadRequest(action='list',source_id=SOURCE,**identity),acquire=acquire)
        assert graph_read.status=='ok' and len(graph_read.records)==3 and graph_read.next_cursor
        async with admin.transaction():
            assert await(await admin.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',([SOURCE],))).fetchone()==(True,)
        async def count(text):return 8
        async def synthetic_vector(text):return [1.]+[0.]*1023
        found=await execute_query(Query(query='fictional council gate',**identity),acquire=acquire,
            tokenizer=PinnedTokenizer(count,REVISION,TOKENIZER_SHA256,SPACE),encode=synthetic_vector)
        assert found.status=='degraded' and found.reason=='bounded_retrieval'
        assert found.mode=='hybrid' and len(found.evidence)==1
        item=found.evidence[0]
        locator=PassageContinuation(database_generation=found.observed_database_generation,
            visibility_epoch=found.observed_visibility_epoch,source=item.dependencies.source_revisions[0],
            passage_id=item.id,text_offset=0,bibliography_offset=0)
        fetched=await execute_passage_read(PassageReadRequest(locator=locator,**identity),acquire=acquire,source_provenance=True)
        assert fetched.status=='ok' and fetched.evidence[0].metadata.source_provenance['pages'][0]['physical_page_index']==1
        # Public execution cannot read the durable private original receipt.
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            async with reader.transaction():await reader.execute('SELECT original_receipt FROM rkb_v4.source_manifests')
        raw,fields=await native_probe_for_schema(DOMAIN_REVISIONS.schema)(acquire=acquire)
        async with admin.transaction():acl=await inspect_acl(admin)
        root=Path(os.environ['RKB_STORY_FIXTURE_SOCKET']).parents[1]
        (root/'native-hardened-catalog.json').write_bytes(raw);(root/'native-hardened-acl.json').write_bytes(acl)
        receipt=dict(scope='synthetic_native_roles_and_reads_not_live_acceptance',schema=DOMAIN_REVISIONS.schema,
            catalog_records=len(json.loads(raw)['records']),catalog_bytes=len(raw),catalog_sha256=hashlib.sha256(raw).hexdigest(),
            acl_bytes=len(acl),acl_sha256=hashlib.sha256(acl).hexdigest(),fields=fields,model_used=False,real_corpus_used=False)
        (root/'native-hardened-reader-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    finally:
        native['connection']=admin
        await writer.close();await reader.close()
