"""Contract and SQL guards for immutable stage bytes + accepted title corrections."""
from pathlib import Path
from uuid import uuid4

import pytest

from regional_knowledge.source_catalog_command import (
    CODEC, ROUTE, catalog_handler, resolved_catalog_command, validate_catalog_command
)
from regional_knowledge.source_activation import ACTIVATION_REVISIONS
from regional_knowledge.source_stage import canonical


def _command(update):
    owner, source, ingestion = (str(uuid4()) for _ in range(3))
    payload = dict(codec=CODEC, ingestion_id=ingestion, owner_person_id=owner,
                   catalog_update=update)
    return resolved_catalog_command(sequence=7, operation_key='catalog:'+ingestion,
        request_id=str(uuid4()), person_id=owner,
        logical_time='2026-10-23T21:00:00+00:00', source_id=source,payload=payload)


def test_catalog_title_command_retains_explicit_null_and_current_source_identity():
    update={'expected_revision':1,'title':'Title corrected from page image',
            'authors':['First','Second'],'publication_year':None,
            'catalog':{'publisher':'Historical publisher','cover_page':0,'cover_kind':'title_page'}}
    command=_command(update)
    assert command.route==ROUTE
    assert command.expected_revisions=={'source_revision':'1'}
    assert validate_catalog_command(command)['catalog_update']==update
    assert canonical(dict(codec=CODEC,ingestion_id=__import__('json').loads(command.payload)['ingestion_id'],
                          owner_person_id=command.person_id,catalog_update=update))==command.payload
    assert command.schema_revision==ACTIVATION_REVISIONS.schema
    handler=catalog_handler(check_sha256='a'*64,apply_sha256='b'*64)
    assert handler.route==ROUTE
    assert handler.check.name=='check_source_catalog_v1'
    assert handler.apply.name=='apply_source_catalog_v1'


@pytest.mark.parametrize('invalid',[
    {'expected_revision':2,'title':'No revision upgrade'},
    {'expected_revision':1,'catalog':{'kind':'journal_issue'}},
    {'expected_revision':1,'owner_user_id':'someone'},
    {'expected_revision':1},
    {'expected_revision':1,'title':''},
])
def test_catalog_rejects_wrong_revision_kind_acl_or_empty_update(invalid):
    with pytest.raises((ValueError,TypeError)):
        _command(invalid)



def test_catalog_command_actor_binding_cannot_be_relabelled():
    from dataclasses import replace
    command=_command({'expected_revision':1,'title':'Verified title page'})
    with pytest.raises(ValueError,match='actor differs'):
        validate_catalog_command(replace(command,person_id=str(uuid4())))


def test_sql034_catalog_correction_is_additive_and_does_not_modify_original_stage():
    base=Path(__file__).resolve().parents[1]
    sql=(base/'sql/kaggle/034_source_catalog_corrections.sql').read_text()
    assert 'CREATE TABLE rkb_v4.source_catalog_corrections' in sql
    assert 'PRIMARY KEY(source_id,accepted_sequence)' in sql
    assert 'REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED' in sql
    assert 'CREATE TRIGGER immutable_source_catalog_correction' not in sql
    assert 'NOT EXISTS(SELECT 1 FROM rkb_v4.source_catalog_corrections' not in sql
    assert 'first_owner' not in sql
    assert "d->>'codec'" not in sql
    assert 'ORDER BY accepted_sequence DESC LIMIT 1' in sql
    assert 'LEFT JOIN LATERAL' in sql
    assert 'check_source_catalog_v1' in sql and 'apply_source_catalog_v1' in sql
    assert 'source_stage_batches b ON b.source_id=session.source_id' in sql
    assert "a.status='applied'" in sql and "b.batch_index=0" in sql
    assert 'CREATE OR REPLACE FUNCTION rkb_v4.find_source_catalog_v1' in sql
    assert 'UPDATE rkb_v4.source_stage_sessions' not in sql
    assert 'UPDATE rkb_v4.source_stage_batches' not in sql
    assert 'UPDATE rkb_v4.source_catalog' not in sql
    assert 'UPDATE rkb_v4.source_catalog_corrections' not in sql
    # GEO PR372 is independent; no GEO migration is required for this SQL034.



def test_native_binding_delegates_catalog_codec_shape_and_actor_to_one_validator():
    from dataclasses import replace
    import json
    from regional_knowledge.kaggle_native_source_bindings import validate_native_source_command
    from regional_knowledge.kaggle_target_launch import MODULES
    assert 'source_catalog_command' in MODULES
    cmd=_command({'expected_revision':1,'title':'Title reviewed on original'})
    assert validate_native_source_command(cmd)==validate_catalog_command(cmd)
    for change in ({'codec':'wrong.catalog.v1'}, {'extra':'not a canonical field'}):
        raw=json.loads(cmd.payload)
        raw.update(change)
        with pytest.raises(ValueError):
            validate_native_source_command(replace(cmd,payload=canonical(raw)))
    with pytest.raises(ValueError,match='actor differs'):
        validate_native_source_command(replace(cmd,person_id=str(uuid4())))


from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,TIME
from test_source_batch_pg import native,packet
from test_native_source_materializer import exact_batch
from test_source_stage import SOURCE,INGESTION,REQUEST
from test_source_activation import PERSON


@PG
@pytest.mark.asyncio
async def test_catalog_second_correction_latest_projection_and_exact_replay_pg17(image_pg_sql18):
    """Actual disposable PG17 + current registry: first and second exact accepted effects."""
    import json
    from regional_knowledge.kaggle_native_source_bindings import validate_native_source_command
    env=image_pg_sql18; db=env['connection']
    staged=exact_batch(count=1)
    assert (await accepted(env,stage_command(staged,1))).status=='applied'
    publish=packet(env,staged)
    prepare,_=native(env)
    await prepare(publish)
    assert (await accepted(env,publish)).status=='applied'
    async with db.transaction():
        original_stage=(await (await db.execute(
            'SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=%s AND batch_index=0',
            (SOURCE,))).fetchone())[0]
        original_catalog=(await (await db.execute(
            'SELECT metadata FROM rkb_v4.source_catalog WHERE source_id=%s',(SOURCE,))).fetchone())[0]
        original_manifest=(await (await db.execute(
            'SELECT manifest FROM rkb_v4.source_manifests WHERE source_id=%s',(SOURCE,))).fetchone())[0]
        passage=(await (await db.execute(
            'SELECT passage_id FROM rkb_v4.source_passage_provenance WHERE source_id=%s LIMIT 1',
            (SOURCE,))).fetchone())[0]
        before_metadata=(await (await db.execute(
            'SELECT metadata FROM rkb_v4.source_passage_metadata_v1(%s::uuid[])',([passage],))).fetchone())[0]
        assert before_metadata['catalog']==original_manifest['metadata']

    # Closed group SQL integrity does not compare original stage person_id with the
    # current accepted catalog actor. Existing controller OAuth/owned-ingestion checks
    # and exact-command person_id==payload.owner_person_id remain unchanged.
    editor=str(uuid4())
    def correction(update,key):
        return resolved_catalog_command(
            sequence=env['sequence']+1,operation_key=key,request_id=REQUEST,
            person_id=editor,source_id=SOURCE,logical_time=TIME,
            payload=dict(codec=CODEC,ingestion_id=INGESTION,owner_person_id=editor,
                         catalog_update=update))
    first=correction(dict(expected_revision=1,title='Alpha title corrected',
        catalog=dict(cover_page=0,cover_kind='title_page')), 'catalog-correction:first')
    assert validate_native_source_command(first)['owner_person_id']==editor
    assert editor!=PERSON
    result=await accepted(env,first)
    assert result.status=='applied' and not result.duplicate
    replay=await accepted(env,first)
    assert replay.duplicate and replay.outcome==result.outcome
    second=correction(dict(expected_revision=1,title='Omega title corrected',
        publication_year=None,authors=['Corrected author']), 'catalog-correction:second')
    result2=await accepted(env,second)
    assert result2.status=='applied' and not result2.duplicate
    repeated=await accepted(env,second)
    assert repeated.duplicate and repeated.outcome==result2.outcome
    assert first.operation_key!=second.operation_key
    async with db.transaction():
        rows=await (await db.execute(
            'SELECT accepted_sequence,metadata FROM rkb_v4.source_catalog_corrections '
            'WHERE source_id=%s ORDER BY accepted_sequence',(SOURCE,))).fetchall()
        assert [row[0] for row in rows]==[first.sequence,second.sequence]
        assert rows[0][1]['title']=='Alpha title corrected'
        assert rows[1][1]['title']=='Omega title corrected'
        assert rows[1][1]['authors']==['Corrected author']
        assert rows[1][1]['publication_year'] is None
        assert rows[1][1]['catalog']['cover_kind']=='title_page'
        assert rows[1][1]['catalog']['cover_page']==0
        current=await (await db.execute(
            'SELECT source_id,metadata FROM rkb_v4.find_source_catalog_v1(%s,%s)',
            ('Omega',8))).fetchall()
        assert len(current)==1 and str(current[0][0])==SOURCE
        assert current[0][1]==rows[-1][1]
        passage_metadata=(await (await db.execute(
            'SELECT metadata FROM rkb_v4.source_passage_metadata_v1(%s::uuid[])',([passage],))).fetchone())[0]
        assert passage_metadata['catalog']==current[0][1]
        assert {k:v for k,v in passage_metadata.items() if k!='catalog'}=={
            k:v for k,v in before_metadata.items() if k!='catalog'}
        assert await (await db.execute(
            'SELECT source_id FROM rkb_v4.find_source_catalog_v1(%s,%s)',
            ('Alpha',8))).fetchall()==[]
        assert (await (await db.execute(
            'SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=%s AND batch_index=0',
            (SOURCE,))).fetchone())[0]==original_stage
        assert (await (await db.execute(
            'SELECT metadata FROM rkb_v4.source_catalog WHERE source_id=%s',(SOURCE,))).fetchone())[0]==original_catalog
        assert (await (await db.execute(
            'SELECT manifest FROM rkb_v4.source_manifests WHERE source_id=%s',(SOURCE,))).fetchone())[0]==original_manifest
        # Bad source identity or ingestion must fail SQL integrity, not insert.
        wrong=json.loads(second.payload);wrong['ingestion_id']=str(uuid4())
        ok=(await (await db.execute(
            'SELECT rkb_v4.check_source_catalog_v1(%s,%s::jsonb,%s::jsonb,%s)',
            (canonical(wrong),json.dumps(dict(second.ids)),
             json.dumps(dict(second.expected_revisions)),TIME))).fetchone())[0]
        assert ok is False
