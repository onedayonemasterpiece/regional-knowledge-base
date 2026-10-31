"""Bounded native SetMetadata and existing editorial/spool behavior without model weights."""
import asyncio
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import ValidationError

from regional_knowledge.story_contracts import NativeStoryTitle
from regional_knowledge.story_title_command import resolved_native_story_title_command, validate_title_command, ROUTE
from regional_knowledge.kaggle_native_source_bindings import native_source_handler, validate_native_source_command
from regional_knowledge.kaggle_apply import HandlerRegistry
from test_story_editorial import STORY
from test_kaggle_story_editorial_backend import editorial, context
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


def test_title_is_strict_narrow_set_metadata_not_a_new_open_patch():
    assert set(NativeStoryTitle.model_json_schema()['properties'])=={'op','title'}
    valid=NativeStoryTitle(op='set_metadata',title='Литаврная собака')
    for patch in ({'summary':None},{'tags':['unapproved']},{'time_scope':'1800'},
                  {'material_type':None},{'title':'   '},{'title':''}):
        with pytest.raises(ValidationError):
            NativeStoryTitle.model_validate({'op':'set_metadata','title':'Good title',**patch})
    c=resolved_native_story_title_command(sequence=4,operation_key='title-command',request_id='title-4',
        person_id='60000000-0000-0000-0000-000000000003',logical_time='2026-10-31T12:00:00Z',
        source_id='10000000-0000-0000-0000-000000000001',story_id=STORY,
        expected_revision=2,source_revision=1,eligibility_revision=2,operation=valid)
    assert validate_title_command(c)['operation']=={'op':'set_metadata','title':'Литаврная собака'}
    assert validate_native_source_command(c)['operation']['title']==valid.title
    assert native_source_handler(ROUTE,check_sha256='a'*64,apply_sha256='b'*64).apply.name=='apply_story_title_v1'
    from dataclasses import replace
    with pytest.raises(ValueError):
        validate_title_command(replace(c,payload=c.payload.replace(b'set_metadata',b'set_metadata_x')))


@pytest.mark.asyncio
async def test_title_only_accepts_one_editor_intent_without_tokenizer_bge_or_variant_job(editorial):
    s=editorial
    from regional_knowledge.source_activation import ACTIVATE_ROUTE
    s.spool.registry=HandlerRegistry([native_source_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
        for route in (ACTIVATE_ROUTE,'story.edit.v1','story.access.v1',ROUTE,'source.withdraw.v1')])
    with s.t.queue.connect() as db:
        row=db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',(s.t.run,)).fetchone()
        values=[]
        for raw in row:
            value=json.loads(raw);value['schema_version']=48;values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',(*values,s.t.run))
    title=NativeStoryTitle(op='set_metadata',title='Литаврная собака')
    args=dict(story_id=STORY,expected_revision=1,idempotency_key='story-title-unique-1',operation=title)
    # Denied preflights precede the durable source-command work item.
    with pytest.raises(ToolError,match='revision_conflict'):
        await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,
            **dict(args,idempotency_key='story-title-stale'))),first_revision=2)
    with pytest.raises(ToolError,match='access_denied'):
        await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,
            **dict(args,idempotency_key='story-title-viewer'))),role='viewer')
    result=await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,**args)),role='editor')
    assert result['status']=='processing' and result['operation_id'].startswith('story-editorial:')
    entry=await s.spool.get(result['operation_id'])
    assert entry.command.route==ROUTE and entry.command.ids['story_id']==STORY
    assert json.loads(entry.command.payload)['operation']=={'op':'set_metadata','title':title.title}
    assert not s.counts
    with s.t.queue.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='document'").fetchone()[0]==0
    duplicate=await s.workflow.mutate(s.t.principal,**args)
    assert duplicate['operation_id']==result['operation_id']
    assert (await s.spool.get(result['operation_id'])).command_bytes==entry.command_bytes
    with pytest.raises(ToolError,match='idempotency_conflict'):
        await s.workflow.mutate(s.t.principal,**dict(args,operation=NativeStoryTitle(op='set_metadata',title='Another title')))
    assert not s.counts
