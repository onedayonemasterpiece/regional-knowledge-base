"""Model selects offsets; current native compilation derives provenance."""
import copy

import pytest

from regional_knowledge.contracts import StageChunkInput,StageRegionSpanRef
from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler,SourceMaterializationRejected
from regional_knowledge.source_batch_context import load_current
from regional_knowledge.source_stage import canonical,digest
from test_native_source_materializer import exact_batch,SOURCE,PERSON
from test_source_batch_context import Connection


def packet(hint=None):
    doc=exact_batch(count=1);page=doc['model_payload']['pages'][0]
    page['regions'][0]['source_text']='Complete synthetic sentence.'
    ref=dict(page_id=page['page_id'],region_key='body',start=0,end=len(page['regions'][0]['source_text']))
    if hint is not None:ref['source_text_sha256']=hint
    chunk=doc['model_payload']['chunks'][0];chunk.pop('region_refs');chunk['span_refs']=[ref]
    return doc


def test_model_schema_has_offsets_without_hash_but_legacy_wire_retains_hash():
    schema=StageChunkInput.model_json_schema()['$defs']['StageRegionSpanRef']
    assert set(schema['properties'])=={'page_id','region_key','start','end'}
    assert set(schema['required'])==set(schema['properties'])
    legacy=packet('a'*64)['model_payload']['chunks'][0]['span_refs'][0]
    assert StageRegionSpanRef.model_validate(legacy).model_dump(mode='json')==legacy


@pytest.mark.asyncio
@pytest.mark.parametrize('hint',[None,'f'*64])
@pytest.mark.parametrize('text_only',[False,True])
async def test_current_native_derives_exact_span_hash_without_mutating_accepted_input(hint,text_only):
    doc=packet(hint);raw=canonical(doc);db=Connection()
    view=await load_current(db,raw,SOURCE,text_only=text_only);frozen=copy.deepcopy(view)
    async def count(_):return 10
    result,_=await NativeSourceCompiler(count=count,current=lambda:True).compile_current(
        raw,view,source_id=SOURCE,person_id=PERSON,text_only=text_only)
    expected=doc['model_payload']['pages'][0]['regions'][0]['source_text']
    assert result['passages'][0]['body']==expected
    stored=result['image_manifest']['passages'][0]['source_spans'][0]
    assert stored['source_text_sha256']==digest(expected.encode())
    assert canonical(doc)==raw and view==frozen and db.queries==[]


@pytest.mark.asyncio
@pytest.mark.parametrize('broken',['beyond','word_cut'])
async def test_server_derived_hash_does_not_relax_source_offsets(broken):
    doc=packet();span=doc['model_payload']['chunks'][0]['span_refs'][0]
    if broken=='beyond':span['end']+=1
    else:span['start']=1
    raw=canonical(doc);view=await load_current(Connection(),raw,SOURCE,text_only=True)
    async def forbidden(_):raise AssertionError('invalid source span reached tokenizer')
    with pytest.raises(SourceMaterializationRejected):
        await NativeSourceCompiler(count=forbidden,current=lambda:True).compile_current(
            raw,view,source_id=SOURCE,person_id=PERSON,text_only=True)
