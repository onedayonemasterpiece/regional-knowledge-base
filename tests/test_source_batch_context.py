"""Current-page references resolve ordinary indexed projections, never history."""
import base64
from types import SimpleNamespace
from uuid import UUID,uuid5

import pytest

from regional_knowledge.source_batch_context import load_current
from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical,digest
from test_native_source_materializer import exact_batch,SOURCE,PERSON


class Connection:
    def __init__(self,prior=None):self.prior=prior;self.queries=[]
    async def execute(self,query,args):
        self.queries.append(query)
        assert 'source_stage_batches' not in query and 'source_stage_objects' not in query
        if 'source_passage_provenance' in query:
            p=self.prior['model_payload']['pages'][0]
            data=dict(passage_id=str(uuid5(UUID(SOURCE),'chunk:1:'+self.prior['model_payload']['chunks'][0]['chunk_key'])),
                page_ids=[p['page_id']],region_ids=[str(uuid5(UUID(p['page_id']),'region:body'))],
                footnote_region_ids=[],source_spans=[])
            rows=[(data['passage_id'],data)]
        elif 'source_stage_pages' in query:
            p=self.prior['model_payload']['pages'][0]
            assert args[1]==[p['page_id']]
            rows=[(p['page_id'],p,base64.b64decode(self.prior['issued_images_b64'][0]))]
        else:raise AssertionError(query)
        async def all_rows():return rows
        return SimpleNamespace(fetchall=all_rows)


@pytest.mark.asyncio
async def test_text_page_ignores_broken_optional_graph_without_any_context_read():
    db=Connection();doc=exact_batch(start=173,count=1)
    doc['model_payload']['entity_candidates']={'entities':[{'invalid_optional_node':True}]}
    raw=canonical(doc);view=await load_current(db,raw,SOURCE,text_only=True)
    counted=[]
    async def count(text):counted.append(text);return 10
    result,media=await NativeSourceCompiler(count=count,current=lambda:True).compile_current(raw,view,
        source_id=SOURCE,person_id=PERSON,text_only=True)
    assert len(result['passages'])==1 and len(counted)==1
    assert result['image_manifest']['pages'][0]['physical_page_index']==173
    assert result['semantic']['findings']==[] and media['illustrations']==[]
    assert db.queries==[]


@pytest.mark.asyncio
async def test_current_story_uses_existing_passage_identity_and_only_explicit_prior_page():
    first=exact_batch(count=1);db=Connection(first)
    second=exact_batch(1,digest(canonical(first)),start=8,count=1)
    p=first['model_payload']['pages'][0]
    second['model_payload']['story_candidates']=[dict(candidate_key='episode',title='A source finding',
        summary='Explicit source evidence, no invented context.',proposition='The source describes the event.',
        narrative_outcome='facts_only',evidence_refs=[dict(page_id=p['page_id'],region_key='body',original_excerpt=p['regions'][0]['source_text'])])]
    raw=canonical(second);view=await load_current(db,raw,SOURCE,text_only=False)
    assert [p['physical_page_index'] for p in view['model_payload']['pages']]==[0,8]
    async def count(text):return 10
    result,_=await NativeSourceCompiler(count=count,current=lambda:True).compile_current(raw,view,
        source_id=SOURCE,person_id=PERSON)
    expected=str(uuid5(UUID(SOURCE),'chunk:1:'+first['model_payload']['chunks'][0]['chunk_key']))
    assert expected in {p['passage_id'] for p in result['passages']}
    assert len(result['passages'])==2 and len(result['semantic']['findings'])==1
    assert len(db.queries)==2
