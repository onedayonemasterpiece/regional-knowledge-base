"""Synthetic packet boundaries; no source files, tokenizer or provider."""
import copy
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler,SourceMaterializationRejected
from regional_knowledge.source_batch_context import load_current
from regional_knowledge.source_stage import canonical,digest
from test_native_source_materializer import exact_batch,SOURCE,PERSON


def tail_packet(partial=False):
    doc=exact_batch(count=2)
    for page in doc['model_payload']['pages']:
        page['regions'][0]['source_text']='A complete synthetic sentence.'
    tail=copy.deepcopy(doc['model_payload']['pages'][-1]['regions'][0])
    tail.update(region_key='tail',reading_order=1,source_text='An unfinished synthetic')
    page=doc['model_payload']['pages'][-1]
    if partial:
        prefix=page['regions'][0]['source_text'];text=prefix+' '+tail['source_text']
        page['regions'][0]['source_text']=text
        chunk=doc['model_payload']['chunks'][-1];chunk.pop('region_refs')
        chunk['span_refs']=[dict(page_id=page['page_id'],region_key='body',start=0,end=len(prefix),
            source_text_sha256=digest(text.encode()))]
    else:page['regions'].append(tail)
    return doc


class PriorPage:
    def __init__(self,doc):self.doc=doc;self.queries=[]
    async def execute(self,sql,args):
        self.queries.append(sql)
        assert 'source_stage_pages' in sql
        import base64
        page=self.doc['model_payload']['pages'][-1]
        assert args[1]==[page['page_id']]
        async def rows():return [(page['page_id'],page,base64.b64decode(self.doc['issued_images_b64'][-1]))]
        return SimpleNamespace(fetchall=rows)


def continuation_packet(first):
    doc=exact_batch(1,digest(canonical(first)),start=2,count=1)
    doc['model_payload']['pages'][0]['regions'][0]['source_text']='sentence finishes here.'
    prior=first['model_payload']['pages'][-1];current=doc['model_payload']['pages'][0]
    chunk=doc['model_payload']['chunks'][0]
    if len(prior['regions'])==1:
        old=prior['regions'][0]['source_text'];new=current['regions'][0]['source_text']
        chunk.pop('region_refs')
        chunk['span_refs']=[dict(page_id=prior['page_id'],region_key='body',
            start=old.index('An unfinished'),end=len(old),source_text_sha256=digest(old.encode())),
            dict(page_id=current['page_id'],region_key='body',start=0,end=len(new),source_text_sha256=digest(new.encode()))]
    else:chunk['region_refs'].insert(0,dict(page_id=prior['page_id'],region_key='tail'))
    return doc


@pytest.mark.asyncio
@pytest.mark.parametrize('text_only',[False,True])
@pytest.mark.parametrize('partial',[False,True])
async def test_tail_survives_and_next_packet_installs_one_complete_cross_page_passage(text_only,partial):
    first=tail_packet(partial);raw=canonical(first)
    async def count(_):return 10
    compiler=NativeSourceCompiler(count=count,current=lambda:True)
    view=await load_current(PriorPage(first),raw,SOURCE,text_only=text_only)
    result,_=await compiler.compile_current(raw,view,source_id=SOURCE,person_id=PERSON,text_only=text_only)
    assert len(result['passages'])==2
    assert any(r['source_text'].endswith('An unfinished synthetic') for r in result['semantic']['regions'])
    assert all('unfinished' not in p['body'] for p in result['passages'])
    second=continuation_packet(first);raw=canonical(second);db=PriorPage(first)
    view=await load_current(db,raw,SOURCE,text_only=text_only)
    result,_=await compiler.compile_current(raw,view,source_id=SOURCE,person_id=PERSON,text_only=text_only)
    assert len(result['passages'])==1
    assert result['passages'][0]['body']=='An unfinished synthetic\nsentence finishes here.'
    assert len(result['image_manifest']['passages'][0]['page_ids'])==2
    assert len(db.queries)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid',['middle_page','hole','complete_research','partial_span'])
async def test_only_incomplete_suffix_on_last_current_page_can_be_deferred(invalid):
    doc=tail_packet();pages=doc['model_payload']['pages']
    if invalid=='middle_page':pages[0]['regions'].append(pages[-1]['regions'].pop())
    elif invalid=='hole':
        region=copy.deepcopy(pages[-1]['regions'][0]);region.update(region_key='after-tail',reading_order=2)
        pages[-1]['regions'].append(region)
        doc['model_payload']['chunks'].append(dict(chunk_key='after-tail',title='Later covered text',
            region_refs=[dict(page_id=pages[-1]['page_id'],region_key='after-tail')]))
    elif invalid=='partial_span':
        chunk=doc['model_payload']['chunks'][-1];chunk.pop('region_refs')
        text=pages[-1]['regions'][0]['source_text']
        chunk['span_refs']=[dict(page_id=pages[-1]['page_id'],region_key='body',start=a,end=b,
            source_text_sha256=digest(text.encode())) for a,b in [(0,1),(11,20)]]
    else:doc['research'].update(state='complete',continuation={})
    raw=canonical(doc);view=await load_current(PriorPage(doc),raw,SOURCE,text_only=True)
    async def forbidden(_):raise AssertionError('invalid coverage reached tokenizer')
    with pytest.raises(SourceMaterializationRejected,match='native_source_stage_requires_review'):
        await NativeSourceCompiler(count=forbidden,current=lambda:True).compile_current(
            raw,view,source_id=SOURCE,person_id=PERSON,text_only=True)


@pytest.mark.asyncio
async def test_system_does_not_choose_semantic_boundaries_from_punctuation():
    doc=tail_packet();doc['model_payload']['pages'][-1]['regions'][-1]['source_text']='A complete sentence retained for the next semantic passage.'
    raw=canonical(doc);view=await load_current(PriorPage(doc),raw,SOURCE,text_only=True)
    async def count(_):return 10
    result,_=await NativeSourceCompiler(count=count,current=lambda:True).compile_current(
        raw,view,source_id=SOURCE,person_id=PERSON,text_only=True)
    assert len(result['passages'])==2
