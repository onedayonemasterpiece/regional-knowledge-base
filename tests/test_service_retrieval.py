"""Service-only source chunks must not consume retrieval/continuation slots."""
import hashlib
from uuid import uuid4
import pytest
from regional_knowledge.sqlite_corpus import SQLiteCorpus, retrieval_clause
from regional_knowledge.sqlite_data import defaults


def fixture(tmp_path):
    corpus=SQLiteCorpus(tmp_path/'source.db')
    owner,doc,page=[str(uuid4()) for _ in range(3)]
    corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}])
    corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'active_revision':1,'source_sha256':'a'*64}])
    corpus.put('rkb_pages',[{'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
    def add(kinds, order, *,illustrations=False):
        chunk=str(uuid4());ids=[];parts=[]
        for n,kind in enumerate(kinds):
            rid=str(uuid4());text='needle '+kind+' '+str(n);parts.append(text);ids.append(rid)
            corpus.put('rkb_regions',[{'id':rid,'page_id':page,'kind':kind,'reading_order':order+n,'source_text':text}])
        text='\n'.join(parts) or 'needle legacy';sha=hashlib.sha256(text.encode()).hexdigest()
        corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'region_ids':ids,'footnote_region_ids':[],'illustration_ids':[str(uuid4())] if illustrations else [],'source_text':text,'search_material':text,'text_sha256':sha,'search_material_sha256':sha,'title':'Evidence'}])
        return chunk
    return corpus,owner,doc,add


@pytest.mark.parametrize('kind',['header','footer','page_number'])
def test_service_filtered_before_limit_preserved_for_fetch(tmp_path,kind):
    corpus,owner,doc,add=fixture(tmp_path)
    service=add([kind],0);body=add(['body'],10)
    original=corpus.one('rkb_chunks',service)
    assert [r['chunk_id'] for r in corpus.lexical(owner,'needle',depth=1)]==[body]
    assert [r['chunk_id'] for r in corpus.candidate_metadata({doc:1})]==[body]
    assert [r['chunk_id'] for r in corpus.candidate_metadata_ids([service,body],{doc:1})]==[body]
    assert corpus.one('rkb_chunks',service)==original
    assert corpus.candidate_metadata_ids([service,body],{})==[]


@pytest.mark.parametrize('kinds',[['header','body'],['heading'],['footnote'],['caption'],['table'],['marginalia'],[]])
def test_content_and_unknown_legacy_remain_retrievable(tmp_path,kinds):
    corpus,owner,doc,add=fixture(tmp_path)
    content=add(kinds,0)
    assert [r['chunk_id'] for r in corpus.lexical(owner,'needle')]==[content]
    assert [r['chunk_id'] for r in corpus.candidate_metadata_ids([content],{doc:1})]==[content]


def test_missing_region_and_illustration_not_silently_hidden(tmp_path):
    corpus,owner,doc,add=fixture(tmp_path)
    image=add(['header'],0,illustrations=True)
    missing=add(['header'],10)
    row=corpus.one('rkb_chunks',missing);row['region_ids'].append(str(uuid4()));corpus.put('rkb_chunks',[row])
    assert set(r['chunk_id'] for r in corpus.lexical(owner,'needle'))=={image,missing}


def test_service_cannot_displace_meaningful_neighbors(tmp_path):
    corpus,owner,doc,add=fixture(tmp_path)
    first=add(['body'],0)
    headers=[add(['header'],n) for n in [10,20,30]]
    last=add(['footnote'],40)
    corpus.build_fragments(doc,1)
    row=corpus.neighbors(owner,[first])[0]
    assert row['next_id']==last
    assert not any(value in headers for key,value in row.items() if key.endswith('_id'))


def test_semantic_footnote_reference_keeps_header_chunk(tmp_path):
    corpus,owner,doc,add=fixture(tmp_path)
    header=add(['header'],0);note=add(['footnote'],10)
    row=corpus.one('rkb_chunks',header)
    row['footnote_region_ids']=corpus.one('rkb_chunks',note)['region_ids']
    corpus.put('rkb_chunks',[row])
    assert header in {r['chunk_id'] for r in corpus.lexical(owner,'needle')}


def test_internal_sql_identifier_only():
    with pytest.raises(ValueError):retrieval_clause('chunk_id); drop table x')
