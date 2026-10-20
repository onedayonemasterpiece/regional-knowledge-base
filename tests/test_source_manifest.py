"""Synthetic receipt fidelity; no actual original, provider or authority."""
import copy
import hashlib
from uuid import UUID, uuid5

import pytest

from regional_knowledge.contracts import StartMetadataInput
from regional_knowledge.original_intake import NativePagesProof, VerifiedOriginalReceipt
from regional_knowledge.source_import import _json
from regional_knowledge.source_import_v4 import resolved_import_v4_command, validate_import_v4_command
from regional_knowledge.source_manifest import build_source_manifest, validate_source_manifest
from regional_knowledge.stage_graph import StagedGraph
from test_source_import import SOURCE
from test_source_import_v2 import REGION, payload

BBOX=dict(left=10,top=20,right=900,bottom=100)


def inputs():
    doc=payload();text=doc['semantic']['regions'][0]['source_text']
    receipt=VerifiedOriginalReceipt(SOURCE,'60000000-0000-0000-0000-000000000011',
        '60000000-0000-0000-0000-000000000012',1,'a'*64,1234,'synthetic-original','synthetic-v1',
        'pdf',2,'b'*64,'c'*64,'d'*64)
    pages=[]
    for index in range(2):
        native_text='А🙂 prefix '+text+' suffix'
        block=dict(block_index=7,bbox=BBOX,text=native_text,original_length=len(native_text),truncated=False,
            text_sha256=hashlib.sha256(native_text.encode()).hexdigest())
        info=dict(complete=True,truncated=False,blocks_truncated=False,
            native_text_sha256=hashlib.sha256(native_text.encode()).hexdigest(),
            native_blocks_sha256=hashlib.sha256(_json([{k:block[k] for k in ('block_index','bbox','text')}])).hexdigest())
        page=dict(format='pdf',page_count=2,page_id=str(uuid5(UUID(SOURCE),f'page:1:{index}')),
            physical_page_index=index,original_sha256=receipt.sha256,parser_sha256=receipt.parser_sha256,
            width=595.276,height=841.89,image_sha256='e'*64,native_text=native_text,native_blocks=[block],
            native_text_info=info,preview_render=dict(policy='rkb.pdf-preview.v1',width=724,height=1024,
                scale=1.216319,max_edge=1024,jpeg_quality=80,byte_limit=256*1024,preview_only=True))
        page['page_receipt_sha256']=hashlib.sha256(_json(page)).hexdigest();pages.append(page)
    proof=NativePagesProof(receipt.receipt_sha256,tuple(_json(p) for p in pages))
    page_id=pages[1]['page_id'];doc['semantic']['regions'][0]['page_id']=page_id
    graph=StagedGraph(revision=1,pages=[dict(page_id=page_id,physical_page_index=1,printed_page_number='105',
        regions=[dict(region_key='body',region_id=REGION,page_id=page_id,kind='body',bbox=BBOX,
            source_text=text,reading_order=0,column_id='right')])])
    metadata=StartMetadataInput(title='Exact title; with delimiters',authors=['A; B','C, D'],publication_year=1910,
        language='simple',catalog=dict(publisher='Exact publisher',edition='2',identifiers=[{'ISBN':'synthetic'}]))
    doc['original']=receipt.original
    return doc,metadata,receipt,proof,graph


def manifest_payload():
    doc,metadata,receipt,proof,graph=inputs()
    doc['codec']='rkb.source-import.v4'
    doc['source_manifest']=build_source_manifest(metadata=metadata,receipt=receipt,proof=proof,graph=graph,semantic=doc['semantic'])
    return doc


def command(doc):
    return resolved_import_v4_command(sequence=1,operation_key='manifest-import-1',request_id='manifest-1',
        person_id='60000000-0000-0000-0000-000000000002',logical_time='2026-10-20T18:00:00Z',
        source_id=SOURCE,payload=doc)


def test_catalogue_and_exact_physical_unicode_provenance_survive_command_replay():
    doc=manifest_payload(); c=command(doc)
    saved=validate_import_v4_command(c)['source_manifest']
    assert saved['metadata']['authors']==['A; B','C, D']
    assert saved['metadata']['catalog']['edition']=='2'
    assert [p['physical_page_index'] for p in saved['pages']]==[0,1]
    assert saved['pages'][0]['printed_page_number'] is None
    assert saved['pages'][1]['printed_page_number']=='105'
    assert saved['pages'][1]['width']==595.276
    assert saved['regions'][0]['start_offset']==len('А🙂 prefix ')
    assert saved['regions'][0]['native_block_index']==7
    doc['source_manifest']['metadata']['title']='modified caller'
    assert validate_import_v4_command(c)['source_manifest']==saved
    assert command(manifest_payload()).encode()==c.encode()


@pytest.mark.parametrize('kind',['missing','source','receipt','page_gap','page_order','region_missing','span',
    'bbox','nonfinite','bool_dimension','metadata','preview','component','codec'])
def test_incomplete_forged_or_unbound_manifest_refuses_before_acceptance(kind):
    doc=manifest_payload();m=doc['source_manifest']
    if kind=='missing':del doc['source_manifest']
    if kind=='source':m['source_id']='60000000-0000-0000-0000-000000000012'
    if kind=='receipt':doc['original']['receipt_sha256']='0'*64
    if kind=='page_gap':m['pages'].pop()
    if kind=='page_order':m['pages'].reverse()
    if kind=='region_missing':m['regions']=[]
    if kind=='span':m['regions'][0]['end_offset']+=1
    if kind=='bbox':m['regions'][0]['bbox']['left']=1001
    if kind=='nonfinite':m['pages'][0]['width']=float('nan')
    if kind=='bool_dimension':m['pages'][0]['width']=True
    if kind=='metadata':m['metadata']['private_extra']='discard me'
    if kind=='preview':m['pages'][0]['preview_render']['preview_only']=False
    if kind=='component':m['metadata']['catalog']['parent_id']=SOURCE
    if kind=='codec':doc['codec']='rkb.source-import.v2'
    with pytest.raises(ValueError):command(doc)


@pytest.mark.parametrize('kind',['page_receipt','original','text_hash','block_hash','ambiguous_block','truncated','wrong_graph_page'])
def test_constructor_requires_complete_native_proof_not_model_geometry(kind):
    doc,metadata,receipt,proof,graph=inputs();pages=copy.deepcopy(list(proof.pages))
    page=pages[1]
    if kind=='page_receipt':page['page_receipt_sha256']='0'*64
    if kind=='original':page['original_sha256']='0'*64
    if kind=='text_hash':page['native_text_info']['native_text_sha256']='0'*64
    if kind=='block_hash':page['native_blocks'][0]['text_sha256']='0'*64
    if kind=='ambiguous_block':
        page['native_blocks'].append(copy.deepcopy(page['native_blocks'][0]))
        page['native_text_info']['native_blocks_sha256']=hashlib.sha256(_json([
            {k:b[k] for k in ('block_index','bbox','text')} for b in page['native_blocks']])).hexdigest()
    if kind=='truncated':page['native_text_info']['blocks_truncated']=True
    if kind=='wrong_graph_page':graph.pages[0].page_id=UUID('60000000-0000-0000-0000-000000000012')
    if kind!='page_receipt':
        page['page_receipt_sha256']=hashlib.sha256(_json({k:v for k,v in page.items() if k!='page_receipt_sha256'})).hexdigest()
    proof=NativePagesProof(receipt.receipt_sha256,tuple(_json(p) for p in pages))
    with pytest.raises(ValueError):
        build_source_manifest(metadata=metadata,receipt=receipt,proof=proof,graph=graph,semantic=doc['semantic'])
