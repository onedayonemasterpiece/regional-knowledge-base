import copy
import hashlib
import pytest
from regional_knowledge.kaggle_editorial_contracts import EditorialReadResult,EditorialReadRequest
from regional_knowledge.kaggle_story_read import story_item
from test_source_import_v2 import payload,STORY,FINDING
from test_source_import import SOURCE


def record():
    doc=payload();st=doc['semantic']['stories'][0];v={k:x for k,x in st['default_variant'].items() if k not in ('embedding','input_tokens','blocks')}
    v.update(blocks=[],source_blocks=st['default_variant']['blocks'],channel=None,attributions=[],media_refs=[],
        state='drafting',approved_revision=None,approval_fingerprint=None)
    return dict(story_id=STORY,revision=1,current_revision=1,title=st['title'],summary=st['summary'],material_type=st['material_type'],
        body=v['body'],body_sha256=hashlib.sha256(v['body'].encode()).hexdigest(),variant=v,
        default_variant_ref=dict(variant_id=v['variant_id'],revision=1),state='drafting',narrative_availability='available',
        visibility='shared',publication_approval=None,bibliography='Synthetic bibliography',
        source=dict(source_id=SOURCE,content_revision=1,eligibility_revision=1),finding_ids=[FINDING],
        view='get',entries=[],next_revision=None,next_variant_id=None)


def result(r):
    return EditorialReadResult(request_id='editorial-test',connection_generation=1,turn_generation=1,status='ok',record=r,
        evidence=(story_item(r),),observed_data_seq=3,observed_projection_seq=3,observed_visibility_epoch=1,observed_database_generation='db')


def test_imported_and_edited_exact_body_records_keep_version_and_block_semantics():
    r=record();assert result(r).record==r
    v=r['variant'];v.pop('source_blocks');v.update(revision=2,blocks=[dict(text='A block with exact source.',assertion_id=FINDING,assertion_revision=1,attribution=None)])
    r.update(revision=3,current_revision=3,default_variant_ref=dict(variant_id=v['variant_id'],revision=2))
    assert result(r).record['default_variant_ref']['revision']==2
    r.update(view='edit_context',role='editor',variant_refs=[r['default_variant_ref']]);assert result(r).record['role']=='editor'
    r.pop('role');r.pop('variant_refs');r.update(view='variants',entries=[{**v,'body_sha256':r['body_sha256']}],variant=None)
    assert result(r).record['entries'][0]['body']==r['body']


@pytest.mark.parametrize('change',['body','approval','source_blocks','audience','language','revision','role'])
def test_forged_editorial_output_fails_closed(change):
    r=record()
    if change=='body':r['body_sha256']='f'*64
    if change=='approval':r['variant']['approved_revision']=1
    if change=='source_blocks':r['variant']['source_blocks'][0]['assertion_ids']=['wrong']
    if change=='audience':r['variant']['audience']='x'*501
    if change=='language':r['variant']['language']='x'*25
    if change=='revision':r['variant']['revision']=True
    if change=='role':r.update(view='edit_context',role='owner-override',variant_refs=[r['default_variant_ref']])
    with pytest.raises((ValueError,TypeError)):result(r)


def test_editorial_page_cursor_is_bound_to_story_view_generation_and_snapshot():
    from regional_knowledge.kaggle_editorial_contracts import editorial_cursor,parse_editorial_cursor
    r=record();r.update(view='variants',next_variant_id=r['default_variant_ref']['variant_id'],revision=3,current_revision=3)
    cursor=editorial_cursor(r,'db-current')
    page,generation=parse_editorial_cursor(cursor,story_id=r['story_id'],view='variants')
    assert generation=='db-current' and page==dict(after_variant_id=r['next_variant_id'],revision=3)
    with pytest.raises(ValueError):parse_editorial_cursor(cursor,story_id=r['story_id'],view='history')
    with pytest.raises(ValueError):parse_editorial_cursor(cursor,story_id='00000000-0000-0000-0000-000000000005',view='variants')
