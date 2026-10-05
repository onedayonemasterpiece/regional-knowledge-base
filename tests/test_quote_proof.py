import pytest
from PIL import Image
from regional_knowledge.quote_proof import validate_localization,normalize,native_page,paint,FlashLiteLocator

@pytest.mark.parametrize('line',[
 {'text':'citation','bbox':[0,0,1001,10],'order':0},
 {'text':'citation','bbox':[100,0,50,10],'order':0},
 {'text':'citation','bbox':[0,0,1000,1000],'order':0},
 {'text':'wrong','bbox':[0,0,100,10],'order':0},
 {'text':'citation','quad':[[0,0],[100,10],[0,10],[100,0]],'order':0},
])
def test_reject_false_exact_geometry(line):
    with pytest.raises(ValueError):validate_localization({'lines':[line]},'citation')

def test_valid_stripes_and_normalization():
    polys,visible=validate_localization({'lines':[{'text':'ﬂower','bbox':[10,10,200,30],'order':0}]},'flower')
    assert normalize('hy-\nphen')=='hyphen';assert visible=='ﬂower'
    assert paint(Image.new('RGB',(100,100),'white'),polys).startswith(b'RIFF')
    with pytest.raises(ValueError):FlashLiteLocator(model='gemini-3-pro')

def test_native_real_pdf_negative_and_repetition(tmp_path):
    fitz=pytest.importorskip('fitz');path=tmp_path/'native.pdf'
    with fitz.open() as doc:
        page=doc.new_page();page.insert_text((40,60),'Exact printed citation.');doc.save(path)
    hit,status=native_page(path,0,'Exact printed citation.')
    assert status=='ok';assert hit[2]=='Exact printed citation.';assert len(hit[1])==1
    assert native_page(path,0,'Wrong citation')[1]=='quote_not_found'
    with fitz.open() as doc:
        page=doc.new_page();page.insert_text((40,60),'Repeated phrase.');page.insert_text((40,100),'Repeated phrase.');doc.save(path)
    assert native_page(path,0,'Repeated phrase.')[1]=='highlight_unavailable'

@pytest.mark.parametrize('bbox',[[None,0,10,10],['0',0,10,10],[True,0,10,10],[float('nan'),0,10,10]])
def test_malformed_numbers_fail_with_explicit_validation(bbox):
    with pytest.raises(ValueError):validate_localization({'lines':[{'text':'citation','bbox':bbox,'order':0}]},'citation')

@pytest.mark.asyncio
async def test_multiple_regions_and_warm_scan_revocation(tmp_path,monkeypatch):
    import hashlib,io
    from uuid import uuid4
    from regional_knowledge.sqlite_backend import SQLiteBackend
    from regional_knowledge.sqlite_data import defaults
    from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
    from regional_knowledge.contracts import Principal
    from regional_knowledge.quote_proof import source_proof
    fitz=pytest.importorskip('fitz');original=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        p=pdf.new_page(width=600,height=300);p.insert_text((40,80),'First printed region.',fontsize=16);p.insert_text((40,120),'Second printed region.',fontsize=16);pdf.save(original)
    data=original.read_bytes();source_sha=hashlib.sha256(data).hexdigest()
    class Store:
        async def download_file(self,key,path):__import__('pathlib').Path(path).write_bytes(data)
    b=SQLiteBackend(corpus_path=tmp_path/'db',embedder=LexicalOnlyEmbedder(),object_store=Store())
    owner,doc,page,r1,r2,chunk,obj=[str(uuid4()) for _ in range(7)];text='First printed region.\nSecond printed region.';sha=hashlib.sha256(text.encode()).hexdigest()
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}]);d={**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'source_sha256':source_sha,'source_format':'pdf','title':'Test','active_revision':1,'page_count':1};b.corpus.put('rkb_documents',[d])
    b.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
    for ident,quote,order in [(r1,'First printed region.',0),(r2,'Second printed region.',1)]:b.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':ident,'page_id':page,'kind':'body','source_text':quote,'reading_order':order,'bbox':{'left':0,'top':0,'right':1000,'bottom':1000}}])
    b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'title':'Evidence','region_ids':[r1,r2],'page_ids':[page],'source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha}]);b.corpus.build_fragments()
    b.corpus.put('rkb_objects',[{**defaults('rkb_objects'),'id':obj,'document_id':doc,'kind':'source_pdf','object_key':'test.pdf','sha256':source_sha}])
    actor=Principal(subject=owner,client_id='test',issuer='test',access_token='test')
    result,image=await source_proof(b,actor,chunk,text,0);assert result['status']=='ok' and result['method']=='native_text' and len(result['polygons'])==2 and image
    b.corpus.put('rkb_documents',[{**d,'source_format':'djvu'}]);calls=[]
    async def locate(self,image,quote,**kwargs):calls.append(quote);return [[[10,10],[200,10],[200,30],[10,30]]],quote,2
    monkeypatch.setattr(FlashLiteLocator,'locate',locate);monkeypatch.setenv('RKB_SCAN_PROOF_ENABLED','1')
    result,image=await source_proof(b,actor,chunk,'First printed region.',0)
    assert result['status']=='ok' and result['method']=='model_localized' and calls
    from regional_knowledge.original_cache import OriginalCache
    cache=OriginalCache.from_env()
    with cache.db() as db:db.execute('update entries set last_used=0 where sha=?',(source_sha,))
    warm,wimage=await source_proof(b,actor,chunk,'First printed region.',0)
    assert warm['cache_hit'] and wimage and len(calls)==1
    with cache.db() as db:assert db.execute('select last_used from entries where sha=?',(source_sha,)).fetchone()[0]>0
    monkeypatch.setenv('RKB_SCAN_PROOF_ENABLED','0')
    result,image=await source_proof(b,actor,chunk,'First printed region.',0)
    assert result['reason']=='scan_capability_disabled' and image is None and len(calls)==1

def test_native_hyphen_across_separate_blocks(tmp_path):
    import fitz
    path=tmp_path/'hyphen.pdf'
    with fitz.open() as d:
        p=d.new_page();p.insert_text((40,80),'Indepen-',fontsize=16);p.insert_text((40,110),'dent archival record.',fontsize=16);d.save(path)
    hit,status=native_page(path,0,'Independent archival record.')
    assert status=='ok' and len(hit[1])==4
    assert normalize(hit[2])=='Independent archival record.'

@pytest.mark.asyncio
async def test_scoped_scan_duplicate_column_and_region_union():
    from PIL import ImageDraw
    from regional_knowledge.quote_proof import scoped_image,scoped_polygons
    image=Image.new('RGB',(1000,500),'white');draw=ImageDraw.Draw(image)
    draw.text((40,40),'Repeated citation.',fill='black');draw.text((600,40),'Repeated citation.',fill='black')
    boxes=[{'left':20,'top':50,'right':300,'bottom':150}]
    crop,bounds,mask=scoped_image(image,boxes)
    assert crop.width<300 and crop.height<100
    assert bounds[2]<600 # Another article's duplicate never reaches either reader.
    inside=[[[100,100],[800,100],[800,700],[100,700]]]
    result=scoped_polygons(inside,bounds,image,mask)
    assert all(20<=x<=300 and 50<=y<=150 for p in result for x,y in p)
    # Disjoint mapped regions must not authorize the bounding-box gap.
    boxes=[{'left':0,'top':0,'right':200,'bottom':200},{'left':800,'top':800,'right':1000,'bottom':1000}]
    crop,bounds,mask=scoped_image(image,boxes)
    assert crop.getpixel((600,40))==(255,255,255)
    with pytest.raises(ValueError,match='escapes mapped'):
        scoped_polygons([[[400,400],[600,400],[600,600],[400,600]]],bounds,image,mask)
