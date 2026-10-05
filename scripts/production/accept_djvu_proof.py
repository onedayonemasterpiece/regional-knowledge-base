"""Real deterministic DjVu decode and on-demand Flash-Lite proof on own fixture."""
import argparse,asyncio,hashlib,json,os,shlex
from pathlib import Path
from uuid import uuid4
import fitz
from PIL import Image,ImageDraw
from regional_knowledge.source_adapter import SourceProcessor
from regional_knowledge.source_adapter import DjVuProcessor
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.contracts import Principal
from regional_knowledge.quote_proof import source_proof
async def run(root):
 root.mkdir(mode=0o700,parents=True,exist_ok=True)
 for raw in Path('/home/dev/.env').read_text().splitlines():
  try:p=shlex.split(raw)
  except ValueError:continue
  if len(p)==1 and p[0].startswith(os.getenv('RKB_PROOF_KEY_ENV','GOOGLE_API_KEY')+'='):os.environ['RKB_GEMINI_API_KEY']=p[0].split('=',1)[1]
 os.environ['RKB_SCAN_PROOF_ENABLED']='1';os.environ['RKB_PROOF_CACHE_DIR']=str(root/'proofs');os.environ['RKB_ORIGINAL_CACHE_DIR']=str(root/'originals')
 quote='Archive record 1945.'
 with fitz.open() as pdf:
  p=pdf.new_page(width=700,height=350);p.insert_text((40,80),quote,fontsize=16);p.insert_text((380,80),'Unrelated column 1234.',fontsize=16);pix=p.get_pixmap(matrix=fitz.Matrix(2,2));pix.save(root/'source.ppm')
 with fitz.open() as reference:
  p=reference.new_page(width=700,height=350);p.insert_text((40,80),quote,fontsize=16)
  words=p.get_text('words')
 processor=DjVuProcessor();processor.command('c44',root/'source.ppm',root/'source.djvu');data=(root/'source.djvu').read_bytes();sha=hashlib.sha256(data).hexdigest()
 class Store:
  async def download_file(self,key,path):Path(path).write_bytes(data)
 b=SQLiteBackend(corpus_path=root/'corpus.sqlite3',embedder=LexicalOnlyEmbedder(),object_store=Store());owner,doc,page,region,chunk,obj=[str(uuid4()) for _ in range(6)];textsha=hashlib.sha256(quote.encode()).hexdigest()
 b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':owner}]);b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'source_sha256':sha,'source_format':'djvu','title':'Own DjVu control','active_revision':1,'page_count':1}]);b.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,'document_id':doc,'revision':1,'physical_page_index':0}]);b.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,'page_id':page,'kind':'body','source_text':quote,'reading_order':0,'bbox':{'left':0,'top':0,'right':500,'bottom':500}}]);b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'title':'Evidence','region_ids':[region],'page_ids':[page],'source_text':quote,'search_material':quote,'text_sha256':textsha,'search_material_sha256':textsha}]);b.corpus.put('rkb_objects',[{**defaults('rkb_objects'),'id':obj,'document_id':doc,'kind':'source_pdf','object_key':'own.djvu','sha256':sha}]);b.corpus.build_fragments();actor=Principal(subject=owner,client_id='acceptance',issuer='local',access_token='internal');results=[]
 try:
  for label in ('cold','warm','revoked'):
   if label=='revoked':os.environ['RKB_SCAN_PROOF_ENABLED']='0'
   result,image=await source_proof(b,actor,chunk,quote,0);results.append({'case':label,'metadata':result,'image':bool(image)})
   if image:(root/(label+'.webp')).write_bytes(image)
   (root/'djvu.json').write_text(json.dumps(results,indent=2))
  assert results[0]['metadata']['status']=='ok' and results[0]['metadata']['method']=='model_localized'
  assert results[1]['metadata']['cache_hit'] and not results[1]['metadata']['model_calls']
  assert results[2]['metadata']['status']!='ok' and not results[2]['image']
  # Independent reference word boxes plus actual decoded source ink, not yellow pixel count.
  _,pages=await SourceProcessor().render_file(root/'source.djvu',start=0,count=1)
  import io
  source=Image.open(io.BytesIO(pages[0].data)).convert('RGB');source.thumbnail((1800,1800))
  stripe=Image.new('L',source.size,0);draw=ImageDraw.Draw(stripe)
  for poly in results[0]['metadata']['polygons']:draw.polygon([(x*source.width/1000,y*source.height/1000) for x,y in poly],fill=255)
  gray=source.convert('L');coverage=[]
  for word in words:
   box=(int(word[0]/700*source.width),int(word[1]/350*source.height),int(word[2]/700*source.width)+1,int(word[3]/350*source.height)+1)
   dark=list(gray.crop(box).getdata());marked=list(stripe.crop(box).getdata());total=sum(v<170 for v in dark)
   coverage.append(sum(v<170 and h>0 for v,h in zip(dark,marked,strict=True))/total if total else 0)
  (root/'word-coverage.json').write_text(json.dumps({'reference_word_ink_coverage':coverage,'source_sha256':sha},indent=2))
  assert coverage and min(coverage)>=0.99
  print(json.dumps({'reference_word_ink_coverage':coverage,'real_djvu':True,'cold_seconds':results[0]['metadata']['seconds'],'warm_seconds':results[1]['metadata']['seconds'],'revocation_rejected':True}))
 finally:await b.aclose()
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);asyncio.run(run(p.parse_args().evidence))
