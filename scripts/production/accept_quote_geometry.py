"""Real independent native and Flash-Lite page-localization acceptance.

Own generated source pages, with independently known word coordinates. No
corpus OCR/reimport; only requested rendered pages are sent to the reader.
"""
import argparse,asyncio,io,json,os,shlex,time
import httpx
from pathlib import Path
import fitz
from PIL import Image,ImageDraw
from regional_knowledge.quote_proof import FlashLiteLocator,native_page,paint

async def run(out):
    out.mkdir(parents=True,exist_ok=True)
    for raw in Path('/home/dev/.env').read_text().splitlines():
        try:p=shlex.split(raw)
        except ValueError:continue
        if len(p)==1 and p[0].startswith(os.getenv('RKB_PROOF_KEY_ENV','GOOGLE_API_KEY')+'='):os.environ['RKB_GEMINI_API_KEY']=p[0].split('=',1)[1]
    report=[]
    for name,rotate,repeat in [('columns',0,False),('rotation',90,False),('hyphen',0,False),('repeat',0,True)]:
        path=out/(name+'.pdf');quote='Archive record 1945.' if name!='hyphen' else 'Independent archival record.'
        with fitz.open() as pdf:
            page=pdf.new_page(width=700,height=350)

            if name=='hyphen':
                page.insert_text((40,80),'Indepen-',fontsize=16);page.insert_text((40,110),'dent archival record.',fontsize=16)
            else:page.insert_text((40,80),quote,fontsize=16)
            page.insert_text((380,80),'Unrelated column 1234.',fontsize=16)
            page.insert_text((40,200),quote if repeat else 'Independent lower paragraph.',fontsize=16)
            if rotate:page.set_rotation(rotate)
            pdf.save(path)
        hit,status=native_page(path,0,quote)
        report.append({'case':name,'path':'native','status':status,'expected':'highlight_unavailable' if repeat else 'ok'})
        if hit:(out/(name+'-native.webp')).write_bytes(paint(hit[0],hit[1]))
        report.append({'case':name+'_wrong_quote','path':'native','status':native_page(path,0,'Invented quotation.')[1],'expected':'quote_not_found'})
        with fitz.open(path) as pdf:
            page=pdf[0];rect=page.rect
            pix=page.get_pixmap(matrix=fitz.Matrix(2,2));image=Image.open(io.BytesIO(pix.tobytes('png'))).convert('RGB');image.thumbnail((1800,1800))
            reference=[fitz.Rect(*w[:4])*page.rotation_matrix for w in page.get_text('words') if w[0]<350 and w[1]<120]
        started=time.monotonic()
        try:
            polys,visible,calls=await FlashLiteLocator().locate(image,quote)
            # Compare the actual proposed word area with independent native
            # geometry. A mere yellow pixel test cannot satisfy this check.
            covered=[];ink_coverage=[]
            stripe=Image.new('L',image.size,0);draw=ImageDraw.Draw(stripe)
            for poly in polys:draw.polygon([(x*image.width/1000,y*image.height/1000) for x,y in poly],fill=255)
            gray=image.convert('L')
            for r in reference:
                x0,y0,x1,y1=r.x0/rect.width*1000,r.y0/rect.height*1000,r.x1/rect.width*1000,r.y1/rect.height*1000
                best=0
                for p in polys:
                    px=[q[0] for q in p];py=[q[1] for q in p]
                    overlap=max(0,min(x1,max(px))-max(x0,min(px)))*max(0,min(y1,max(py))-max(y0,min(py)))
                    best=max(best,overlap/((x1-x0)*(y1-y0)))
                covered.append(best)
                box=(max(0,int(x0*image.width/1000)),max(0,int(y0*image.height/1000)),min(image.width,int(x1*image.width/1000)+1),min(image.height,int(y1*image.height/1000)+1))
                dark=list(gray.crop(box).getdata());highlight=list(stripe.crop(box).getdata())
                total=sum(v<170 for v in dark)
                ink_coverage.append(sum(v<170 and h>0 for v,h in zip(dark,highlight,strict=True))/total if total else 0)
            good=bool(covered) and min(covered)>.65 and not repeat
            (out/(name+'-scan.webp')).write_bytes(paint(image,polys))
            report.append({'case':name,'path':'flash_lite_scan','status':'ok' if good else 'geometry_miss','reference_word_coverage':covered,'reference_word_ink_coverage':ink_coverage,'expected':'highlight_unavailable' if repeat else 'ok','calls':calls,'seconds':time.monotonic()-started})
        except (ValueError,RuntimeError,httpx.HTTPError) as error:report.append({'case':name,'path':'flash_lite_scan','status':'highlight_unavailable','reason':type(error).__name__,'seconds':time.monotonic()-started})
    (out/'geometry.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);asyncio.run(run(p.parse_args().evidence))
