"""Articles inside an issue reuse immutable source/page/region/chunk IDs."""
import json
from uuid import UUID,uuid5
from .contracts import CatalogMetadata,StartMetadataInput,BookIngestOutput
from .sqlite_corpus import canonical


def create(corpus,actor,payload):
    metadata=StartMetadataInput.model_validate(payload)
    catalog=metadata.catalog
    if not catalog or catalog.kind!='article' or not catalog.parent_id or not catalog.region_ids:raise ValueError('component article requires parent issue and semantic region_ids')
    parent=corpus.authorize(actor,catalog.parent_id,owner=True)
    if (parent.get('catalog') or {}).get('kind')!='journal_issue' or not parent['active_revision']:raise ValueError('selected journal issue required')
    pages={p['id']:p for p in corpus.rows('rkb_pages',{'document_id':parent['id'],'revision':parent['active_revision']})}
    regions=[corpus.one('rkb_regions',str(UUID(r))) for r in catalog.region_ids]
    if not all(r and r['page_id'] in pages for r in regions):raise ValueError('article regions escape issue revision')
    physical=sorted(set(pages[r['page_id']]['physical_page_index'] for r in regions))
    if catalog.physical_page_start is not None and physical[0]!=catalog.physical_page_start or catalog.physical_page_end is not None and physical[-1]!=catalog.physical_page_end:raise ValueError('article physical range mismatch')
    chunks=[];refs=set(catalog.region_ids)
    for c in corpus.rows('rkb_chunks',{'document_id':parent['id'],'revision':parent['active_revision']}):
        cr=set(c['region_ids']+c['footnote_region_ids'])
        if cr&refs:
            if not cr<=refs:raise ValueError('article boundary cuts a chunk; stage separate semantic chunks before activation')
            chunks.append(c['id'])
    if not chunks:raise ValueError('article has no mapped evidence')
    ident=str(uuid5(UUID(parent['id']),canonical([parent['active_revision'],metadata.title,catalog.region_ids])))
    row={'id':ident,'parent_id':parent['id'],'title':metadata.title or 'Untitled article','authors':metadata.authors,'publication_year':metadata.publication_year,'language':metadata.language,'active_revision':parent['active_revision'],'page_count':len(physical),'source_format':parent.get('source_format'),'source_archive_status':parent.get('source_archive_status'),'source_document_id':parent['id'],'source_sha256':parent['source_sha256'],'page_ids':list(dict.fromkeys(r['page_id'] for r in regions)),'chunk_ids':chunks,'catalog':{**catalog.model_dump(mode='json',exclude_none=True),'physical_page_start':physical[0],'physical_page_end':physical[-1]}}
    with corpus.connect() as db:
        old=db.execute('select payload from catalog_components where id=?',(ident,)).fetchone()
        if old and old[0]!=canonical(row):raise ValueError('component identity conflict')
        for chunk in chunks:
            previous=db.execute('select component_id from component_chunks where chunk_id=?',(chunk,)).fetchone()
            if previous and previous[0]!=ident:raise ValueError('overlapping article semantic range')
            db.execute('insert into component_chunks values(?,?) on conflict(chunk_id) do nothing',(chunk,ident))
        db.execute('insert into catalog_components values(?,?,?,?) on conflict(id) do nothing',(ident,parent['id'],parent['active_revision'],canonical(row)))
    return BookIngestOutput(document_id=ident,state='finalized',next_action='done',message='Article registered with shared issue original and exact semantic ranges')


def visible(corpus,actor):
    rows=[]
    with corpus.connect() as db:raw=db.execute('select payload from catalog_components').fetchall()
    for item in raw:
        row=json.loads(item[0])
        try:parent=corpus.authorize(actor,row['parent_id'])
        except (PermissionError,LookupError):continue
        # Catalog can preserve an old issue component, with its frozen revision.
        rows.append(row)
    return rows

async def cover(backend,principal,document_id):
    import asyncio,hashlib,io,os,tempfile
    from pathlib import Path
    from PIL import Image
    from .source_archive import download_source
    entry=await backend.catalog(principal,document_id=document_id)
    source_id=entry.get('source_document_id',entry['id'])
    doc=backend.corpus.authorize(principal.subject,source_id,owner=True)
    reference=entry['catalog'].get('cover_page');kind=entry['catalog'].get('cover_kind')
    if reference is None:return {'status':'cover_unknown','document_id':document_id},None
    if not kind or not 0<=reference<doc['page_count']:raise ValueError('invalid cover reference')
    objects=backend.corpus.rows('rkb_objects',{'document_id':doc['id'],'kind':'source_pdf','sha256':doc['source_sha256']})
    if not objects:return {'status':'highlight_unavailable','reason':'original_unavailable'},None
    work=Path(os.getenv('RKB_PROOF_WORK_DIR','/home/dev/.local/state/regional-knowledge-base/proof-work'));work.mkdir(mode=0o700,parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        path=Path(tmp)/('source.'+(doc.get('source_format') or 'pdf'))
        await download_source(backend,principal,doc['id'],objects[0],path)
        _,pages=await backend.pdf_processor.render_file(path,start=reference,count=1)
        image=Image.open(io.BytesIO(pages[0].data)).convert('RGB');image.thumbnail((1800,1800));out=io.BytesIO();image.save(out,'WEBP',quality=92)
        return {'status':'ok','document_id':document_id,'source_document_id':doc['id'],'source_sha256':doc['source_sha256'],'physical_page_index':reference,'kind':kind,'revision':entry['active_revision']},out.getvalue()
