"""Distinguishing #504 P0: all accepted spans across pages, never a preview."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
import asyncio
import base64
import re
import threading

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from uuid import UUID, uuid5

import pytest

pymupdf = pytest.importorskip("pymupdf")
from regional_knowledge.contracts import StagePageInput
from regional_knowledge.source_unit_export import render_accepted_source_pdf
from regional_knowledge.source_unit_pdf import SourceUnitPdfError, SERVICE_CAPTION
from regional_knowledge.kaggle_source_read import SourceReadRequest, execute_source_read
from regional_knowledge.kaggle_public_backend import source_unit_pdf_result

SOURCE = "31111111-2222-4333-8444-555555555555"


@pytest.mark.asyncio
async def test_cancelled_pdf_export_retains_work_until_renderer_finishes(monkeypatch):
    from regional_knowledge import source_unit_export
    entered, finish = threading.Event(), threading.Event()
    def render(**kwargs):
        entered.set()
        assert finish.wait(2)
        return b'%PDF-synthetic'
    monkeypatch.setattr(source_unit_export, 'render_accepted_unit', render)
    connection = Connection(fixture_pages(complete=True))
    @asynccontextmanager
    async def acquire():
        yield connection
    task = asyncio.create_task(render_accepted_source_pdf(
        acquire=acquire, source_id=SOURCE, article_id='albinus-unit'))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done(), 'cancellation must retain work until the actual renderer exits'
    finally:
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task


def fixture_pages(*, complete, order=(0,1)):
    pages = []
    for position,index in enumerate(order):
        page_id = str(uuid5(UUID(SOURCE), f"page:1:{index}"))
        regions = [dict(region_key=f"r-{n}", kind="body", reading_order=n,
            source_text=f"Seite {index+1}, Abschnitt {n}: Принятый авторский текст №{index*18+n+1}.",
            bbox=dict(left=0,top=0,right=100,bottom=100)) for n in range(18)]
        markers = ([dict(article_id="albinus-unit",action="start",region_key="r-0",
            offset=0,kind="article",title="Немецкая статья / Eintrag")] if position==0 else
            [dict(article_id="albinus-unit",action="end",region_key="r-17",
                offset=len(regions[-1]["source_text"]))] if complete and position==len(order)-1 else [])
        page = StagePageInput.model_validate(dict(page_id=page_id,physical_page_index=index,
            previous_physical_page_index=order[position-1] if position else None,
            source_material="visual_reviewed",source_review_note="Reviewed fixture",
            source_unit_version=1,regions=regions,source_unit_boundaries=markers))
        pages.append(page.model_dump(mode="json",exclude_none=True)
            | {"printed_page_number":str(61+position)})
    return pages


def projection(p):
    return dict(page_id=p["page_id"],physical_page_index=p["physical_page_index"],
        previous_physical_page_index=p.get("previous_physical_page_index"),
        source_unit_boundaries=p["source_unit_boundaries"],
        regions=[dict(region_key=r["region_key"],reading_order=r["reading_order"],
            source_length=len(r["source_text"]),kind=r["kind"]) for r in p["regions"]])


class Cursor:
    def __init__(self,pages):
        self.pages=pages
        self.rows=()
        self.reads=[]
    async def execute(self,sql,params=None):
        self.reads.append(sql)
        if "read_source_unit_export_pages_v1" in sql:
            assert params==(SOURCE,[p["physical_page_index"] for p in self.pages])
            self.rows=[(p,dict(title="Проверенная книга",authors=["Автор"],
                publication_year=1902)) for p in self.pages]
        elif "read_source_unit_pages_v1" in sql:
            self.rows=[(p["physical_page_index"],projection(p),max(p["physical_page_index"] for p in self.pages)+1,False) for p in self.pages]
        else:
            self.rows=[]  # Existing no-media fixture still has no accepted images.
    async def fetchall(self):
        return self.rows
    async def __aenter__(self):
        return self
    async def __aexit__(self,*_):
        return False


class Connection:
    def __init__(self,pages):
        self.cursor_value=Cursor(pages)
        self.closed=False
        self.info=SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.row_factory=tuple_row
    def transaction(self):
        return self
    def cursor(self):
        return self.cursor_value
    async def __aenter__(self):
        return self
    async def __aexit__(self,*_):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize('order',[(0,1),(64,126,125)])
async def test_complete_all_span_model_order_mcp_pdf_and_incomplete_is_actionable(monkeypatch,order):
    pages=fixture_pages(complete=True,order=order)
    connection=Connection(pages)
    @asynccontextmanager
    async def acquire():
        yield connection

    async def frontier(cursor,execute,request,observed,require_dense=False):
        observed.update(data=1,projection=1,visibility=1,database="gen1")
    monkeypatch.setattr("regional_knowledge.kaggle_source_read._frontier",frontier)
    request=SourceReadRequest(request_id="synthetic-export",connection_generation=1,
        turn_generation=0,operation="source_unit_pdf",source_id=SOURCE,
        article_id="albinus-unit",limit=1,min_data_seq=0,deadline_ms=2000,
        actor=dict(person_id="reader",audience="personal",auth_epoch=1,
                   visibility_epoch=0,context_ref="request"))
    assert SourceReadRequest.model_validate(request.model_dump() | dict(
        operation='source_units',article_id=None,after_ordinal=8)).after_ordinal==8
    with pytest.raises(ValueError):
        SourceReadRequest.model_validate(request.model_dump() | dict(after_ordinal=8))
    native=await execute_source_read(request,acquire=acquire)
    assert native.status=="ok" and native.operation=="source_unit_pdf"
    assert len(native.records)==1
    # This is the exact MCP tools/call content, not merely a PDF helper's bytes.
    result=source_unit_pdf_result(native.records[0]).model_dump(
        mode="json",by_alias=True,exclude_none=True)
    assert result["content"][0]["type"]=="text"
    embedded=result["content"][1]
    assert embedded["type"]=="resource"
    assert embedded["resource"]["mimeType"]=="application/pdf"
    assert embedded["resource"]["uri"].startswith("rkb://source/")
    pdf=base64.b64decode(embedded["resource"]["blob"],validate=True)
    assert pdf.startswith(b"%PDF-") and embedded["resource"]["blob"]==native.records[0]["blob"]
    assert len([q for q in connection.cursor_value.reads
                if "read_source_unit_export_pages_v1" in q])==1
    with pymupdf.open(stream=pdf,filetype="pdf") as doc:
        assert len(doc)>=2
        assert "Проверенная книга" in doc[0].get_text()
        assert "Автор" in doc[0].get_text()
        bibliography=doc[0].get_text()
        labels=[str(61+i) for i in range(len(order))]
        assert [bibliography.index(label) for label in labels]==sorted(bibliography.index(label) for label in labels)
        assert SERVICE_CAPTION.replace(" ","") in re.sub(r"\s+","",doc[0].get_text())
        authored="".join(block[4] for page in list(doc)[1:]
            for block in page.get_text('blocks')
            if block[6]==0 and 49<=block[1]<page.rect.height-43)
        authored=authored.removeprefix('Немецкая статья / Eintrag\n')
        expected="".join(r["source_text"] for p in pages for r in p["regions"])
        assert re.sub(r"\s+","",authored)==re.sub(r"\s+","",expected)
        assert "Abschnitt 17" in authored and "Abschnitt 0" in authored
        # Every full accepted region: the 16-span public preview omits later pages.

    incomplete=Connection(fixture_pages(complete=False,order=order))
    @asynccontextmanager
    async def acquire_incomplete():
        yield incomplete
    failed=await execute_source_read(request,acquire=acquire_incomplete)
    assert failed.status=="unavailable" and failed.reason=="unit_not_ready"
    assert failed.records==()
    assert not any("read_source_unit_export_pages_v1" in q
                   for q in incomplete.cursor_value.reads)


@pytest.mark.asyncio
async def test_pdf_v2_optional_text_crosses_native_mcp_and_oversize_is_explicit(monkeypatch):
    pages=fixture_pages(complete=True)
    connection=Connection(pages)
    @asynccontextmanager
    async def acquire():
        yield connection
    async def frontier(cursor,execute,request,observed,require_dense=False):
        observed.update(data=1,projection=1,visibility=1,database='gen1')
    monkeypatch.setattr("regional_knowledge.kaggle_source_read._frontier",frontier)
    request=SourceReadRequest(request_id="v2-context",connection_generation=1,
        turn_generation=0,operation="source_unit_pdf",source_id=SOURCE,
        article_id="albinus-unit",limit=1,min_data_seq=0,deadline_ms=2000,
        annotation_ru="Краткое изложение по-русски.",
        translation_ru="Полный текст на русском языке.",
        actor=dict(person_id="reader",audience="personal",auth_epoch=1,
                   visibility_epoch=0,context_ref="request"))
    native=await execute_source_read(request,acquire=acquire)
    assert native.status=='ok' and len(native.records)==1
    embedded=source_unit_pdf_result(native.records[0]).content[1]
    with pymupdf.open(stream=base64.b64decode(embedded.resource.blob),filetype='pdf') as doc:
        assert 'Краткое изложение по-русски.' in doc[0].get_text()
        body=''.join(page.get_text() for page in list(doc)[1:])
        assert body.index('Seite 1') < body.index('Автоматический перевод')
        assert 'Полный текст на русском языке.' in body
        assert all('Страница PDF' not in page.get_text() for page in doc)
    with pytest.raises(ValueError,match='PDF additions require'):
        SourceReadRequest.model_validate(request.model_dump() | dict(
            operation='source_units',article_id=None))

    async def oversized(**_):
        return b'%PDF-' + b'x'*200_000
    monkeypatch.setattr('regional_knowledge.source_unit_export.render_accepted_source_pdf',oversized)
    denied=await execute_source_read(request,acquire=acquire)
    assert denied.status=='unavailable' and denied.reason=='pdf_transport_limit'
    assert denied.records==()  # Never a truncated/partial PDF.


@pytest.mark.asyncio
async def test_pdf_uses_only_accepted_figure_inside_unit_and_kaggle_page_pixels():
    from io import BytesIO
    from PIL import Image,ImageDraw
    from regional_knowledge.source_unit_export import _unit_figures
    from regional_knowledge.source_unit_export import complete_unit
    pages=fixture_pages(complete=True,order=(0,1))
    first=pages[0]
    for region in first['regions']:
        if region['reading_order']>=10:region['reading_order']+=1
    first['regions'].append(dict(region_key='fig-1',kind='figure',reading_order=10,
        source_text='',bbox=dict(left=100,top=100,right=800,bottom=800)))
    # A different figure after the accepted END marker on the final page must
    # never leak into this article just because it shares a source page.
    pages[1]['regions'].append(dict(region_key='fig-outside',kind='figure',
        reading_order=20,source_text='',bbox=dict(left=0,top=0,right=400,bottom=400)))

    def data(page,key,bbox,caption):
        return dict(page_id=page['page_id'],source_region_id=str(uuid5(
            UUID(page['page_id']),'region:'+key)),bbox=bbox,
            display_rotation_degrees=0,caption_text=caption,
            caption_region_ids=[],crop_binding=dict(original_sha256='a'*64))
    rows=[
        (data(pages[0],'fig-1',dict(left=100,top=100,right=800,bottom=800),
             'Принятая подпись Huldigung'),0,'import-accepted'),
        (data(pages[1],'fig-outside',dict(left=0,top=0,right=400,bottom=400),
             'Чужая подпись'),1,'import-accepted')]
    unit=complete_unit([projection(p) for p in pages],source_id=SOURCE,
        page_count=2,read_complete=False,article_id='albinus-unit')
    selected=_unit_figures(unit,pages,rows)
    assert len(selected)==1 and selected[0]['caption_text']=='Принятая подпись Huldigung'

    class MediaCursor(Cursor):
        async def execute(self,sql,params=None):
            if 'FROM rkb_v4.source_illustrations i' in sql:
                assert params[:2]==(SOURCE,[0,1])
                self.rows=rows
            else:await super().execute(sql,params)
    connection=Connection(pages)
    connection.cursor_value=MediaCursor(pages)
    @asynccontextmanager
    async def acquire():yield connection
    image=Image.new('RGB',(600,600),'white')
    ImageDraw.Draw(image).rectangle((60,60,480,480),fill='darkgreen')
    page_bytes=BytesIO();image.save(page_bytes,format='JPEG',quality=75)
    observed=[]
    async def native_page(figure):
        observed.append(figure['caption_text'])
        return page_bytes.getvalue()
    pdf=await render_accepted_source_pdf(acquire=acquire,source_id=SOURCE,
        article_id='albinus-unit',illustration_page=native_page)
    assert observed==['Принятая подпись Huldigung']
    with pymupdf.open(stream=pdf,filetype='pdf') as doc:
        assert sum(len(page.get_images()) for page in doc)==1
        text=''.join(page.get_text() for page in list(doc)[1:])
        assert text.index('Abschnitt 9')<text.index('Принятая подпись Huldigung')<text.index('Abschnitt 10')
        assert 'Исходная страница: 61' in text
        assert 'Чужая подпись' not in text
