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
