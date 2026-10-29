"""Distinguishing #504 P0: all accepted spans across pages, never a preview."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
import base64
import re

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


def fixture_pages(*, complete):
    pages = []
    for index in range(2):
        page_id = str(uuid5(UUID(SOURCE), f"page:1:{index}"))
        regions = [dict(region_key=f"r-{n}", kind="body", reading_order=n,
            source_text=f"Seite {index+1}, Abschnitt {n}: Принятый авторский текст №{index*18+n+1}.",
            bbox=dict(left=0,top=0,right=100,bottom=100)) for n in range(18)]
        markers = ([dict(article_id="albinus-unit",action="start",region_key="r-0",
            offset=0,kind="article",title="Немецкая статья / Eintrag")] if index==0 else
            [dict(article_id="albinus-unit",action="end",region_key="r-17",
                offset=len(regions[-1]["source_text"]))] if complete else [])
        page = StagePageInput.model_validate(dict(page_id=page_id,physical_page_index=index,
            source_material="visual_reviewed",source_review_note="Reviewed fixture",
            source_unit_version=1,regions=regions,source_unit_boundaries=markers))
        pages.append(page.model_dump(mode="json",exclude_none=True)
            | {"printed_page_number":str(61+index)})
    return pages


def projection(p):
    return dict(page_id=p["page_id"],physical_page_index=p["physical_page_index"],
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
            assert params==(SOURCE,0,1)
            self.rows=[(p,dict(title="Проверенная книга",authors=["Автор"],
                publication_year=1902)) for p in self.pages]
        elif "read_source_unit_pages_v1" in sql:
            self.rows=[(p["physical_page_index"],projection(p),2,False) for p in self.pages]
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
async def test_complete_two_page_36_span_mcp_pdf_and_incomplete_is_actionable(monkeypatch):
    pages=fixture_pages(complete=True)
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
        assert "61" in doc[0].get_text() and "62" in doc[0].get_text()
        assert SERVICE_CAPTION.replace(" ","") in re.sub(r"\s+","",doc[0].get_text())
        authored="".join(page.get_text().removesuffix(f"Страница PDF {i}\n")
            for i,page in enumerate(doc,start=1) if i>1)
        expected="".join(r["source_text"] for p in pages for r in p["regions"])
        assert re.sub(r"\s+","",authored)==re.sub(r"\s+","",expected)
        assert "Abschnitt 17" in authored and "Abschnitt 0" in authored
        # 36 full accepted regions: the 16-span public preview would omit page 2.

    incomplete=Connection(fixture_pages(complete=False))
    @asynccontextmanager
    async def acquire_incomplete():
        yield incomplete
    failed=await execute_source_read(request,acquire=acquire_incomplete)
    assert failed.status=="unavailable" and failed.reason=="unit_not_ready"
    assert failed.records==()
    assert not any("read_source_unit_export_pages_v1" in q
                   for q in incomplete.cursor_value.reads)
