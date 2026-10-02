import hashlib
import json

import httpx
import pytest

from regional_knowledge.contracts import ChatFile, Principal
from regional_knowledge.ingestion import (
    PdfInfo,
    PdfRenderedPage,
    PyMuPdfProcessor,
    _validate_download_url,
)
from regional_knowledge.supabase_backend import SupabaseConfig, SupabaseRestBackend


def principal():
    return Principal(
        subject="11111111-1111-1111-1111-111111111111",
        client_id="chatgpt-knowledge",
        issuer="https://issuer.example/auth/v1",
        access_token="user-jwt",
    )


class FakeStore:
    def __init__(self, source=b"%PDF-fake"):
        self.source = source
        self.puts = []

    async def put_bytes(self, key, data, content_type):
        self.puts.append((key, data, content_type))

    async def get_bytes(self, key):
        assert key == "private/source.pdf"
        return self.source

    async def get_range(self, key, start, end):
        raise AssertionError("ingestion pages read the source PDF, not a text range")


class FakeDownloader:
    def __init__(self, data=b"%PDF-fake"):
        self.data = data
        self.files = []

    async def download(self, file):
        self.files.append(file.file_id)
        return self.data


class FakePdfProcessor:
    async def inspect(self, data):
        assert data.startswith(b"%PDF-")
        return PdfInfo(page_count=2, title="PDF metadata title")

    async def render(self, data, *, start, count):
        assert data.startswith(b"%PDF-")
        assert start == 0
        assert count == 2
        return 2, (
            PdfRenderedPage(
                physical_page_index=0,
                mime_type="image/jpeg",
                data=b"jpeg-0",
                native_text="Первая страница",
                native_blocks=(
                    {
                        "bbox": {"left": 10, "top": 20, "right": 300, "bottom": 80},
                        "text": "Первая страница",
                    },
                ),
            ),
            PdfRenderedPage(
                physical_page_index=1,
                mime_type="image/jpeg",
                data=b"jpeg-1",
                native_text=None,
                native_blocks=(),
            ),
        )


def config():
    return SupabaseConfig(
        url="https://db.example",
        anon_key="anon",
        public_base_url="https://knowledge.example",
        service_role_key="service-role",
    )


def attached_file():
    return ChatFile(
        download_url="https://files.example/book.pdf",
        file_id="file_123",
        mime_type="application/pdf",
        file_name="book.pdf",
    )


def test_download_url_requires_plain_public_https():
    assert _validate_download_url("https://files.example/book.pdf").startswith("https://")
    for bad in (
        "http://files.example/book.pdf",
        "https://localhost/book.pdf",
        "https://127.0.0.1/book.pdf",
        "https://user:pass@files.example/book.pdf",
        "https://files.example/book.pdf#fragment",
    ):
        with pytest.raises(ValueError):
            _validate_download_url(bad)


@pytest.mark.asyncio
async def test_start_ingestion_stores_private_source_and_activates_staged_job():
    calls = []
    source = b"%PDF-fake"
    store = FakeStore(source)

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.headers.get("authorization")))
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_ingestion_jobs":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/rest/v1/rpc/rkb_start_ingestion":
            assert request.headers["authorization"] == "Bearer user-jwt"
            body = json.loads(request.content)
            store.document_id = body["p_document_id"]
            assert body["p_title"] == "Восточная Пруссия"
            assert body["p_page_count"] == 2
            assert body["p_source_sha256"] == hashlib.sha256(source).hexdigest()
            return httpx.Response(200, json=[{
                "ingestion_id": body["p_ingestion_id"],
                "document_id": body["p_document_id"],
            }])
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer service-role"
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer service-role"
            return httpx.Response(201, json={})
        if request.method == "PATCH" and request.url.path == "/rest/v1/rkb_ingestion_jobs":
            assert request.headers["authorization"] == "Bearer user-jwt"
            body = json.loads(request.content)
            return httpx.Response(200, json=[{
                "id": request.url.params["id"].removeprefix("eq."),
                "document_id": store.document_id,
                "state": body["state"],
                "cursor": body.get("cursor", "0"),
                "warnings": [],
            }])
        raise AssertionError((request.method, request.url))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        config(),
        client=client,
        object_store=store,
        file_downloader=FakeDownloader(source),
        pdf_processor=FakePdfProcessor(),
    )

    result = await backend.book_ingest(
        command="start",
        principal=principal(),
        file=attached_file(),
        ingestion_id=None,
        cursor=None,
        payload={"title": "Восточная Пруссия", "authors": ["Автор"]},
    )
    await client.aclose()

    assert result.state == "staged"
    assert result.document_id == store.document_id
    assert result.next_cursor == "0"
    assert len(store.puts) == 1
    key, payload, mime = store.puts[0]
    assert payload == source and mime == "application/pdf"
    assert key.endswith(f"/source/{hashlib.sha256(source).hexdigest()}.pdf")
    assert ("GET", "/rest/v1/rkb_objects", "Bearer service-role") in calls
    assert ("POST", "/rest/v1/rkb_objects", "Bearer service-role") in calls


@pytest.mark.asyncio
async def test_failed_start_reuses_same_ingestion_and_existing_source_object():
    source = b"%PDF-fake"
    source_hash = hashlib.sha256(source).hexdigest()
    document_id = "22222222-2222-2222-2222-222222222222"
    ingestion_id = "44444444-4444-4444-4444-444444444444"
    object_id = "33333333-3333-3333-3333-333333333333"
    calls = []
    store = FakeStore(source)

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_ingestion_jobs":
            return httpx.Response(200, json=[{
                "id": ingestion_id,
                "document_id": document_id,
                "source_file_id": "file_123",
                "source_object_id": None,
                "source_sha256": source_hash,
                "state": "failed",
                "cursor": "0",
                "staged_revision": 1,
                "warnings": [],
                "error_code": "network",
                "created_at": "2026-10-02T00:00:00Z",
            }])
        if request.method == "PATCH" and request.url.path == "/rest/v1/rkb_ingestion_jobs":
            body = json.loads(request.content)
            return httpx.Response(200, json=[{
                "id": ingestion_id,
                "document_id": document_id,
                "state": body["state"],
                "cursor": body.get("cursor", "0"),
                "warnings": [],
            }])
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_objects":
            return httpx.Response(200, json=[{
                "id": object_id,
                "object_key": (
                    f"users/{principal().subject}/documents/{document_id}/"
                    f"source/{source_hash}.pdf"
                ),
                "sha256": source_hash,
                "mime_type": "application/pdf",
            }])
        if request.url.path == "/rest/v1/rpc/rkb_start_ingestion":
            raise AssertionError("failed retry must not create another document/job")
        if request.method == "POST" and request.url.path == "/rest/v1/rkb_objects":
            raise AssertionError("existing source object row must be reused")
        raise AssertionError((request.method, request.url))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        config(),
        client=client,
        object_store=store,
        file_downloader=FakeDownloader(source),
        pdf_processor=FakePdfProcessor(),
    )
    result = await backend.book_ingest(
        command="start",
        principal=principal(),
        file=attached_file(),
        ingestion_id=None,
        cursor=None,
        payload=None,
    )
    await client.aclose()

    assert result.ingestion_id == ingestion_id
    assert result.document_id == document_id
    assert result.state == "staged"
    assert len(store.puts) == 1
    assert ("POST", "/rest/v1/rpc/rkb_start_ingestion") not in calls
    assert ("POST", "/rest/v1/rkb_objects") not in calls


@pytest.mark.asyncio
async def test_book_pages_requires_user_job_then_server_only_source_lookup():
    source = b"%PDF-fake"
    source_hash = hashlib.sha256(source).hexdigest()
    document_id = "22222222-2222-2222-2222-222222222222"
    object_id = "33333333-3333-3333-3333-333333333333"

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_ingestion_jobs":
            assert request.headers["authorization"] == "Bearer user-jwt"
            return httpx.Response(200, json=[{
                "id": "44444444-4444-4444-4444-444444444444",
                "document_id": document_id,
                "source_file_id": "file_123",
                "source_object_id": object_id,
                "source_sha256": source_hash,
                "state": "staged",
                "cursor": "0",
                "staged_revision": 1,
                "warnings": [],
                "error_code": None,
                "created_at": "2026-10-02T00:00:00Z",
            }])
        if request.method == "GET" and request.url.path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer service-role"
            assert request.url.params["id"] == f"eq.{object_id}"
            assert request.url.params["document_id"] == f"eq.{document_id}"
            return httpx.Response(200, json=[{
                "id": object_id,
                "object_key": "private/source.pdf",
                "sha256": source_hash,
                "mime_type": "application/pdf",
            }])
        raise AssertionError((request.method, request.url))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        config(),
        client=client,
        object_store=FakeStore(source),
        pdf_processor=FakePdfProcessor(),
    )
    batch = await backend.book_pages(
        ingestion_id="44444444-4444-4444-4444-444444444444",
        principal=principal(),
        cursor="0",
        batch_size=2,
    )
    await client.aclose()

    assert batch.next_cursor is None
    assert [page.physical_page_index for page in batch.pages] == [0, 1]
    assert batch.pages[0].native_text == "Первая страница"
    assert batch.pages[0].native_blocks[0]["bbox"]["left"] == 10
    assert batch.pages[0].page_id != batch.pages[1].page_id


@pytest.mark.asyncio
async def test_real_pymupdf_processor_returns_text_blocks_and_jpeg():
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    page = document.new_page(width=595, height=842)
    page.insert_text((72, 100), "Koenigsberg 1930")
    data = document.tobytes()
    document.close()

    processor = PyMuPdfProcessor()
    info = await processor.inspect(data)
    total, pages = await processor.render(data, start=0, count=1)

    assert info.page_count == 1
    assert total == 1
    assert len(pages) == 1
    assert pages[0].mime_type == "image/jpeg"
    assert pages[0].data.startswith(b"\xff\xd8")
    assert "Koenigsberg 1930" in (pages[0].native_text or "")
    assert pages[0].native_blocks
