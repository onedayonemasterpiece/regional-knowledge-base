import hashlib
import json
from pathlib import Path

import httpx
import pytest

from regional_knowledge.contracts import ChatFile, Principal
from regional_knowledge.file_ingress import DownloadedFile, validated_public_https_host
from regional_knowledge.ingestion import PdfInfo, PdfRenderedPage, PyMuPdfProcessor
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
        self.puts.append((key, bytes(data), content_type))

    async def get_bytes(self, key):
        return self.source

    async def get_range(self, key, start, end):
        raise AssertionError("ingestion pages do not use a text range")

    async def put_file(self, key, path, content_type):
        data = Path(path).read_bytes()
        self.puts.append((key, data, content_type))

    async def download_file(self, key, path):
        assert key == "private/source.pdf"
        Path(path).write_bytes(self.source)


class FakeDownloader:
    def __init__(self, data=b"%PDF-fake"):
        self.data = data
        self.urls = []

    async def download(self, url, directory):
        self.urls.append(url)
        path = Path(directory) / "downloaded.pdf"
        path.write_bytes(self.data)
        return DownloadedFile(
            path=path,
            sha256=hashlib.sha256(self.data).hexdigest(),
            size_bytes=len(self.data),
        )


class FakePdfProcessor:
    async def inspect_file(self, path):
        assert Path(path).read_bytes().startswith(b"%PDF-")
        return PdfInfo(page_count=2, title="PDF metadata title")

    async def render_file(self, path, *, start, count):
        assert Path(path).read_bytes().startswith(b"%PDF-")
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


def test_download_url_requires_plain_public_https_hostname():
    assert validated_public_https_host("https://files.example/book.pdf") == "files.example"
    for bad in (
        "http://files.example/book.pdf",
        "https://localhost/book.pdf",
        "https://127.0.0.1/book.pdf",
        "https://user:pass@files.example/book.pdf",
        "https://files.example/book.pdf#fragment",
    ):
        with pytest.raises(ValueError):
            validated_public_https_host(bad)


@pytest.mark.asyncio
async def test_start_ingestion_streams_private_source_file_and_activates_staged_job():
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
        if request.method=='PATCH' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(204)
        if request.method=='GET' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(200,json=[{'owner_user_id':principal().subject}])
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
        if request.method=='PATCH' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(204)
        if request.method=='GET' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(200,json=[{'owner_user_id':principal().subject}])
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


@pytest.mark.parametrize("cursor", ["0", "text:0:0:1000"])
@pytest.mark.asyncio
async def test_book_pages_downloads_to_file_after_user_job_authorization(cursor):
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
        if request.method=='PATCH' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(204)
        if request.method=='GET' and request.url.path=='/rest/v1/rkb_documents':
            return httpx.Response(200,json=[{'owner_user_id':principal().subject}])
        raise AssertionError((request.method, request.url))

    class CursorProcessor(FakePdfProcessor):
        async def render_file(self, path, *, start, count, text_part=None):
            if text_part is None:
                return await super().render_file(path, start=start, count=count)
            assert start == 0 and count == 1 and text_part == (0, 1000)
            total, pages = await super().render_file(path, start=0, count=2)
            from dataclasses import replace
            return total, (replace(pages[0], native_text_info={"original_length": 2001, "truncated": True}),)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        config(),
        client=client,
        object_store=FakeStore(source),
        pdf_processor=CursorProcessor(),
    )
    batch = await backend.book_pages(
        ingestion_id="44444444-4444-4444-4444-444444444444",
        principal=principal(),
        cursor=cursor,
        batch_size=2,
    )
    await client.aclose()

    assert batch.next_cursor == (None if cursor == "0" else "1")
    assert [page.physical_page_index for page in batch.pages] == ([0, 1] if cursor == "0" else [0])
    assert batch.pages[0].native_text == "Первая страница"
    assert batch.pages[0].native_blocks[0]["bbox"]["left"] == 10
    if cursor == "0":
        assert batch.pages[0].page_id != batch.pages[1].page_id
    else:
        assert batch.pages[0].native_text_info["original_length"] == 2001


@pytest.mark.asyncio
async def test_real_pymupdf_processor_returns_text_blocks_and_jpeg(tmp_path):
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    page = document.new_page(width=595, height=842)
    page.insert_text((72, 100), "Koenigsberg 1930")
    data = document.tobytes()
    document.close()
    path = tmp_path / "book.pdf"
    path.write_bytes(data)

    processor = PyMuPdfProcessor()
    info = await processor.inspect_file(path)
    total, pages = await processor.render_file(path, start=0, count=1)

    assert info.page_count == 1
    assert total == 1
    assert len(pages) == 1
    assert pages[0].mime_type == "image/jpeg"
    assert pages[0].data.startswith(b"\xff\xd8")
    assert "Koenigsberg 1930" in (pages[0].native_text or "")
    assert pages[0].native_blocks


@pytest.mark.asyncio
async def test_native_block_preview_continuations_are_bounded_and_repeatable(tmp_path):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=600, height=2000)
    page.insert_text((50, 50), "\n".join(f"Line {i:03} " + "material " * 5 for i in range(80)), fontsize=10)
    expected = page.get_text("blocks")[0][4].strip()
    assert len(expected) > 2000
    path = tmp_path / "long-block.pdf"
    doc.save(path)
    doc.close()
    processor = PyMuPdfProcessor()
    _, pages = await processor.render_file(path, start=0, count=1)
    block = pages[0].native_blocks[0]
    assert block["original_length"] == len(expected)
    assert len(block["text"]) == 1000
    assert block["truncated"] and block["material"] == "preview"
    pieces = [block["text"]]
    while block["continuation"]:
        _, index, number, offset = block["continuation"].split(":")
        _, continued = await processor.render_file(path, start=int(index), count=1, text_part=(int(number), int(offset)))
        _, repeated = await processor.render_file(path, start=int(index), count=1, text_part=(int(number), int(offset)))
        block = continued[0].native_blocks[0]
        assert repeated[0].native_blocks == continued[0].native_blocks
        assert 0 < len(block["text"]) <= 1000
        pieces.append(block["text"])
    assert "".join(pieces) == expected
    assert block["end"] == len(expected)
    assert pages[0].native_text_info["requires_visual_review"]
    with pytest.raises(ValueError):
        await processor.render_file(path, start=0, count=1, text_part=(0, len(expected)))


@pytest.mark.asyncio
@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
async def test_rotated_pdf_native_block_bbox_uses_display_frame(tmp_path, rotation):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=300, height=500)
    page.insert_text((40, 80), "Rotated evidence block", fontsize=12)
    page.set_rotation(rotation)
    path = tmp_path / f"rotated-{rotation}.pdf"
    doc.save(path)
    doc.close()

    with fitz.open(path) as check:
        page = check.load_page(0)
        raw = next(block for block in page.get_text("blocks", sort=False) if str(block[4]).strip())
        expected_rect = fitz.Rect(*map(float, raw[:4]))
        if page.rotation:
            expected_rect = expected_rect * page.rotation_matrix
        rect = page.rect
        expected = {
            "left": max(0, min(1000, round(expected_rect.x0 / rect.width * 1000))),
            "top": max(0, min(1000, round(expected_rect.y0 / rect.height * 1000))),
            "right": max(0, min(1000, round(expected_rect.x1 / rect.width * 1000))),
            "bottom": max(0, min(1000, round(expected_rect.y1 / rect.height * 1000))),
        }

    _, pages = await PyMuPdfProcessor().render_file(path, start=0, count=1)
    assert pages[0].native_blocks[0]["bbox"] == expected


@pytest.mark.asyncio
async def test_textless_image_page_requires_visual_review(tmp_path):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=100, height=100)
    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 10, 10))
    pix.clear_with(100)
    page.insert_image(page.rect, stream=pix.tobytes("png"))
    path = tmp_path / "image-only.pdf"
    doc.save(path)
    doc.close()
    _, pages = await PyMuPdfProcessor().render_file(path, start=0, count=1)
    assert pages[0].data.startswith(b"\xff\xd8")
    assert pages[0].native_text is None
    assert pages[0].native_blocks == ()
    assert pages[0].native_text_info["original_length"] == 0
    assert pages[0].native_text_info["requires_visual_review"] is True


@pytest.mark.asyncio
async def test_native_blocks_beyond_preview_cap_remain_addressable(tmp_path):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=300, height=9000)
    for i in range(82):
        page.insert_text((20, 50+i*100), f"Native block {i}", fontsize=10)
    assert len(page.get_text("blocks")) == 82
    path = tmp_path / "many-blocks.pdf"
    doc.save(path)
    doc.close()
    processor = PyMuPdfProcessor()
    _, initial = await processor.render_file(path, start=0, count=1)
    info = initial[0].native_text_info
    assert len(initial[0].native_blocks) == 80
    assert info["blocks_truncated"] and info["block_count"] == 82
    assert info["blocks_continuation"] == "text:0:80:0"
    _, continued = await processor.render_file(path, start=0, count=1, text_part=(80,0))
    assert continued[0].native_blocks[0]["text"] == "Native block 80"
    assert continued[0].native_text_info["blocks_continuation"] == "text:0:81:0"
    _, last = await processor.render_file(path, start=0, count=1, text_part=(81,0))
    assert last[0].native_blocks[0]["text"] == "Native block 81"
    assert last[0].native_text_info["blocks_continuation"] is None
