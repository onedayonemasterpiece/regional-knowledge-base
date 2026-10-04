import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid5

import httpx
import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.supabase_backend import SupabaseConfig, SupabaseRestBackend


DOCUMENT_ID = "22222222-2222-2222-2222-222222222222"
INGESTION_ID = "44444444-4444-4444-4444-444444444444"
SOURCE_OBJECT_ID = "33333333-3333-3333-3333-333333333333"


def principal():
    return Principal(
        subject="11111111-1111-1111-1111-111111111111",
        client_id="chatgpt-knowledge",
        issuer="https://issuer.example/auth/v1",
        access_token="user-jwt",
    )


def make_pdf() -> bytes:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    page = document.new_page(width=600, height=800)
    page.insert_text((70, 100), "East Prussia 1930")
    page.insert_text((70, 650), "Historical photograph")
    data = document.tobytes()
    document.close()
    return data


class FakeStore:
    def __init__(self, source_pdf: bytes):
        self.objects = {"private/source.pdf": source_pdf}

    async def put_bytes(self, key, data, content_type):
        self.objects[key] = bytes(data)

    async def get_bytes(self, key):
        return self.objects[key]

    async def get_range(self, key, start, end):
        return self.objects[key][start:end]

    async def put_file(self, key, path, content_type):
        self.objects[key] = Path(path).read_bytes()

    async def download_file(self, key, path):
        Path(path).write_bytes(self.objects[key])


class FakeEmbedder:
    embedding_space = "test:fake-768:v1"

    async def embed(self, text):
        assert text
        return [0.001] * 768


@pytest.mark.asyncio
async def test_stage_validate_finalize_materializes_only_after_ready(tmp_path):
    source_pdf = make_pdf()
    source_sha = hashlib.sha256(source_pdf).hexdigest()
    store = FakeStore(source_pdf)

    ingestion = {
        "id": INGESTION_ID,
        "document_id": DOCUMENT_ID,
        "source_file_id": "file_123",
        "source_object_id": SOURCE_OBJECT_ID,
        "source_sha256": source_sha,
        "staged_graph_object_id": None,
        "state": "staged",
        "cursor": "0",
        "staged_revision": 1,
        "warnings": [],
        "error_code": None,
        "created_at": "2026-10-02T00:00:00Z",
    }
    object_rows = {
        SOURCE_OBJECT_ID: {
            "id": SOURCE_OBJECT_ID,
            "document_id": DOCUMENT_ID,
            "kind": "source_pdf",
            "object_key": "private/source.pdf",
            "sha256": source_sha,
            "mime_type": "application/pdf",
        }
    }
    posted = {
        "rkb_pages": [],
        "rkb_regions": [],
        "rkb_region_relations": [],
        "rkb_illustrations": [],
        "chunks": [],
    }
    activation_calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method

        if method == "GET" and path == "/rest/v1/rkb_ingestion_jobs":
            return httpx.Response(200, json=[dict(ingestion)])

        if method == "PATCH" and path == "/rest/v1/rkb_ingestion_jobs":
            assert request.headers["authorization"] == "Bearer user-jwt"
            values = json.loads(request.content)
            ingestion.update(values)
            return httpx.Response(200, json=[dict(ingestion)])

        if method == "GET" and path == "/rest/v1/rkb_documents":
            assert request.headers["authorization"] == "Bearer user-jwt"
            return httpx.Response(
                200,
                json=[{
                    "id": DOCUMENT_ID,
                    "owner_user_id":principal().subject,
                    "page_count": 1,
                    "active_revision": 0,
                    "title": "Regional history",
                }],
            )

        if method == "GET" and path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer service-role"
            object_id = request.url.params.get("id", "").removeprefix("eq.")
            object_key = request.url.params.get("object_key", "").removeprefix("eq.")
            rows = list(object_rows.values())
            if object_id:
                rows = [row for row in rows if row["id"] == object_id]
            if object_key:
                rows = [row for row in rows if row["object_key"] == object_key]
            kind = request.url.params.get("kind", "").removeprefix("eq.")
            if kind:
                rows = [row for row in rows if row["kind"] == kind]
            return httpx.Response(200, json=rows[:1])

        if method == "POST" and path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer service-role"
            row = json.loads(request.content)
            object_rows[row["id"]] = row
            return httpx.Response(201, json={})

        if method == "DELETE" and path in {
            "/rest/v1/rkb_chunks",
            "/rest/v1/rkb_pages",
        }:
            assert request.headers["authorization"] == "Bearer user-jwt"
            return httpx.Response(204)

        if method == "POST" and path.startswith("/rest/v1/rkb_"):
            table = path.rsplit("/", 1)[-1]
            if table in posted:
                body = json.loads(request.content)
                posted[table].extend(body if isinstance(body, list) else [body])
                return httpx.Response(201, json={})

        if method == "POST" and path == "/rest/v1/rpc/rkb_insert_chunks":
            body = json.loads(request.content)
            assert body["p_document_id"] == DOCUMENT_ID
            assert body["p_ingestion_id"] == INGESTION_ID
            posted["chunks"].extend(body["p_chunks"])
            return httpx.Response(200, json=1)

        if method == "POST" and path == "/rest/v1/rpc/rkb_activate_revision":
            body = json.loads(request.content)
            activation_calls.append(body)
            ingestion["state"] = "finalized"
            return httpx.Response(
                200,
                json=[{
                    "document_id": DOCUMENT_ID,
                    "active_revision": 1,
                    "ingestion_state": "finalized",
                }],
            )

        raise AssertionError((method, request.url, request.content))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="anon",
            public_base_url="https://knowledge.example",
            service_role_key="service-role",
        ),
        embedder=FakeEmbedder(),
        client=client,
        object_store=store,
    )

    page_id = str(uuid5(UUID(DOCUMENT_ID), "page:1:0"))
    stage = await backend.book_ingest(
        command="stage",
        principal=principal(),
        file=None,
        ingestion_id=INGESTION_ID,
        cursor="1",
        payload={
            "pages": [{
                "page_id": page_id,
                "physical_page_index": 0,
                "source_material": "visual_reviewed",
                "source_review_note": "Synthetic page image checked; text and figure represented",
                "layout_kind": "single_column",
                "regions": [
                    {
                        "region_key": "body-1",
                        "kind": "body",
                        "bbox": {
                            "left": 80,
                            "top": 80,
                            "right": 920,
                            "bottom": 300,
                        },
                        "reading_order": 0,
                        "source_text": "East Prussia 1930",
                    },
                    {
                        "region_key": "figure-1",
                        "kind": "figure",
                        "bbox": {
                            "left": 150,
                            "top": 350,
                            "right": 850,
                            "bottom": 700,
                        },
                        "reading_order": 1,
                    },
                    {
                        "region_key": "caption-1",
                        "kind": "caption",
                        "bbox": {
                            "left": 150,
                            "top": 710,
                            "right": 850,
                            "bottom": 780,
                        },
                        "reading_order": 2,
                        "source_text": "Historical photograph",
                    },
                ],
                "relations": [{
                    "kind": "caption_of",
                    "source_region_key": "caption-1",
                    "target_region_key": "figure-1",
                }],
                "illustrations": [{
                    "illustration_key": "image-1",
                    "source_region_key": "figure-1",
                    "kind": "photo",
                    "caption_region_keys": ["caption-1"],
                    "nearby_region_keys": ["body-1"],
                }],
            }],
            "chunks": [{
                "chunk_key": "main",
                "title": "Regional history",
                "region_refs": [
                    {"page_id": page_id, "region_key": "body-1"},
                    {"page_id": page_id, "region_key": "caption-1"},
                ],
                "illustration_refs": [{
                    "page_id": page_id,
                    "illustration_key": "image-1",
                }],
            }],
        },
    )
    assert stage.state == "processing"
    assert stage.next_action == "validate"
    resumed = await backend.book_ingest(command="status", principal=principal(),
        file=None, ingestion_id=INGESTION_ID, cursor=None, payload=None)
    assert resumed.next_action == "validate"
    assert ingestion["staged_graph_object_id"]
    assert posted["rkb_pages"] == []

    validated = await backend.book_ingest(
        command="validate",
        principal=principal(),
        file=None,
        ingestion_id=INGESTION_ID,
        cursor=None,
        payload=None,
    )
    assert validated.state == "ready"
    assert posted["rkb_pages"] == []

    finalizing = await backend.book_ingest(
        command="finalize",
        principal=principal(),
        file=None,
        ingestion_id=INGESTION_ID,
        cursor=None,
        payload=None,
    )
    assert finalizing.state == "processing"
    assert finalizing.next_cursor == "finalize"
    assert "running server-side" in finalizing.message

    task = backend._finalize_tasks[INGESTION_ID]
    await task
    finalized = await backend.book_ingest(
        command="status",
        principal=principal(),
        file=None,
        ingestion_id=INGESTION_ID,
        cursor=None,
        payload=None,
    )
    await client.aclose()

    assert finalized.state == "finalized"
    assert len(posted["rkb_pages"]) == 1
    assert all(p["page_object_id"] is None for p in posted["rkb_pages"])
    assert not any(o["kind"] in {"page_render","text_projection"} for o in object_rows.values())
    assert posted["chunks"][0]["source_text"]
    assert len(posted["rkb_regions"]) == 3
    assert len(posted["rkb_region_relations"]) == 1
    assert len(posted["rkb_illustrations"]) == 1
    assert len(posted["chunks"]) == 1
    assert posted["chunks"][0]["embedding"].startswith("[")
    assert posted["chunks"][0]["metadata"]["embedding_space"] == "test:fake-768:v1"
    assert activation_calls == [{
        "p_document_id": DOCUMENT_ID,
        "p_ingestion_id": INGESTION_ID,
        "p_revision": 1,
        "p_poi_events": [],
    }]
    crop_row = posted["rkb_illustrations"][0]
    assert crop_row["source_crop_sha256"]
    crop_object = object_rows[crop_row["crop_object_id"]]
    assert store.objects[crop_object["object_key"]].startswith(b"\x89PNG")
