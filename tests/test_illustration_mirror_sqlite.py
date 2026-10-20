"""Exercise mirror state transitions through the real SQLite compatibility layer."""
import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.illustration_mirror import IllustrationMirror
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from test_illustration_mirror import Vibe


class CropStore:
    async def get_bytes(self, key):
        assert key == "fixture-crop"
        return b"private fixture crop"


@pytest.fixture
def mirror_source(tmp_path, monkeypatch):
    # Synthetic crops must not depend on free space on the shared test host.
    capacity = SimpleNamespace(free=4 * 1024**3)
    monkeypatch.setattr(
        "regional_knowledge.illustration_mirror.shutil",
        SimpleNamespace(disk_usage=lambda path: capacity),
    )
    backend = SQLiteBackend(
        corpus_path=tmp_path / "corpus.db",
        embedder=LexicalOnlyEmbedder(),
        object_store=CropStore(),
    )
    owner, document, page, region, obj, illustration = (
        str(uuid4()) for _ in range(6)
    )
    digest = hashlib.sha256(b"private fixture crop").hexdigest()
    values = {
        "rkb_users": {"id": owner, "status": "active"},
        "rkb_documents": {
            "id": document, "owner_user_id": owner,
            "title": "Synthetic SQLite mirror", "source_sha256": "a" * 64,
            "active_revision": 1, "page_count": 1,
        },
        "rkb_pages": {
            "id": page, "document_id": document, "revision": 1,
            "physical_page_index": 0,
        },
        "rkb_regions": {
            "id": region, "page_id": page, "kind": "figure",
            "bbox": {"left": 0, "top": 0, "right": 1000, "bottom": 1000},
            "reading_order": 0,
        },
        "rkb_objects": {
            "id": obj, "document_id": document, "kind": "illustration_crop",
            "object_key": "fixture-crop", "sha256": digest,
            "mime_type": "image/png", "size_bytes": len(b"private fixture crop"),
        },
        "rkb_illustrations": {
            "id": illustration, "document_id": document, "page_id": page,
            "source_region_id": region, "crop_object_id": obj, "kind": "drawing",
            "visibility": "private", "source_crop_sha256": digest,
            "display_crop_sha256": digest,
        },
    }
    for table, value in values.items():
        backend.corpus.put(table, [{**defaults(table), **value}])
    return SimpleNamespace(
        backend=backend, owner=owner, document=document, page=page,
        illustration=illustration, capacity=capacity,
    )


@pytest.mark.asyncio
async def test_sqlite_mirror_attempt_error_restart_and_verified_readback(mirror_source):
    source = mirror_source
    backend = source.backend
    client = Vibe(source.owner)
    worker = IllustrationMirror(backend, client)
    original_document = backend.corpus.one("rkb_documents", source.document)
    original = backend.corpus.one("rkb_illustrations", source.illustration)
    try:
        # Failed fake put must persist both the attempt and error, without losing
        # the crop. Previously both updates raised SQLite's syntax error.
        assert await worker.tick() == 0
        failed = backend.corpus.one("rkb_illustrations", source.illustration)
        assert failed["mirror_attempt_at"]
        assert failed["mirror_error_type"] == "OSError"
        assert not failed["mirror_operation_id"]
        assert not client.operations

        client.fail = False
        assert await worker.tick() == 0
        admitted = backend.corpus.one("rkb_illustrations", source.illustration)
        assert admitted["mirror_operation_id"]
        assert admitted["mirror_error_type"] is None
        client.advance(0)

        # A fresh reconciler resumes the persisted operation. No real provider,
        # credentials, uploads or grants are involved.
        assert await IllustrationMirror(backend, client).tick() == 1
        verified = backend.corpus.one("rkb_illustrations", source.illustration)
        assert verified["mirror_operation_id"] == admitted["mirror_operation_id"]
        assert verified["mirror_read_operation_id"]
        assert verified["vibepublish_entry_ref"]
        assert verified["provider_crop_sha256"] == hashlib.sha256(
            b"provider-sanitized:private fixture crop"
        ).hexdigest()
        assert verified["mirror_error_type"] is None
        assert await IllustrationMirror(backend, client).tick() == 0
        assert len(client.operations) == len(client.entries) == 1

        for key in (
            "id", "document_id", "page_id", "source_region_id",
            "crop_object_id", "source_crop_sha256", "display_crop_sha256",
            "visibility",
        ):
            assert verified[key] == original[key]
        assert backend.corpus.one("rkb_documents", source.document) == original_document
    finally:
        await backend.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fence", ["foreign_owner", "inactive_owner", "old_revision", "public"])
async def test_sqlite_mirror_update_preserves_existing_fences(mirror_source, fence):
    source = mirror_source
    backend = source.backend
    owner = source.owner
    if fence == "foreign_owner":
        owner = str(uuid4())
    elif fence == "inactive_owner":
        user = backend.corpus.one("rkb_users", source.owner)
        backend.corpus.put("rkb_users", [{**user, "status": "disabled"}])
    elif fence == "old_revision":
        document = backend.corpus.one("rkb_documents", source.document)
        backend.corpus.put("rkb_documents", [{**document, "active_revision": 2}])
    else:
        item = backend.corpus.one("rkb_illustrations", source.illustration)
        backend.corpus.put("rkb_illustrations", [{**item, "visibility": "public"}])
    before = backend.corpus.one("rkb_illustrations", source.illustration)
    worker = IllustrationMirror(backend, Vibe(owner))
    try:
        await worker.update(
            {"id": source.illustration},
            mirror_attempt_at=datetime.now(timezone.utc),
            mirror_error_type="OSError",
            vibepublish_entry_ref="fixture-entry",
        )
        assert backend.corpus.one("rkb_illustrations", source.illustration) == before
    finally:
        await backend.aclose()


@pytest.mark.asyncio
async def test_sqlite_mirror_capacity_defers_only_new_puts(mirror_source):
    source = mirror_source
    backend = source.backend

    class CountingVibe(Vibe):
        put_calls = 0

        async def put(self, *args):
            self.put_calls += 1
            return await super().put(*args)

    client = CountingVibe(source.owner)
    client.fail = False
    worker = IllustrationMirror(backend, client)
    original = backend.corpus.one("rkb_illustrations", source.illustration)
    try:
        source.capacity.free = 2 * 1024**3 - 1
        assert await worker.tick() == 0
        deferred = backend.corpus.one("rkb_illustrations", source.illustration)
        assert deferred["mirror_error_type"] == "MirrorAdmissionDeferred"
        for key in ("mirror_operation_id", "mirror_read_operation_id",
                    "vibepublish_entry_ref", "provider_crop_sha256"):
            assert not deferred[key]
        assert deferred["crop_object_id"] == original["crop_object_id"]
        assert await backend.object_store.get_bytes("fixture-crop") == b"private fixture crop"
        assert client.put_calls == 0
        assert not client.operations

        # Exactly the reserve boundary admits a new PUT.
        source.capacity.free = 2 * 1024**3
        assert await worker.tick() == 0
        admitted = backend.corpus.one("rkb_illustrations", source.illustration)
        assert admitted["mirror_operation_id"]
        assert client.put_calls == 1

        # Falling below reserve cannot strand an already admitted operation.
        source.capacity.free = 0
        assert await IllustrationMirror(backend, client).tick() == 0
        client.advance(0)
        assert await IllustrationMirror(backend, client).tick() == 1
        verified = backend.corpus.one("rkb_illustrations", source.illustration)
        assert verified["mirror_operation_id"] == admitted["mirror_operation_id"]
        assert verified["mirror_read_operation_id"]
        assert verified["vibepublish_entry_ref"]
        assert verified["provider_crop_sha256"] == hashlib.sha256(
            b"provider-sanitized:private fixture crop"
        ).hexdigest()
        assert verified["mirror_error_type"] is None
        assert verified["crop_object_id"] == original["crop_object_id"]
        assert client.put_calls == 1
        assert len(client.operations) == len(client.entries) == 1
    finally:
        await backend.aclose()
