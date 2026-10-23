"""B2: real shape/byte-bound DjVu intake through the existing native owner.

All files here are synthetic, including a >2 MiB IFF envelope. The real
owner-supplied 139-page original remains private and must never enter Git.
Passing these tests proves acquisition and a verified original locator, NOT
page decoding, search, model reading or activation.
"""
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_original_parser import parse
from regional_knowledge.kaggle_original_native import HISTORICAL_PDF_SCRIPT_SHA256
from regional_knowledge.original_intake import OriginalIntakeSpec, canonical, digest
from regional_knowledge.kaggle_importer import MAX_BYTES, MAX_PAGES
from test_kaggle_djvu_inspect import fixture as djvu_fixture, _chunk
from test_kaggle_original_intake import (
    SOURCE, INGEST, REQUEST, frame, ticket, native,
)


def _large_djvu():
    payload = djvu_fixture(139, dictionaries=14)
    # Synthetic navigation data inflates the original over the former 2 MiB
    # pilot; it is not recognized as a page and does not invent OCR content.
    extra = _chunk(b"NAVM", b"n" * (2 * 1024 * 1024 + 251))
    source_size = int.from_bytes(payload[8:12], "big")
    payload = payload[:8] + (source_size + len(extra)).to_bytes(4, "big") + payload[12:] + extra
    assert len(payload) > 2 * 1024 * 1024
    return payload


def test_full_size_and_139_page_acceptance_are_not_pilot_caps(tmp_path):
    data = _large_djvu()
    path = tmp_path / "synthetic-139.djvu"
    path.write_bytes(data)
    result = parse(path, "inspect")
    assert result == {"format": "djvu", "page_count": 139}
    assert MAX_BYTES >= len(data)
    assert MAX_PAGES >= 139
    # Receipt schema keeps the earlier explicit smaller per-request bound
    # valid for replay, while new books can use the full current budget.
    spec = OriginalIntakeSpec(SOURCE, INGEST, REQUEST, "source-id",
                              mime_type="application/octet-stream",
                              max_bytes=len(data))
    assert spec.max_pages >= 139
    assert OriginalIntakeSpec(SOURCE, INGEST, REQUEST, "old-pilot",
                              max_bytes=len(data), max_pages=32).max_pages == 32


@pytest.mark.asyncio
async def test_native_verified_djvu_archive_over_2mib_139_pages_without_bulk_render(native, tmp_path):
    raw = _large_djvu()
    calls = []

    async def download(url, folder, *, max_bytes):
        calls.append(("download", max_bytes))
        assert url == "https://attachment.invalid/private"
        assert max_bytes == len(raw)
        p = folder / "real-structure.djvu"
        p.write_bytes(raw)
        return SimpleNamespace(path=p, sha256=digest(raw), size_bytes=len(raw))

    async def pinned_parser(path, action, index=0, *, cursor=None):
        calls.append((action, index))
        return parse(path, action, index, cursor=cursor)

    native.handler.downloader = SimpleNamespace(download=download)
    native.handler.parser = pinned_parser
    spec = OriginalIntakeSpec(
        SOURCE, INGEST, REQUEST, "djvu-file",
        file_name="synthetic.djvu", mime_type="application/octet-stream",
        max_bytes=len(raw),
    )
    original = frame("original_acquire", {"spec": spec.wire()},
                     private={"download_url": "https://attachment.invalid/private"})
    acquired = (await native.handler(original))["value"]
    assert acquired["format"] == "djvu"
    assert acquired["page_count"] == 139
    assert acquired["size"] == len(raw) and acquired["sha256"] == digest(raw)
    assert acquired["parser_sha256"] == native.handler.parser_sha256
    # Existing receipt identities are unchanged; only one parser is bundled.
    # The DjVu inspector remains pinned to the same executable parser.
    from regional_knowledge.kaggle_target_launch import MODULES
    import regional_knowledge.kaggle_original_native as native_module
    assert "kaggle_djvu_inspect" in MODULES
    assert "kaggle_original_parser" in MODULES
    assert "kaggle_original_parser_pdf_v1" not in MODULES
    parser_path = Path(native_module.__file__).with_name("kaggle_original_parser.py")
    inspector_path = Path(native_module.__file__).with_name("kaggle_djvu_inspect.py")
    expected_parser_hash = digest(canonical(dict(
        packages={"PyMuPDF": "pinned-version"},
        code=digest(parser_path.read_bytes()),
        djvu_inspector_sha256=digest(inspector_path.read_bytes()),
        legacy_pdf_parser_sha256=HISTORICAL_PDF_SCRIPT_SHA256,
    )))
    assert acquired["parser_sha256"] == expected_parser_hash
    # Reconciliation remains idempotent and must not reread the attachment.
    assert (await native.handler(original))["value"] == acquired
    assert calls == [("download", len(raw)), ("inspect", 0)]
    assert not native.transfer.objects

    args = dict(handle=acquired["handle"], original_sha256=acquired["sha256"])
    upload = frame("original_archive", args)
    upload["original_transfer"] = {"ticket": ticket(upload, acquired)}
    stored = (await native.handler(upload))["value"]
    assert stored["sha256"] == digest(raw)
    verify = frame("original_verify", args)
    verify["original_transfer"] = {"ticket": ticket(verify, acquired, "GET")}
    proven = (await native.handler(verify))["value"]
    assert proven["sha256"] == digest(raw)
    assert proven["native_version"] == "native-etag"
    assert native.transfer.objects
    assert all(value == raw for value in native.transfer.objects.values())
    assert calls == [("download", len(raw)), ("inspect", 0)]
    # Import does not claim there is a verified page image merely because
    # structural page count and whole-original bytes are known.
    with pytest.raises(ValueError, match="original_djvu_decoder_required"):
        parse(native.handler._records[acquired["handle"]]["path"], "page", 0)


def test_inspect_never_claims_djvu_image_pixel_decoder(tmp_path):
    path = tmp_path / "synthetic-139.djvu"
    path.write_bytes(_large_djvu())
    assert parse(path, "inspect")["page_count"] == 139
    with pytest.raises(ValueError, match="original_djvu_decoder_required"):
        parse(path, "page", 0)
    with pytest.raises(ValueError, match="original_djvu_decoder_required"):
        parse(path, "text", 0)


@pytest.mark.parametrize("corrupt", ["empty_page", "truncated", "directory"])
def test_malformed_djvu_is_terminal_invalid_not_an_endless_pending_decoder(corrupt, tmp_path):
    raw = _large_djvu()
    if corrupt == "empty_page":
        raw = b"AT&TFORM" + (4).to_bytes(4, "big") + b"DJVU"
    elif corrupt == "truncated":
        raw = raw[:-71]
    else:
        broken = bytearray(raw)
        first = broken.index(b"DIRM") + 8 + 3
        broken[first + 2] ^= 0x80
        raw = bytes(broken)
    path = tmp_path / "corrupt.djvu"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="original_djvu_invalid"):
        parse(path, "inspect")


@pytest.mark.asyncio
async def test_legacy_stored_intake_retries_same_exact_operation_after_bounds_upgrade(tmp_path):
    """A former 2MiB/32-page operation must not be silently rewritten at retry."""
    import asyncio
    import json
    import sqlite3
    from contextlib import contextmanager
    from uuid import UUID, uuid5
    from regional_knowledge.contracts import ChatFile
    from regional_knowledge.original_intake import OriginalIntakeError
    from regional_knowledge.original_intake_controller import OriginalIntakeController

    class Queue:
        def __init__(self, path):
            self.path = path
        @contextmanager
        def connect(self, write=False):
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            try:
                with db:
                    yield db
            finally:
                db.close()

    queue = Queue(str(tmp_path / "control.sqlite3"))
    with queue.connect() as db:
        db.execute("""CREATE TABLE rkb_original_intakes(
            ingestion_id TEXT PRIMARY KEY,actor TEXT NOT NULL,operation_id TEXT NOT NULL,
            spec_json TEXT NOT NULL,storage_sha256 TEXT NOT NULL,acquisition_json TEXT,
            receipt_json TEXT,failure_code TEXT,restore_json TEXT,telegram_locator_json TEXT,
            UNIQUE(actor,operation_id))""")
    op = "source-original:" + INGEST
    old = OriginalIntakeSpec(SOURCE, INGEST, str(uuid5(UUID(INGEST), "original:" + op)),
                             "same-file",file_name="old.pdf",mime_type="application/pdf",
                             max_bytes=2*1024*1024,max_pages=32)
    actor = "50000000-0000-0000-0000-000000000005"
    with queue.connect() as db:
        db.execute("""INSERT INTO rkb_original_intakes
            (ingestion_id,actor,operation_id,spec_json,storage_sha256)
            VALUES(?,?,?,?,?)""",(INGEST,actor,op,canonical(old.wire()).decode(),"a"*64))

    controller=object.__new__(OriginalIntakeController)
    controller.queue=queue
    controller._lock=asyncio.Lock()
    controller.max_bytes=128*1024*1024
    controller._storage="a"*64
    async def authorize(_):
        return actor, SimpleNamespace(run_id="legacy-test-run")
    controller._authorize=authorize
    requests=[]
    async def phase(binding, name, args, private=None):
        requests.append((name,args))
        raise OriginalIntakeError("synthetic_pending_without_receipt")
    controller._phase=phase
    file=ChatFile(download_url="https://attachment.invalid/private", file_id="same-file",
                  file_name="old.pdf", mime_type="application/pdf")
    result=await controller.begin(None,operation_id=op,source_id=SOURCE,ingestion_id=INGEST,
                                  file=file,max_bytes=128*1024*1024)
    assert result.state=="pending"
    assert requests == [("original_acquire",{"spec":old.wire()})]
    with queue.connect(write=True) as db:
        saved=db.execute("SELECT spec_json FROM rkb_original_intakes WHERE ingestion_id=?",(INGEST,)).fetchone()[0]
    assert saved == canonical(old.wire()).decode()
    with pytest.raises(OriginalIntakeError,match="original_operation_changed"):
        await controller.begin(None,operation_id=op,source_id=SOURCE,ingestion_id=INGEST,
                               file=file.model_copy(update={"file_id":"another-file"}),
                               max_bytes=128*1024*1024)
    assert len(requests)==1
