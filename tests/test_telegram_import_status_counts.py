"""Focused projection checks: rejected operations are metrics, missing pages are warnings."""
import base64
import hashlib
import json
import sqlite3

from regional_knowledge.telegram_import_status import started_statuses


class _Queue:
    def __init__(self, db):
        self.db = db

    def connect(self, write=False):
        return self.db


def _status(page_count, text_pages, semantic_pages, *, finished=False, rejections=0):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE rkb_source_intake_control (
            ingestion_id TEXT, source_id TEXT, person_id TEXT,
            metadata_json TEXT, phase TEXT, created TEXT);
        CREATE TABLE rkb_native_source_intake_state (
            ingestion_id TEXT, phase TEXT, pending_operation_key TEXT,
            batch_count INTEGER, published_batch_count INTEGER,
            enriched_batch_count INTEGER, rejected_batch_index INTEGER);
        CREATE TABLE rkb_original_intakes (
            ingestion_id TEXT, actor TEXT, receipt_json TEXT, telegram_locator_json TEXT);
        CREATE TABLE rkb_control_operations (
            operation_key TEXT, command_bytes TEXT, apply_state TEXT,
            source_id TEXT, person_id TEXT, sequence INTEGER);
    """)
    phase = "finalized" if finished else "staging"
    db.execute("INSERT INTO rkb_source_intake_control VALUES(?,?,?,?,?,?)",
               ("ing", "src", "actor", "{}", phase, "2026-11-04"))
    db.execute("INSERT INTO rkb_native_source_intake_state VALUES(?,?,?,?,?,?,?)",
               ("ing", phase, None, None, None, None, None))
    db.execute("INSERT INTO rkb_original_intakes VALUES(?,?,?,?)",
               ("ing", "actor", json.dumps({"page_count": page_count}), None))
    sequence = 0

    def record(key, packet, state="APPLIED"):
        nonlocal sequence
        sequence += 1
        command = json.dumps({"payload_b64": base64.b64encode(
            json.dumps(packet).encode()).decode()})
        db.execute("INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?)",
                   (key, command, state, "src", "actor", sequence))

    for i in range(rejections):
        record(f"source-packet:ing:rejected:{i}", {}, "REJECTED_PRECONDITION")
    for page in range(page_count):
        stage = {"ingestion_id": "ing", "batch_index": page,
                 "model_payload": {"pages": [{"physical_page_index": page}]}}
        digest = hashlib.sha256(json.dumps(stage).encode()).hexdigest()
        record(f"source-stage:ing:{page}", stage)
        for component, accepted in (("text", text_pages), ("semantic", semantic_pages)):
            if page in accepted:
                record(f"source-packet:ing:{page}:{component}",
                       {"ingestion_id": "ing", "batch_index": page,
                        "last_batch_sha256": digest, "component": component})
    result = started_statuses(_Queue(db), "actor")
    db.close()
    return result[0][1]


def test_rejected_then_applied_is_counted_without_warning():
    status = _status(2, {0, 1}, {0, 1}, finished=True, rejections=3)
    assert status == "✅ Импорт завершён · страниц 2/2 · отклонений 3"
    assert "⚠️" not in status


def test_finalized_book_with_missing_page_warns():
    assert _status(3, {0, 1}, {0, 1}, finished=True) == (
        "🏁⚠️ Обработка закончена · страниц 2/3")


def test_text_without_semantic_shows_missing_entities():
    assert _status(3, {0, 1}, {0}) == (
        "⏳⚠️ Импортируется · страниц 1/3 · без сущностей 1")
