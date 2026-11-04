"""Telegram import captions report current gaps, not historical rejections."""
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


def _status(page_count, text_pages, semantic_pages, *, finished=False,
            rejections=0, rejected_batch=None, pending=False, last_processing=False,
            prior_failed=False, failed=False):
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
    locator = json.dumps({"chat_id": -100123, "message_id": 42}) if prior_failed else None
    if prior_failed:
        db.execute("INSERT INTO rkb_source_intake_control VALUES(?,?,?,?,?,?)",
                   ("old", "old-source", "actor", "{}", "failed", "2026-11-03"))
        db.execute("INSERT INTO rkb_native_source_intake_state VALUES(?,?,?,?,?,?,?)",
                   ("old", "failed", None, 0, 0, 0, None))
        db.execute("INSERT INTO rkb_original_intakes VALUES(?,?,?,?)",
                   ("old", "actor", json.dumps({"page_count": page_count}), locator))
    phase = "failed" if failed else "finalized" if finished else "staging"
    db.execute("INSERT INTO rkb_source_intake_control VALUES(?,?,?,?,?,?)",
               ("ing", "src", "actor", "{}", phase, "2026-11-04"))
    db.execute("INSERT INTO rkb_native_source_intake_state VALUES(?,?,?,?,?,?,?)",
               ("ing", phase, "pending-key" if pending else None, page_count,
                page_count, page_count - int(last_processing), rejected_batch))
    db.execute("INSERT INTO rkb_original_intakes VALUES(?,?,?,?)",
               ("ing", "actor", json.dumps({"page_count": page_count}), locator))
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


def test_rejection_then_acceptance_is_clean_without_history():
    status = _status(2, {0, 1}, {0, 1}, finished=True, rejections=3)
    assert status == "✅ Импорт завершён"
    assert "отклонений" not in status and "⚠️" not in status


def test_finalized_missing_text_reports_real_page_number():
    assert _status(3, {0, 2}, {0, 2}, finished=True) == (
        "🏁⚠️ Обработка закончена · страниц 2/3 · не импортированы стр. 2")


def test_running_book_flags_middle_gap_but_not_unread_tail():
    assert _status(4, {0, 2}, {0, 2}) == (
        "⏳⚠️ Импортируется · страниц 2/4 · не импортированы стр. 2")


def test_text_without_semantic_is_current_problem():
    assert _status(2, {0, 1}, {0}, finished=True) == (
        "🏁⚠️ Обработка закончена · страниц 1/2 · не разобраны 1 стр.")


def test_old_failed_ingestion_with_same_telegram_message_is_ignored():
    assert _status(1, {0}, {0}, finished=True, prior_failed=True) == (
        "✅ Импорт завершён")


def test_current_rejected_packet_flags_retry():
    assert _status(2, {0}, {0}, rejections=1, rejected_batch=1) == (
        "⏳⚠️ Импортируется · страниц 1/2 · пакет отклонён, ждёт переотправки")


def test_current_failed_ingestion_alone_is_failure():
    assert _status(1, set(), set(), failed=True, prior_failed=True) == (
        "❌ Ошибка импорта")


def test_pending_finalize_only_is_warning():
    assert _status(2, {0, 1}, {0, 1}, finished=True, pending=True) == (
        "🏁⚠️ Обработка закончена · страниц 2/2 · сохранение не завершено")


def test_last_still_enriching_packet_is_not_prematurely_missing_entities():
    assert _status(3, {0, 1, 2}, {0, 1}, last_processing=True) == (
        "⏳ Импортируется · страниц 2/3")


def test_missing_pages_list_is_bounded_to_five():
    assert _status(10, {9}, {9}) == (
        "⏳⚠️ Импортируется · страниц 1/10 · не импортированы стр. 1, 2, 3, 4, 5 и ещё 4")
