"""Verify an Online Backup and independent Story Registry restore, without external effects.

Only the authorized owner operates this program on DevCoveer. It preserves a
private, hashed SQLite snapshot and verifies a separate disposable restored DB.
No provider calls, publications, LLM, embeddings, original source exports or
private content in stdout. The observed RTO is measured, not assumed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from uuid import UUID, uuid4

STATE = Path("/home/dev/.local/state/regional-knowledge-base")
BACKUPS = STATE / "backups/story-registry"
SERVICE_ENV = STATE / "service.env"
RELEASE_ENV = STATE / "story-registry-release.env"


def config(path, key):
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith(key + "="):
            value = line.split("=", 1)[1].strip()
            return value[1:-1] if len(value) > 1 and value[0] == value[-1] and value[0] in "'\"" else value
    raise RuntimeError(key + " missing from " + path.name)


def checksum(path):
    value = hashlib.sha256()
    with open(path, "rb") as source:
        for part in iter(lambda: source.read(1024 * 1024), b""):
            value.update(part)
    return value.hexdigest()


def write_receipt(path, record):
    temp = path.with_name(path.name + ".new-" + str(os.getpid()))
    with open(temp, "x", encoding="utf8") as stream:
        json.dump(record, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temp, 0o600)
    os.replace(temp, path)


def run(args):
    started = time.monotonic()
    live = Path(config(SERVICE_ENV, "RKB_SQLITE_CORPUS_PATH"))
    pythonpath = Path(config(RELEASE_ENV, "PYTHONPATH"))
    release_sha = config(RELEASE_ENV, "RKB_RELEASE_SHA")
    if not live.is_file() or not (pythonpath / "regional_knowledge/story_registry.py").is_file():
        raise RuntimeError("Production authority/release unavailable")
    sys.path.insert(0, str(pythonpath))
    from regional_knowledge.contracts import Principal
    from regional_knowledge.sqlite_corpus import SQLiteCorpus
    from regional_knowledge.story_contracts import SeedInput, StoryMetadata, SourceRef
    from regional_knowledge.story_registry import StoryRegistry, StoryError
    expected = json.loads(args.acceptance.read_text(encoding="utf8"))
    if expected.get("status") != "passed" or len(expected.get("stories", [])) < 2:
        raise RuntimeError("Missing successful live acceptance to verify")

    BACKUPS.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(BACKUPS, 0o700)
    if shutil.disk_usage(BACKUPS).free < max(256 * 1024 * 1024, live.stat().st_size * 3):
        raise RuntimeError("Insufficient disk for preserved snapshot and isolated restore")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    backup = BACKUPS / ("post-story-" + stamp + "-" + str(os.getpid()) + ".sqlite3")
    try:
        with sqlite3.connect(f"file:{live}?mode=ro", uri=True, timeout=20) as source:
            with sqlite3.connect(backup, timeout=20) as destination:
                source.backup(destination, pages=250)
        os.chmod(backup, 0o600)
        saved_sha = checksum(backup)
        with tempfile.TemporaryDirectory(prefix="restore-drill-", dir=BACKUPS) as scratch:
            test_db = Path(scratch) / "restored.sqlite3"
            shutil.copyfile(backup, test_db)
            if checksum(test_db) != saved_sha:
                raise RuntimeError("Copy/readback checksum mismatch")
            with sqlite3.connect(test_db, timeout=15) as sql:
                if sql.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("Restored SQLite integrity_check failed")
                fk = sql.execute("PRAGMA foreign_key_check").fetchall()
                if fk:
                    raise RuntimeError("Restored SQLite foreign_key_check failed")
                corpus_count = sql.execute("SELECT count(*) FROM corpus_rows").fetchone()[0]
                story_count = sql.execute("SELECT count(*) FROM story_records").fetchone()[0]
                receipt_count = sql.execute("SELECT count(*) FROM story_receipts").fetchone()[0]
                decisions = sql.execute("SELECT count(*) FROM story_review_decisions").fetchone()[0]
                if min(corpus_count, story_count, receipt_count, decisions) <= 0:
                    raise RuntimeError("Incomplete corpus/story/receipt/approval snapshot")
            registry = StoryRegistry(SQLiteCorpus(test_db))
            owner_id = str(UUID(config(SERVICE_ENV, "RKB_OWNER_SUBJECT")))
            owner = Principal(subject=owner_id, client_id="restore-drill", issuer="restore",
                              access_token="synthetic-local-principal")
            outsider = Principal(subject=str(uuid4()), client_id="restore-drill",
                                  issuer="restore", access_token="synthetic-local-principal")
            cards = []
            for accepted in expected["stories"]:
                sid = accepted["story_id"]
                snapshot = registry.get(owner, sid, view="evidence")
                ready = snapshot["readiness"]["ready_variants"]
                if (snapshot["revision"] != accepted["revision"]
                        or accepted["variant_id"] not in ready):
                    raise RuntimeError("Approved story/version missing from restored authority")
                history = registry.history(owner, sid)
                if len(history["history"]) < 5:
                    raise RuntimeError("Editorial history missing from restore")
                try:
                    registry.get(outsider, sid)
                except StoryError as error:
                    if error.code != "not_found_or_not_accessible":
                        raise
                else:
                    raise RuntimeError("Restored ACL leaked a private story")
                cards.append({"story_id": sid, "revision": snapshot["revision"],
                              "variant_id": accepted["variant_id"],
                              "evidence_count": len(snapshot["snapshot"]["assertions"][0]["evidence_ids"]),
                              "historical_versions": len(history["history"]),
                              "outsider_access": "denied"})
            with sqlite3.connect(test_db) as db:
                receipts = db.execute("SELECT count(*) FROM story_receipts").fetchone()[0]
                if receipts != receipt_count:
                    raise RuntimeError("Receipt snapshot changed")
            elapsed = round(time.monotonic() - started, 2)
            result = {"status": "passed", "runtime_sqlite_version": sqlite3.sqlite_version,
                      "release_sha": release_sha, "external_copy_verified": False,
                      "online_backup": str(backup), "backup_bytes": backup.stat().st_size,
                      "backup_sha256": saved_sha, "checksum_readback": "passed",
                      "sqlite_integrity": "ok", "foreign_keys": "ok",
                      "corpus_rows": corpus_count, "stories": story_count,
                      "idempotency_receipts": receipt_count,
                      "review_decisions": decisions, "reviewed_cases": cards,
                      "external_effects": 0, "restore_drill_seconds": elapsed}
            write_receipt(args.output, result)
            print(json.dumps({k:v for k,v in result.items()
                              if k not in ("online_backup", "backup_sha256", "reviewed_cases")},
                             ensure_ascii=False), flush=True)
            print(json.dumps({"story_ids": [x["story_id"] for x in cards],
                              "restore_verified": True}, ensure_ascii=False), flush=True)
            return
    except Exception:
        # A failed snapshot must not be retained as a validated recovery point.
        backup.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())
