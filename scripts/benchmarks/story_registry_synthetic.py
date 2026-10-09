"""Isolated no-model Story Registry load acceptance; synthetic data only.

Local SQLite/functional boundary. It is NOT public OAuth MCP latency acceptance.
"""
from __future__ import annotations

import json
import math
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from regional_knowledge.contracts import Principal
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.story_contracts import AddGap
from regional_knowledge.story_registry import StoryError, StoryRegistry


def user():
    return Principal(subject=str(uuid4()), client_id="synthetic-benchmark",
                     issuer="synthetic", access_token="not-a-real-token")


def p95(samples):
    ordered = sorted(samples)
    return round(1000 * ordered[math.ceil(.95 * len(ordered)) - 1], 2)


def run(count=10000, readers=10, writers=3):
    assert count >= 1000 and readers > 0 and writers > 0
    with tempfile.TemporaryDirectory(prefix="rkb-synthetic-story-load-") as temp:
        registry = StoryRegistry(SQLiteCorpus(Path(temp) / "corpus.sqlite3"))
        actors = [user() for _ in range(writers)]
        writable = {}
        # One transaction simulates a normal bulk extraction/import; no model,
        # network, production DB or vector plane is involved.
        started = time.monotonic()
        with registry.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            for actor in actors:
                registry._actor(db, actor, create=True)
            for i in range(count):
                actor = actors[i % len(actors)]
                story_id, _ = registry._create_record(
                    db, actor, {"text": f"Фонарь синтетический номер {i}",
                                "origin_status": "unknown", "origin_note": None, "speaker": None},
                    {"title": f"Фонарь номер {i}", "summary": f"Синтетический тестовый сюжет {i}",
                     "material_type": "everyday_life"})
                if i < writers * 3:
                    writable.setdefault(actor.subject, []).append(story_id)
        ingest_s = round(time.monotonic() - started, 2)
        with registry.corpus.connect() as db:
            total = db.execute("SELECT count(*) FROM story_records").fetchone()[0]
            assert total == count
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert not db.execute("PRAGMA foreign_key_check").fetchone()
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        def reader_task(index):
            actor = actors[index % len(actors)]
            timings = []
            for _ in range(20):
                tic = time.monotonic()
                found = registry.search(actor, "Фонарь", limit=3)
                assert found["results"] and all(
                    registry.corpus.one("rkb_users", actor.subject) is not None
                    for _ in found["results"][:1])
                timings.append(time.monotonic() - tic)
                tic = time.monotonic()
                registry.get(actor, writable[actor.subject][0])
                timings.append(time.monotonic() - tic)
            return timings
        def writer_task(index):
            actor = actors[index]
            story_id = writable[actor.subject][0]
            timings = []
            for rev in range(1, 21):
                operation = [AddGap(op="add_gap", text=f"Синтетический вопрос {rev}")]
                key = f"stress-write-{index}-{rev:04d}"
                tic = time.monotonic()
                result = registry.edit(actor, story_id, rev, operation, key)
                timings.append(time.monotonic() - tic)
                assert registry.edit(actor, story_id, rev, operation, key) == result
                assert result["committed_revision"] == rev + 1
            return timings
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=readers + writers) as pool:
            tasks = [pool.submit(reader_task, i) for i in range(readers)]
            tasks += [pool.submit(writer_task, i) for i in range(writers)]
            all_results = [future.result() for future in tasks]
        read_durations = [t for group in all_results[:readers] for t in group]
        write_durations = [t for group in all_results[readers:] for t in group]
        elapsed = round(time.monotonic() - started, 2)
        with registry.corpus.connect() as db:
            result_counts = {
                "stories": db.execute("SELECT count(*) FROM story_records").fetchone()[0],
                "revisions": db.execute("SELECT count(*) FROM story_revisions").fetchone()[0],
                "receipts": db.execute("SELECT count(*) FROM story_receipts").fetchone()[0],
                "conflicts": 0,
                "foreign_key_violations": len(db.execute("PRAGMA foreign_key_check").fetchall())
            }
        assert result_counts["stories"] == count
        assert result_counts["revisions"] == count + writers * 20
        assert result_counts["receipts"] == writers * 20
        assert result_counts["foreign_key_violations"] == 0
        record = {
            "fixture": "synthetic only; not production OAuth or remote MCP",
            "count": count, "readers": readers, "writers": writers,
            "ingest_seconds": ingest_s, "concurrent_seconds": elapsed,
            "read_calls": len(read_durations), "writes": len(write_durations),
            "local_read_p95_ms": p95(read_durations),
            "local_write_p95_ms": p95(write_durations),
            "result": result_counts,
            "threshold_scope": "local SQLite functional load, NOT remote MCP"
        }
        print(json.dumps(record, ensure_ascii=False, sort_keys=True), flush=True)
        assert record["local_read_p95_ms"] <= 1500, record
        assert record["local_write_p95_ms"] <= 2000, record


if __name__ == "__main__":
    run()
