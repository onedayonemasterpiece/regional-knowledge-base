"""Synthetic vector story search and outbox tests; no user books or raw evidence."""
from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.story_contracts import SeedInput
from regional_knowledge.story_registry import StoryRegistry
from regional_knowledge.story_vector_index import (
    StoryVectorIndexer, install, material, material_hash, pack_vector, pending,
    similarity,
)


def actor():
    return SimpleNamespace(subject=str(uuid4()), client_id="fixture")


def vector(axis: int):
    values = [0.0] * 1024
    values[axis] = 1.0
    return values


def setup(tmp_path):
    registry = StoryRegistry(SQLiteCorpus(tmp_path / "test.sqlite3"))
    return registry, actor(), actor()


def seed(registry, principal, text: str, tag: str):
    return registry.create(
        principal, SeedInput(text=text), None, None, None,
        "synthetic-story-" + tag,
    )["story_id"]


def item(registry, story_id):
    with registry.corpus.connect() as db:
        row = db.execute(
            "SELECT revision,snapshot FROM story_records WHERE id=?", (story_id,)
        ).fetchone()
    text = material(json.loads(row["snapshot"]))
    return {"story_id": story_id, "revision": row["revision"],
            "text": text, "sha256": material_hash(text)}


def test_real_semantic_ranking_and_no_hidden_lexical_fallback(tmp_path):
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Плоты на затопленном берегу", "a")
    other = seed(registry, owner, "Купеческий рынок", "b")
    assert registry.search(owner, "непохожие слова", mode="semantic")[
        "retrieval_mode"] == "lexical_degraded"
    assert install(registry.corpus, item(registry, target), vector(1))
    assert install(registry.corpus, item(registry, other), vector(0))
    result = registry.search(owner, "непохожие слова", mode="semantic",
                             query_vector=vector(1), limit=2)
    assert result["indexing_state"] == "story_bge_ready"
    assert result["retrieval_mode"] == "story_bge"
    assert [hit["story_id"] for hit in result["results"]] == [target, other]
    assert registry.search(owner, "Купеческий", mode="lexical")[
        "results"][0]["story_id"] == other


def test_partial_index_never_claims_fully_ready(tmp_path):
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Рыбацкие причалы", "p1")
    seed(registry, owner, "Оборонительные стены", "p2")
    assert install(registry.corpus, item(registry, target), vector(2))
    result = registry.search(owner, "неродственная фраза", mode="semantic",
                             query_vector=vector(2))
    assert result["retrieval_mode"] == "story_bge_partial"
    assert result["indexing_state"] == "story_bge_partial"
    assert [hit["story_id"] for hit in result["results"]] == [target]


def test_revision_guard_reindex_and_old_vectors_never_surface(tmp_path):
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Дубовые мосты", "rev")
    before = item(registry, target)
    assert install(registry.corpus, before, vector(0))
    from regional_knowledge.story_contracts import AddGap
    registry.edit(owner, target, 1, [AddGap(op="add_gap", text="Уточнить год")],
                  "edit-gap-001")
    assert not install(registry.corpus, before, vector(1))
    old_search = registry.search(owner, "Дубовые мосты", mode="semantic",
                                 query_vector=vector(0))
    assert old_search["retrieval_mode"] == "lexical_degraded"
    assert install(registry.corpus, item(registry, target), vector(1))
    current = registry.search(owner, "без ключевых слов", mode="semantic",
                              query_vector=vector(1))
    assert current["retrieval_mode"] == "story_bge"
    assert current["results"][0]["story_id"] == target


def test_actor_scope_is_applied_before_dense_ranking(tmp_path):
    registry, owner, guest = setup(tmp_path)
    private = seed(registry, owner, "Секретный маяк", "owner")
    public_to_guest = seed(registry, guest, "Обычная пристань", "guest")
    assert install(registry.corpus, item(registry, private), vector(0))
    assert install(registry.corpus, item(registry, public_to_guest), vector(1))
    response = registry.search(guest, "любой вопрос", mode="semantic",
                               query_vector=vector(0), limit=8)
    assert response["retrieval_mode"] == "story_bge"
    assert [hit["story_id"] for hit in response["results"]] == [public_to_guest]
    assert "Секретный" not in str(response)


@pytest.mark.asyncio
async def test_existing_outbox_is_drained_one_at_a_time(tmp_path):
    registry, owner, _ = setup(tmp_path)
    first = seed(registry, owner, "Большой деревянный причал", "w1")
    second = seed(registry, owner, "Площадь с рынком", "w2")

    class KaggleQueue:
        def __init__(self):
            self.jobs = {}
            self.done = False
            self.actors = []
            self.retired = []

        def enqueue(self, actor_id, key, texts, *, kind, identity):
            assert kind == "document"
            assert len(texts) == 1 and "story_id" in identity
            self.actors.append(actor_id)
            self.jobs[key] = identity
            return key

        def result(self, actor_id, job_id):
            assert job_id in self.jobs
            return {
                "state": "done" if self.done else "pending",
                "identity": self.jobs[job_id],
                "result": {"space": "bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1",
                           "vectors": [vector(0)]} if self.done else None,
            }

        def retire_done_documents(self, actor_id, ids):
            self.retired.extend(ids)
            return 1

    queue = KaggleQueue()
    worker = StoryVectorIndexer(registry.corpus, queue)
    assert await worker.tick() == 0  # enqueue only: no fabricated local vector
    queue.done = True
    assert await worker.tick() == 1
    assert await worker.tick() == 1
    assert await worker.tick() == 0
    assert len(queue.retired) == 2
    assert queue.actors and all(queue.actors)
    with registry.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM story_bge_index").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM story_outbox WHERE state='indexed'").fetchone()[0] >= 2
    assert {first, second} == {r["story_id"] for r in
        registry.search(owner, "другие слова", mode="semantic",
                        query_vector=vector(0), limit=10)["results"]}


@pytest.mark.asyncio
async def test_kaggle_story_query_is_actor_scoped_durable_and_degraded_while_pending():
    from regional_knowledge.story_vector_index import query_vector, ENCODER_REVISION
    from regional_knowledge.bge_contract import SPACE as BGE_SPACE, REVISION
    assert ENCODER_REVISION == REVISION  # do not reuse local ONNX embeddings

    class Queue:
        def __init__(self):
            self.identity = None
            self.actor_id = None
            self.job_key = None
            self.complete = False

        def enqueue(self, actor_id, key, texts, *, kind, identity):
            assert kind == "query" and texts == ["где старинный мост"]
            self.actor_id, self.identity, self.job_key = actor_id, identity, key
            return "one-durable-id"

        def result(self, actor_id, job_id):
            assert actor_id == self.actor_id and job_id == "one-durable-id"
            return {"identity": self.identity,
                    "state": "done" if self.complete else "pending",
                    "result": {"space": BGE_SPACE, "vectors": [vector(7)]} if self.complete else None}

        def status(self):
            return {"state": "starting"}

    queue = Queue()
    assert await query_vector(queue, "authorized-actor", "где старинный мост") == (
        None, "kaggle_starting")
    queue.complete = True
    values, state = await query_vector(queue, "authorized-actor", "где старинный мост")
    assert state == "kaggle_ready" and values == vector(7)
    assert queue.job_key.startswith("story-query:" + BGE_SPACE + ":")


def test_legacy_local_onnx_revision_is_not_claimed_as_kaggle_story_readiness(tmp_path):
    from regional_knowledge.story_vector_index import ENCODER_REVISION
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Старый островной рынок", "legacy")
    assert install(registry.corpus, item(registry, target), vector(2))
    with registry.corpus.connect() as db:
        db.execute("UPDATE story_bge_index SET encoder_revision=? WHERE story_id=?",
                   ("onnx-community/bge-m3-ONNX:int8:2237f770", target))
    assert pending(registry.corpus)["story_id"] == target
    response = registry.search(owner, "не ключевой вопрос", mode="semantic",
                               query_vector=vector(2))
    assert response["retrieval_mode"] == "lexical_degraded"
    with registry.corpus.connect() as db:
        row = db.execute("SELECT encoder_revision FROM story_bge_index WHERE story_id=?",
                         (target,)).fetchone()
    assert row["encoder_revision"] != ENCODER_REVISION

def test_invalid_vectors_fail_closed(tmp_path):
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Улица", "invalid")
    with pytest.raises(ValueError):
        pack_vector([1.0])
    with pytest.raises(ValueError):
        install(registry.corpus, item(registry, target), [float("nan")] * 1024)
    with pytest.raises(ValueError):
        registry.search(owner, "Улица", mode="semantic", query_vector=[1.0])


def test_migration_is_reentrant_and_leaves_prior_history(tmp_path):
    registry, owner, _ = setup(tmp_path)
    target = seed(registry, owner, "Новый трамвай", "migration")
    assert pending(registry.corpus)["story_id"] == target
    registry.migrate()
    reopened = StoryRegistry(registry.corpus)
    assert reopened.search(owner, "Новый", mode="lexical")["results"]
    with registry.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM story_schema_migrations WHERE version=13").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM story_outbox WHERE story_id=?", (target,)).fetchone()[0] == 1
