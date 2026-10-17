"""Editorial-story BGE index using ONLY the existing Kaggle durable queue.

Story bodies, source ACL and editorial revisions remain in the SQLite authority.
Kaggle receives one bounded authenticated text job and returns a model-pinned
vector; neither local BGE inference nor a second scheduling service is used.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import struct

from .bge_contract import DIMENSION, REVISION, SPACE, validate_vector

# Fail closed on legacy local-ONNX embeddings; Kaggle FP32 is the authority.
ENCODER_REVISION = REVISION
MATERIAL_VERSION = "story-card-bge-v1"


def material(snapshot: dict) -> str:
    """Bounded public-editorial wording, not source excerpts or private metadata."""
    metadata = snapshot.get("metadata") or {}
    parts = [str(metadata.get("title") or ""), str(metadata.get("summary") or "")]
    parts.extend(str(item.get("proposition") or "") for item in
                 (snapshot.get("assertions") or []))
    parts = [part.strip() for part in parts if part and part.strip()]
    return "\n".join(parts)[:2500] or " "


def material_hash(text: str) -> str:
    return hashlib.sha256((MATERIAL_VERSION + "\0" + text).encode("utf-8")).hexdigest()


def pack_vector(values: list[float]) -> bytes:
    return struct.pack("<" + str(DIMENSION) + "f", *validate_vector(values))


def similarity(blob: bytes, query: list[float]) -> float:
    if len(blob) != DIMENSION * 4:
        raise ValueError("Invalid story vector size")
    values = struct.unpack("<" + str(DIMENSION) + "f", blob)
    score = math.fsum(x * y for x, y in zip(values, query, strict=True))
    if not math.isfinite(score):
        raise ValueError("Nonfinite story ranking")
    return score


def pending(corpus):
    """Select only current, not archived revisions. Stale outbox is inert."""
    with corpus.connect() as db:
        row = db.execute("""
          SELECT s.id,s.revision,s.snapshot,s.owner_id
          FROM story_records s
          JOIN story_outbox o ON o.story_id=s.id AND o.story_revision=s.revision
          LEFT JOIN story_bge_index v ON v.story_id=s.id
          WHERE s.archived=0 AND s.merged_into IS NULL
            AND (v.story_id IS NULL OR v.story_revision<>s.revision
                 OR v.embedding_space<>? OR v.encoder_revision<>?)
          ORDER BY o.created_at,s.id LIMIT 1
        """, (SPACE, ENCODER_REVISION)).fetchone()
    if row is None:
        return None
    payload = material(json.loads(row["snapshot"]))
    return {"story_id": row["id"], "revision": row["revision"],
            "owner_id": row["owner_id"],
            "text": payload, "sha256": material_hash(payload)}


def install(corpus, item: dict, embedding: list[float]) -> bool:
    """Recheck revision and exact embedding material inside the short write."""
    blob = pack_vector(embedding)
    with corpus.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute(
            "SELECT revision,snapshot,archived,merged_into FROM story_records WHERE id=?",
            (item["story_id"],),
        ).fetchone()
        if (current is None or current["revision"] != item["revision"]
                or current["archived"] or current["merged_into"]
                or material_hash(material(json.loads(current["snapshot"]))) != item["sha256"]):
            return False
        db.execute("""
          INSERT INTO story_bge_index(
            story_id,story_revision,embedding_space,encoder_revision,
            material_sha256,vector_blob,updated_at)
          VALUES(?,?,?,?,?,?,datetime('now'))
          ON CONFLICT(story_id) DO UPDATE SET
            story_revision=excluded.story_revision,
            embedding_space=excluded.embedding_space,
            encoder_revision=excluded.encoder_revision,
            material_sha256=excluded.material_sha256,
            vector_blob=excluded.vector_blob,updated_at=excluded.updated_at
        """, (item["story_id"], item["revision"], SPACE, ENCODER_REVISION,
              item["sha256"], blob))
        db.execute("""UPDATE story_outbox SET state='indexed'
            WHERE story_id=? AND story_revision=?""",
                   (item["story_id"], item["revision"]))
        return True


def job_key(item: dict) -> str:
    return ("story-bge:" + SPACE + ":" + str(item["story_id"]) + ":"
            + str(item["revision"]) + ":" + item["sha256"])


def job_identity(item: dict) -> dict:
    return {"story_id": str(item["story_id"]),
            "story_revision": item["revision"],
            "material_sha256": item["sha256"],
            "model_revision": ENCODER_REVISION}


async def query_vector(queue, actor: str, query: str):
    """Request one actor-bound Kaggle query, returning explicit pending state.

    Repeat calls deduplicate via the existing durable queue; no local inference.
    """
    digest = hashlib.sha256(query.encode("utf-8")).hexdigest()
    key = "story-query:" + SPACE + ":" + digest
    identity = {"query_sha256": digest, "model_revision": ENCODER_REVISION}
    job_id = await asyncio.to_thread(
        queue.enqueue, actor, key, [query], kind="query", identity=identity)
    row = await asyncio.to_thread(queue.result, actor, job_id)
    if row["identity"] != identity:
        raise ValueError("Kaggle story query identity mismatch")
    if row["state"] != "done":
        state = await asyncio.to_thread(queue.status)
        return None, ("kaggle_" + state["state"])
    result = row["result"]
    if result is None or result.get("space") != SPACE or len(result.get("vectors", [])) != 1:
        raise ValueError("Kaggle BGE story query space mismatch")
    return validate_vector(result["vectors"][0]), "kaggle_ready"


class StoryVectorIndexer:
    """One existing Kaggle document job per durable outbox entry."""

    def __init__(self, corpus, queue):
        self.corpus = corpus
        self.queue = queue

    async def tick(self) -> int:
        item = await asyncio.to_thread(pending, self.corpus)
        if item is None:
            return 0
        if self.queue is None:
            return 0
        actor = str(item["owner_id"])
        key = job_key(item)
        identity = job_identity(item)
        job_id = await asyncio.to_thread(
            self.queue.enqueue, actor, key, [item["text"]],
            kind="document", identity=identity)
        row = await asyncio.to_thread(self.queue.result, actor, job_id)
        if row["identity"] != identity:
            raise ValueError("Kaggle story document identity mismatch")
        if row["state"] != "done":
            return 0
        result = row["result"]
        if result is None or result.get("space") != SPACE or len(result.get("vectors", [])) != 1:
            raise ValueError("Kaggle BGE story document space mismatch")
        written = int(await asyncio.to_thread(install, self.corpus, item, result["vectors"][0]))
        if written:
            await asyncio.to_thread(self.queue.retire_done_documents, actor, [job_id])
        return written
