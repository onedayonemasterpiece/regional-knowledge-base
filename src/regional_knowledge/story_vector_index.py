"""Derived BGE index for editorial stories, using the existing local encoder.

Story bodies, source ACL and editorial revisions remain in the SQLite authority.
The bounded index contains only reconstructible vectors. No private material is
sent to Supabase, a paid provider or another service. The existing indexing
daemon drains the already durable story_outbox, one story per idle pass.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import struct

from .bge_contract import DIMENSION, SPACE, validate_vector
from .local_bge import LocalBGEQueryEmbedder

ENCODER_REVISION = LocalBGEQueryEmbedder.encoder_revision
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
          SELECT s.id,s.revision,s.snapshot
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


class StoryVectorIndexer:
    """One bounded local query-encoder job per daemon pass, after corpus work."""

    def __init__(self, corpus, encoder):
        self.corpus = corpus
        self.encoder = encoder

    async def tick(self) -> int:
        if self.encoder is None:
            return 0
        item = await asyncio.to_thread(pending, self.corpus)
        if item is None:
            return 0
        vector = await self.encoder.embed(item["text"])
        return int(await asyncio.to_thread(install, self.corpus, item, vector))
