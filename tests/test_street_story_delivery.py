"""Outbox -> Street Story delivery (owner decision 2026-10-19: book facts reach Street Story POIs)."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest

from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.street_story_delivery import MAX_ATTEMPTS, StreetStoryDelivery

OWNER = "6f7f732e-6ff3-5425-a3f1-b2062f4d9e50"
OTHER = str(uuid4())


def outbox_row(owner=OWNER, state="pending_authorization"):
    event_id = str(uuid4())
    return {**defaults("rkb_integration_outbox"), "id": str(uuid4()), "owner_user_id": owner,
            "document_id": str(uuid4()), "revision": 1, "target_service": "street_story",
            "event_type": "poi.fact_evidence.v1", "event_id": event_id, "idempotency_key": "knowledge:x:1:" + event_id,
            "visibility": "private", "payload": {"event_id": event_id, "contract_version": "poi.fact_evidence.v1"},
            "state": state, "attempts": 0, "created_at": "2026-10-03 19:57:11.844278+00:00"}


def worker(tmp_path, handler, owners=(OWNER,)):
    corpus = SQLiteCorpus(tmp_path / "corpus.db")
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return corpus, StreetStoryDelivery(corpus, url="http://127.0.0.1:8188/v1/internal/rkb-poi-evidence",
                                       token="service-key", granted_owners=set(owners), http=http)


@pytest.mark.asyncio
async def test_granted_private_event_is_delivered_with_the_service_key_and_receipt(tmp_path):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"event_id": "e", "state": "attached", "poi_id": "p", "replayed": False})

    corpus, delivery = worker(tmp_path, handler)
    granted, foreign = outbox_row(), outbox_row(owner=OTHER)
    corpus.put("rkb_integration_outbox", [granted, foreign])
    assert await delivery.tick() == 1
    assert seen[0].headers["authorization"] == "Bearer service-key"
    rows = {row["id"]: row for row in corpus.rows("rkb_integration_outbox")}
    assert rows[granted["id"]]["state"] == "delivered" and rows[granted["id"]]["remote_receipt"]["poi_id"] == "p"
    assert rows[foreign["id"]]["state"] == "pending_authorization"  # no grant: nothing private leaves RKB
    assert await delivery.tick() == 0


@pytest.mark.asyncio
async def test_outage_waits_and_retries_refusal_is_a_dead_letter(tmp_path):
    status = {"code": 503}
    corpus, delivery = worker(tmp_path, lambda request: httpx.Response(status["code"], json={}))
    row = outbox_row(state="pending_delivery")
    corpus.put("rkb_integration_outbox", [row])
    await delivery.tick()
    waiting = corpus.one("rkb_integration_outbox", row["id"])
    assert waiting["state"] == "retry_wait" and waiting["attempts"] == 1 and waiting["last_error_code"] == "street_story_http_503"
    assert delivery.due() == []  # not before next_attempt_at
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    assert [r["id"] for r in delivery.due(later)] == [row["id"]]
    status["code"] = 422
    await delivery.deliver(waiting)
    assert corpus.one("rkb_integration_outbox", row["id"])["state"] == "dead_letter"


@pytest.mark.asyncio
async def test_attempts_are_bounded(tmp_path):
    corpus, delivery = worker(tmp_path, lambda request: httpx.Response(502, json={}))
    row = {**outbox_row(state="retry_wait"), "attempts": MAX_ATTEMPTS - 1}
    corpus.put("rkb_integration_outbox", [row])
    await delivery.deliver(row)
    assert corpus.one("rkb_integration_outbox", row["id"])["state"] == "dead_letter"
