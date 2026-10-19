"""Deliver finalized POI evidence events from the RKB outbox to Street Story.

Book finalize appends `poi.fact_evidence.v1` events to `rkb_integration_outbox`
(see docs/poi-integration.md). This worker is the asynchronous delivery step:
it POSTs each due event to Street Story's same-host intake with a dedicated
service key and records the outcome on the outbox row. Street Story is
idempotent by `event_id`/`idempotency_key`, so a retry after a lost reply is
safe and returns the stored receipt.

Private/workspace events stay `pending_authorization` unless their owner is
listed in `RKB_STREET_STORY_GRANTED_OWNERS` — the owner's explicit grant to
share their book evidence with their Street Story (owner decision 2026-10-19).
Without a grant nothing private leaves RKB.

States: pending_authorization -> pending_delivery -> delivered, with
retry_wait between failed attempts and dead_letter for permanent refusal or
exhausted attempts. A Street Story outage never touches book finalization.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

TABLE = "rkb_integration_outbox"
DEFAULT_URL = "http://127.0.0.1:8188/v1/internal/rkb-poi-evidence"
MAX_ATTEMPTS = 12


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class StreetStoryDelivery:
    def __init__(self, corpus, *, url: str, token: str, granted_owners: set[str],
                 http: httpx.AsyncClient | None = None):
        if not token:
            raise ValueError("Street Story delivery token required")
        self.corpus = corpus
        self.url = url
        self.token = token
        self.granted_owners = {owner.strip() for owner in granted_owners if owner.strip()}
        self.http = http

    def _save(self, row: dict[str, Any], **changes: Any) -> dict[str, Any]:
        updated = {**row, **changes, "updated_at": _now().isoformat()}
        self.corpus.put(TABLE, [updated])
        return updated

    def due(self, now: datetime | None = None) -> list[dict[str, Any]]:
        """Grant-eligible private events move to delivery; returns events due now, oldest first."""
        now = now or _now()
        rows = [row for row in self.corpus.rows(TABLE) if row.get("target_service") == "street_story"]
        due = []
        for row in rows:
            if row.get("state") == "pending_authorization" and str(row.get("owner_user_id")) in self.granted_owners:
                row = self._save(row, state="pending_delivery", last_error_code=None)
                log.info(json.dumps({"event": "street_story_delivery_authorized", "event_id": row.get("event_id")}))
            if row.get("state") in {"pending_delivery", "retry_wait"}:
                at = _parse(row.get("next_attempt_at"))
                if at is None or at <= now:
                    due.append(row)
        return sorted(due, key=lambda row: str(row.get("created_at") or ""))

    async def deliver(self, row: dict[str, Any]) -> dict[str, Any]:
        attempts = int(row.get("attempts") or 0) + 1
        row = self._save(row, state="delivering", attempts=attempts)
        own = self.http is None
        client = self.http or httpx.AsyncClient(timeout=30)
        try:
            response = await client.post(self.url, json=row["payload"],
                                          headers={"Authorization": "Bearer " + self.token})
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return self._retry(row, attempts, "street_story_unreachable:" + type(exc).__name__)
        finally:
            if own:
                await client.aclose()
        if response.status_code == 200:
            receipt = response.json()
            log.info(json.dumps({"event": "street_story_delivered", "event_id": row.get("event_id"),
                                 "poi_id": receipt.get("poi_id"), "state": receipt.get("state"),
                                 "replayed": receipt.get("replayed")}))
            return self._save(row, state="delivered", remote_receipt=receipt, last_error_code=None,
                              next_attempt_at=None)
        if response.status_code in {409, 413, 422}:
            code = f"street_story_http_{response.status_code}"
            log.warning(json.dumps({"event": "street_story_dead_letter", "event_id": row.get("event_id"), "code": code}))
            return self._save(row, state="dead_letter", last_error_code=code, next_attempt_at=None)
        return self._retry(row, attempts, f"street_story_http_{response.status_code}")

    def _retry(self, row: dict[str, Any], attempts: int, code: str) -> dict[str, Any]:
        if attempts >= MAX_ATTEMPTS:
            log.warning(json.dumps({"event": "street_story_dead_letter", "event_id": row.get("event_id"), "code": code}))
            return self._save(row, state="dead_letter", last_error_code=code, next_attempt_at=None)
        wait = min(3600, 30 * 2 ** (attempts - 1))
        log.warning(json.dumps({"event": "street_story_retry", "event_id": row.get("event_id"), "code": code,
                                "attempts": attempts, "wait_seconds": wait}))
        return self._save(row, state="retry_wait", last_error_code=code,
                          next_attempt_at=(_now() + timedelta(seconds=wait)).isoformat())

    async def tick(self) -> int:
        delivered = 0
        for row in self.due()[:20]:
            await self.deliver(row)
            delivered += 1
        return delivered


def from_env() -> StreetStoryDelivery:
    from .sqlite_corpus import SQLiteCorpus
    token_file = os.getenv("RKB_STREET_STORY_POI_TOKEN_FILE", "")
    token = Path(token_file).read_text().strip() if token_file else os.getenv("RKB_STREET_STORY_POI_TOKEN", "").strip()
    owners = set(os.getenv("RKB_STREET_STORY_GRANTED_OWNERS", "").split(","))
    return StreetStoryDelivery(SQLiteCorpus(os.environ["RKB_SQLITE_CORPUS_PATH"]),
                               url=os.getenv("RKB_STREET_STORY_POI_URL", DEFAULT_URL), token=token,
                               granted_owners=owners)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker = from_env()
    while True:
        try:
            if not await worker.tick():
                await asyncio.sleep(30)
        except Exception as exc:  # keep the loop alive; the row state already records per-event outcomes
            log.warning(json.dumps({"event": "street_story_delivery_loop_error", "error_type": type(exc).__name__}))
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
