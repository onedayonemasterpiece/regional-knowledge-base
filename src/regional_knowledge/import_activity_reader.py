"""Read-only loopback import activity for a dedicated Launcher service credential.

The existing DevCoveer intake control and accepted-command journal are the
authority. No Kaggle status RPC, model work, private source content or writes.
"""
import asyncio
import base64
from datetime import datetime, timezone
import json
from uuid import UUID

from starlette.responses import JSONResponse

from .projects_hub_reader import PrivateInput

PATH = "/internal/import/activity"
SERVICE_ID = "launcher-import-watch"


def _uuid(value):
    try:
        return type(value) is str and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


class ImportActivityReader:
    def __init__(self, *, importer, service_id, credential_id, credential_epoch, current):
        if (service_id != SERVICE_ID or not _uuid(credential_id)
                or type(credential_epoch) is not int or credential_epoch < 1
                or not callable(current) or importer is None
                or importer.startup.importer is not importer
                or importer.startup.config.queue is not importer.queue):
            raise ValueError("existing importer and dedicated service scope required")
        self.importer = importer
        self.provider = importer.startup.config.oauth_provider
        if not callable(getattr(self.provider, "verify_service_credential", None)):
            raise ValueError("existing service credential authority required")
        self.service_id, self.credential_id = service_id, credential_id
        self.credential_epoch, self.current = credential_epoch, current

    @classmethod
    def from_file(cls, path, *, importer):
        config = PrivateInput(path, 4096)
        value = json.loads(config.value)
        if (type(value) is not dict
                or set(value) != {"codec", "service_id", "service_credential_id", "service_credential_epoch"}
                or value["codec"] != "rkb.import-activity-reader.v1"):
            raise ValueError("exact private import activity descriptor required")
        return cls(importer=importer, service_id=value["service_id"],
                   credential_id=value["service_credential_id"],
                   credential_epoch=value["service_credential_epoch"], current=config.current)

    def snapshot(self, ingestion_id, owner_person_id):
        # This method never calls book_ingest(status): native status can progress
        # commands and start Kaggle work. Only bounded local SQLite reads.
        with self.importer.queue.connect(write=False) as db:
            row = db.execute(
                "SELECT source_id,person_id,phase,last_operation_at,research_json "
                "FROM rkb_source_intake_control WHERE ingestion_id=? AND person_id=?",
                (ingestion_id, owner_person_id),
            ).fetchone()
            if row is None:
                if db.execute("SELECT 1 FROM rkb_source_intake_control WHERE ingestion_id=?",
                              (ingestion_id,)).fetchone():
                    raise PermissionError('activity target scope denied')
                return None
            native = db.execute(
                "SELECT phase,pending_operation_key,packet_mode,batch_count,"
                "published_batch_count,enriched_batch_count,rejected_batch_index,"
                "read_completion_requested FROM rkb_native_source_intake_state "
                "WHERE ingestion_id=? AND source_id=? AND person_id=?",
                (ingestion_id, row["source_id"], row["person_id"]),
            ).fetchone()
            accepted = set()
            last_stage = None
            # Stage commands are already in the existing local journal. Batch
            # count is NOT page count (a reviewed batch can contain four pages).
            if native and native["packet_mode"]:
                entries = db.execute(
                    "SELECT command_bytes FROM rkb_control_operations "
                    "WHERE source_id=? AND person_id=? AND operation_key LIKE ? "
                    "AND seal_state='ACCEPTED' AND apply_state='APPLIED' "
                    "ORDER BY sequence DESC LIMIT 10000",
                    (row["source_id"], row["person_id"], "source-stage:" + ingestion_id + ":%"),
                )
                for entry in entries:
                    command = json.loads(entry["command_bytes"])
                    stage = json.loads(base64.b64decode(command["payload_b64"], validate=True))
                    if stage.get("ingestion_id") != ingestion_id:
                        continue
                    pages = stage.get("model_payload", {}).get("pages", [])
                    if last_stage is None:
                        last_stage = (stage, pages)
                    if (type(stage.get("batch_index")) is int
                            and stage["batch_index"] < native["published_batch_count"]):
                        accepted.update(page["physical_page_index"] for page in pages)
        phase = row["phase"]
        if phase == "failed" or native and native["phase"] == "failed":
            state, action = "failed", "blocker"
        elif native and native["phase"] == "finalized" or phase == "finalized":
            state, action = "finalized", "done"
        elif phase == "acquiring" or native is None:
            state, action = "processing", "wait"
        elif (native["pending_operation_key"]
              or native["enriched_batch_count"] < native["batch_count"]
              or native["read_completion_requested"]):
            state, action = "processing", "wait"
        else:
            state, action = "staged", "continue_pages"
        # A cold or unverified Kaggle is not a ready-to-read book.
        if not self.importer.startup.reads_current() and state not in ("finalized", "failed"):
            state, action = "processing", "wait"

        cursor = None
        if state not in ("finalized", "failed"):
            try:
                research = json.loads(row["research_json"]) if row["research_json"] else {}
                cursor = research.get("continuation", {}).get("arguments", {}).get("cursor")
                if cursor is None and last_stage:
                    cursor = last_stage[0].get("research", {}).get("continuation", {}).get("arguments", {}).get("cursor")
                if cursor is None and last_stage and last_stage[1]:
                    cursor = str(last_stage[1][-1]["physical_page_index"] + 1)
            except (TypeError, ValueError, KeyError, IndexError):
                cursor = None
            if type(cursor) is not str or not cursor.isdecimal() or len(cursor) > 4 or int(cursor) > 2499:
                cursor = None
        timestamp = row["last_operation_at"]
        return dict(state=state, next_action=action,
                    last_operation_at=(datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                                       .replace("+00:00", "Z") if timestamp is not None else None),
                    accepted_pages=len(accepted), next_cursor=cursor)

    async def __call__(self, request):
        client = request.scope.get("client")
        supplied = request.headers.get("authorization", "")
        token = supplied[7:] if supplied.startswith("Bearer ") else ""
        if (not client or client[0] != "127.0.0.1" or not self.current()
                or request.headers.get("x-rkb-service") != self.service_id
                or request.headers.get("x-rkb-service-credential") != self.credential_id
                or request.headers.get("x-rkb-service-epoch") != str(self.credential_epoch)
                or not 32 <= len(token) <= 128):
            return JSONResponse({"error": "service_scope_required"}, status_code=403)
        service = self.provider.verify_service_credential(
            token, service_id=self.service_id, credential_id=self.credential_id,
            epoch=self.credential_epoch, operation="import_activity",
        )
        if service is None:
            return JSONResponse({"error": "service_scope_required"}, status_code=403)
        params = list(request.query_params.multi_items())
        if len(params) != 1 or params[0][0] != "ingestion_id" or not _uuid(params[0][1]):
            return JSONResponse({"error": "invalid_ingestion_id"}, status_code=422)
        try:
            result = await asyncio.to_thread(self.snapshot, params[0][1], service.scope_owner_person_id)
        except PermissionError:
            return JSONResponse({'error': 'service_scope_required'}, status_code=403)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return JSONResponse({"error": "activity_unavailable"}, status_code=503)
        if not self.current() or not self.provider.service_credential_current(service):
            return JSONResponse({"error": "service_scope_required"}, status_code=403)
        if result is None:
            return JSONResponse({"error": "ingestion_not_found"}, status_code=404)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})
