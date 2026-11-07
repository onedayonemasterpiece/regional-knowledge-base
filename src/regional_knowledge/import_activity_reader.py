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
from .source_page_progress import publication_progress,next_unaccepted,enrichment_gaps

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
                "SELECT ingestion_id,source_id,person_id,phase,last_operation_at,research_json "
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
            pending = native["pending_operation_key"] if native else None
            # A committed catalog is not an in-flight save; status settles its local cursor later.
            if pending and pending.startswith(f"source-catalog:{ingestion_id}:") and db.execute(
                    "SELECT 1 FROM rkb_control_operations WHERE operation_key=? AND source_id=? AND person_id=? "
                    "AND seal_state='ACCEPTED' AND apply_state='APPLIED' AND outcome_state IN ('NOT_SENT','DURABLE')",
                    (pending,row["source_id"],row["person_id"])).fetchone():
                pending = None
            accepted, text_batches = publication_progress(db,row) if native and native['packet_mode'] else (set(),set())
            missing_semantic = enrichment_gaps(db,row) if native and native['packet_mode'] and (native['phase']=='finalized' or row['phase']=='finalized') else set()
            last_stage = rejected_pages = None
            # Stage commands are already in the existing local journal. Batch
            # count is NOT page count (a reviewed batch can contain four pages).
            if native and native["packet_mode"]:
                entries = db.execute(
                    "SELECT command_bytes FROM rkb_control_operations "
                    "WHERE source_id=? AND person_id=? AND operation_key LIKE ? "
                    "AND command_bytes IS NOT NULL AND seal_state='ACCEPTED' AND (apply_state='APPLIED' OR operation_key LIKE ?) "
                    "ORDER BY sequence DESC LIMIT 10000",
                    (row["source_id"], row["person_id"], "source-stage:" + ingestion_id + ":%",
                     f"source-stage:{ingestion_id}:{native['rejected_batch_index']}:%"),
                )
                for entry in entries:
                    command = json.loads(entry["command_bytes"])
                    stage = json.loads(base64.b64decode(command["payload_b64"], validate=True))
                    if stage.get("ingestion_id") != ingestion_id:
                        continue
                    pages = stage.get("model_payload", {}).get("pages", [])
                    if rejected_pages is None and stage.get('batch_index') == native['rejected_batch_index']:
                        rejected_pages = pages
                    if last_stage is None:
                        last_stage = (stage, pages)
                    if last_stage is not None and (native['rejected_batch_index'] is None or rejected_pages is not None):
                        break
                    # Stage APPLIED without matching source.packet TEXT APPLIED is not publication.
        phase = row["phase"]
        unpublished_rejection = bool(rejected_pages and native['rejected_batch_index'] not in text_batches)
        if phase == "failed" or native and native["phase"] == "failed":
            state, action = "failed", "blocker"
        elif (native and native["phase"] == "finalized" or phase == "finalized") and not (missing_semantic or unpublished_rejection):
            state, action = "finalized", "done"
        elif phase == "acquiring":
            state, action = "processing", "wait"
        elif native and (pending
              or native["enriched_batch_count"] < native["batch_count"]):
            state, action = "processing", "wait"
        else:
            state, action = "staged", "continue_pages"

        cursor = None
        if state not in ("finalized", "failed"):
            try:
                research = json.loads(row["research_json"]) if row["research_json"] else {}
                cursor = (str(rejected_pages[0]['physical_page_index']) if rejected_pages else
                          research.get("continuation", {}).get("arguments", {}).get("cursor"))
                if cursor is None and last_stage:
                    cursor = last_stage[0].get("research", {}).get("continuation", {}).get("arguments", {}).get("cursor")
                if cursor is None and last_stage and last_stage[1]:
                    cursor = str(last_stage[1][-1]["physical_page_index"] + 1)
            except (TypeError, ValueError, KeyError, IndexError):
                cursor = None
            if type(cursor) is not str or not cursor.isdecimal() or len(cursor) > 4 or int(cursor) > 2499:
                cursor = None
            elif not unpublished_rejection:
                cursor = next_unaccepted(cursor,accepted)
        if missing_semantic and not unpublished_rejection and not pending and state != 'failed':
            cursor = str(min(missing_semantic))
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
        params = list(request.query_params.multi_items())
        valid = len(params) == 1 and params[0][0] == "ingestion_id" and _uuid(params[0][1])

        def serve():
            # Credential checks read and decrypt the OAuth state file: keep them, like the
            # snapshot, off the event loop that also carries other requests and WSS heartbeats.
            service = self.provider.verify_service_credential(
                token, service_id=self.service_id, credential_id=self.credential_id,
                epoch=self.credential_epoch, operation="import_activity",
            )
            if service is None:
                raise PermissionError
            if not valid:
                return "invalid"
            result = self.snapshot(params[0][1], service.scope_owner_person_id)
            if not self.current() or not self.provider.service_credential_current(service):
                raise PermissionError
            return result
        try:
            result = await asyncio.to_thread(serve)
        except PermissionError:
            return JSONResponse({'error': 'service_scope_required'}, status_code=403)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return JSONResponse({"error": "activity_unavailable"}, status_code=503)
        if result == "invalid":
            return JSONResponse({"error": "invalid_ingestion_id"}, status_code=422)
        if result is None:
            return JSONResponse({"error": "ingestion_not_found"}, status_code=404)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})
