"""Internal execution fence shared by participant checks and request workers."""
import hashlib
import hmac
import json


def token_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def execution_allowed(db, context, timestamp):
    if not all((context.request_id,context.request_generation,context.executor_id,context.execution_token)):
        return False
    row=db.execute("""SELECT r.*,s.enabled AS service_enabled,s.epoch AS current_service_epoch,
        s.app_id AS service_app_id,s.team_id AS service_team_id
        FROM rkb_expert_requests r JOIN rkb_expert_services s ON s.id=r.executor_id
        WHERE r.id=?""",(context.request_id,)).fetchone()
    return bool(row and json.loads(row["origin"])["app_id"]==row["service_app_id"]
        and json.loads(row["origin"])["team_id"]==row["service_team_id"]
        and row["state"]=="leased" and row["generation"]==context.request_generation
        and row["executor_id"]==context.executor_id and row["lease_until"]>timestamp
        and row["service_enabled"] and row["service_epoch"]==row["current_service_epoch"]
        and row["person_id"]==str(context.principal.subject)
        and row["link_id"]==context.link_id and row["link_revision"]==context.link_revision
        and context.public_only==(row["output_scope"]=="public")
        and hmac.compare_digest(row["execution_token_hash"],token_hash(context.execution_token)))


def track_resources(db,context,*,document_ids=(),case_ids=()):
    """Called after the reader's ACL check, in its current transaction."""
    if not getattr(context,"request_id",None):
        return
    for kind,ids in (("document",document_ids),("case",case_ids)):
        for ident in ids:
            db.execute("INSERT OR IGNORE INTO rkb_expert_request_resources VALUES(?,?,?,?)",(context.request_id,context.request_generation,kind,str(ident)))
