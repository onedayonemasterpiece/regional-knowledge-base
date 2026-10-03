from pathlib import Path


def test_application_identity_migration_contract() -> None:
    sql = Path("sql/006_application_identity.sql").read_text(encoding="utf-8")
    lowered = sql.lower()

    assert "create table if not exists public.rkb_users" in lowered
    assert "create or replace function public.rkb_current_actor_id()" in lowered
    assert "current_setting('rkb.actor_id', true)" in lowered
    assert "create role rkb_app" in lowered
    assert "nobypassrls" in lowered
    assert "set local role" not in lowered  # runtime responsibility, not migration
    assert "pg_get_functiondef" in lowered
    assert "pg_policies" in lowered
    assert "public.rkb_current_actor_id()" in lowered

    # Every historical auth.users ownership edge has an app-user replacement.
    for constraint in (
        "rkb_workspaces_owner_rkb_user_fkey",
        "rkb_workspace_members_user_rkb_user_fkey",
        "rkb_documents_owner_rkb_user_fkey",
        "rkb_document_grants_user_rkb_user_fkey",
        "rkb_ingestion_jobs_owner_rkb_user_fkey",
        "rkb_author_profiles_verified_rkb_user_fkey",
        "rkb_author_authority_verified_rkb_user_fkey",
        "rkb_outbox_owner_rkb_user_fkey",
    ):
        assert constraint in sql
