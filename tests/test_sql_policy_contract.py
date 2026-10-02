from pathlib import Path


CORE = (Path(__file__).parents[1] / "sql" / "001_core.sql").read_text()


def test_raw_object_table_is_not_client_readable():
    assert "revoke all on public.rkb_objects from anon, authenticated;" in CORE
    assert "source_visibility <> 'public'" in CORE


def test_public_content_and_media_require_verified_rights():
    assert "rkb_public_content_requires_rights" in CORE
    assert "rkb_public_media_requires_rights" in CORE
    for allowed in (
        "licensed",
        "permission_granted",
        "public_domain_verified",
        "statutory_access_verified",
    ):
        assert allowed in CORE
    assert "rights_policy_version is not null" in CORE
    assert "rights_evidence -> 'public_distribution'" in CORE


def test_illustrations_have_independent_visibility_policy():
    start = CORE.index("create or replace function public.rkb_can_read_asset")
    helper = CORE[start : start + 2200]
    assert "target_visibility = 'public'" in helper
    assert "target_visibility = 'workspace'" in helper
    assert "target_visibility = 'private'" in helper
    assert "create policy rkb_illustrations_read" in CORE
    assert "public.rkb_can_read_asset(document_id, visibility)" in CORE


def test_owner_can_write_derived_document_graph_rows():
    for policy in (
        "rkb_pages_owner_write",
        "rkb_regions_owner_write",
        "rkb_relations_owner_write",
        "rkb_illustrations_owner_write",
        "rkb_chunks_owner_write",
    ):
        assert policy in CORE

def test_rls_uses_non_recursive_boolean_helpers():
    for helper in (
        "rkb_is_workspace_owner",
        "rkb_is_workspace_member",
        "rkb_is_document_owner",
        "rkb_has_document_grant",
        "rkb_can_read_document",
        "rkb_can_read_asset",
    ):
        assert f"function public.{helper}" in CORE
    assert CORE.count("security definer") >= 6


def test_postgres_does_not_store_corpus_body_text():
    region = CORE[
        CORE.index("create table if not exists public.rkb_regions"):
        CORE.index("create table if not exists public.rkb_region_relations")
    ]
    chunk = CORE[
        CORE.index("create table if not exists public.rkb_chunks"):
        CORE.index("create index if not exists rkb_chunks_fts_idx")
    ]
    assert "source_text" not in region
    assert "normalized_text" not in region
    assert "source_text" not in chunk
    assert "normalized_text" not in chunk
    assert "text_object_id" in chunk
    assert "text_start" in chunk and "text_end" in chunk
    assert "fts tsvector not null" in chunk

def test_ingestion_job_keeps_only_opaque_source_object_id():
    start = CORE.index("create table if not exists public.rkb_ingestion_jobs")
    end = CORE.index("alter table public.rkb_workspaces", start)
    table = CORE[start:end]
    assert "source_object_id uuid" in table
    assert "source_object_key" not in table
    assert "rkb_start_ingestion" in CORE

def test_ingestion_start_is_db_idempotent_under_model_retries():
    start = CORE.index("create or replace function public.rkb_start_ingestion")
    end = CORE.index("revoke all on function public.rkb_start_ingestion", start)
    function = CORE[start:end]
    assert "source_file_id = p_source_file_id" in function
    assert "source_sha256 is distinct from p_source_sha256" in function
    assert "exception when unique_violation" in function
    assert "subtransaction rolls back the unused document row" in function

def test_active_revision_and_verified_rights_are_not_direct_user_patch_fields():
    assert "revoke update on public.rkb_documents from authenticated;" in CORE
    grant_start = CORE.index("grant update (", CORE.index("rkb_documents_update_owner"))
    grant_end = CORE.index(";", grant_start)
    grant = CORE[grant_start:grant_end]
    assert "content_visibility" in grant
    assert "active_revision" not in grant
    assert "rights_status" not in grant
    assert "rights_evidence" not in grant
    assert "source_sha256" not in grant


def test_materialized_rows_are_mutable_only_before_activation():
    for policy in (
        "rkb_pages_owner_write",
        "rkb_regions_owner_write",
        "rkb_relations_owner_write",
        "rkb_illustrations_owner_write",
        "rkb_chunks_owner_write",
    ):
        start = CORE.index(f"create policy {policy}")
        section = CORE[start:start + 2200]
        assert "active_revision" in section


def test_relation_and_illustration_writes_cannot_cross_document_graphs():
    relation = CORE[
        CORE.index("create policy rkb_relations_owner_write"):
        CORE.index("drop policy if exists rkb_illustrations_read")
    ]
    assert "tp.document_id = sp.document_id" in relation
    assert "tp.revision = sp.revision" in relation

    illustration = CORE[
        CORE.index("create policy rkb_illustrations_owner_write"):
        CORE.index("drop policy if exists rkb_chunks_read")
    ]
    assert "p.id = rkb_illustrations.page_id" in illustration
    assert "p.document_id = rkb_illustrations.document_id" in illustration
    assert "r.id = rkb_illustrations.source_region_id" in illustration
    assert "r.page_id = p.id" in illustration