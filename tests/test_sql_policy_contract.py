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