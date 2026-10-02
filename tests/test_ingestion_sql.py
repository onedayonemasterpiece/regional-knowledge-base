from pathlib import Path


SQL = (Path(__file__).parents[1] / "sql" / "003_ingestion_finalize.sql").read_text()


def test_ingestion_graph_pointer_is_server_object_id_not_object_key():
    assert "staged_graph_object_id uuid" in SQL
    assert "source_object_key" not in SQL


def test_chunk_materialization_builds_fts_without_storing_body_text():
    assert "rkb_insert_chunks" in SQL
    assert "to_tsvector('simple'" in SQL
    assert "text_object_id" in SQL
    assert "text_start" in SQL and "text_end" in SQL
    assert "normalized_text" in SQL
    assert "source_text" not in SQL


def test_activation_requires_ready_job_and_existing_chunks():
    assert "rkb_activate_revision" in SQL
    assert "job.state <> 'ready'" in SQL
    assert "revision has no chunks" in SQL
    assert "active_revision = p_revision" in SQL
    assert "state = 'finalized'" in SQL