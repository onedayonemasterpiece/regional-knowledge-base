"""Read-only, aggregate-only RKB storage audit. Never emits source text or IDs.

Run with the project's existing Python environment. Reuses the operator's service
configuration. One connection, a read-only transaction and per-query time bounds.
The JSON is operator evidence, not a corpus export; no VACUUM/DDL/DML is executed.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from operator_env import load_service_env

QUERIES = {
    "database": """
        select pg_database_size(current_database()) bytes,
               current_setting('transaction_read_only') read_only,
               current_setting('server_version') server_version
    """,
    "schemas": """
        select n.nspname schema,
               sum(pg_total_relation_size(c.oid)) total_bytes,
               sum(pg_table_size(c.oid)) table_toast_bytes,
               sum(pg_indexes_size(c.oid)) index_bytes
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where c.relkind in ('r','m') and n.nspname not like 'pg_toast%'
        group by n.nspname order by total_bytes desc
    """,
    "largest_relations": """
        select n.nspname schema,c.relname relation,
               pg_total_relation_size(c.oid) total_bytes,
               pg_table_size(c.oid) table_toast_bytes,
               pg_indexes_size(c.oid) index_bytes,
               coalesce(s.n_live_tup,0) estimated_live_rows,
               coalesce(s.n_dead_tup,0) estimated_dead_rows
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        left join pg_stat_user_tables s on s.relid=c.oid
        where c.relkind in ('r','m') and n.nspname not like 'pg_toast%'
        order by pg_total_relation_size(c.oid) desc limit 20
    """,
    "largest_indexes": """
        select schemaname schema,relname relation,indexrelname index_name,
               pg_relation_size(indexrelid) bytes,idx_scan
        from pg_stat_user_indexes order by pg_relation_size(indexrelid) desc limit 12
    """,
    "documents": """
        select count(*) total_roots,count(distinct source_sha256) distinct_source_bytes,
               count(*) filter(where active_revision>0) active_roots,
               count(*) filter(where active_revision=0) unactivated_roots,
               count(*) filter(where source_archive_status='verified') verified_archives,
               coalesce(sum(page_count) filter(where active_revision>0),0) active_declared_pages
        from public.rkb_documents
    """,
    "chunks": """
        select (c.revision=d.active_revision) active,count(*) rows,
               count(distinct c.document_id) roots,
               sum(octet_length(coalesce(c.source_text,''))) source_utf8_bytes,
               sum(octet_length(coalesce(c.search_material,''))) search_material_utf8_bytes,
               sum(pg_column_size(c.fts)) fts_value_bytes,
               count(c.embedding) legacy_embeddings,
               coalesce(sum(pg_column_size(c.embedding)),0) legacy_embedding_value_bytes
        from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id
        group by (c.revision=d.active_revision)
    """,
    "regions": """
        select (p.revision=d.active_revision) active,count(*) rows,
               sum(octet_length(coalesce(r.source_text,''))) source_utf8_bytes,
               sum(pg_column_size(r.bbox)) bbox_value_bytes
        from public.rkb_regions r join public.rkb_pages p on p.id=r.page_id
        join public.rkb_documents d on d.id=p.document_id
        group by (p.revision=d.active_revision)
    """,
    "pages": """
        select (p.revision=d.active_revision) active,count(*) rows,
               count(p.page_object_id) page_object_refs
        from public.rkb_pages p join public.rkb_documents d on d.id=p.document_id
        group by (p.revision=d.active_revision)
    """,
    "e5": """
        select (c.revision=d.active_revision) active,count(*) rows,
               sum(pg_column_size(e.embedding)) vector_value_bytes
        from public.rkb_chunk_embeddings_e5 e join public.rkb_chunks c on c.id=e.chunk_id
        join public.rkb_documents d on d.id=c.document_id
        group by (c.revision=d.active_revision)
    """,
    "bge": """
        select (c.revision=d.active_revision) active,count(*) rows,
               sum(pg_column_size(e.embedding)) vector_value_bytes
        from public.rkb_chunk_embeddings_bge e join public.rkb_chunks c on c.id=e.chunk_id
        join public.rkb_documents d on d.id=c.document_id
        group by (c.revision=d.active_revision)
    """,
    "objects": """
        select kind,(deleted_at is not null) marked_deleted,count(*) rows,
               sum(size_bytes) declared_object_bytes
        from public.rkb_objects group by kind,(deleted_at is not null) order by kind
    """,
    "ingestion_jobs": """
        select state,count(*) rows from public.rkb_ingestion_jobs group by state
    """,
    "active_root_shapes": """
        select count(*) roots,min(n) min_chunks,max(n) max_chunks,
               round(avg(n),2) mean_chunks
        from (select d.id,count(c.id) n from public.rkb_documents d
              left join public.rkb_chunks c on c.document_id=d.id and c.revision=d.active_revision
              where d.active_revision>0 group by d.id) x
    """,
    "public_columns": """
        select table_name,column_name,data_type,udt_name
        from information_schema.columns
        where table_schema='public' and table_name in
          ('rkb_documents','rkb_pages','rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge')
        order by table_name,ordinal_position
    """,
}


def main() -> int:
    load_service_env()
    dsn = os.environ.get('KB_SUPABASE_SESSION_CONNECTION')
    if not dsn:
        print(json.dumps({'error': 'configured_database_connection_missing'}))
        return 2
    report = {'observed_at': datetime.now(timezone.utc).isoformat(),
              'measurement_note': 'Allocated relation bytes include indexes and TOAST. Dead-row counts and index use are estimates since statistics reset. Object bytes are catalog declarations, not a bucket audit.'}
    try:
        with psycopg.connect(dsn, connect_timeout=10, row_factory=dict_row,
                             application_name='rkb-storage-budget-audit',
                             options='-c default_transaction_read_only=on -c statement_timeout=8000 -c lock_timeout=1000') as db:
            # Session poolers may ignore startup options; enforce read-only
            # explicitly on the actual SQL transaction, not only the connection.
            db.read_only = True
            db.execute("SET LOCAL statement_timeout = '8s'")
            for label, query in QUERIES.items():
                try:
                    with db.transaction():
                        report[label] = db.execute(query).fetchall()
                except Exception as error:
                    report[label] = {'error_type': type(error).__name__}
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__}))
        return 1
    print(json.dumps(report, ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
