"""Measure alternative BGE-only pgvector storage representations in temp tables.

The benchmark copies only existing vector values and UUIDs into session-local
temporary tables. It emits aggregate relation sizes only and leaves production
schema/data unchanged.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path

# Benchmarks execute as scripts; reuse the existing operator env loader.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "production"))
import psycopg
from psycopg.rows import dict_row
from operator_env import load_service_env


def sizes(db, table: str) -> dict:
    row=db.execute(
        """select count(*) rows,
                  pg_table_size(%s::regclass)::bigint table_bytes,
                  pg_indexes_size(%s::regclass)::bigint index_bytes,
                  pg_total_relation_size(%s::regclass)::bigint total_bytes
           from """+table,
        (table,table,table),
    ).fetchone()
    return dict(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded temporary BGE storage experiment")
    parser.add_argument("--sample-cap", type=int, default=6000)
    opts = parser.parse_args()
    if not 1 <= opts.sample_cap <= 20000:
        parser.error("--sample-cap must be 1..20000 to bound temp HNSW work")
    load_service_env()
    dsn=os.environ["KB_SUPABASE_SESSION_CONNECTION"]
    with psycopg.connect(
        dsn,row_factory=dict_row,connect_timeout=10,
        application_name="rkb-vector-storage-design",
        options="-c statement_timeout=120000 -c lock_timeout=2000",
    ) as db:
        version=db.execute("select extversion from pg_extension where extname='vector'").fetchone()
        existing=db.execute(
            """select count(*) rows,
                      pg_table_size('public.rkb_chunk_embeddings_bge')::bigint table_bytes,
                      pg_indexes_size('public.rkb_chunk_embeddings_bge')::bigint index_bytes,
                      pg_total_relation_size('public.rkb_chunk_embeddings_bge')::bigint total_bytes
               from public.rkb_chunk_embeddings_bge"""
        ).fetchone()
        anchor=db.execute(
            """select count(*) rows,
                      pg_total_relation_size('public.rkb_vector_items')::bigint total_bytes
               from public.rkb_vector_items"""
        ).fetchone()

        db.execute("create temp table stress_bge_half(chunk_id uuid primary key, embedding halfvec(1024) not null)")
        db.execute(
            "insert into stress_bge_half select chunk_id,embedding::halfvec(1024) "
            "from public.rkb_chunk_embeddings_bge order by chunk_id limit %s"
            , (opts.sample_cap,)
        )
        db.execute(
            "create index stress_bge_half_hnsw on stress_bge_half "
            "using hnsw (embedding halfvec_cosine_ops)"
        )
        db.execute("analyze stress_bge_half")
        half=sizes(db,"stress_bge_half")

        db.execute("create temp table stress_bge_binary(chunk_id uuid primary key, embedding halfvec(1024) not null)")
        db.execute(
            "insert into stress_bge_binary select chunk_id,embedding::halfvec(1024) "
            "from public.rkb_chunk_embeddings_bge order by chunk_id limit %s"
            , (opts.sample_cap,)
        )
        db.execute(
            "create index stress_bge_binary_hnsw on stress_bge_binary "
            "using hnsw ((binary_quantize(embedding)::bit(1024)) bit_hamming_ops)"
        )
        db.execute("analyze stress_bge_binary")
        binary=sizes(db,"stress_bge_binary")

        result={
            "pgvector_version":version["extversion"] if version else None,
            "existing_bge_vector_hnsw":dict(existing),
            "existing_vector_anchor":dict(anchor),
            "benchmark_sample_cap":opts.sample_cap,
            "measurement_mode":"temporary_tables_rollback_no_production_mutation",
            "bge_halfvec_hnsw":half,
            "bge_halfvec_binary_hnsw":binary,
        }
        for key in ("existing_bge_vector_hnsw","bge_halfvec_hnsw","bge_halfvec_binary_hnsw"):
            row=result[key]
            row["bytes_per_row"]=row["total_bytes"]/row["rows"] if row["rows"] else None
            row["projected_100k_total_bytes_linear"]=row["bytes_per_row"]*100000 if row["rows"] else None
        result["existing_vector_anchor"]["bytes_per_row"]=anchor["total_bytes"]/anchor["rows"]
        result["existing_vector_anchor"]["projected_100k_total_bytes_linear"]=result["existing_vector_anchor"]["bytes_per_row"]*100000
        print(json.dumps(result,indent=2))
        db.rollback()


if __name__=="__main__":
    main()