import hashlib,json,subprocess,sys
from pathlib import Path
import psycopg
output=Path(sys.argv[1])
subprocess.run([sys.executable,'scripts/production/verify_robust_migration.py',str(output.with_suffix('.prerequisite.json'))],check=True)
source=Path('sql/017_telegram_archive_text.sql').read_text()
with psycopg.connect('postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test',autocommit=True) as db:
    db.execute(source);db.execute(source)
    definition=db.execute("select pg_get_functiondef('public.rkb_insert_chunks(uuid,uuid,bigint,uuid,jsonb)'::regprocedure)").fetchone()[0]
    assert 'if p_text_object_id is not null and not exists' in definition
    assert 'source_text = excluded.source_text' in definition
output.write_text(json.dumps({'migration_sha256':hashlib.sha256(source.encode()).hexdigest(),'migration_twice':True,'null_projection_guard':True,'source_hash_constraint':True},indent=2))
