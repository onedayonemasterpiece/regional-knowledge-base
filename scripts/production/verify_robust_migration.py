"""Isolated additive migration proof; never uses a production DSN."""
import json,hashlib,subprocess,sys
from pathlib import Path
import psycopg

def main(output):
    subprocess.run([sys.executable,'scripts/production/verify_graph_migration.py',str(output.with_suffix('.graph.json'))],check=True,capture_output=True)
    with psycopg.connect('postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test',autocommit=True) as db:
        assert db.info.host=='127.0.0.1' and db.info.dbname=='rkb_graph_test'
        before=db.execute('select count(*) from rkb_chunks').fetchone()
        hashes={}
        for name in ('015_source_identity.sql','016_visual_material.sql'):
            source=Path('sql',name).read_text();hashes[name]=hashlib.sha256(source.encode()).hexdigest()
            db.execute(source);db.execute(source)
        assert db.execute('select count(*) from rkb_chunks').fetchone()==before
        assert db.execute("select count(*) from rkb_chunks where search_material_sha256 is distinct from text_sha256").fetchone()[0]==0
        for signature in ('rkb_index_counts(uuid)','rkb_fast_e5_search(text,text,text,integer)','rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer)'):
            assert 'search_material_sha256' in db.execute('select pg_get_functiondef(%s::regprocedure)',('public.'+signature,)).fetchone()[0]
    output.write_text(json.dumps({'migration_hashes':hashes,'migration_twice':True,'legacy_chunks_preserved':True,'material_snapshot_guards':True},indent=2))

if __name__=='__main__':main(Path(sys.argv[1]))
