"""Isolated guard/trigger proof. Only the dedicated synthetic fixture DSN."""
import argparse,hashlib,json,subprocess,sys
from pathlib import Path
import psycopg

def main(output):
    subprocess.run([sys.executable,'scripts/production/verify_graph_migration.py',str(output.with_name('graph-prerequisite-proof.json'))],check=True,capture_output=True)
    sql=Path('sql/014_automatic_indexing.sql').read_text()
    with psycopg.connect('postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test',autocommit=True) as db:
        assert db.info.host=='127.0.0.1' and db.info.dbname=='rkb_graph_test'
        db.execute(sql);db.execute(sql)
        for signature in ('rkb_fast_e5_search(text,text,text,integer)','rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer)'):
            definition=db.execute('select pg_get_functiondef(%s::regprocedure)',(signature,)).fetchone()[0]
            assert definition.count('from rkb_index_counts(null)')==(1 if 'fast_e5' in signature else 2)
        assert db.execute("select count(*) from pg_trigger where tgname='rkb_index_activation' and not tgisinternal").fetchone()[0]==1
    result={'migration_sha256':hashlib.sha256(sql.encode()).hexdigest(),'migration_twice':True,'snapshot_semantic_guards':True,'one_activation_trigger':True}
    output.write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);main(p.parse_args().output)
