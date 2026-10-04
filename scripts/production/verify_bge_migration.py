"""Isolated BGE1024/E5384/legacy768 coexistence, ACL and rollback proof."""
import argparse,hashlib,json
from pathlib import Path
import psycopg
from regional_knowledge.bge_contract import SPACE,REVISION
from regional_knowledge.e5_contract import SPACE as E5_SPACE

def main():
    parser=argparse.ArgumentParser();parser.add_argument('dsn');parser.add_argument('output',type=Path);args=parser.parse_args()
    with psycopg.connect(args.dsn,autocommit=True) as db:
        if db.info.host!='127.0.0.1' or db.info.port!=54329 or db.info.dbname!='rkb_e5_test':raise RuntimeError('isolated fixture required')
        # Base synthetic fixture was installed by verify_e5_migration.py. Its
        # own rollback leaves only the legacy document/chunk fixture intact.
        db.execute(Path('sql/010_fast_e5.sql').read_text())
        migration=Path('sql/011_bge_rankings.sql').read_text();db.execute(migration);db.execute(migration)
        chunk,doc,revision,hash_=db.execute('select id,document_id,revision,text_sha256 from rkb_chunks limit 1').fetchone()
        owner=db.execute('select owner_user_id from rkb_documents where id=%s',(doc,)).fetchone()[0]
        bvec='['+','.join(['1']+['0']*1023)+']';evec='['+','.join(['1']+['0']*383)+']'
        db.execute('insert into rkb_chunk_embeddings_bge(chunk_id,embedding_space,model_revision,revision,text_sha256,embedding) values(%s,%s,%s,%s,%s,%s::vector)',(chunk,SPACE,REVISION,revision,hash_,bvec))
        db.execute('insert into rkb_chunk_embeddings_e5(chunk_id,embedding_space,revision,text_sha256,batch_sha256,embedding) values(%s,%s,%s,%s,%s,%s::vector) on conflict(chunk_id) do update set text_sha256=excluded.text_sha256',(chunk,E5_SPACE,revision,hash_,'b'*64,evec))
        aliases=json.dumps([{'name':'synthetic','kind':'historical'}])
        call='select * from rkb_multilingual_rankings(%s,%s,%s,%s,%s,%s::jsonb,100)'
        params=('synthetic',bvec,SPACE,evec,E5_SPACE,aliases)
        db.execute('set role rkb_app');db.execute("select set_config('rkb.actor_id',%s,false)",(str(owner),))
        rows=db.execute(call,params).fetchall();assert {row[1] for row in rows}=={'bge','e5','lexical','exact_historical_alias'}
        for bge,space,e5,e5space in [(evec,SPACE,evec,E5_SPACE),(bvec,E5_SPACE,evec,E5_SPACE),(bvec,SPACE,bvec,E5_SPACE)]:
            try:db.execute(call,('synthetic',bge,space,e5,e5space,aliases))
            except psycopg.Error:pass
            else:raise AssertionError('mixed vector spaces accepted')
        db.execute("select set_config('rkb.actor_id',%s,false)",('00000000-0000-0000-0000-000000000000',));assert db.execute(call,params).fetchall()==[]
        db.execute('reset role');db.execute('update rkb_chunk_embeddings_bge set revision=revision+1')
        assert all(row[1]!='bge' for row in db.execute(call,params).fetchall())
        db.execute(Path('sql/011_bge_rankings.rollback.sql').read_text())
        assert db.execute('select count(*) from rkb_chunks').fetchone()[0]==1
        assert db.execute('select count(*) from rkb_chunk_embeddings_e5').fetchone()[0]==1
        result={'migration_sha256':hashlib.sha256(migration.encode()).hexdigest(),'migration_twice':True,'dimension_space_rejection':True,'acl_denied':True,'independent_branches':True,'stale_revision_excluded':True,'rollback_preserves_e5_legacy':True}
        args.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
