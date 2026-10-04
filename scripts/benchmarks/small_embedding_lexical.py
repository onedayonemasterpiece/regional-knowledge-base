"""Read-only production SQL lexical baseline. No embedding calls; SQL query vectors are NULL."""
import asyncio
import json
import sys
import time
from pathlib import Path
from export_small_embedding_corpus import load_service_env, DOCUMENT_ID
from regional_knowledge.supabase_backend import backend_from_env

async def main():
    root = Path(sys.argv[1])
    questions = json.loads(Path('scripts/benchmarks/small_embedding_questions.json').read_text())['questions']
    load_service_env()
    backend = backend_from_env()
    assert type(backend.embedder).__name__ == 'LexicalOnlyEmbedder', 'external embeddings must be disabled'
    output = {'rankings': {}, 'query_seconds': [], 'rows': {}, 'mode': 'production simple websearch AND, ts_rank_cd, active revision, document-filtered, top100'}
    try:
        async with backend.data_client._connection({'x-rkb-service':'1'}) as conn:
            await conn.execute('set transaction read only')
            for i in range(104):
                q = questions[i%len(questions)]
                start = time.perf_counter()
                rows = await (await conn.execute('''
                    select c.id, ts_rank_cd(c.fts,p.tsq) as lexical_score
                    from public.rkb_chunks c
                    join public.rkb_documents d on d.id=c.document_id
                    cross join (select websearch_to_tsquery('simple',%s) as tsq) p
                    where c.document_id=%s and c.revision=d.active_revision
                      and p.tsq<>''::tsquery and c.fts@@p.tsq
                    order by ts_rank_cd(c.fts,p.tsq) desc,c.id limit 100
                ''',(q['query'],DOCUMENT_ID))).fetchall()
                output['query_seconds'].append(time.perf_counter()-start)
                output['rankings'][q['id']] = [str(row['id']) for row in rows]
                output['rows'][q['id']] = [{'id':str(row['id']),'score':row['lexical_score']} for row in rows]
            # Also capture actual RPC top20 to verify reproduction.
            for q in questions:
                rpc = await (await conn.execute('select * from public.rkb_hybrid_search(%s,NULL,NULL,20)',(q['query'],))).fetchall()
                expected = output['rankings'][q['id']][:20]
                assert [str(row['chunk_id']) for row in rpc if str(row['document_id'])==str(DOCUMENT_ID)] == expected
        (root/'lexical.json').write_text(json.dumps(output,indent=2))
        print('lexical',len(questions),'verified RPC; empty rankings',sum(not r for r in output['rankings'].values()))
    finally:
        await backend.aclose()

if __name__=='__main__': asyncio.run(main())
