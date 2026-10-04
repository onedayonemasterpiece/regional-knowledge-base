begin;
create table if not exists public.rkb_chunk_embeddings_bge (
 chunk_id uuid primary key references public.rkb_chunks(id) on delete cascade,
 embedding_space text not null check(embedding_space='bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'),
 model_revision text not null check(model_revision='5617a9f61b028005a4858fdac845db406aefb181'),
 revision bigint not null,text_sha256 text not null check(text_sha256 ~ '^[a-f0-9]{64}$'),
 embedding vector(1024) not null,updated_at timestamptz not null default now(),
 check((embedding <#> embedding) between -1.004 and -.996)
);
create index if not exists rkb_bge_cosine on public.rkb_chunk_embeddings_bge using hnsw(embedding vector_cosine_ops);
alter table public.rkb_chunk_embeddings_bge enable row level security;
drop policy if exists rkb_bge_read on public.rkb_chunk_embeddings_bge;
create policy rkb_bge_read on public.rkb_chunk_embeddings_bge for select using(
 exists(select 1 from public.rkb_chunks c where c.id=chunk_id and public.rkb_can_read_document(c.document_id))
);
grant select on public.rkb_chunk_embeddings_bge to anon,authenticated,rkb_app;

create or replace function public.rkb_multilingual_rankings(query_text text,bge_vector text,bge_space text,e5_vector text,e5_space text,aliases jsonb,depth integer)
returns table(chunk_id uuid,branch text,rank bigint,matched_alias text)
language plpgsql stable security invoker set search_path=public as $$
begin
 if bge_vector is not null and bge_space is distinct from 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1' then raise exception 'BGE space mismatch' using errcode='22023';end if;
 if e5_vector is not null and e5_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then raise exception 'E5 space mismatch' using errcode='22023';end if;
 if jsonb_typeof(coalesce(aliases,'[]'::jsonb))<>'array' or jsonb_array_length(coalesce(aliases,'[]'::jsonb))>20 then raise exception 'Alias bound' using errcode='22023';end if;
 return query
 with params as(select websearch_to_tsquery('simple',coalesce(query_text,'')) tsq,nullif(bge_vector,'')::vector(1024) bq,nullif(e5_vector,'')::vector(384) eq),
 visible as(select c.* from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id where c.revision=d.active_revision),
 bge as(select c.id,row_number() over(order by e.embedding <=> p.bq,c.id) r from public.rkb_chunk_embeddings_bge e join visible c on c.id=e.chunk_id cross join params p
  where p.bq is not null and e.embedding_space=bge_space and e.revision=c.revision and e.text_sha256=c.text_sha256
  order by e.embedding <=> p.bq,c.id limit least(greatest(depth,1),100)),
 e5 as(select c.id,row_number() over(order by e.embedding <=> p.eq,c.id) r from public.rkb_chunk_embeddings_e5 e join visible c on c.id=e.chunk_id cross join params p
  where p.eq is not null and e.embedding_space=e5_space and e.revision=c.revision and e.text_sha256=c.text_sha256
  order by e.embedding <=> p.eq,c.id limit least(greatest(depth,1),100)),
 lex as(select c.id,row_number() over(order by ts_rank_cd(c.fts,p.tsq) desc,c.id) r from visible c cross join params p where p.tsq<>''::tsquery and c.fts@@p.tsq
  order by ts_rank_cd(c.fts,p.tsq) desc,c.id limit least(greatest(depth,1),100)),
 alias_input as(select distinct left(a->>'name',200) name,case when a->>'kind'='current' then 'exact_current_alias' when a->>'kind'='historical' then 'exact_historical_alias' else 'exact_alias' end signal
  from jsonb_array_elements(coalesce(aliases,'[]'::jsonb)) a where length(a->>'name') between 1 and 200),
 alias_hits as(select c.id,a.signal,a.name,row_number() over(partition by a.signal order by c.id,a.name) r from visible c join alias_input a on c.fts@@phraseto_tsquery('simple',a.name))
 select id,'bge'::text,r,null::text from bge union all select id,'e5'::text,r,null::text from e5 union all select id,'lexical'::text,r,null::text from lex
 union all select id,signal,r,name from alias_hits where r<=least(greatest(depth,1),100);
end $$;
revoke all on function public.rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer) from public;
grant execute on function public.rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer) to anon,authenticated,rkb_app;
commit;
