begin;
create table if not exists public.rkb_chunk_embeddings_e5 (
 chunk_id uuid primary key references public.rkb_chunks(id) on delete cascade,
 embedding_space text not null check (embedding_space='e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1'),
 revision bigint not null,
 text_sha256 text not null check (text_sha256 ~ '^[a-f0-9]{64}$'),
 batch_sha256 text not null check (batch_sha256 ~ '^[a-f0-9]{64}$'),
 embedding vector(384) not null,
 updated_at timestamptz not null default now(),
 check ((embedding <#> embedding) between -1.004 and -0.996)
);
create index if not exists rkb_e5_cosine on public.rkb_chunk_embeddings_e5
 using hnsw (embedding vector_cosine_ops);
alter table public.rkb_chunk_embeddings_e5 enable row level security;
drop policy if exists rkb_e5_read on public.rkb_chunk_embeddings_e5;
create policy rkb_e5_read on public.rkb_chunk_embeddings_e5 for select using (
 exists (select 1 from public.rkb_chunks c where c.id=chunk_id and public.rkb_can_read_document(c.document_id))
);
grant select on public.rkb_chunk_embeddings_e5 to anon,authenticated,rkb_app;

create or replace function public.rkb_fast_e5_search(query_text text,query_embedding text,query_embedding_space text,match_count integer)
returns table(chunk_id uuid,document_id uuid,title text,page_ids uuid[],illustration_ids uuid[],score double precision,retrieval_mode text)
language plpgsql stable security invoker set search_path=public as $$
begin
 if query_embedding is not null and query_embedding_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then
  raise exception 'E5 embedding space mismatch' using errcode='22023';
 end if;
 return query
 with params as (
  select websearch_to_tsquery('simple',coalesce(query_text,'')) tsq,
   nullif(query_embedding,'')::vector(384) qvec
 ), lexical as (
  select c.id,row_number() over(order by ts_rank_cd(c.fts,p.tsq) desc,c.id) r
  from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id cross join params p
  where c.revision=d.active_revision and p.tsq<>''::tsquery and c.fts@@p.tsq
  order by ts_rank_cd(c.fts,p.tsq) desc,c.id limit greatest(least(greatest(match_count,1),20)*5,20)
 ), semantic as (
  select c.id,row_number() over(order by e.embedding <=> p.qvec,c.id) r
  from public.rkb_chunk_embeddings_e5 e join public.rkb_chunks c on c.id=e.chunk_id
  join public.rkb_documents d on d.id=c.document_id cross join params p
  where p.qvec is not null and e.embedding_space=query_embedding_space
    and c.revision=d.active_revision and e.revision=c.revision and e.text_sha256=c.text_sha256
  order by e.embedding <=> p.qvec,c.id limit greatest(least(greatest(match_count,1),20)*5,20)
 ), candidates as (select id from lexical union select id from semantic)
 select c.id,c.document_id,c.title,c.page_ids,c.illustration_ids,
  (coalesce(1.0/(60+l.r),0)+coalesce(1.0/(60+v.r),0))::double precision,
  case when exists(select 1 from semantic) then 'fast_e5' else 'lexical_only' end
 from candidates x join public.rkb_chunks c on c.id=x.id
 left join lexical l on l.id=c.id left join semantic v on v.id=c.id
 order by 6 desc,c.id limit least(greatest(match_count,1),20);
end $$;
revoke all on function public.rkb_fast_e5_search(text,text,text,integer) from public;
grant execute on function public.rkb_fast_e5_search(text,text,text,integer) to anon,authenticated,rkb_app;
commit;
