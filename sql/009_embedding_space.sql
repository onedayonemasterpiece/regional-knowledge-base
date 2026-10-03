begin;

create or replace function public.rkb_hybrid_search(
  query_text text,
  query_embedding text,
  query_embedding_space text,
  match_count integer
)
returns table (
  chunk_id uuid,
  document_id uuid,
  title text,
  page_ids uuid[],
  illustration_ids uuid[],
  score double precision
)
language sql
stable
security invoker
set search_path = public
as $$
  with
  params as (
    select
      websearch_to_tsquery('simple', coalesce(query_text,'')) as tsq,
      case
        when query_embedding is null or btrim(query_embedding) = '' then null
        else query_embedding::halfvec(768)
      end as qvec,
      nullif(btrim(query_embedding_space), '') as qspace
  ),
  lexical as (
    select
      c.id,
      row_number() over (
        order by ts_rank_cd(c.fts, p.tsq) desc, c.id
      ) as rank
    from public.rkb_chunks c
    cross join params p
    join public.rkb_documents d on d.id = c.document_id
    where c.revision = d.active_revision
      and p.tsq <> ''::tsquery
      and c.fts @@ p.tsq
    order by ts_rank_cd(c.fts, p.tsq) desc
    limit greatest(match_count * 5, 20)
  ),
  vector as (
    select
      c.id,
      row_number() over (
        order by c.embedding <=> p.qvec, c.id
      ) as rank
    from public.rkb_chunks c
    cross join params p
    join public.rkb_documents d on d.id = c.document_id
    where c.revision = d.active_revision
      and p.qvec is not null
      and p.qspace is not null
      and c.embedding is not null
      and c.metadata->>'embedding_space' = p.qspace
    order by c.embedding <=> p.qvec
    limit greatest(match_count * 5, 20)
  ),
  candidates as (
    select id from lexical
    union
    select id from vector
  )
  select
    c.id as chunk_id,
    c.document_id,
    c.title,
    c.page_ids,
    c.illustration_ids,
    (
      coalesce(1.0 / (60.0 + l.rank), 0.0)
      + coalesce(1.0 / (60.0 + v.rank), 0.0)
    )::double precision as score
  from candidates x
  join public.rkb_chunks c on c.id = x.id
  left join lexical l on l.id = c.id
  left join vector v on v.id = c.id
  order by score desc, c.id
  limit least(greatest(match_count,1),20);
$$;

revoke all on function public.rkb_hybrid_search(text,text,text,integer) from public;
grant execute on function public.rkb_hybrid_search(text,text,text,integer) to anon, authenticated;

-- Compatibility wrapper is deliberately lexical-only for callers that do not
-- identify their vector space. This prevents legacy clients from comparing
-- query vectors against embeddings created by an unknown model.
create or replace function public.rkb_hybrid_search(
  query_text text,
  query_embedding text default null,
  match_count integer default 8
)
returns table (
  chunk_id uuid,
  document_id uuid,
  title text,
  page_ids uuid[],
  illustration_ids uuid[],
  score double precision
)
language sql
stable
security invoker
set search_path = public
as $$
  select *
  from public.rkb_hybrid_search(
    query_text,
    query_embedding,
    null,
    match_count
  );
$$;

revoke all on function public.rkb_hybrid_search(text,text,integer) from public;
grant execute on function public.rkb_hybrid_search(text,text,integer) to anon, authenticated;

commit;
