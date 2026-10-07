begin;

-- Hot-path candidate retrieval for the SQLite-authority architecture.
-- The application has already admitted the actor and exact active revisions.
-- Read the transaction-local scope once, then rank directly without invoking
-- revision-scope/RLS helper functions once per vector row.
create or replace function public.rkb_vector_candidates_v4(
  e5_query text,e5_space text,bge_query text,bge_space text,depth integer
)
returns table(
  chunk_id uuid,branch text,rank bigint,revision bigint,
  text_sha256 text,search_material_sha256 text
)
language plpgsql stable security definer set search_path='' as $rkb_v4$
declare
  admitted jsonb;
  admitted_documents text[];
  actor uuid;
begin
  actor:=public.rkb_current_actor_id();
  admitted:=nullif(pg_catalog.current_setting('rkb.vector_revisions',true),'')::jsonb;
  admitted_documents:=pg_catalog.string_to_array(
    nullif(pg_catalog.current_setting('rkb.vector_documents',true),''),
    ','
  );
  if actor is null
     or admitted is null
     or pg_catalog.jsonb_typeof(admitted)<>'object'
     or admitted_documents is null then
    raise exception 'vector actor/revision scope required';
  end if;
  if exists(
       select 1
       from pg_catalog.jsonb_object_keys(admitted) as k(document_id)
       where not (k.document_id=any(admitted_documents))
     ) then
    raise exception 'vector document/revision scope mismatch';
  end if;

  if e5_query is not null
     and e5_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then
    raise exception 'E5 space mismatch';
  end if;
  if bge_query is not null
     and bge_space is distinct from 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1' then
    raise exception 'BGE space mismatch';
  end if;

  if e5_query is not null then
    return query
    with scope as materialized (
      select key::uuid document_id,value::bigint revision
      from pg_catalog.jsonb_each_text(admitted)
    )
    select a.chunk_id,'e5'::text,
      row_number() over(order by e.embedding OPERATOR(public.<=>) e5_query::public.vector(384),a.chunk_id),
      a.revision,a.text_sha256,a.search_material_sha256
    from scope s
    join public.rkb_vector_items a
      on a.document_id=s.document_id and a.revision=s.revision
    join public.rkb_chunk_embeddings_e5 e on e.chunk_id=a.chunk_id
    where e.embedding_space=e5_space
      and e.revision=a.revision
      and e.text_sha256=a.text_sha256
      and e.search_material_sha256=a.search_material_sha256
    order by e.embedding OPERATOR(public.<=>) e5_query::public.vector(384),a.chunk_id
    limit least(greatest(depth,1),100);
  end if;

  if bge_query is not null then
    return query
    with scope as materialized (
      select key::uuid document_id,value::bigint revision
      from pg_catalog.jsonb_each_text(admitted)
    )
    select a.chunk_id,'bge'::text,
      row_number() over(order by e.embedding OPERATOR(public.<=>) bge_query::public.vector(1024),a.chunk_id),
      a.revision,a.text_sha256,a.search_material_sha256
    from scope s
    join public.rkb_vector_items a
      on a.document_id=s.document_id and a.revision=s.revision
    join public.rkb_chunk_embeddings_bge e on e.chunk_id=a.chunk_id
    where e.model_revision='5617a9f61b028005a4858fdac845db406aefb181'
      and e.embedding_space=bge_space
      and e.revision=a.revision
      and e.text_sha256=a.text_sha256
      and e.search_material_sha256=a.search_material_sha256
    order by e.embedding OPERATOR(public.<=>) bge_query::public.vector(1024),a.chunk_id
    limit least(greatest(depth,1),100);
  end if;
end
$rkb_v4$;

revoke all on function public.rkb_vector_candidates_v4(text,text,text,text,integer) from public;
grant execute on function public.rkb_vector_candidates_v4(text,text,text,text,integer) to rkb_app;

commit;
