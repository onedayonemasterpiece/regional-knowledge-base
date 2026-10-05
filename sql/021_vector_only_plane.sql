begin;
-- Corpus/ACL authority lives in SQLite. This is only a trusted actor bridge scope.
alter table public.rkb_vector_items add column if not exists source_sha256 text;
alter table public.rkb_vector_items add column if not exists owner_user_id uuid;
alter table public.rkb_vector_items drop column if exists active;
create or replace function public.rkb_vector_scope(document uuid) returns boolean
language sql stable security invoker set search_path='' as $$
 select public.rkb_current_actor_id() is not null and document::text=any(string_to_array(current_setting('rkb.vector_documents',true),','))
$$;
revoke all on function public.rkb_vector_scope(uuid) from public;
grant execute on function public.rkb_vector_scope(uuid) to rkb_app;
create or replace function public.rkb_vector_revision_scope(document uuid, revision bigint) returns boolean
language sql stable security invoker set search_path='' as $rkb_scope$
 select public.rkb_vector_scope(document)
   and (nullif(current_setting('rkb.vector_revisions',true),'')::jsonb ->> document::text)::bigint = revision
$rkb_scope$;
revoke all on function public.rkb_vector_revision_scope(uuid,bigint) from public;
grant execute on function public.rkb_vector_revision_scope(uuid,bigint) to rkb_app;
drop policy if exists rkb_vector_items_read on public.rkb_vector_items;
create policy rkb_vector_items_read on public.rkb_vector_items for select to rkb_app using(public.rkb_vector_scope(document_id));
-- Independent foreign keys and read policies: no reference to corpus detail rows.
do $$declare t text;c record;begin
 foreach t in array array['rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge'] loop
  for c in select conname from pg_constraint where conrelid=('public.'||t)::regclass and contype='f' loop
   execute format('alter table public.%I drop constraint %I',t,c.conname);
  end loop;
  execute format('alter table public.%I add constraint %I foreign key(chunk_id) references public.rkb_vector_items(chunk_id)',t,t||'_anchor_fkey');
  for c in select policyname from pg_policies where schemaname='public' and tablename=t loop
   execute format('drop policy %I on public.%I',c.policyname,t);
  end loop;
  execute format('create policy vector_read on public.%I for select to rkb_app using(exists(select 1 from public.rkb_vector_items a where a.chunk_id=%I.chunk_id and public.rkb_vector_scope(a.document_id)))',t,t);
 end loop;
end $$;
create or replace function public.rkb_vector_candidates_v2(e5_query text,e5_space text,bge_query text,bge_space text,depth integer,active_chunks uuid[])
returns table(chunk_id uuid,branch text,rank bigint,revision bigint,text_sha256 text,search_material_sha256 text)
language plpgsql stable security invoker set search_path=public as $$
begin
 if e5_query is not null and e5_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then raise exception 'E5 space mismatch';end if;
 if bge_query is not null and bge_space is distinct from 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1' then raise exception 'BGE space mismatch';end if;
 if e5_query is not null then
 return query select a.chunk_id,'e5'::text,row_number() over(order by e.embedding <=> e5_query::vector(384),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_e5 e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where a.chunk_id=any(active_chunks) and e.embedding_space=e5_space and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> e5_query::vector(384),a.chunk_id limit least(greatest(depth,1),100);
 end if;
 if bge_query is not null then
 return query select a.chunk_id,'bge'::text,row_number() over(order by e.embedding <=> bge_query::vector(1024),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_bge e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where a.chunk_id=any(active_chunks) and e.model_revision='5617a9f61b028005a4858fdac845db406aefb181' and e.embedding_space=bge_space and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> bge_query::vector(1024),a.chunk_id limit least(greatest(depth,1),100);
 end if;
end $$;
revoke all on function public.rkb_vector_candidates_v2(text,text,text,text,integer,uuid[]) from public;
grant execute on function public.rkb_vector_candidates_v2(text,text,text,text,integer,uuid[]) to rkb_app;

-- Compact actor/document+revision scope. Unlike v2, this does not receive every
-- active chunk UUID on each request. RLS admits only server-authorized documents,
-- this function restricts ranking to their active revisions, and SQLite validates
-- every bounded returned candidate hash/revision before hydration.
create or replace function public.rkb_vector_candidates_v3(
  e5_query text,e5_space text,bge_query text,bge_space text,depth integer
)
returns table(chunk_id uuid,branch text,rank bigint,revision bigint,text_sha256 text,search_material_sha256 text)
language plpgsql stable security invoker set search_path=public as $rkb_v3$
begin
 if e5_query is not null and e5_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then raise exception 'E5 space mismatch';end if;
 if bge_query is not null and bge_space is distinct from 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1' then raise exception 'BGE space mismatch';end if;
 if e5_query is not null then
 return query select a.chunk_id,'e5'::text,row_number() over(order by e.embedding <=> e5_query::vector(384),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_e5 e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where public.rkb_vector_revision_scope(a.document_id,a.revision)
   and e.embedding_space=e5_space and e.revision=a.revision
   and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> e5_query::vector(384),a.chunk_id limit least(greatest(depth,1),100);
 end if;
 if bge_query is not null then
 return query select a.chunk_id,'bge'::text,row_number() over(order by e.embedding <=> bge_query::vector(1024),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_bge e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where public.rkb_vector_revision_scope(a.document_id,a.revision)
   and e.model_revision='5617a9f61b028005a4858fdac845db406aefb181'
   and e.embedding_space=bge_space and e.revision=a.revision
   and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> bge_query::vector(1024),a.chunk_id limit least(greatest(depth,1),100);
 end if;
end $rkb_v3$;
revoke all on function public.rkb_vector_candidates_v3(text,text,text,text,integer) from public;
grant execute on function public.rkb_vector_candidates_v3(text,text,text,text,integer) to rkb_app;
commit;