begin;
-- Compact candidate anchors; accepted text/catalog/geometry never enters this table.
create table if not exists public.rkb_vector_items(
 chunk_id uuid primary key,document_id uuid not null,revision bigint not null,
 text_sha256 text not null check(text_sha256 ~ '^[a-f0-9]{64}$'),
 search_material_sha256 text not null check(search_material_sha256 ~ '^[a-f0-9]{64}$'),active boolean not null
);
alter table public.rkb_vector_items enable row level security;
drop policy if exists rkb_vector_items_read on public.rkb_vector_items;
create policy rkb_vector_items_read on public.rkb_vector_items for select using(public.rkb_can_read_document(document_id));
grant select on public.rkb_vector_items to rkb_app;
create or replace function public.rkb_vector_candidates(e5_query text,e5_space text,bge_query text,bge_space text,depth integer)
returns table(chunk_id uuid,branch text,rank bigint,revision bigint,text_sha256 text,search_material_sha256 text)
language plpgsql stable security invoker set search_path=public as $$
begin
 if e5_query is not null and e5_space is distinct from 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1' then raise exception 'E5 space mismatch';end if;
 if bge_query is not null and bge_space is distinct from 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1' then raise exception 'BGE space mismatch';end if;
 if e5_query is not null then
 return query select a.chunk_id,'e5'::text,row_number() over(order by e.embedding <=> e5_query::vector(384),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_e5 e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where a.active and e.embedding_space=e5_space and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> e5_query::vector(384),a.chunk_id limit least(greatest(depth,1),100);
 end if;
 if bge_query is not null then
 return query select a.chunk_id,'bge'::text,row_number() over(order by e.embedding <=> bge_query::vector(1024),a.chunk_id),a.revision,a.text_sha256,a.search_material_sha256
 from rkb_chunk_embeddings_bge e join rkb_vector_items a on a.chunk_id=e.chunk_id
 where a.active and e.embedding_space=bge_space and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
 order by e.embedding <=> bge_query::vector(1024),a.chunk_id limit least(greatest(depth,1),100);
 end if;
end $$;
revoke all on function public.rkb_vector_candidates(text,text,text,text,integer) from public;
grant execute on function public.rkb_vector_candidates(text,text,text,text,integer) to rkb_app;
commit;
