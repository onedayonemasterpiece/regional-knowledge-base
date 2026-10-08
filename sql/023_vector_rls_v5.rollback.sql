begin;

-- Exact v4-compatible policy rollback. Data, vectors and source authority
-- remain intact; revert runtime dispatch to v4 before applying.
drop policy if exists vector_read on public.rkb_chunk_embeddings_e5;
create policy vector_read on public.rkb_chunk_embeddings_e5
    for select to rkb_app using (
        exists(
            select 1 from public.rkb_vector_items a
            where a.chunk_id=rkb_chunk_embeddings_e5.chunk_id
              and public.rkb_vector_scope(a.document_id)
        )
    );
drop policy if exists vector_read on public.rkb_chunk_embeddings_bge;
create policy vector_read on public.rkb_chunk_embeddings_bge
    for select to rkb_app using (
        exists(
            select 1 from public.rkb_vector_items a
            where a.chunk_id=rkb_chunk_embeddings_bge.chunk_id
              and public.rkb_vector_scope(a.document_id)
        )
    );

drop policy if exists rkb_vector_items_read on public.rkb_vector_items;
create policy rkb_vector_items_read on public.rkb_vector_items
    for select to rkb_app using (public.rkb_vector_scope(document_id));

drop function if exists public.rkb_vector_candidates_v5(text,text,text,text,integer);
drop function if exists public.rkb_vector_readable_documents();

commit;
