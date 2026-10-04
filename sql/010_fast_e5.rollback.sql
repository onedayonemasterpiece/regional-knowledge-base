begin;
drop function if exists public.rkb_fast_e5_search(text,text,text,integer);
drop table if exists public.rkb_chunk_embeddings_e5;
commit;
