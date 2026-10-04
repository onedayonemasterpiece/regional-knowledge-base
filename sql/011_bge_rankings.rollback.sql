begin;
drop function if exists public.rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer);
drop table if exists public.rkb_chunk_embeddings_bge;
commit;
