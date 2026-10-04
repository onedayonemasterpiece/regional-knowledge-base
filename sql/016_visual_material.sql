begin;
alter table public.rkb_illustrations add column if not exists caption_text text not null default '';
alter table public.rkb_illustrations add column if not exists visual_description text check(length(visual_description) between 1 and 2000);
alter table public.rkb_illustrations add column if not exists visual_description_provenance text not null default 'model_observation' check(visual_description_provenance='model_observation');
alter table public.rkb_illustrations add column if not exists visual_description_language text;
alter table public.rkb_illustrations add column if not exists mirror_operation_id text;
alter table public.rkb_illustrations add column if not exists mirror_read_operation_id text;
alter table public.rkb_illustrations add column if not exists mirror_attempt_at timestamptz;
alter table public.rkb_illustrations add column if not exists mirror_error_type text;
alter table public.rkb_chunks add column if not exists search_material text;
alter table public.rkb_chunks add column if not exists search_material_sha256 text check(search_material_sha256 ~ '^[a-f0-9]{64}$');
alter table public.rkb_chunk_embeddings_e5 add column if not exists search_material_sha256 text check(search_material_sha256 ~ '^[a-f0-9]{64}$');
alter table public.rkb_chunk_embeddings_bge add column if not exists search_material_sha256 text check(search_material_sha256 ~ '^[a-f0-9]{64}$');
update public.rkb_chunks set search_material_sha256=text_sha256 where search_material_sha256 is null;
update public.rkb_chunk_embeddings_e5 set search_material_sha256=text_sha256 where search_material_sha256 is null;
update public.rkb_chunk_embeddings_bge set search_material_sha256=text_sha256 where search_material_sha256 is null;
create or replace function public.rkb_default_material_identity() returns trigger language plpgsql set search_path=public as $$
begin
 if NEW.search_material_sha256 is null then NEW.search_material_sha256:=NEW.text_sha256;end if;
 return NEW;
end $$;
do $$declare tab text;begin
 foreach tab in array array['rkb_chunks','rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge'] loop
  execute format('drop trigger if exists rkb_material_identity on public.%I',tab);
  execute format('create trigger rkb_material_identity before insert or update on public.%I for each row execute function public.rkb_default_material_identity()',tab);
 end loop;
end $$;
-- An image-only chunk has empty canonical source text, never an invented quote.
alter table public.rkb_chunks drop constraint if exists rkb_chunks_text_end_check;
alter table public.rkb_chunks drop constraint if exists rkb_chunks_check;
alter table public.rkb_chunks drop constraint if exists rkb_chunks_text_range;
alter table public.rkb_chunks add constraint rkb_chunks_text_range check(text_end>text_start or (text_end=text_start and cardinality(illustration_ids)>0 and text_sha256='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' and length(search_material)>0));

do $$declare definition text;signature text;begin
 definition:=pg_get_functiondef('public.rkb_insert_chunks(uuid,uuid,bigint,uuid,jsonb)'::regprocedure);
 if position('search_material_sha256' in definition)=0 then
  definition:=replace(definition,'text_end, text_sha256,','text_end, text_sha256, search_material, search_material_sha256,');
  definition:=replace(definition,'item->>''text_sha256'',','item->>''text_sha256'', item->>''search_material'', coalesce(item->>''search_material_sha256'',item->>''text_sha256''),');
  definition:=replace(definition,'text_sha256 = excluded.text_sha256,','text_sha256 = excluded.text_sha256, search_material = excluded.search_material, search_material_sha256 = excluded.search_material_sha256,');
  if position('search_material = excluded.search_material' in definition)=0 then raise exception 'unexpected chunk insertion definition';end if;
  execute definition;
 end if;
 foreach signature in array array['public.rkb_index_counts(uuid)','public.rkb_fast_e5_search(text,text,text,integer)','public.rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer)'] loop
  definition:=pg_get_functiondef(signature::regprocedure);
  if position('e.search_material_sha256=c.search_material_sha256' in definition)=0 then
   definition:=replace(definition,'e.text_sha256=c.text_sha256','e.text_sha256=c.text_sha256 and e.search_material_sha256=c.search_material_sha256');
   definition:=replace(definition,'b.text_sha256=c.text_sha256','b.text_sha256=c.text_sha256 and b.search_material_sha256=c.search_material_sha256');
   execute definition;
  end if;
 end loop;
end $$;
commit;
