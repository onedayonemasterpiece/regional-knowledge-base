begin;
alter table public.rkb_chunks add column if not exists source_text text;
alter table public.rkb_chunks alter column text_object_id drop not null;
alter table public.rkb_regions add column if not exists source_text text;
alter table public.rkb_documents add column if not exists source_format text check(source_format in ('pdf','djvu'));
alter table public.rkb_documents add column if not exists source_filename text;
alter table public.rkb_documents add column if not exists source_archive_ref text;
alter table public.rkb_documents add column if not exists source_archive_origin_ref text;
alter table public.rkb_documents add column if not exists source_archive_operation_id text;
alter table public.rkb_documents add column if not exists source_archive_read_id text;
alter table public.rkb_documents add column if not exists source_archive_status text not null default 'pending' check(source_archive_status in ('pending','verified'));
alter table public.rkb_documents add column if not exists source_archive_error text;
alter table public.rkb_documents add column if not exists source_archive_attempt_at timestamptz;
alter table public.rkb_objects add column if not exists deleted_at timestamptz;
do $$begin
 if not exists(select 1 from pg_constraint where conname='rkb_source_text_hash') then
  alter table public.rkb_chunks add constraint rkb_source_text_hash check(source_text is null or encode(sha256(convert_to(source_text,'UTF8')),'hex')=text_sha256);
 end if;
end $$;
do $$declare definition text;begin
 definition:=pg_get_functiondef('public.rkb_insert_chunks(uuid,uuid,bigint,uuid,jsonb)'::regprocedure);
 if position('source_text' in definition)=0 then
  definition:=replace(definition,'if not exists ('||chr(10)||'    select 1'||chr(10)||'    from public.rkb_objects o','if p_text_object_id is not null and not exists ('||chr(10)||'    select 1'||chr(10)||'    from public.rkb_objects o');
  definition:=replace(definition,'text_end, text_sha256,','text_end, text_sha256, source_text,');
  definition:=replace(definition,'item->>''text_sha256'',','item->>''text_sha256'', item->>''source_text'',');
  definition:=replace(definition,'text_sha256 = excluded.text_sha256,','text_sha256 = excluded.text_sha256, source_text = excluded.source_text,');
  if position('source_text = excluded.source_text' in definition)=0 then raise exception 'unexpected insertion definition';end if;
  execute definition;
 end if;
end $$;
commit;
