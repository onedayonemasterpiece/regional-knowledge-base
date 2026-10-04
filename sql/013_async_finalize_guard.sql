-- Async HTTP finalize marks a validated job processing/cursor=finalize.
-- Keep every ownership, revision, coverage and evidence guard from 003.
begin;
do $$
declare signature text; definition text;
 old_guard text := 'job.state <> ''ready''';
 new_guard text := 'not (job.state = ''ready'' or (job.state = ''processing'' and coalesce(job.cursor, '''') = ''finalize''))';
begin
 foreach signature in array array[
  'public.rkb_insert_chunks(uuid,uuid,bigint,uuid,jsonb)',
  'public.rkb_activate_revision(uuid,uuid,bigint,jsonb)',
  'public.rkb_activate_revision_core(uuid,uuid,bigint,jsonb)'
 ] loop
  definition := pg_get_functiondef(signature::regprocedure);
  if position(new_guard in definition)>0 then continue; end if;
  if position(old_guard in definition)=0 then
   raise exception 'unexpected finalize guard in %', signature;
  end if;
  execute replace(definition,old_guard,new_guard);
 end loop;
end $$;
commit;
