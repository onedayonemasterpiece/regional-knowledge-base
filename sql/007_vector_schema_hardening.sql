begin;

-- Supabase installs pgvector types in public for this project. Functions with a
-- deliberately empty search_path must therefore qualify halfvec explicitly.
-- Rewrite the live definitions so prior application-identity hardening from 006
-- is preserved verbatim.
do $rkb$
declare
  item record;
  definition text;
begin
  for item in
    select p.oid
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.proname like 'rkb_%'
      and p.prokind = 'f'
      and pg_catalog.pg_get_functiondef(p.oid) like '%::halfvec(%'
  loop
    definition := replace(
      pg_catalog.pg_get_functiondef(item.oid),
      '::halfvec(',
      '::public.halfvec('
    );
    execute definition;
  end loop;
end
$rkb$;

commit;
