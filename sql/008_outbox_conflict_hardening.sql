begin;

-- PL/pgSQL local variable event_id collides with the identically named outbox
-- column in an ON CONFLICT column list. Target the named UNIQUE constraint so
-- the statement is unambiguous while preserving the current live function body
-- (including the application-actor rewrite from migration 006).
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
      and p.proname like 'rkb_activate_revision%'
      and p.prokind = 'f'
      and pg_catalog.pg_get_functiondef(p.oid)
          like '%on conflict (target_service,event_id)%'
  loop
    definition := replace(
      pg_catalog.pg_get_functiondef(item.oid),
      'on conflict (target_service,event_id)',
      'on conflict on constraint rkb_integration_outbox_target_service_event_id_key'
    );
    execute definition;
  end loop;
end
$rkb$;

commit;
