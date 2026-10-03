begin;

create table if not exists public.rkb_users (
  id uuid primary key,
  status text not null default 'active'
    check (status in ('active','disabled')),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Preserve every identity already referenced by the corpus before replacing the
-- Supabase Auth foreign keys.  Regional Knowledge owns these UUIDs from this
-- migration forward; no personal secret or profile data is copied.
insert into public.rkb_users(id)
select user_id
from (
  select owner_user_id as user_id from public.rkb_workspaces
  union
  select user_id from public.rkb_workspace_members
  union
  select owner_user_id from public.rkb_documents
  union
  select grantee_user_id from public.rkb_document_grants
  union
  select owner_user_id from public.rkb_ingestion_jobs
  union
  select verified_by from public.rkb_author_profiles where verified_by is not null
  union
  select verified_by from public.rkb_author_authority where verified_by is not null
  union
  select owner_user_id from public.rkb_integration_outbox
) existing
where user_id is not null
on conflict (id) do nothing;

-- The application actor is transaction-local. Missing/invalid context fails
-- closed: NULL matches no owner/member/grant row and invalid UUID text aborts
-- the current transaction rather than falling back to an elevated identity.
create or replace function public.rkb_current_actor_id()
returns uuid
language sql
stable
security invoker
set search_path = ''
as $$
  select nullif(pg_catalog.current_setting('rkb.actor_id', true), '')::uuid;
$$;

revoke all on function public.rkb_current_actor_id() from public;

-- Replace only the live FK constraints that point at auth.users. Applied
-- migrations 001-005 remain immutable history.
do $rkb$
declare
  item record;
begin
  for item in
    select c.oid::regclass as table_name, con.conname
    from pg_catalog.pg_constraint con
    join pg_catalog.pg_class c on c.oid = con.conrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    join pg_catalog.pg_class rc on rc.oid = con.confrelid
    join pg_catalog.pg_namespace rn on rn.oid = rc.relnamespace
    where con.contype = 'f'
      and n.nspname = 'public'
      and c.relname like 'rkb_%'
      and rn.nspname = 'auth'
      and rc.relname = 'users'
  loop
    execute format(
      'alter table %s drop constraint %I',
      item.table_name,
      item.conname
    );
  end loop;
end
$rkb$;

do $rkb$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_workspaces'::regclass
      and conname = 'rkb_workspaces_owner_rkb_user_fkey'
  ) then
    alter table public.rkb_workspaces
      add constraint rkb_workspaces_owner_rkb_user_fkey
      foreign key (owner_user_id) references public.rkb_users(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_workspace_members'::regclass
      and conname = 'rkb_workspace_members_user_rkb_user_fkey'
  ) then
    alter table public.rkb_workspace_members
      add constraint rkb_workspace_members_user_rkb_user_fkey
      foreign key (user_id) references public.rkb_users(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_documents'::regclass
      and conname = 'rkb_documents_owner_rkb_user_fkey'
  ) then
    alter table public.rkb_documents
      add constraint rkb_documents_owner_rkb_user_fkey
      foreign key (owner_user_id) references public.rkb_users(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_document_grants'::regclass
      and conname = 'rkb_document_grants_user_rkb_user_fkey'
  ) then
    alter table public.rkb_document_grants
      add constraint rkb_document_grants_user_rkb_user_fkey
      foreign key (grantee_user_id) references public.rkb_users(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_ingestion_jobs'::regclass
      and conname = 'rkb_ingestion_jobs_owner_rkb_user_fkey'
  ) then
    alter table public.rkb_ingestion_jobs
      add constraint rkb_ingestion_jobs_owner_rkb_user_fkey
      foreign key (owner_user_id) references public.rkb_users(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_author_profiles'::regclass
      and conname = 'rkb_author_profiles_verified_rkb_user_fkey'
  ) then
    alter table public.rkb_author_profiles
      add constraint rkb_author_profiles_verified_rkb_user_fkey
      foreign key (verified_by) references public.rkb_users(id) on delete set null;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_author_authority'::regclass
      and conname = 'rkb_author_authority_verified_rkb_user_fkey'
  ) then
    alter table public.rkb_author_authority
      add constraint rkb_author_authority_verified_rkb_user_fkey
      foreign key (verified_by) references public.rkb_users(id) on delete set null;
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.rkb_integration_outbox'::regclass
      and conname = 'rkb_outbox_owner_rkb_user_fkey'
  ) then
    alter table public.rkb_integration_outbox
      add constraint rkb_outbox_owner_rkb_user_fkey
      foreign key (owner_user_id) references public.rkb_users(id) on delete cascade;
  end if;
end
$rkb$;

-- Rewrite the active function definitions in-place. This intentionally targets
-- only public.rkb_* functions and does not mutate historical migration files.
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
      and pg_catalog.pg_get_functiondef(p.oid) like '%auth.uid()%'
  loop
    definition := replace(
      pg_catalog.pg_get_functiondef(item.oid),
      'auth.uid()',
      'public.rkb_current_actor_id()'
    );
    execute definition;
  end loop;
end
$rkb$;

-- Recreate only policies whose live predicates still depend on auth.uid().
-- Roles and permissive/restrictive semantics are preserved from pg_policies.
do $rkb$
declare
  item record;
  role_sql text;
  command_sql text;
  statement text;
begin
  for item in
    select *
    from pg_catalog.pg_policies
    where schemaname = 'public'
      and tablename like 'rkb_%'
      and (
        coalesce(qual, '') like '%auth.uid()%'
        or coalesce(with_check, '') like '%auth.uid()%'
      )
  loop
    select string_agg(
      case when value = 'public' then 'public' else quote_ident(value) end,
      ', '
    )
    into role_sql
    from unnest(item.roles) value;

    command_sql := upper(item.cmd);
    execute format(
      'drop policy %I on %I.%I',
      item.policyname,
      item.schemaname,
      item.tablename
    );

    statement := format(
      'create policy %I on %I.%I as %s for %s to %s',
      item.policyname,
      item.schemaname,
      item.tablename,
      case when item.permissive = 'PERMISSIVE' then 'permissive' else 'restrictive' end,
      command_sql,
      coalesce(role_sql, 'public')
    );

    if item.qual is not null then
      statement := statement || ' using (' ||
        replace(item.qual, 'auth.uid()', 'public.rkb_current_actor_id()') || ')';
    end if;
    if item.with_check is not null then
      statement := statement || ' with check (' ||
        replace(item.with_check, 'auth.uid()', 'public.rkb_current_actor_id()') || ')';
    end if;
    execute statement;
  end loop;
end
$rkb$;

-- A dedicated NOLOGIN/NOBYPASSRLS execution role ensures a privileged Session
-- Pooler connection cannot accidentally bypass row policies during user work.
do $rkb$
begin
  if not exists (
    select 1 from pg_catalog.pg_roles where rolname = 'rkb_app'
  ) then
    create role rkb_app
      nologin
      nosuperuser
      nocreatedb
      nocreaterole
      noinherit
      nobypassrls;
  end if;
  execute format('grant rkb_app to %I', session_user);
end
$rkb$;

grant usage on schema public to rkb_app;
grant select, insert, update, delete on all tables in schema public to rkb_app;
grant execute on all functions in schema public to rkb_app;
grant execute on function public.rkb_current_actor_id() to rkb_app;

alter table public.rkb_users enable row level security;
revoke all on public.rkb_users from rkb_app;

commit;
