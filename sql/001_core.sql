begin;

create extension if not exists pgcrypto;
create extension if not exists vector;

do $$ begin
  create type public.rkb_visibility as enum ('private','workspace','public');
exception when duplicate_object then null; end $$;

do $$ begin
  create type public.rkb_rights_status as enum (
    'unknown','restricted','licensed','permission_granted',
    'public_domain_candidate','public_domain_verified','statutory_access_verified'
  );
exception when duplicate_object then null; end $$;

create table if not exists public.rkb_workspaces (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  name text not null check (char_length(name) between 1 and 160),
  created_at timestamptz not null default now()
);

create table if not exists public.rkb_workspace_members (
  workspace_id uuid not null references public.rkb_workspaces(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null check (role in ('viewer','editor','admin')),
  created_at timestamptz not null default now(),
  primary key (workspace_id,user_id)
);

create table if not exists public.rkb_documents (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  workspace_id uuid references public.rkb_workspaces(id) on delete set null,
  title text not null,
  authors jsonb not null default '[]'::jsonb,
  publication_year integer,
  language text,
  source_sha256 text not null check (source_sha256 ~ '^[a-f0-9]{64}$'),
  source_visibility public.rkb_visibility not null default 'private'
    check (source_visibility <> 'public'),
  content_visibility public.rkb_visibility not null default 'private',
  rights_status public.rkb_rights_status not null default 'unknown',
  rights_evidence jsonb not null default '{}'::jsonb,
  rights_policy_version text,
  active_revision bigint not null default 0,
  page_count integer check (page_count is null or page_count > 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint rkb_public_content_requires_rights check (
    content_visibility <> 'public'
    or (
      rights_status in (
        'licensed','permission_granted','public_domain_verified',
        'statutory_access_verified'
      )
      and rights_policy_version is not null
      and (rights_evidence -> 'public_distribution') = 'true'::jsonb
    )
  )
);

create table if not exists public.rkb_document_grants (
  document_id uuid not null references public.rkb_documents(id) on delete cascade,
  grantee_user_id uuid not null references auth.users(id) on delete cascade,
  role text not null check (role in ('viewer','editor')),
  created_at timestamptz not null default now(),
  primary key(document_id,grantee_user_id)
);

create table if not exists public.rkb_objects (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.rkb_documents(id) on delete cascade,
  kind text not null check (kind in (
    'source_pdf','page_render','illustration_crop','document_graph','text_projection'
  )),
  object_key text not null,
  sha256 text not null check (sha256 ~ '^[a-f0-9]{64}$'),
  mime_type text not null,
  size_bytes bigint not null check (size_bytes > 0),
  access_class public.rkb_visibility not null default 'private',
  created_at timestamptz not null default now(),
  unique(document_id,object_key)
);

create table if not exists public.rkb_pages (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.rkb_documents(id) on delete cascade,
  physical_page_index integer not null check (physical_page_index >= 0),
  printed_page_number text,
  width integer not null check (width > 0),
  height integer not null check (height > 0),
  layout_kind text,
  page_object_id uuid references public.rkb_objects(id) on delete set null,
  revision bigint not null,
  unique(document_id,physical_page_index,revision)
);

create table if not exists public.rkb_regions (
  id uuid primary key default gen_random_uuid(),
  page_id uuid not null references public.rkb_pages(id) on delete cascade,
  kind text not null check (kind in (
    'heading','body','caption','footnote','figure','table',
    'header','footer','page_number','marginalia'
  )),
  bbox jsonb not null,
  column_id text,
  reading_order integer not null check (reading_order >= 0),
  text_sha256 text check (
    text_sha256 is null
    or (char_length(text_sha256) = 64 and text_sha256 ~ '^[a-f0-9]+$')
  ),
  confidence real check (confidence is null or (confidence >= 0 and confidence <= 1)),
  needs_review boolean not null default false
);

create table if not exists public.rkb_region_relations (
  source_region_id uuid not null references public.rkb_regions(id) on delete cascade,
  target_region_id uuid not null references public.rkb_regions(id) on delete cascade,
  kind text not null check (kind in ('caption_of','footnote_of','continues_to','illustrates','refers_to')),
  primary key(source_region_id,target_region_id,kind)
);

create table if not exists public.rkb_illustrations (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.rkb_documents(id) on delete cascade,
  page_id uuid not null references public.rkb_pages(id) on delete cascade,
  source_region_id uuid not null references public.rkb_regions(id) on delete cascade,
  crop_object_id uuid references public.rkb_objects(id) on delete set null,
  kind text not null check (kind in ('photo','map','drawing','diagram','facsimile','other')),
  caption_region_ids uuid[] not null default '{}',
  nearby_region_ids uuid[] not null default '{}',
  visibility public.rkb_visibility not null default 'private',
  rights_status public.rkb_rights_status not null default 'unknown',
  rights_evidence jsonb not null default '{}'::jsonb,
  rights_policy_version text,
  vibepublish_entry_ref text,
  source_crop_sha256 text check (source_crop_sha256 is null or source_crop_sha256 ~ '^[a-f0-9]{64}$'),
  constraint rkb_public_media_requires_rights check (
    visibility <> 'public'
    or (
      rights_status in (
        'licensed','permission_granted','public_domain_verified',
        'statutory_access_verified'
      )
      and rights_policy_version is not null
      and (rights_evidence -> 'public_distribution') = 'true'::jsonb
    )
  )
);

create table if not exists public.rkb_chunks (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.rkb_documents(id) on delete cascade,
  revision bigint not null,
  region_ids uuid[] not null default '{}',
  page_ids uuid[] not null default '{}',
  illustration_ids uuid[] not null default '{}',
  footnote_region_ids uuid[] not null default '{}',
  text_object_id uuid not null references public.rkb_objects(id) on delete restrict,
  text_start bigint not null check (text_start >= 0),
  text_end bigint not null check (text_end > text_start),
  text_sha256 text not null check (
    char_length(text_sha256) = 64 and text_sha256 ~ '^[a-f0-9]+$'
  ),
  title text not null,
  embedding halfvec(768),
  fts tsvector not null,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists rkb_chunks_fts_idx on public.rkb_chunks using gin(fts);
create index if not exists rkb_chunks_embedding_idx
  on public.rkb_chunks using hnsw (embedding halfvec_cosine_ops);
create index if not exists rkb_chunks_document_idx on public.rkb_chunks(document_id,revision);

create table if not exists public.rkb_ingestion_jobs (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  document_id uuid references public.rkb_documents(id) on delete cascade,
  source_file_id text,
  source_object_key text,
  source_sha256 text check (source_sha256 is null or source_sha256 ~ '^[a-f0-9]{64}$'),
  state text not null check (state in (
    'staged','processing','needs_review','ready','finalized','failed'
  )),
  cursor text,
  staged_revision bigint not null default 1,
  warnings jsonb not null default '[]'::jsonb,
  error_code text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.rkb_workspaces enable row level security;
alter table public.rkb_workspace_members enable row level security;
alter table public.rkb_documents enable row level security;
alter table public.rkb_document_grants enable row level security;
alter table public.rkb_objects enable row level security;
alter table public.rkb_pages enable row level security;
alter table public.rkb_regions enable row level security;
alter table public.rkb_region_relations enable row level security;
alter table public.rkb_illustrations enable row level security;
alter table public.rkb_chunks enable row level security;
alter table public.rkb_ingestion_jobs enable row level security;

-- Boolean authorization helpers deliberately bypass table RLS to prevent policy
-- recursion. They expose no rows or object keys and bind every decision to auth.uid().
create or replace function public.rkb_is_workspace_owner(target_workspace uuid)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select exists (
    select 1
    from public.rkb_workspaces w
    where w.id = target_workspace and w.owner_user_id = auth.uid()
  );
$$;

create or replace function public.rkb_is_workspace_member(target_workspace uuid)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select
    public.rkb_is_workspace_owner(target_workspace)
    or exists (
      select 1
      from public.rkb_workspace_members m
      where m.workspace_id = target_workspace and m.user_id = auth.uid()
    );
$$;

create or replace function public.rkb_is_document_owner(target_document uuid)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select exists (
    select 1
    from public.rkb_documents d
    where d.id = target_document and d.owner_user_id = auth.uid()
  );
$$;

create or replace function public.rkb_has_document_grant(target_document uuid)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select exists (
    select 1
    from public.rkb_document_grants g
    where g.document_id = target_document and g.grantee_user_id = auth.uid()
  );
$$;

create or replace function public.rkb_can_read_document(target_document uuid)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select exists (
    select 1
    from public.rkb_documents d
    where d.id = target_document
      and (
        d.content_visibility = 'public'
        or d.owner_user_id = auth.uid()
        or exists (
          select 1
          from public.rkb_document_grants g
          where g.document_id = d.id and g.grantee_user_id = auth.uid()
        )
        or (
          d.workspace_id is not null
          and d.content_visibility in ('workspace','public')
          and (
            d.owner_user_id = auth.uid()
            or exists (
              select 1
              from public.rkb_workspace_members m
              where m.workspace_id = d.workspace_id and m.user_id = auth.uid()
            )
          )
        )
      )
  );
$$;

create or replace function public.rkb_can_read_asset(
  target_document uuid,
  target_visibility public.rkb_visibility
)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select exists (
    select 1
    from public.rkb_documents d
    where d.id = target_document
      and (
        d.owner_user_id = auth.uid()
        or (
          target_visibility = 'public'
          and d.content_visibility = 'public'
        )
        or (
          target_visibility = 'workspace'
          and d.workspace_id is not null
          and exists (
            select 1
            from public.rkb_workspace_members m
            where m.workspace_id = d.workspace_id and m.user_id = auth.uid()
          )
        )
        or (
          target_visibility = 'private'
          and exists (
            select 1
            from public.rkb_document_grants g
            where g.document_id = d.id and g.grantee_user_id = auth.uid()
          )
        )
      )
  );
$$;

revoke all on function public.rkb_is_workspace_owner(uuid) from public;
revoke all on function public.rkb_is_workspace_member(uuid) from public;
revoke all on function public.rkb_is_document_owner(uuid) from public;
revoke all on function public.rkb_has_document_grant(uuid) from public;
revoke all on function public.rkb_can_read_document(uuid) from public;
revoke all on function public.rkb_can_read_asset(uuid, public.rkb_visibility) from public;

grant execute on function public.rkb_is_workspace_owner(uuid) to authenticated;
grant execute on function public.rkb_is_workspace_member(uuid) to authenticated;
grant execute on function public.rkb_is_document_owner(uuid) to authenticated;
grant execute on function public.rkb_has_document_grant(uuid) to authenticated;
grant execute on function public.rkb_can_read_document(uuid) to anon, authenticated;
grant execute on function public.rkb_can_read_asset(uuid, public.rkb_visibility) to anon, authenticated;

drop policy if exists rkb_workspaces_read on public.rkb_workspaces;
create policy rkb_workspaces_read on public.rkb_workspaces for select using (
  owner_user_id = auth.uid() or public.rkb_is_workspace_member(id)
);

drop policy if exists rkb_workspaces_owner_insert on public.rkb_workspaces;
create policy rkb_workspaces_owner_insert on public.rkb_workspaces for insert
with check (owner_user_id = auth.uid());

drop policy if exists rkb_workspaces_owner_update on public.rkb_workspaces;
create policy rkb_workspaces_owner_update on public.rkb_workspaces for update
using (owner_user_id = auth.uid())
with check (owner_user_id = auth.uid());

drop policy if exists rkb_workspaces_owner_delete on public.rkb_workspaces;
create policy rkb_workspaces_owner_delete on public.rkb_workspaces for delete
using (owner_user_id = auth.uid());

drop policy if exists rkb_members_read_self on public.rkb_workspace_members;
create policy rkb_members_read_self on public.rkb_workspace_members for select using (
  user_id = auth.uid() or public.rkb_is_workspace_owner(workspace_id)
);

drop policy if exists rkb_members_owner_write on public.rkb_workspace_members;
create policy rkb_members_owner_write on public.rkb_workspace_members for all
using (public.rkb_is_workspace_owner(workspace_id))
with check (public.rkb_is_workspace_owner(workspace_id));

drop policy if exists rkb_documents_read on public.rkb_documents;
create policy rkb_documents_read on public.rkb_documents for select using (
  public.rkb_can_read_document(id)
);

drop policy if exists rkb_documents_insert on public.rkb_documents;
create policy rkb_documents_insert on public.rkb_documents for insert
with check (owner_user_id = auth.uid() and source_visibility = 'private');

drop policy if exists rkb_documents_update_owner on public.rkb_documents;
create policy rkb_documents_update_owner on public.rkb_documents for update
using (owner_user_id = auth.uid())
with check (owner_user_id = auth.uid());

drop policy if exists rkb_grants_read on public.rkb_document_grants;
create policy rkb_grants_read on public.rkb_document_grants for select using (
  grantee_user_id = auth.uid() or public.rkb_is_document_owner(document_id)
);

drop policy if exists rkb_grants_owner_write on public.rkb_document_grants;
create policy rkb_grants_owner_write on public.rkb_document_grants for all
using (public.rkb_is_document_owner(document_id))
with check (public.rkb_is_document_owner(document_id));

-- Raw storage locators are server-only. A public normalized work never makes a
-- user's exact source PDF, private scan, or object key directly client-readable.
drop policy if exists rkb_objects_read on public.rkb_objects;
drop policy if exists rkb_objects_owner_write on public.rkb_objects;
revoke all on public.rkb_objects from anon, authenticated;

drop policy if exists rkb_pages_read on public.rkb_pages;
create policy rkb_pages_read on public.rkb_pages for select using (
  public.rkb_can_read_document(document_id)
);

drop policy if exists rkb_pages_owner_write on public.rkb_pages;
create policy rkb_pages_owner_write on public.rkb_pages for all
using (public.rkb_is_document_owner(document_id))
with check (public.rkb_is_document_owner(document_id));

drop policy if exists rkb_regions_read on public.rkb_regions;
create policy rkb_regions_read on public.rkb_regions for select using (
  exists (
    select 1 from public.rkb_pages p
    where p.id = page_id and public.rkb_can_read_document(p.document_id)
  )
);

drop policy if exists rkb_regions_owner_write on public.rkb_regions;
create policy rkb_regions_owner_write on public.rkb_regions for all
using (
  exists (
    select 1 from public.rkb_pages p
    where p.id = page_id and public.rkb_is_document_owner(p.document_id)
  )
)
with check (
  exists (
    select 1 from public.rkb_pages p
    where p.id = page_id and public.rkb_is_document_owner(p.document_id)
  )
);

drop policy if exists rkb_relations_read on public.rkb_region_relations;
create policy rkb_relations_read on public.rkb_region_relations for select using (
  exists (
    select 1
    from public.rkb_regions r
    join public.rkb_pages p on p.id = r.page_id
    where r.id = source_region_id and public.rkb_can_read_document(p.document_id)
  )
);

drop policy if exists rkb_relations_owner_write on public.rkb_region_relations;
create policy rkb_relations_owner_write on public.rkb_region_relations for all
using (
  exists (
    select 1
    from public.rkb_regions r
    join public.rkb_pages p on p.id = r.page_id
    where r.id = source_region_id and public.rkb_is_document_owner(p.document_id)
  )
)
with check (
  exists (
    select 1
    from public.rkb_regions r
    join public.rkb_pages p on p.id = r.page_id
    where r.id = source_region_id and public.rkb_is_document_owner(p.document_id)
  )
);

drop policy if exists rkb_illustrations_read on public.rkb_illustrations;
create policy rkb_illustrations_read on public.rkb_illustrations for select using (
  public.rkb_can_read_asset(document_id, visibility)
);

drop policy if exists rkb_illustrations_owner_write on public.rkb_illustrations;
create policy rkb_illustrations_owner_write on public.rkb_illustrations for all
using (public.rkb_is_document_owner(document_id))
with check (public.rkb_is_document_owner(document_id));

drop policy if exists rkb_chunks_read on public.rkb_chunks;
create policy rkb_chunks_read on public.rkb_chunks for select using (
  public.rkb_can_read_document(document_id)
);

drop policy if exists rkb_chunks_owner_write on public.rkb_chunks;
create policy rkb_chunks_owner_write on public.rkb_chunks for all
using (public.rkb_is_document_owner(document_id))
with check (public.rkb_is_document_owner(document_id));

drop policy if exists rkb_ingestion_owner on public.rkb_ingestion_jobs;
create policy rkb_ingestion_owner on public.rkb_ingestion_jobs for all
using (owner_user_id = auth.uid())
with check (owner_user_id = auth.uid());

commit;
