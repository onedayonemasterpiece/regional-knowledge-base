begin;
-- Identity candidates remain owner-scoped; all projections require active evidence.
create table if not exists public.rkb_entities(
 id uuid primary key,owner_user_id uuid not null references rkb_users(id),
 kind text not null check(kind in ('person','event','historical_thread','poi_ref')),
 canonical_label text not null check(length(canonical_label) between 1 and 200),
 external_ref text check(external_ref ~ '^streetstory://poi/[0-9a-f-]{36}$'),
 document_id uuid not null references rkb_documents(id),revision bigint not null,
 state text not null check(state in ('candidate','reviewed','unresolved')),
 metadata jsonb not null default '{}',
 check(kind='poi_ref' or external_ref is null),
 check(kind<>'poi_ref' or state='unresolved' or external_ref is not null)
);
create table if not exists public.rkb_entity_mentions(
 id uuid primary key,entity_id uuid not null references rkb_entities(id),
 document_id uuid not null references rkb_documents(id),revision bigint not null,
 chunk_id uuid not null references rkb_chunks(id) on delete cascade,
 page_id uuid not null references rkb_pages(id) on delete cascade,
 region_id uuid not null references rkb_regions(id) on delete cascade,
 exact_source_spelling text not null,evidence jsonb not null,
 state text not null check(state in ('candidate','reviewed')),signals jsonb not null default '{}'
);
create table if not exists public.rkb_entity_aliases(
 id uuid primary key,entity_id uuid not null references rkb_entities(id),
 value text not null check(length(value) between 1 and 200),normalized_value text not null,
 language text,alias_type text not null check(alias_type in ('current','historical','former','transliteration','spelling_variant')),
 time_scope text,document_id uuid not null references rkb_documents(id),revision bigint not null,
 evidence jsonb not null,unique(entity_id,normalized_value,document_id,revision)
);
create table if not exists public.rkb_entity_relations(
 id uuid primary key,source_id uuid not null references rkb_entities(id),target_id uuid not null references rkb_entities(id),
 kind text not null check(kind in ('participated_in','occurred_at','member_of')),
 document_id uuid not null references rkb_documents(id),revision bigint not null,
 evidence jsonb not null check(jsonb_typeof(evidence)='array' and jsonb_array_length(evidence) between 1 and 8),
 time_scope text,state text not null check(state in ('candidate','reviewed')),review_note text
);
create table if not exists public.rkb_graph_discovery_jobs(
 id uuid primary key,actor_id uuid references rkb_users(id),entity_id uuid references rkb_entities(id),
 document_id uuid references rkb_documents(id),revision bigint,
 job_key text not null unique,payload jsonb not null default '{}',
 state text not null default 'pending' check(state in ('pending','running','done','failed')),
 attempts integer not null default 0,available_at timestamptz not null default now(),
 lease_until timestamptz,claim uuid,error_code text,created_at timestamptz not null default now()
);
create index if not exists rkb_graph_jobs_due on rkb_graph_discovery_jobs(state,available_at);
create index if not exists rkb_graph_mentions_entity on rkb_entity_mentions(entity_id,document_id,revision);
create index if not exists rkb_graph_edges_source on rkb_entity_relations(source_id);
create index if not exists rkb_graph_edges_target on rkb_entity_relations(target_id);
create index if not exists rkb_graph_alias_normal on rkb_entity_aliases(normalized_value);

create or replace function public.rkb_graph_active(doc uuid,rev bigint) returns boolean
language sql stable security definer set search_path='' as $$
 select exists(select 1 from public.rkb_documents d where d.id=doc and d.active_revision=rev)
 and public.rkb_can_read_document(doc)
$$;
create or replace function public.rkb_graph_owned(doc uuid) returns boolean
language sql stable security definer set search_path='' as $$
 select exists(select 1 from public.rkb_documents d where d.id=doc and d.owner_user_id=public.rkb_current_actor_id())
$$;
-- Never expose canonical labels or alias existence through an inaccessible seed.
do $$ declare t text; begin
 foreach t in array array['rkb_entities','rkb_entity_mentions','rkb_entity_aliases','rkb_entity_relations'] loop
  execute format('alter table public.%I enable row level security',t);
  execute format('drop policy if exists graph_read on public.%I',t);
  execute format('create policy graph_read on public.%I for select using(public.rkb_graph_active(document_id,revision) or public.rkb_graph_owned(document_id))',t);
  execute format('drop policy if exists graph_write on public.%I',t);
  execute format('create policy graph_write on public.%I for insert with check(public.rkb_graph_owned(document_id))',t);
  execute format('drop policy if exists graph_update on public.%I',t);
  execute format('create policy graph_update on public.%I for update using(public.rkb_graph_owned(document_id)) with check(public.rkb_graph_owned(document_id))',t);
  execute format('grant select,insert,update on public.%I to rkb_app',t);
 end loop;
end $$;
drop policy if exists graph_read on rkb_entities;
create policy graph_read on rkb_entities for select using(rkb_graph_active(document_id,revision) or rkb_graph_owned(document_id));
alter table rkb_graph_discovery_jobs enable row level security;
drop policy if exists graph_job_actor on rkb_graph_discovery_jobs;
create policy graph_job_actor on rkb_graph_discovery_jobs for all using(actor_id=rkb_current_actor_id()) with check(actor_id=rkb_current_actor_id());
grant select,insert,update on rkb_graph_discovery_jobs to rkb_app;
drop policy if exists graph_write on rkb_entities;
drop policy if exists graph_update on rkb_entities;
create policy graph_update on rkb_entities for update using(rkb_graph_owned(document_id) and owner_user_id=rkb_current_actor_id()) with check(rkb_graph_owned(document_id) and owner_user_id=rkb_current_actor_id());
create policy graph_write on rkb_entities for insert with check(rkb_graph_owned(document_id) and owner_user_id=rkb_current_actor_id());
drop policy if exists graph_write on rkb_entity_mentions;
create policy graph_write on rkb_entity_mentions for insert with check(rkb_can_read_document(document_id) and exists(select 1 from rkb_entities n where n.id=entity_id and n.owner_user_id=rkb_current_actor_id()));
drop policy if exists graph_write on rkb_entity_aliases;
create policy graph_write on rkb_entity_aliases for insert with check(rkb_can_read_document(document_id) and exists(select 1 from rkb_entities n where n.id=entity_id and n.owner_user_id=rkb_current_actor_id()));
-- SQL defence in depth: authorizing a document is not enough to forge locators.
create or replace function public.rkb_graph_check_evidence(doc uuid,rev bigint,e jsonb) returns boolean
language sql stable security invoker set search_path=public as $$
 select exists(select 1 from rkb_chunks c join rkb_pages p on p.id=(e->>'page_id')::uuid
 join rkb_regions r on r.id=(e->>'region_id')::uuid and r.page_id=p.id
 where c.id=(e->>'chunk_id')::uuid and c.document_id=doc and c.revision=rev
 and p.document_id=doc and p.revision=rev and p.id=any(c.page_ids) and r.id=any(c.region_ids)
 and length(e->>'exact_quote') between 1 and 2000)
$$;
create or replace function public.rkb_graph_evidence_guard() returns trigger
language plpgsql security invoker set search_path=public as $$
declare item jsonb;sk text;tk text;begin
 if TG_TABLE_NAME='rkb_entity_relations' then
  select kind into sk from rkb_entities where id=NEW.source_id and owner_user_id=rkb_current_actor_id();
  select kind into tk from rkb_entities where id=NEW.target_id and owner_user_id=rkb_current_actor_id();
  if sk is null or tk is null or not ((NEW.kind='participated_in' and sk='person' and tk='event') or
          (NEW.kind='occurred_at' and sk='event' and tk='poi_ref') or
          (NEW.kind='member_of' and sk in ('person','event','poi_ref') and tk='historical_thread')) then raise exception 'invalid graph relation shape';end if;
  for item in select value from jsonb_array_elements(NEW.evidence) loop
   if not rkb_graph_check_evidence(NEW.document_id,NEW.revision,item) then raise exception 'invalid graph evidence';end if;
  end loop;
 else
  if not exists(select 1 from rkb_entities where id=NEW.entity_id and owner_user_id=rkb_current_actor_id()) then raise exception 'entity ownership required';end if;
  if not rkb_graph_check_evidence(NEW.document_id,NEW.revision,NEW.evidence) then raise exception 'invalid graph evidence';end if;
 end if;
 return NEW;
end $$;
do $$ declare t text;begin
 foreach t in array array['rkb_entity_mentions','rkb_entity_aliases','rkb_entity_relations'] loop
  execute format('drop trigger if exists graph_evidence_guard on %I',t);
  execute format('create trigger graph_evidence_guard before insert or update on %I for each row execute function rkb_graph_evidence_guard()',t);
 end loop;
end $$;
-- One job for a new revision; the worker pages known entities, not the corpus.
create or replace function public.rkb_graph_document_finalized() returns trigger
language plpgsql security definer set search_path=public as $$
begin
 if NEW.active_revision is distinct from OLD.active_revision and NEW.active_revision>0 then
  insert into rkb_graph_discovery_jobs(id,actor_id,document_id,revision,job_key,payload)
   values(gen_random_uuid(),NEW.owner_user_id,NEW.id,NEW.active_revision,'document:'||NEW.id||':'||NEW.active_revision,'{"kind":"document","offset":0}'::jsonb) on conflict(job_key) do nothing;
 end if;return NEW;
end $$;
drop trigger if exists rkb_graph_finalize on rkb_documents;
create trigger rkb_graph_finalize after update of active_revision on rkb_documents for each row execute function rkb_graph_document_finalized();
revoke all on function rkb_graph_document_finalized() from public;
commit;
