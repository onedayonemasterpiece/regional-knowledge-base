begin;

create table if not exists public.rkb_author_profiles (
  id uuid primary key default gen_random_uuid(),
  canonical_name text not null
    check (char_length(btrim(canonical_name)) between 1 and 300),
  aliases text[] not null default '{}',
  profile_evidence jsonb not null default '[]'::jsonb,
  verification_state text not null default 'verified'
    check (verification_state in ('candidate','verified','retired')),
  verified_by uuid references auth.users(id) on delete set null,
  verified_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists rkb_author_profiles_name_unique
  on public.rkb_author_profiles(lower(btrim(canonical_name)));

create table if not exists public.rkb_author_authority (
  author_id uuid not null
    references public.rkb_author_profiles(id) on delete cascade,
  geography text not null,
  subject text not null,
  score integer not null check (score between 0 and 100),
  policy_version text not null
    check (char_length(policy_version) between 1 and 100),
  evidence jsonb not null default '[]'::jsonb,
  is_active boolean not null default true,
  verified_by uuid references auth.users(id) on delete set null,
  verified_at timestamptz,
  created_at timestamptz not null default now(),
  primary key(author_id,geography,subject,policy_version)
);

create index if not exists rkb_author_authority_lookup
  on public.rkb_author_authority(geography,subject,is_active);

create table if not exists public.rkb_integration_outbox (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  document_id uuid not null
    references public.rkb_documents(id) on delete cascade,
  revision bigint not null,
  target_service text not null
    check (target_service in ('street_story')),
  event_type text not null
    check (event_type in ('poi.fact_evidence.v1')),
  event_id uuid not null,
  idempotency_key text not null
    check (char_length(idempotency_key) between 1 and 300),
  visibility public.rkb_visibility not null,
  workspace_id uuid
    references public.rkb_workspaces(id) on delete set null,
  payload jsonb not null,
  state text not null check (state in (
    'pending_authorization','pending_delivery','delivering',
    'delivered','retry_wait','dead_letter','cancelled'
  )),
  attempts integer not null default 0 check (attempts >= 0),
  next_attempt_at timestamptz,
  last_error_code text,
  remote_receipt jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(target_service,event_id),
  unique(target_service,idempotency_key)
);

create index if not exists rkb_integration_outbox_delivery
  on public.rkb_integration_outbox(
    target_service,state,next_attempt_at,created_at
  );

alter table public.rkb_author_profiles enable row level security;
alter table public.rkb_author_authority enable row level security;
alter table public.rkb_integration_outbox enable row level security;

drop policy if exists rkb_author_profiles_read
  on public.rkb_author_profiles;
create policy rkb_author_profiles_read
on public.rkb_author_profiles for select
using (verification_state = 'verified');

drop policy if exists rkb_author_authority_read
  on public.rkb_author_authority;
create policy rkb_author_authority_read
on public.rkb_author_authority for select
using (is_active);

drop policy if exists rkb_outbox_owner_read
  on public.rkb_integration_outbox;
create policy rkb_outbox_owner_read
on public.rkb_integration_outbox for select
using (owner_user_id = auth.uid());

revoke insert, update, delete
  on public.rkb_author_profiles from authenticated;
revoke insert, update, delete
  on public.rkb_author_authority from authenticated;
revoke insert, update, delete
  on public.rkb_integration_outbox from authenticated;

create or replace function public.rkb_author_authority_for_names(
  p_names text[],
  p_subject text,
  p_geography text
)
returns table(
  input_name text,
  author_id uuid,
  score integer,
  policy_version text
)
language sql
stable
security invoker
set search_path = ''
as $$
  with requested as (
    select distinct btrim(value) as input_name
    from unnest(p_names) value
    where btrim(value) <> ''
  ),
  matched as (
    select
      requested.input_name,
      profile.id as author_id
    from requested
    join public.rkb_author_profiles profile
      on profile.verification_state = 'verified'
     and (
       lower(btrim(profile.canonical_name)) = lower(requested.input_name)
       or exists (
         select 1
         from unnest(profile.aliases) alias
         where lower(btrim(alias)) = lower(requested.input_name)
       )
     )
  ),
  unambiguous as (
    select
      matched.input_name,
      min(matched.author_id) as author_id
    from matched
    group by matched.input_name
    having count(distinct matched.author_id) = 1
  ),
  ranked as (
    select
      unambiguous.input_name,
      unambiguous.author_id,
      authority.score,
      authority.policy_version,
      row_number() over (
        partition by unambiguous.input_name
        order by
          case
            when authority.geography = p_geography
             and authority.subject = p_subject then 4
            when authority.geography = p_geography
             and authority.subject = '*' then 3
            when authority.geography = '*'
             and authority.subject = p_subject then 2
            when authority.geography = '*'
             and authority.subject = '*' then 1
            else 0
          end desc,
          authority.verified_at desc nulls last,
          authority.created_at desc
      ) as preference
    from unambiguous
    join public.rkb_author_authority authority
      on authority.author_id = unambiguous.author_id
     and authority.is_active
     and authority.geography in (p_geography, '*')
     and authority.subject in (p_subject, '*')
  )
  select
    ranked.input_name,
    ranked.author_id,
    ranked.score,
    ranked.policy_version
  from ranked
  where ranked.preference = 1
  order by ranked.input_name;
$$;

revoke all on function
  public.rkb_author_authority_for_names(text[],text,text)
  from public;
grant execute on function
  public.rkb_author_authority_for_names(text[],text,text)
  to authenticated;

drop function if exists public.rkb_activate_revision(uuid,uuid,bigint);

create or replace function public.rkb_activate_revision(
  p_document_id uuid,
  p_ingestion_id uuid,
  p_revision bigint,
  p_poi_events jsonb default '[]'::jsonb
)
returns table(
  document_id uuid,
  active_revision bigint,
  ingestion_state text
)
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  job public.rkb_ingestion_jobs%rowtype;
  document_row public.rkb_documents%rowtype;
  expected_pages integer;
  staged_pages integer;
  event jsonb;
  event_visibility public.rkb_visibility;
  event_id uuid;
  candidate_id uuid;
  event_page_ids uuid[];
  event_region_ids uuid[];
  source_family_id text;
begin
  if auth.uid() is null then
    raise exception 'authentication required';
  end if;
  if not public.rkb_is_document_owner(p_document_id) then
    raise exception 'document ownership required';
  end if;

  select *
  into job
  from public.rkb_ingestion_jobs j
  where j.id = p_ingestion_id
    and j.document_id = p_document_id
    and j.owner_user_id = auth.uid()
  for update;

  if not found then
    raise exception 'ingestion not found';
  end if;
  if job.state <> 'ready' then
    raise exception 'ingestion must be ready';
  end if;
  if job.staged_revision <> p_revision then
    raise exception 'staged revision mismatch';
  end if;

  select *
  into document_row
  from public.rkb_documents d
  where d.id = p_document_id
    and d.owner_user_id = auth.uid()
  for update;

  if not found then
    raise exception 'document not found';
  end if;

  expected_pages := document_row.page_count;
  if expected_pages is null or expected_pages < 1 then
    raise exception 'document page count is invalid';
  end if;

  select count(*)
  into staged_pages
  from public.rkb_pages p
  where p.document_id = p_document_id
    and p.revision = p_revision;

  if staged_pages <> expected_pages then
    raise exception 'revision page coverage is incomplete';
  end if;

  if exists (
    select 1
    from generate_series(0, expected_pages - 1) expected(index)
    where not exists (
      select 1
      from public.rkb_pages p
      where p.document_id = p_document_id
        and p.revision = p_revision
        and p.physical_page_index = expected.index
    )
  ) then
    raise exception 'revision page indexes are incomplete';
  end if;

  if exists (
    select 1
    from public.rkb_regions r
    join public.rkb_pages p on p.id = r.page_id
    where p.document_id = p_document_id
      and p.revision = p_revision
      and r.needs_review
  ) then
    raise exception 'revision still contains review-required regions';
  end if;

  if not exists (
    select 1
    from public.rkb_chunks c
    where c.document_id = p_document_id
      and c.revision = p_revision
  ) then
    raise exception 'revision has no chunks';
  end if;

  if exists (
    select 1
    from public.rkb_regions r
    join public.rkb_pages p on p.id = r.page_id
    where p.document_id = p_document_id
      and p.revision = p_revision
      and r.text_sha256 is not null
      and not exists (
        select 1
        from public.rkb_chunks c
        where c.document_id = p_document_id
          and c.revision = p_revision
          and (
            r.id = any(c.region_ids)
            or r.id = any(c.footnote_region_ids)
          )
      )
  ) then
    raise exception
      'textual region is not covered by retrieval chunks';
  end if;

  if exists (
    select 1
    from public.rkb_chunks c
    where c.document_id = p_document_id
      and c.revision = p_revision
      and (
        exists (
          select 1
          from unnest(c.page_ids) value
          where not exists (
            select 1
            from public.rkb_pages p
            where p.id = value
              and p.document_id = p_document_id
              and p.revision = p_revision
          )
        )
        or exists (
          select 1
          from unnest(c.region_ids || c.footnote_region_ids) value
          where not exists (
            select 1
            from public.rkb_regions r
            join public.rkb_pages p on p.id = r.page_id
            where r.id = value
              and p.document_id = p_document_id
              and p.revision = p_revision
          )
        )
        or exists (
          select 1
          from unnest(c.illustration_ids) value
          where not exists (
            select 1
            from public.rkb_illustrations i
            join public.rkb_pages p on p.id = i.page_id
            where i.id = value
              and i.document_id = p_document_id
              and p.revision = p_revision
          )
        )
      )
  ) then
    raise exception 'chunk provenance escapes staged revision';
  end if;

  if exists (
    select 1
    from public.rkb_illustrations i
    join public.rkb_pages p on p.id = i.page_id
    where i.document_id = p_document_id
      and p.revision = p_revision
      and (
        exists (
          select 1
          from unnest(i.caption_region_ids || i.nearby_region_ids) value
          where not exists (
            select 1
            from public.rkb_regions r
            where r.id = value
              and r.page_id = i.page_id
          )
        )
        or not exists (
          select 1
          from public.rkb_regions r
          where r.id = i.source_region_id
            and r.page_id = i.page_id
            and r.kind = 'figure'
        )
      )
  ) then
    raise exception 'illustration provenance escapes staged page';
  end if;

  if jsonb_typeof(coalesce(p_poi_events,'[]'::jsonb)) <> 'array' then
    raise exception 'POI events must be an array';
  end if;
  if jsonb_array_length(coalesce(p_poi_events,'[]'::jsonb)) > 500 then
    raise exception 'too many POI events';
  end if;

  for event in
    select value
    from jsonb_array_elements(coalesce(p_poi_events,'[]'::jsonb))
  loop
    if event->>'contract_version' <> 'poi.fact_evidence.v1'
       or event->>'producer' <> 'regional_knowledge' then
      raise exception 'invalid POI event contract';
    end if;

    event_id := (event->>'event_id')::uuid;
    candidate_id := (event#>>'{claim,candidate_id}')::uuid;

    if (event#>>'{source,document_ref}')
       <> ('knowledge://documents/' || p_document_id::text) then
      raise exception 'POI event document_ref mismatch';
    end if;
    if (event#>>'{source,revision}')::bigint <> p_revision then
      raise exception 'POI event revision mismatch';
    end if;
    if (event#>>'{evidence,evidence_ref}')
       <> ('knowledge://evidence/' || candidate_id::text) then
      raise exception 'POI evidence_ref mismatch';
    end if;

    event_visibility :=
      (event#>>'{scope,visibility}')::public.rkb_visibility;
    if event_visibility <> document_row.content_visibility then
      raise exception 'POI event visibility mismatch';
    end if;
    if event#>>'{scope,owner_sub}' <> auth.uid()::text then
      raise exception 'POI event owner mismatch';
    end if;
    if coalesce(event#>>'{scope,workspace_id}','')
       <> coalesce(document_row.workspace_id::text,'') then
      raise exception 'POI event workspace mismatch';
    end if;

    event_page_ids := coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(event#>'{evidence,page_ids}')
    ), '{}'::uuid[]);
    event_region_ids := coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(event#>'{evidence,region_ids}')
    ), '{}'::uuid[]);

    if cardinality(event_page_ids) < 1
       or cardinality(event_region_ids) < 1 then
      raise exception
        'POI event evidence must include pages and regions';
    end if;

    if exists (
      select 1
      from unnest(event_page_ids) value
      where not exists (
        select 1
        from public.rkb_pages p
        where p.id = value
          and p.document_id = p_document_id
          and p.revision = p_revision
      )
    ) then
      raise exception 'POI event references foreign page';
    end if;

    if exists (
      select 1
      from unnest(event_region_ids) value
      where not exists (
        select 1
        from public.rkb_regions r
        join public.rkb_pages p on p.id = r.page_id
        where r.id = value
          and p.document_id = p_document_id
          and p.revision = p_revision
          and p.id = any(event_page_ids)
      )
    ) then
      raise exception 'POI event references foreign region';
    end if;

    source_family_id :=
      nullif(btrim(event#>>'{evidence,source_family_id}'), '');
    if source_family_id is null
       or source_family_id like 'unresolved:%' then
      source_family_id := 'unknown';
      event := jsonb_set(
        event,
        '{evidence,source_family_id}',
        to_jsonb(source_family_id),
        true
      );
    end if;

    if event#>>'{evidence,provenance_precision_score}' is null
       or (event#>>'{evidence,provenance_precision_score}')::integer
          not between 0 and 100 then
      raise exception 'POI provenance score invalid';
    end if;
    if event#>>'{evidence,author_subject_authority}' is not null
       and (event#>>'{evidence,author_subject_authority}')::integer
           not between 0 and 100 then
      raise exception 'POI author score invalid';
    end if;
    if event#>>'{evidence,publication_method_score}' is not null
       and (event#>>'{evidence,publication_method_score}')::integer
           not between 0 and 100 then
      raise exception 'POI publication score invalid';
    end if;
    if event#>>'{evidence,evidence_verification_score}' is not null
       and (event#>>'{evidence,evidence_verification_score}')::integer
           not between 0 and 100 then
      raise exception 'POI verification score invalid';
    end if;

    insert into public.rkb_integration_outbox(
      owner_user_id,
      document_id,
      revision,
      target_service,
      event_type,
      event_id,
      idempotency_key,
      visibility,
      workspace_id,
      payload,
      state
    ) values (
      auth.uid(),
      p_document_id,
      p_revision,
      'street_story',
      'poi.fact_evidence.v1',
      event_id,
      event->>'idempotency_key',
      event_visibility,
      document_row.workspace_id,
      event,
      case
        when event_visibility = 'public'
          then 'pending_delivery'
        else 'pending_authorization'
      end
    )
    on conflict (target_service,event_id) do update set
      updated_at = now()
    where public.rkb_integration_outbox.payload = excluded.payload;

    if not found then
      raise exception 'POI event idempotency conflict';
    end if;
  end loop;

  update public.rkb_documents
  set active_revision = p_revision,
      updated_at = now()
  where id = p_document_id
    and owner_user_id = auth.uid();

  update public.rkb_ingestion_jobs
  set state = 'finalized',
      cursor = null,
      error_code = null,
      updated_at = now()
  where id = p_ingestion_id;

  return query
  select p_document_id, p_revision, 'finalized'::text;
end;
$$;

revoke all on function
  public.rkb_activate_revision(uuid,uuid,bigint,jsonb)
  from public;
grant execute on function
  public.rkb_activate_revision(uuid,uuid,bigint,jsonb)
  to authenticated;

commit;