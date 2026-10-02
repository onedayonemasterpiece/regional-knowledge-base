begin;

alter table public.rkb_ingestion_jobs
  add column if not exists staged_graph_object_id uuid
  references public.rkb_objects(id) on delete set null;

grant update (staged_graph_object_id)
  on public.rkb_ingestion_jobs to authenticated;

create or replace function public.rkb_insert_chunks(
  p_document_id uuid,
  p_ingestion_id uuid,
  p_revision bigint,
  p_text_object_id uuid,
  p_chunks jsonb
)
returns integer
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  inserted_count integer := 0;
  job public.rkb_ingestion_jobs%rowtype;
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

  if not exists (
    select 1
    from public.rkb_objects o
    where o.id = p_text_object_id
      and o.document_id = p_document_id
      and o.kind = 'text_projection'
  ) then
    raise exception 'text projection object mismatch';
  end if;
  if jsonb_typeof(p_chunks) <> 'array' then
    raise exception 'chunks must be an array';
  end if;
  if jsonb_array_length(p_chunks) > 100 then
    raise exception 'chunk batch too large';
  end if;

  insert into public.rkb_chunks(
    id, document_id, revision,
    region_ids, page_ids, illustration_ids, footnote_region_ids,
    text_object_id, text_start, text_end, text_sha256,
    title, embedding, fts, metadata
  )
  select
    (item->>'id')::uuid,
    p_document_id,
    p_revision,
    coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(coalesce(item->'region_ids','[]'::jsonb))
    ), '{}'::uuid[]),
    coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(coalesce(item->'page_ids','[]'::jsonb))
    ), '{}'::uuid[]),
    coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(
        coalesce(item->'illustration_ids','[]'::jsonb)
      )
    ), '{}'::uuid[]),
    coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(
        coalesce(item->'footnote_region_ids','[]'::jsonb)
      )
    ), '{}'::uuid[]),
    p_text_object_id,
    (item->>'text_start')::bigint,
    (item->>'text_end')::bigint,
    item->>'text_sha256',
    item->>'title',
    case
      when nullif(item->>'embedding','') is null then null
      else (item->>'embedding')::halfvec(768)
    end,
    to_tsvector('simple', coalesce(item->>'normalized_text','')),
    coalesce(item->'metadata','{}'::jsonb)
  from jsonb_array_elements(p_chunks) item
  on conflict (id) do update set
    region_ids = excluded.region_ids,
    page_ids = excluded.page_ids,
    illustration_ids = excluded.illustration_ids,
    footnote_region_ids = excluded.footnote_region_ids,
    text_object_id = excluded.text_object_id,
    text_start = excluded.text_start,
    text_end = excluded.text_end,
    text_sha256 = excluded.text_sha256,
    title = excluded.title,
    embedding = excluded.embedding,
    fts = excluded.fts,
    metadata = excluded.metadata
  where public.rkb_chunks.document_id = p_document_id
    and public.rkb_chunks.revision = p_revision;

  get diagnostics inserted_count = row_count;
  return inserted_count;
end;
$$;

revoke all on function public.rkb_insert_chunks(
  uuid,uuid,bigint,uuid,jsonb
) from public;
grant execute on function public.rkb_insert_chunks(
  uuid,uuid,bigint,uuid,jsonb
) to authenticated;

create or replace function public.rkb_activate_revision(
  p_document_id uuid,
  p_ingestion_id uuid,
  p_revision bigint
)
returns table(document_id uuid, active_revision bigint, ingestion_state text)
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  job public.rkb_ingestion_jobs%rowtype;
  expected_pages integer;
  staged_pages integer;
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

  select d.page_count
  into expected_pages
  from public.rkb_documents d
  where d.id = p_document_id
    and d.owner_user_id = auth.uid();

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
    raise exception 'textual region is not covered by retrieval chunks';
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

revoke all on function public.rkb_activate_revision(uuid,uuid,bigint) from public;
grant execute on function public.rkb_activate_revision(uuid,uuid,bigint)
  to authenticated;

commit;
