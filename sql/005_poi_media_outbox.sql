begin;

alter table public.rkb_integration_outbox
  drop constraint if exists rkb_integration_outbox_event_type_check;

alter table public.rkb_integration_outbox
  add constraint rkb_integration_outbox_event_type_check
  check (event_type in ('poi.fact_evidence.v1','poi.media_evidence.v1'));

do $
begin
  if to_regprocedure(
    'public.rkb_activate_revision_core(uuid,uuid,bigint,jsonb)'
  ) is null then
    alter function public.rkb_activate_revision(uuid,uuid,bigint,jsonb)
      rename to rkb_activate_revision_core;
  end if;
end
$;

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
  event jsonb;
  fact_events jsonb := '[]'::jsonb;
  event_id uuid;
  illustration_id uuid;
  illustration_row public.rkb_illustrations%rowtype;
  event_visibility public.rkb_visibility;
  event_page_id uuid;
  event_source_region_id uuid;
  caption_region_ids uuid[];
  evidence_region_ids uuid[];
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
  if job.state <> 'ready' or job.staged_revision <> p_revision then
    raise exception 'ingestion revision is not ready';
  end if;

  select *
  into document_row
  from public.rkb_documents d
  where d.id = p_document_id
    and d.owner_user_id = auth.uid();

  if not found then
    raise exception 'document not found';
  end if;

  if jsonb_typeof(coalesce(p_poi_events,'[]'::jsonb)) <> 'array' then
    raise exception 'POI events must be an array';
  end if;
  if jsonb_array_length(coalesce(p_poi_events,'[]'::jsonb)) > 1000 then
    raise exception 'too many POI events';
  end if;

  for event in
    select value
    from jsonb_array_elements(coalesce(p_poi_events,'[]'::jsonb))
  loop
    if event->>'producer' <> 'regional_knowledge' then
      raise exception 'invalid POI producer';
    end if;

    if event->>'contract_version' = 'poi.fact_evidence.v1' then
      fact_events := fact_events || jsonb_build_array(event);
      continue;
    end if;

    if event->>'contract_version' <> 'poi.media_evidence.v1' then
      raise exception 'invalid POI event contract';
    end if;

    event_id := (event->>'event_id')::uuid;
    illustration_id := (event#>>'{media,illustration_id}')::uuid;
    event_page_id := (event#>>'{media,page_id}')::uuid;
    event_source_region_id :=
      (event#>>'{media,source_region_id}')::uuid;

    if (event#>>'{source,document_ref}')
       <> ('knowledge://documents/' || p_document_id::text) then
      raise exception 'POI media document_ref mismatch';
    end if;
    if (event#>>'{source,revision}')::bigint <> p_revision then
      raise exception 'POI media revision mismatch';
    end if;
    if (event#>>'{media,illustration_ref}')
       <> ('knowledge://illustrations/' || illustration_id::text) then
      raise exception 'POI media illustration_ref mismatch';
    end if;
    if event#>>'{media,relation}' not in (
      'depicts','illustrates','map_of','detail_of'
    ) then
      raise exception 'POI media relation invalid';
    end if;

    select i.*
    into illustration_row
    from public.rkb_illustrations i
    join public.rkb_pages p on p.id = i.page_id
    where i.id = illustration_id
      and i.document_id = p_document_id
      and p.document_id = p_document_id
      and p.revision = p_revision;

    if not found then
      raise exception 'POI media illustration is not in staged revision';
    end if;
    if illustration_row.page_id <> event_page_id
       or illustration_row.source_region_id <> event_source_region_id then
      raise exception 'POI media provenance mismatch';
    end if;
    if event#>>'{media,source_crop_sha256}'
       <> illustration_row.source_crop_sha256 then
      raise exception 'POI media crop hash mismatch';
    end if;
    if event#>>'{media,rights_status}'
       <> illustration_row.rights_status::text then
      raise exception 'POI media rights mismatch';
    end if;

    event_visibility :=
      (event#>>'{scope,visibility}')::public.rkb_visibility;
    if event_visibility <> illustration_row.visibility
       or event#>>'{media,visibility}' <> illustration_row.visibility::text then
      raise exception 'POI media visibility mismatch';
    end if;
    if event#>>'{scope,owner_sub}' <> auth.uid()::text then
      raise exception 'POI media owner mismatch';
    end if;
    if event_visibility = 'workspace'
       and coalesce(event#>>'{scope,workspace_id}','')
           <> coalesce(document_row.workspace_id::text,'') then
      raise exception 'POI media workspace mismatch';
    end if;

    caption_region_ids := coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(
        coalesce(event#>'{media,caption_region_ids}','[]'::jsonb)
      )
    ), '{}'::uuid[]);
    if caption_region_ids <> illustration_row.caption_region_ids then
      raise exception 'POI media caption provenance mismatch';
    end if;

    evidence_region_ids := coalesce(array(
      select value::uuid
      from jsonb_array_elements_text(
        coalesce(event#>'{evidence,region_ids}','[]'::jsonb)
      )
    ), '{}'::uuid[]);
    if cardinality(evidence_region_ids) < 1 then
      raise exception 'POI media evidence regions required';
    end if;
    if exists (
      select 1
      from unnest(evidence_region_ids) value
      where not exists (
        select 1
        from public.rkb_regions r
        where r.id = value
          and r.page_id = illustration_row.page_id
      )
    ) then
      raise exception 'POI media evidence escapes illustration page';
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
      'poi.media_evidence.v1',
      event_id,
      event->>'idempotency_key',
      event_visibility,
      case
        when event_visibility = 'workspace'
          then document_row.workspace_id
        else null
      end,
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
      raise exception 'POI media event idempotency conflict';
    end if;
  end loop;

  return query
  select *
  from public.rkb_activate_revision_core(
    p_document_id,
    p_ingestion_id,
    p_revision,
    fact_events
  );
end;
$$;

revoke all on function public.rkb_activate_revision(
  uuid,uuid,bigint,jsonb
) from public;
grant execute on function public.rkb_activate_revision(
  uuid,uuid,bigint,jsonb
) to authenticated;

commit;