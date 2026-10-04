begin;
-- Honor an explicitly selected owned root without merging historical duplicates.
create or replace function public.rkb_start_ingestion(
 p_ingestion_id uuid,p_document_id uuid,p_title text,p_authors jsonb,p_publication_year integer,
 p_language text,p_source_sha256 text,p_source_file_id text,p_page_count integer,p_duplicate_policy text
) returns table(ingestion_id uuid,document_id uuid)
language plpgsql volatile security definer set search_path=public as $$
declare actor uuid:=public.rkb_current_actor_id();doc public.rkb_documents%rowtype;
 job public.rkb_ingestion_jobs%rowtype;root_count integer;next_revision bigint;
begin
 if actor is null or not exists(select 1 from rkb_users where id=actor and status='active') then raise exception 'authentication required';end if;
 if p_duplicate_policy not in('reuse','new_revision') or p_duplicate_policy is null then raise exception 'invalid duplicate_policy';end if;
 if p_page_count<1 or nullif(btrim(p_source_file_id),'') is null then raise exception 'valid page_count and source_file_id required';end if;
 perform pg_advisory_xact_lock(hashtextextended(actor::text||':'||p_source_sha256,0));
 select * into job from rkb_ingestion_jobs where owner_user_id=actor and source_file_id=p_source_file_id and duplicate_policy=p_duplicate_policy;
 if found then
  if job.source_sha256 is distinct from p_source_sha256 then raise exception 'source_file_id is already bound to different bytes';end if;
  return query select job.id,job.document_id;return;
 end if;
 -- A named reprocess already selected one logical root under actor authorization.
 -- Attached starts pass a fresh UUID and retain the fail-closed SHA lookup.
 if p_duplicate_policy='new_revision' then
  select * into doc from rkb_documents where id=p_document_id
   and owner_user_id=actor and source_sha256=p_source_sha256 for update;
 end if;
 if doc.id is null then
  select count(*) into root_count from rkb_documents where owner_user_id=actor and source_sha256=p_source_sha256;
  if root_count>1 then raise exception 'historical_source_identity_ambiguous';end if;
  select * into doc from rkb_documents where owner_user_id=actor and source_sha256=p_source_sha256 for update;
 end if;
 if doc.id is not null then
  select * into job from rkb_ingestion_jobs j where j.document_id=doc.id
   order by staged_revision desc,created_at desc,id limit 1;
  if found and (p_duplicate_policy='reuse' or job.state<>'finalized') then
   return query select job.id,job.document_id;return;
  end if;
  select greatest(doc.active_revision,coalesce(max(staged_revision),0))+1 into next_revision from rkb_ingestion_jobs where rkb_ingestion_jobs.document_id=doc.id;
 else
  insert into rkb_documents(id,owner_user_id,title,authors,publication_year,language,source_sha256,page_count)
   values(p_document_id,actor,p_title,coalesce(p_authors,'[]'::jsonb),p_publication_year,p_language,p_source_sha256,p_page_count) returning * into doc;
  next_revision:=1;
 end if;
 insert into rkb_ingestion_jobs(id,owner_user_id,document_id,source_file_id,source_sha256,state,cursor,staged_revision,duplicate_policy)
  values(p_ingestion_id,actor,doc.id,p_source_file_id,p_source_sha256,'processing','0',next_revision,p_duplicate_policy) returning * into job;
 return query select job.id,job.document_id;
end $$;
revoke all on function public.rkb_start_ingestion(uuid,uuid,text,jsonb,integer,text,text,text,integer,text) from public;
grant execute on function public.rkb_start_ingestion(uuid,uuid,text,jsonb,integer,text,text,text,integer,text) to authenticated,rkb_app;
commit;
