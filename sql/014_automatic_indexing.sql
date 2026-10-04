begin;
-- Counts are evidence-scoped by ordinary actor RLS; never use a service role
-- for a model/client status request. Inactive revisions contribute nothing.
create or replace function public.rkb_index_counts(target_document uuid default null)
returns table(active_chunks bigint,e5_ready bigint,bge_ready bigint)
language sql stable security invoker set search_path=public as $$
 select count(*),count(e.chunk_id),count(b.chunk_id)
 from rkb_chunks c join rkb_documents d on d.id=c.document_id
 left join rkb_chunk_embeddings_e5 e on e.chunk_id=c.id and e.revision=c.revision and e.text_sha256=c.text_sha256
  and e.embedding_space='e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1'
 left join rkb_chunk_embeddings_bge b on b.chunk_id=c.id and b.revision=c.revision and b.text_sha256=c.text_sha256
  and b.embedding_space='bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'
  and b.model_revision='5617a9f61b028005a4858fdac845db406aefb181'
 where c.revision=d.active_revision and (target_document is null or d.id=target_document)
$$;
revoke all on function public.rkb_index_counts(uuid) from public;
grant execute on function public.rkb_index_counts(uuid) to anon,authenticated,rkb_app;

-- NOTIFY is only a prompt wakeup. Active missing vectors are durable recovery
-- state; disconnected listeners recover on the bounded periodic pass.
create or replace function public.rkb_index_activation_wakeup() returns trigger
language plpgsql security invoker set search_path=public as $$
begin
 if NEW.active_revision>0 and NEW.active_revision is distinct from OLD.active_revision then
  perform pg_notify('rkb_index_activation','');
 end if;
 return NEW;
end $$;
drop trigger if exists rkb_index_activation on public.rkb_documents;
create trigger rkb_index_activation after update of active_revision on public.rkb_documents
 for each row execute function public.rkb_index_activation_wakeup();

-- Gate semantic branches in the SAME SQL snapshot as active candidates. Even
-- activation during a query cannot mix old/partial vectors into a ready pack.
do $$
declare signature text;definition text;old_guard text;new_guard text;
begin
 foreach signature in array array[
 'public.rkb_fast_e5_search(text,text,text,integer)',
 'public.rkb_multilingual_rankings(text,text,text,text,text,jsonb,integer)'
 ] loop
  definition:=pg_get_functiondef(signature::regprocedure);
  if signature like '%fast_e5%' then
   old_guard:='where p.qvec is not null and e.embedding_space=query_embedding_space';
   new_guard:=old_guard||' and (select e5_ready=active_chunks from rkb_index_counts(null))';
   if position(new_guard in definition)=0 then
    if position(old_guard in definition)=0 then raise exception 'unexpected E5 ranking definition';end if;
    definition:=replace(definition,old_guard,new_guard);
   end if;
  else
   old_guard:='where p.bq is not null and e.embedding_space=bge_space';
   new_guard:=old_guard||' and (select bge_ready=active_chunks from rkb_index_counts(null))';
   if position(new_guard in definition)=0 then
    if position(old_guard in definition)=0 then raise exception 'unexpected BGE ranking definition';end if;
    definition:=replace(definition,old_guard,new_guard);
   end if;
   old_guard:='where p.eq is not null and e.embedding_space=e5_space';
   new_guard:=old_guard||' and (select e5_ready=active_chunks from rkb_index_counts(null))';
   if position(new_guard in definition)=0 then
    if position(old_guard in definition)=0 then raise exception 'unexpected multilingual E5 definition';end if;
    definition:=replace(definition,old_guard,new_guard);
   end if;
  end if;
  execute definition;
 end loop;
end $$;
commit;
