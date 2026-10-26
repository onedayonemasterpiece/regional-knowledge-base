CREATE OR REPLACE FUNCTION rkb_v4.hydrate(candidate_ids uuid[])
RETURNS TABLE (passage_id uuid, source_id uuid, revision bigint, locator text,
               body text, bibliography text) LANGUAGE sql STABLE AS $$
 SELECT p.passage_id,p.source_id,p.revision,p.locator,p.body,s.bibliography
 FROM unnest(candidate_ids[1:30]) WITH ORDINALITY ids(id,rank)
 JOIN rkb_v4.passage_search p ON p.passage_id=ids.id
 JOIN rkb_v4.active_revision a USING(source_id,revision)
 JOIN rkb_v4.sources s USING(source_id)
 WHERE s.accepted AND NOT s.withdrawn
 ORDER BY ids.rank
$$;
CREATE OR REPLACE FUNCTION rkb_v4.hydrate_scoped(candidate_ids uuid[],source_ids uuid[])
RETURNS TABLE (passage_id uuid, source_id uuid, revision bigint, locator text,
               body text, bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH scope AS MATERIALIZED (SELECT rkb_v4.checked_source_scope(source_ids) ids)
 SELECT p.passage_id,p.source_id,p.revision,p.locator,p.body,s.bibliography
 FROM unnest(candidate_ids[1:30]) WITH ORDINALITY ids(id,rank)
 JOIN rkb_v4.passage_search p ON p.passage_id=ids.id
 JOIN rkb_v4.active_revision a USING(source_id,revision)
 JOIN rkb_v4.sources s USING(source_id)
 WHERE p.source_id=ANY((SELECT ids FROM scope)::uuid[])
   AND s.accepted AND NOT s.withdrawn
 ORDER BY ids.rank
$$;
