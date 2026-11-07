-- A pure dictionary "see also" entry redirects the reader; it does not
-- provide standalone semantic evidence. Keep it in lexical_candidates.
-- Preserve the HNSW distance order and existing dense/search ABI.
CREATE OR REPLACE FUNCTION rkb_v4.dense_candidates(query_vector public.vector(1024), budget integer)
RETURNS TABLE (passage_id uuid, distance double precision) LANGUAGE sql STABLE AS $$
    SELECT p.passage_id, p.embedding <=> query_vector
    FROM rkb_v4.passage_search p
    JOIN rkb_v4.active_revision a USING (source_id, revision)
    JOIN rkb_v4.sources s USING (source_id)
    WHERE s.accepted AND NOT s.withdrawn
      AND p.body !~* '^[[:space:]]*([^.!?[:cntrl:]]|[[:alpha:]]{1,8}\.)+[[:space:],]s\.[[:space:]]+[^.!?[:cntrl:]]+\.[[:space:]]*$'
    ORDER BY p.embedding <=> query_vector
    LIMIT greatest(0, least(budget, 200))
$$;

CREATE OR REPLACE FUNCTION rkb_v4.dense_candidates_scoped(
    query_vector public.vector(1024), budget integer, source_ids uuid[])
RETURNS TABLE (passage_id uuid, distance double precision)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog AS $$
    WITH scope AS MATERIALIZED (SELECT rkb_v4.checked_source_scope(source_ids) ids),
    eligible AS MATERIALIZED (
        SELECT p.passage_id, p.embedding
        FROM rkb_v4.passage_search p
        JOIN rkb_v4.active_revision a USING (source_id, revision)
        JOIN rkb_v4.sources s USING (source_id)
        WHERE p.source_id = ANY ((SELECT ids FROM scope)::uuid[])
          AND s.accepted AND NOT s.withdrawn
          AND p.body !~* '^[[:space:]]*([^.!?[:cntrl:]]|[[:alpha:]]{1,8}\.)+[[:space:],]s\.[[:space:]]+[^.!?[:cntrl:]]+\.[[:space:]]*$'
    )
    SELECT p.passage_id, p.embedding OPERATOR(public.<=>) query_vector
    FROM eligible p
    ORDER BY p.embedding OPERATOR(public.<=>) query_vector, p.passage_id
    LIMIT greatest(0, least(budget, 200))
$$;
