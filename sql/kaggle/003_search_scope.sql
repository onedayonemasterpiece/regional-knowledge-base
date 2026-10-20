-- Compatible expansion after 002; immutable payload for the ordered migration authority.
-- Existing unscoped function identities and behavior remain unchanged.
CREATE FUNCTION rkb_v4.checked_source_scope(source_ids uuid[]) RETURNS uuid[]
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
    IF source_ids IS NULL OR cardinality(source_ids) NOT BETWEEN 1 AND 32
       OR array_ndims(source_ids) <> 1 OR array_position(source_ids, NULL) IS NOT NULL
       OR (SELECT count(DISTINCT id) FROM unnest(source_ids) id) <> cardinality(source_ids) THEN
        RAISE EXCEPTION 'source scope requires 1..32 distinct UUIDs' USING ERRCODE = '22023';
    END IF;
    RETURN source_ids;
END
$$;

CREATE FUNCTION rkb_v4.lexical_candidates_scoped(query_text text, budget integer, source_ids uuid[])
RETURNS TABLE (passage_id uuid, score double precision)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog AS $$
    WITH scope AS MATERIALIZED (SELECT rkb_v4.checked_source_scope(source_ids) ids),
    configs AS (
        SELECT c, websearch_to_tsquery(c, query_text) q FROM unnest(ARRAY[
            'pg_catalog.russian'::regconfig, 'pg_catalog.german'::regconfig,
            'pg_catalog.simple'::regconfig]) c
    ), candidates AS (
        SELECT hit.* FROM configs CROSS JOIN LATERAL (
            SELECT p.passage_id, ts_rank_cd(p.lexemes, configs.q)::double precision score
            FROM rkb_v4.passage_search p
            JOIN rkb_v4.active_revision a USING (source_id, revision)
            JOIN rkb_v4.sources s USING (source_id)
            WHERE p.source_id = ANY ((SELECT ids FROM scope)::uuid[])
              AND s.accepted AND NOT s.withdrawn AND p.config = configs.c
              AND p.lexemes @@ configs.q
            ORDER BY score DESC, p.passage_id
            LIMIT greatest(0, least(budget, 200))
        ) hit
    ) SELECT * FROM candidates ORDER BY score DESC, passage_id
      LIMIT greatest(0, least(budget, 200))
$$;

CREATE FUNCTION rkb_v4.dense_candidates_scoped(query_vector public.vector(1024), budget integer, source_ids uuid[])
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
    )
    -- Exact distance over the indexed source subset, under the caller deadline.
    -- The materialization deliberately avoids a broad approximate ANN scan.
    SELECT p.passage_id, p.embedding OPERATOR(public.<=>) query_vector
    FROM eligible p
    ORDER BY p.embedding OPERATOR(public.<=>) query_vector, p.passage_id
    LIMIT greatest(0, least(budget, 200))
$$;

CREATE FUNCTION rkb_v4.hydrate_scoped(candidate_ids uuid[], source_ids uuid[])
RETURNS TABLE (passage_id uuid, source_id uuid, revision bigint, locator text,
               body text, bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog AS $$
    WITH scope AS MATERIALIZED (SELECT rkb_v4.checked_source_scope(source_ids) ids)
    SELECT p.passage_id, p.source_id, p.revision, p.locator, p.body, s.bibliography
    FROM unnest(candidate_ids[1:5]) WITH ORDINALITY ids(id, rank)
    JOIN rkb_v4.passage_search p ON p.passage_id = ids.id
    JOIN rkb_v4.active_revision a USING (source_id, revision)
    JOIN rkb_v4.sources s USING (source_id)
    WHERE p.source_id = ANY ((SELECT ids FROM scope)::uuid[])
      AND s.accepted AND NOT s.withdrawn
    ORDER BY ids.rank
$$;
