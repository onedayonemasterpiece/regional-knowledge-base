-- Restore short factual matches when a natural question has extra words.
CREATE OR REPLACE FUNCTION rkb_v4.lexical_candidates(query_text text, budget integer)
RETURNS TABLE (passage_id uuid, score double precision) LANGUAGE sql STABLE AS $$
    WITH configs AS (
        SELECT c, replace(websearch_to_tsquery(c, query_text)::text, ' & ', ' | ')::tsquery q
        FROM unnest(ARRAY['pg_catalog.russian'::regconfig,
            'pg_catalog.german'::regconfig, 'pg_catalog.simple'::regconfig]) c
    ), candidates AS (
        SELECT hit.* FROM configs CROSS JOIN LATERAL (
            SELECT p.passage_id, ts_rank_cd(p.lexemes, configs.q)::double precision score
            FROM rkb_v4.passage_search p
            JOIN rkb_v4.active_revision a USING (source_id, revision)
            JOIN rkb_v4.sources s USING (source_id)
            WHERE s.accepted AND NOT s.withdrawn AND p.config = configs.c
              AND p.lexemes @@ configs.q
            ORDER BY score DESC, p.passage_id
            LIMIT greatest(0, least(budget, 200))
        ) hit
    ) SELECT * FROM candidates ORDER BY score DESC, passage_id
      LIMIT greatest(0, least(budget, 200))
$$;
CREATE OR REPLACE FUNCTION rkb_v4.lexical_candidates_scoped(
    query_text text, budget integer, source_ids uuid[])
RETURNS TABLE (passage_id uuid, score double precision)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog AS $$
    WITH scope AS MATERIALIZED (SELECT rkb_v4.checked_source_scope(source_ids) ids),
    configs AS (
        SELECT c, replace(websearch_to_tsquery(c, query_text)::text, ' & ', ' | ')::tsquery q
        FROM unnest(ARRAY['pg_catalog.russian'::regconfig,
            'pg_catalog.german'::regconfig, 'pg_catalog.simple'::regconfig]) c
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
