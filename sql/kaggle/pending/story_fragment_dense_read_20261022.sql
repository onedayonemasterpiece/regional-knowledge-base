-- PENDING READ INTEGRATION: no schema number and NOT installed by the runner.
-- Depends on PR283's ordinary sql/kaggle/018_story_embedding_fragments.sql
-- stacked after Shkiper's real SQL017, and existing editorial SQL009/014.
-- Shkiper alone assigns the next ordered migration version and coordinates
-- source bundle, GRANT/readiness, checkpoint and activation with PR2.
--
-- Reuse the *same immutable* source_activation_vectors rows that PR277 writes.
-- No new vector table, original-content copy, model, transport or scheduler.
-- This index is a bounded ANN candidate path for additional Story fragments;
-- existing editorial_story_search_dense remains the first/default vector path.
CREATE INDEX source_activation_vectors_story_dense ON rkb_v4.source_activation_vectors
 USING hnsw (embedding public.vector_cosine_ops);

-- Preserve the existing public SQL signature called by kaggle_search._search.
-- All eligibility checks from SQL009 remain (current default variant, source
-- accepted/not withdrawn, active revision, verified bounded source scope).
-- Unlike a pure first-vector rank, the best candidate across every *selected*
-- installed fragment contributes to one score per Story; approximate candidate
-- limits remain explicit, and existing 2s query deadline stays with the caller.
--
-- SECURITY DEFINER is deliberately narrow: the already-granted rkb_reader can
-- EXECUTE this existing function but cannot SELECT raw/possibly unactivated
-- source_activation_vectors or the invoker-security fragment view. No table
-- GRANT is broadened. Fixed SQL, qualified identifiers/operators, fixed
-- search_path, read-only STABLE and same public output (story_id,score).
CREATE OR REPLACE FUNCTION rkb_v4.editorial_dense_candidates_v1(
 v public.vector,n integer,scope uuid[])
RETURNS TABLE(story_id uuid,score double precision)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 WITH first_candidates AS MATERIALIZED (
  SELECT p.story_id,p.embedding OPERATOR(public.<=>) v AS distance
  FROM rkb_v4.editorial_story_search p
  JOIN rkb_v4.editorial_story_heads h ON h.story_id=p.story_id
    AND h.default_variant_id=p.variant_id AND h.default_variant_revision=p.variant_revision
  JOIN rkb_v4.sources s ON s.source_id=p.source_id
  JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
  WHERE s.accepted AND NOT s.withdrawn
    AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
  ORDER BY p.embedding OPERATOR(public.<=>) v
  LIMIT least(greatest(coalesce(n,1),1),200)
 ), fragment_candidates AS MATERIALIZED (
  -- Filter to installed, hash-verified fragments of eligible *current*
  -- default Story variants BEFORE the bounded top-K. Selecting nearest raw
  -- activation slots first lets >n*32 closer passage vectors evict every
  -- relevant later Story fragment from the candidate set.
  SELECT f.story_id,f.embedding OPERATOR(public.<=>) v AS distance
  FROM rkb_v4.story_embedding_fragments f
  JOIN rkb_v4.editorial_story_search p ON p.story_id=f.story_id
    AND p.variant_id=f.variant_id AND p.variant_revision=f.revision
    AND p.source_id=f.source_id
  JOIN rkb_v4.editorial_story_heads h ON h.story_id=p.story_id
    AND h.default_variant_id=p.variant_id AND h.default_variant_revision=p.variant_revision
  JOIN rkb_v4.sources s ON s.source_id=p.source_id
  JOIN rkb_v4.active_revision active ON active.source_id=p.source_id
    AND active.revision=p.source_revision
  WHERE s.accepted AND NOT s.withdrawn
    AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
  ORDER BY f.embedding OPERATOR(public.<=>) v
  LIMIT least(greatest(coalesce(n,1),1)*32,800)
 )
 SELECT hits.story_id,1-min(hits.distance) AS score
 FROM (SELECT * FROM first_candidates UNION ALL SELECT * FROM fragment_candidates) hits
 GROUP BY hits.story_id
 ORDER BY min(hits.distance),hits.story_id
 LIMIT least(greatest(coalesce(n,1),1),200)
$$;
-- SQL009 already grants EXECUTE only to the role configured by the owner.
-- CREATE OR REPLACE retains those existing explicit per-role grants.
REVOKE ALL ON FUNCTION rkb_v4.editorial_dense_candidates_v1(public.vector,integer,uuid[]) FROM PUBLIC;
