-- SQL048: optional or multiple source scope on the existing accepted graph.
-- SQL028's projection/provenance, visibility, receipt and keyset semantics are retained.
CREATE FUNCTION rkb_v4.read_source_graph_global_v1(
 wanted_sources uuid[], after_source uuid, after_key text, selector_kinds text[],
 name_query text, take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,
 record_id text,data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT n.source_id,1::bigint,v.eligibility_revision,n.entity_key,n.data||jsonb_build_object('mention',coalesce(n.data->'mention',n.data->'mentions'->0)),
    coalesce(coalesce(n.data->'mention',n.data->'mentions'->0)->'evidence'->>'exact_quote',left(p.body,2000)),s.bibliography
  FROM rkb_v4.source_entity_representations n
  JOIN rkb_v4.sources s USING(source_id)
  JOIN rkb_v4.active_revision active USING(source_id)
  JOIN rkb_v4.source_visibility v USING(source_id)
  JOIN rkb_v4.source_graph_receipts receipt USING(source_id)
  LEFT JOIN rkb_v4.passage_search p ON p.passage_id=(coalesce(n.data->'mention',n.data->'mentions'->0)->'evidence'->>'chunk_id')::uuid
    AND p.source_id=n.source_id AND p.revision=1
  WHERE (wanted_sources IS NULL OR n.source_id=ANY(wanted_sources))
    AND s.accepted AND NOT s.withdrawn AND active.revision=1
    AND (after_source IS NULL OR n.source_id>after_source
      OR n.source_id=after_source AND n.entity_key COLLATE "C">after_key COLLATE "C")
    AND (selector_kinds IS NULL OR n.data->>'kind'=ANY(selector_kinds))
    AND (name_query IS NULL OR
      position(rkb_v4.source_graph_name_fold_v1(name_query)
        IN rkb_v4.source_graph_name_fold_v1(n.data->>'canonical_label'))>0 OR
      position(rkb_v4.source_graph_name_fold_v1(name_query)
        IN rkb_v4.source_graph_name_fold_v1(coalesce(n.data->'mention',n.data->'mentions'->0)->>'exact_source_spelling'))>0 OR
      EXISTS (
        SELECT 1 FROM jsonb_array_elements(n.data->'aliases') alias(value)
        WHERE jsonb_typeof(alias.value->'value')='string'
          AND position(rkb_v4.source_graph_name_fold_v1(name_query)
            IN rkb_v4.source_graph_name_fold_v1(alias.value->>'value'))>0))
  ORDER BY n.source_id,n.entity_key COLLATE "C"
  LIMIT least(greatest(take,1),4)
$$;


REVOKE ALL ON FUNCTION rkb_v4.read_source_graph_global_v1(uuid[],uuid,text,text[],text,integer) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='rkb_reader') THEN
  GRANT EXECUTE ON FUNCTION rkb_v4.read_source_graph_global_v1(uuid[],uuid,text,text[],text,integer) TO rkb_reader;
 END IF;
END $$;
