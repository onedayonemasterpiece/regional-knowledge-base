-- SQL022: one source-scoped entity_list selector before LIMIT.
-- Keep SQL001-021 and read_source_graph_v1 unchanged.
-- Functional signoff: Opus chose PG lower plus German sharp-S and Greek
-- final-sigma folds. No mapping table, new search engine or extra read pass.
CREATE FUNCTION rkb_v4.source_graph_name_fold_v1(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT replace(replace(lower(value), 'ß', 'ss'), 'ς', 'σ')
$$;

-- Eligibility/revision/provenance joins, C keyset and row ABI match SQL011.
-- Literal substring match; no dynamic SQL, ILIKE wildcards or full-text schema.
CREATE FUNCTION rkb_v4.read_source_graph_filtered_v1(
 wanted_source uuid, after_source uuid, after_key text, selector_kinds text[],
 name_query text, take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,
 record_id text,data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT n.source_id,1::bigint,v.eligibility_revision,n.entity_key,n.data,
    n.data->'mention'->'evidence'->>'exact_quote',s.bibliography
  FROM rkb_v4.source_entity_representations n
  JOIN rkb_v4.sources s USING(source_id)
  JOIN rkb_v4.active_revision active USING(source_id)
  JOIN rkb_v4.source_visibility v USING(source_id)
  JOIN rkb_v4.source_graph_receipts receipt USING(source_id)
  WHERE wanted_source IS NOT NULL AND n.source_id=wanted_source
    AND s.accepted AND NOT s.withdrawn AND active.revision=1
    AND (after_source IS NULL OR n.source_id>after_source
      OR n.source_id=after_source AND n.entity_key COLLATE "C">after_key COLLATE "C")
    AND (selector_kinds IS NULL OR n.data->>'kind'=ANY(selector_kinds))
    AND (name_query IS NULL OR
      position(rkb_v4.source_graph_name_fold_v1(name_query)
        IN rkb_v4.source_graph_name_fold_v1(n.data->>'canonical_label'))>0 OR
      position(rkb_v4.source_graph_name_fold_v1(name_query)
        IN rkb_v4.source_graph_name_fold_v1(n.data->'mention'->>'exact_source_spelling'))>0 OR
      EXISTS (
        SELECT 1 FROM jsonb_array_elements(n.data->'aliases') alias(value)
        WHERE jsonb_typeof(alias.value->'value')='string'
          AND position(rkb_v4.source_graph_name_fold_v1(name_query)
            IN rkb_v4.source_graph_name_fold_v1(alias.value->>'value'))>0))
  ORDER BY n.source_id,n.entity_key COLLATE "C"
  LIMIT least(greatest(take,1),4)
$$;

REVOKE ALL ON FUNCTION rkb_v4.source_graph_name_fold_v1(text),
 rkb_v4.read_source_graph_filtered_v1(uuid,uuid,text,text[],text,integer) FROM PUBLIC;
-- Only the preexisting authorized SQL011 reader may execute the new read.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
    IF has_function_privilege('rkb_reader',
        'rkb_v4.read_source_graph_v1(uuid,uuid,text,uuid,text,integer)','EXECUTE') THEN
      GRANT EXECUTE ON FUNCTION rkb_v4.source_graph_name_fold_v1(text),
       rkb_v4.read_source_graph_filtered_v1(uuid,uuid,text,text[],text,integer) TO rkb_reader;
    END IF;
  END IF;
END $$;
