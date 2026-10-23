-- Current native packets refer to reviewed source passages. Model/JSON shape is
-- checked by the Python compiler; SQL checks relationships and existing rights.
-- Historical migration files and accepted payloads are not rewritten.
CREATE FUNCTION rkb_v4.check_source_passage_evidence_v1(doc jsonb,e jsonb) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE region jsonb;passage jsonb;selection jsonb;ident text:=coalesce(e->>'passage_id',e->>'chunk_id');
BEGIN
 SELECT value INTO STRICT region FROM jsonb_array_elements(doc->'semantic'->'regions')
  WHERE value->>'region_id'=e->>'region_id';
 SELECT value INTO STRICT passage FROM jsonb_array_elements(doc->'passages') WHERE value->>'passage_id'=ident;
 IF e?'page_id' AND e->>'page_id' IS DISTINCT FROM region->>'page_id' THEN RETURN false;END IF;
 IF doc?'image_manifest' THEN
  SELECT value INTO STRICT selection FROM jsonb_array_elements(doc->'image_manifest'->'passages') WHERE value->>'passage_id'=ident;
  IF NOT selection->'page_ids' @> jsonb_build_array(region->'page_id') OR NOT
   (coalesce(selection->'region_ids','[]'::jsonb)||coalesce(selection->'footnote_region_ids','[]'::jsonb))
    @> jsonb_build_array(e->'region_id') THEN RETURN false;END IF;
 END IF;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

-- Keep the frozen quoted-fact semantics for historical imports. New native
-- findings reach this function with passage links, never copied quotations.
ALTER FUNCTION rkb_v4.check_source_facts_v1(jsonb) RENAME TO check_source_quoted_facts_v1;
CREATE FUNCTION rkb_v4.check_source_facts_v1(finding jsonb) RETURNS boolean
 LANGUAGE plpgsql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE fact jsonb;proof jsonb;
BEGIN
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(finding->'evidence') e WHERE e?'original_excerpt') THEN
  RETURN rkb_v4.check_source_quoted_facts_v1(finding);
 END IF;
 FOR fact IN SELECT value FROM jsonb_array_elements(finding->'event_dates')
  UNION ALL SELECT value FROM jsonb_array_elements(finding->'observations') LOOP
  FOR proof IN SELECT value FROM jsonb_array_elements(rkb_v4.source_fact_proofs_v1(finding,fact->'evidence_indexes')) LOOP
   IF proof->>'passage_id' IS NULL OR proof->>'region_id' IS NULL THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR raise_exception THEN RETURN false;
END $$;

CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_semantic_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb:=convert_from(p,'UTF8')::jsonb;base bytea;finding jsonb;story jsonb;proof jsonb;block jsonb;ref jsonb;
BEGIN
 base:=convert_to(((doc-'semantic')||'{"codec":"rkb.source-activation-base.v1"}'::jsonb)::text,'UTF8');
 IF NOT rkb_v4.check_source_batch_base_v1(base,ids,expected,logical_time) THEN RETURN false;END IF;
 FOR finding IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
  FOR proof IN SELECT value FROM jsonb_array_elements(finding->'evidence') LOOP
   IF NOT rkb_v4.check_source_passage_evidence_v1(doc,proof) THEN RETURN false;END IF;
  END LOOP;
  IF NOT rkb_v4.check_source_facts_v1(finding) THEN RETURN false;END IF;
 END LOOP;
 FOR story IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'stories')
  UNION ALL SELECT value FROM jsonb_array_elements(doc->'semantic'->'pending') LOOP
  FOR ref IN SELECT value FROM jsonb_array_elements(story->'finding_ids') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') f
    WHERE f->'assertion_id'=ref) THEN RETURN false;END IF;
  END LOOP;
  FOR block IN SELECT value FROM jsonb_array_elements(story->'default_variant'->'blocks') LOOP
   FOR ref IN SELECT value FROM jsonb_array_elements(block->'assertion_ids') LOOP
    IF NOT story->'finding_ids' @> jsonb_build_array(ref) THEN RETURN false;END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_graph_v1(doc jsonb,source uuid,batches integer) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE g jsonb:=doc->'source_graph';owner uuid;e jsonb;mention jsonb;alias_row jsonb;r jsonb;proof jsonb;f jsonb;
 ident uuid;k text;n integer;ids uuid[]:='{}';binding jsonb:='{}';from_kind text;to_kind text;
BEGIN
 -- Findings must point into the same manifest, including printed footnotes.
 FOR f IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
  FOR proof IN SELECT value FROM jsonb_array_elements(f->'evidence') LOOP
   IF NOT rkb_v4.check_source_passage_evidence_v1(doc,proof) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF g IS NULL THEN RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') AS finding_rows(value)
  WHERE finding_rows.value->'entity_keys'<>'[]'::jsonb);END IF;
 IF g->>'source_id' IS DISTINCT FROM source::text THEN RETURN false;END IF;
 owner:=(g->>'owner_person_id')::uuid;
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  ident:=(e->>'entity_id')::uuid;
  IF e->>'mode'='create' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids WHERE entity_id=ident
    AND (owner_person_id<>owner OR kind<>e->>'kind')) OR e->>'mode'='reuse' AND NOT EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner AND h.kind=e->>'kind'
    AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
     JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn))
    THEN RETURN false;END IF;
  ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(e->>'key',ident::text);
  FOR mention IN SELECT value FROM jsonb_array_elements(e->'mentions') LOOP
   IF NOT rkb_v4.check_source_passage_evidence_v1(doc,mention->'evidence') THEN RETURN false;END IF;
  END LOOP;
  FOR alias_row IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   FOR proof IN SELECT value FROM jsonb_array_elements(alias_row->'evidences') LOOP
    IF NOT rkb_v4.check_source_passage_evidence_v1(doc,proof) THEN RETURN false;END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  ident:=(r->>'entity_id')::uuid;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner
   AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
    JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn))
   THEN RETURN false;END IF;
  ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(r->>'key',ident::text);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  IF NOT ((r->>'source_id')::uuid=ANY(ids)) OR NOT ((r->>'target_id')::uuid=ANY(ids))
   OR (r->>'source_id')::uuid=(r->>'target_id')::uuid THEN RETURN false;END IF;
  from_kind:=rkb_v4.source_graph_endpoint_kind_v1(g,(r->>'source_id')::uuid);
  to_kind:=rkb_v4.source_graph_endpoint_kind_v1(g,(r->>'target_id')::uuid);
  IF NOT coalesce(CASE r->>'kind'
   WHEN 'participated_in' THEN from_kind IN ('person','organization') AND to_kind='event'
   WHEN 'occurred_at' THEN from_kind='event' AND to_kind='poi_ref'
   WHEN 'member_of' THEN from_kind IN ('person','organization','event','poi_ref') AND to_kind='historical_thread'
   WHEN 'affiliated_with' THEN from_kind='person' AND to_kind='organization'
   WHEN 'operated_at' THEN from_kind='organization' AND to_kind='poi_ref'
   WHEN 'predecessor_of' THEN from_kind='organization' AND to_kind='organization'
   WHEN 'founded_by' THEN from_kind='organization' AND to_kind='person'
   WHEN 'located_in' THEN from_kind='poi_ref' AND to_kind='poi_ref' ELSE false END,false) THEN RETURN false;END IF;
  FOR proof IN SELECT value FROM jsonb_array_elements(r->'evidence') LOOP
   IF NOT rkb_v4.check_source_passage_evidence_v1(doc,proof) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  FOR proof IN SELECT value FROM jsonb_array_elements(f->'evidence') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'regions') x
    WHERE x->>'region_id'=proof->>'region_id' AND x->>'page_id'=proof->>'page_id') THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x
   WHERE x->>'assertion_id'=f->>'assertion_id' AND x->'entity_keys'=f->'entity_keys') THEN RETURN false;END IF;
  FOR k,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   IF binding->>k IS NULL OR binding->>k IS DISTINCT FROM f->'entity_ids'->>(n-1) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

-- Existing graph readers hydrate current passage links and still read old stored records.
CREATE OR REPLACE FUNCTION rkb_v4.read_source_graph_v1(wanted_entity uuid,wanted_source uuid,action text,
 after_source uuid,after_key text,take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,record_id text,
 data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH candidates AS (
  SELECT n.source_id,n.entity_key record_id,n.data||jsonb_build_object('mention',coalesce(n.data->'mention',n.data->'mentions'->0)) AS data,
    coalesce(n.data->'mention',n.data->'mentions'->0)->'evidence' AS proof,NULL::text AS fallback_text
   FROM rkb_v4.source_entity_representations n
   WHERE action IN ('entity','list') AND ((action='entity' AND wanted_entity IS NOT NULL AND n.entity_id=wanted_entity)
    OR (action='list' AND wanted_source IS NOT NULL AND n.source_id=wanted_source))
  UNION ALL
  SELECT m.source_id,m.mention_id::text,m.data,m.data->'evidence',NULL::text
   FROM rkb_v4.source_entity_mentions m WHERE action='mentions' AND wanted_entity IS NOT NULL AND m.entity_id=wanted_entity
  UNION ALL
  SELECT a.source_id,a.alias_id::text,a.data,a.data->'evidence',NULL::text
   FROM rkb_v4.source_entity_aliases a WHERE action='aliases' AND wanted_entity IS NOT NULL AND a.entity_id=wanted_entity
  UNION ALL
  SELECT r.source_id,r.relation_id::text,r.data,r.data->'evidence'->0,NULL::text
   FROM rkb_v4.source_entity_relations r WHERE action='relations' AND wanted_entity IS NOT NULL AND (r.from_id=wanted_entity OR r.to_id=wanted_entity)
  UNION ALL
  SELECT f.source_id,f.candidate_id::text,f.data,NULL::jsonb,f.data->>'text'
   FROM rkb_v4.source_poi_hypotheses f WHERE action='poi_facts' AND wanted_source IS NOT NULL AND f.source_id=wanted_source
 ) SELECT c.source_id,1::bigint,v.eligibility_revision,c.record_id,c.data,coalesce(c.proof->>'exact_quote',left(p.body,2000),c.fallback_text),s.bibliography
 FROM candidates c JOIN rkb_v4.sources s USING(source_id) JOIN rkb_v4.active_revision active USING(source_id)
 JOIN rkb_v4.source_visibility v USING(source_id) JOIN rkb_v4.source_graph_receipts receipt USING(source_id)
 LEFT JOIN rkb_v4.passage_search p ON p.passage_id=(c.proof->>'chunk_id')::uuid AND p.source_id=c.source_id AND p.revision=1
 WHERE s.accepted AND NOT s.withdrawn AND active.revision=1 AND (wanted_source IS NULL OR c.source_id=wanted_source)
  AND (after_source IS NULL OR c.source_id>after_source OR c.source_id=after_source AND c.record_id COLLATE "C">after_key COLLATE "C")
 ORDER BY c.source_id,c.record_id COLLATE "C" LIMIT least(greatest(take,1),4)
$$;

CREATE OR REPLACE FUNCTION rkb_v4.read_source_graph_filtered_v1(
 wanted_source uuid, after_source uuid, after_key text, selector_kinds text[],
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
  WHERE wanted_source IS NOT NULL AND n.source_id=wanted_source
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

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rkb_v4 FROM PUBLIC;
