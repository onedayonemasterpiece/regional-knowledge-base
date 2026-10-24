-- SQL032: measured PG17 scale optimization of one existing validator only.
-- Old SQL026/028/029 sources, accepted commands and native materializer hashes
-- are immutable. The original owner-only/graph/evidence checks remain intact.
-- Both transformed branches used nested jsonb_array_elements per proof:
-- relational left joins preserve original NULL and EXISTS/duplicate semantics.
-- CREATE OR REPLACE retains the existing routine owner/grants/ABI.
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
 -- Both links are existence checks, not UNIQUE lookups: a duplicate
 -- referenced region/finding retains the original EXISTS truth value.
 -- Expand each source array once; hash-join the exact JSON/text keys.
 IF EXISTS (
  WITH regions AS MATERIALIZED (
   SELECT x.value->>'region_id' AS region_id,x.value->>'page_id' AS page_id
   FROM jsonb_array_elements(doc->'semantic'->'regions') AS x(value)
  ),poi_proofs AS (
   SELECT p.value->>'region_id' AS region_id,p.value->>'page_id' AS page_id
   FROM jsonb_array_elements(g->'poi_facts') AS f(value)
   CROSS JOIN LATERAL jsonb_array_elements(f.value->'evidence') AS p(value)
  )
  SELECT 1 FROM poi_proofs p
   LEFT JOIN regions r ON r.region_id=p.region_id AND r.page_id=p.page_id
   WHERE r.region_id IS NULL LIMIT 1
 ) THEN RETURN false;END IF;
 IF EXISTS (
  WITH findings AS MATERIALIZED (
   SELECT x.value->>'assertion_id' AS assertion_id,x.value->'entity_keys' AS entity_keys
   FROM jsonb_array_elements(doc->'semantic'->'findings') AS x(value)
  ),mappings AS (
   SELECT x.value->>'assertion_id' AS assertion_id,x.value->'entity_keys' AS entity_keys
   FROM jsonb_array_elements(g->'finding_entities') AS x(value)
  )
  SELECT 1 FROM mappings m LEFT JOIN findings f
   ON f.assertion_id=m.assertion_id AND f.entity_keys=m.entity_keys
   WHERE f.assertion_id IS NULL LIMIT 1
 ) THEN RETURN false;END IF;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  FOR k,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   IF binding->>k IS NULL OR binding->>k IS DISTINCT FROM f->'entity_ids'->>(n-1) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;
