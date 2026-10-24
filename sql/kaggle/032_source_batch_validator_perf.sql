-- SQL032: guarded in-place CREATE OR REPLACE of the existing SQL028 validator.
-- The previously installed SQL028 function is canonical; only the two
-- measured nested JSON scans are substituted, with strict exact-once guards.
-- No new function, role, grant, ABI, wire, timeout or materializer file.
DO $sql032$
DECLARE definition text;old_poi text:=$old_poi$
 FOR f IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  FOR proof IN SELECT value FROM jsonb_array_elements(f->'evidence') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'regions') x
    WHERE x->>'region_id'=proof->>'region_id' AND x->>'page_id'=proof->>'page_id') THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
$old_poi$;
replacement text:=$new_poi$
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
$new_poi$;
old_finding text:=$old_finding$
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x
   WHERE x->>'assertion_id'=f->>'assertion_id' AND x->'entity_keys'=f->'entity_keys') THEN RETURN false;END IF;
$old_finding$;
BEGIN
 definition:=pg_get_functiondef('rkb_v4.check_source_batch_graph_v1(jsonb,uuid,integer)'::regprocedure);
 IF definition IS NULL
   OR strpos(definition,old_poi)=0
   OR strpos(substr(definition,strpos(definition,old_poi)+length(old_poi)),old_poi)>0
   OR strpos(definition,old_finding)=0
   OR strpos(substr(definition,strpos(definition,old_finding)+length(old_finding)),old_finding)>0
 THEN RAISE EXCEPTION 'sql032 original graph validator body differs';END IF;
 definition:=replace(replace(definition,old_poi,replacement),old_finding,'');
 -- pg_get_functiondef returns the original CREATE OR REPLACE FUNCTION DDL,
 -- retaining owner, volatility/security, original STRICT evidence calls.
 EXECUTE definition;
END $sql032$;
