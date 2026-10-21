-- Explicit schema12: preserve schema11 functions and every original outcome.
-- GeoManifestItem permits 256 provenance dependencies; native SQL must agree.
-- Install this reviewed artifact before its ordered schema-transition command.
CREATE FUNCTION rkb_v4.check_geo_manifest_install_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb; act text; manifest text; h rkb_v4.geo_manifest_heads; entry jsonb; body bytea;
 item jsonb; idx jsonb; prefix bytea;suffix bytea; header jsonb;index_bytes bytea;
 total bigint;joined bytea; k text; wanted jsonb; fields text[];
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 720896 OR jsonb_typeof(ids) IS DISTINCT FROM 'object'
 OR NOT ids?'manifest_id' OR ids-'manifest_id'<>'{}'
 OR jsonb_typeof(ids->'manifest_id') IS DISTINCT FROM 'string' OR ids->>'manifest_id' !~ '^[0-9a-f]{64}$'
 OR jsonb_typeof(expected) IS DISTINCT FROM 'object'
 OR NOT expected ?& ARRAY['manifest_state','manifest_index_sha256','installer_person_id']
 OR expected-ARRAY['manifest_state','manifest_index_sha256','installer_person_id']<>'{}'
 OR jsonb_typeof(expected->'installer_person_id') IS DISTINCT FROM 'string'
 OR jsonb_typeof(expected->'manifest_index_sha256') IS DISTINCT FROM 'string'
 OR (expected->>'installer_person_id')::uuid::text IS DISTINCT FROM expected->>'installer_person_id'
 OR expected->>'manifest_index_sha256' !~ '^[0-9a-f]{64}$' THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;act:=doc->>'action';manifest:=ids->>'manifest_id';
 IF doc->>'codec' IS DISTINCT FROM 'rkb.geo-manifest-command.v1' THEN RETURN false;END IF;
 SELECT * INTO h FROM rkb_v4.geo_manifest_heads WHERE manifest_id=manifest;
 IF act='open' THEN
  IF h.manifest_id IS NOT NULL OR expected->>'manifest_state' IS DISTINCT FROM 'absent'
   OR NOT doc ?& ARRAY['codec','action','header_prefix_b64','header_suffix_b64','index_b64']
   OR doc-ARRAY['codec','action','header_prefix_b64','header_suffix_b64','index_b64']<>'{}' THEN RETURN false;END IF;
  prefix:=decode(doc->>'header_prefix_b64','base64');suffix:=decode(doc->>'header_suffix_b64','base64');
  index_bytes:=decode(doc->>'index_b64','base64');idx:=convert_from(index_bytes,'UTF8')::jsonb;
  IF octet_length(prefix)>8192 OR octet_length(suffix)>8192 OR octet_length(index_bytes)>720896
   OR encode(sha256(index_bytes),'hex') IS DISTINCT FROM expected->>'manifest_index_sha256'
   OR jsonb_typeof(idx) IS DISTINCT FROM 'array' OR jsonb_array_length(idx)>4096 THEN RETURN false;END IF;
  header:=convert_from(prefix||suffix,'UTF8')::jsonb;
  IF jsonb_typeof(header) IS DISTINCT FROM 'object'
   OR NOT header ?& ARRAY['codec','source_inventory_sha256','inventory_counts','missing_dependencies','canonical_owners_copied','accepted_target_sources_created','items']
   OR header-ARRAY['codec','source_inventory_sha256','inventory_counts','missing_dependencies','canonical_owners_copied','accepted_target_sources_created','items']<>'{}'
   OR jsonb_typeof(header->'source_inventory_sha256') IS DISTINCT FROM 'string'
   OR header->>'codec' IS DISTINCT FROM 'rkb.geo-migration-manifest.v1'
   OR header->'accepted_target_sources_created' IS DISTINCT FROM 'false'::jsonb
   OR header->'canonical_owners_copied' IS DISTINCT FROM 'false'::jsonb
   OR header->'missing_dependencies' IS DISTINCT FROM '[]'::jsonb OR header->'items' IS DISTINCT FROM '[]'::jsonb
   OR header->>'source_inventory_sha256' !~ '^[0-9a-f]{64}$' THEN RETURN false;END IF;
  fields:=ARRAY['entities','geo_places','mentions','aliases','relations','geo_intents','accepted_geo_metadata'];
  IF jsonb_typeof(header->'inventory_counts') IS DISTINCT FROM 'object'
   OR NOT (header->'inventory_counts') ?& fields OR (header->'inventory_counts')-fields<>'{}' THEN RETURN false;END IF;
  FOREACH k IN ARRAY fields LOOP
   IF jsonb_typeof(header->'inventory_counts'->k) IS DISTINCT FROM 'number'
    OR header->'inventory_counts'->>k !~ '^(0|[1-9][0-9]{0,3})$'
    OR (header->'inventory_counts'->>k)::integer>4096 THEN RETURN false;END IF;
  END LOOP;
  k:='';
  FOR entry IN SELECT value FROM jsonb_array_elements(idx) LOOP
   IF jsonb_typeof(entry) IS DISTINCT FROM 'object' OR NOT entry ?& ARRAY['key','sha256']
    OR entry-ARRAY['key','sha256']<>'{}' OR jsonb_typeof(entry->'key') IS DISTINCT FROM 'string'
    OR octet_length(entry->>'key') NOT BETWEEN 1 AND 2048
    OR jsonb_typeof(entry->'sha256') IS DISTINCT FROM 'string' OR entry->>'sha256' !~ '^[0-9a-f]{64}$'
    OR (entry->>'key') COLLATE "C"<=k COLLATE "C" THEN RETURN false;END IF;
   k:=entry->>'key';
  END LOOP;
  RETURN true;
 END IF;
 IF h.manifest_id IS NULL OR h.state<>'staging' OR expected->>'manifest_state' IS DISTINCT FROM 'staging'
 OR h.installer_person_id::text IS DISTINCT FROM expected->>'installer_person_id'
 OR expected->>'manifest_index_sha256' IS DISTINCT FROM h.index_sha256 THEN RETURN false;END IF;
 IF act='batch' THEN
  IF octet_length(p)>262144 OR doc-ARRAY['codec','action','items']<>'{}'
   OR jsonb_typeof(doc->'items') IS DISTINCT FROM 'array' OR jsonb_array_length(doc->'items') NOT BETWEEN 1 AND 128 THEN RETURN false;END IF;
  k:='';
  FOR entry IN SELECT value FROM jsonb_array_elements(doc->'items') LOOP
   IF jsonb_typeof(entry) IS DISTINCT FROM 'object' OR NOT entry ?& ARRAY['key','sha256','wire_b64','lookup_names']
    OR entry-ARRAY['key','sha256','wire_b64','lookup_names']<>'{}' OR (entry->>'key') COLLATE "C"<=k COLLATE "C"
    OR jsonb_typeof(entry->'lookup_names') IS DISTINCT FROM 'array' OR jsonb_array_length(entry->'lookup_names')>22
    THEN RETURN false;END IF;
   k:=entry->>'key';body:=decode(entry->>'wire_b64','base64');item:=convert_from(body,'UTF8')::jsonb;
   SELECT value INTO wanted FROM jsonb_array_elements(h.item_index) WHERE value->>'key'=k;
   IF wanted IS NULL OR octet_length(body) NOT BETWEEN 1 AND 65536
    OR encode(sha256(body),'hex') IS DISTINCT FROM entry->>'sha256' OR wanted->>'sha256' IS DISTINCT FROM entry->>'sha256'
    OR jsonb_typeof(item) IS DISTINCT FROM 'object'
    OR NOT item ?& ARRAY['stable_id','source_owner','source_store','source_revision','owner_person_id','access','status','dependencies','payload','payload_sha256']
    OR item-ARRAY['stable_id','source_owner','source_store','source_revision','owner_person_id','access','status','dependencies','payload','payload_sha256']<>'{}'
    OR jsonb_typeof(item->'stable_id') IS DISTINCT FROM 'string' OR octet_length(item->>'stable_id') NOT BETWEEN 1 AND 2048
    OR jsonb_typeof(item->'source_revision') IS DISTINCT FROM 'string' OR octet_length(item->>'source_revision') NOT BETWEEN 1 AND 512
    OR jsonb_typeof(item->'owner_person_id') IS DISTINCT FROM 'string'
    OR jsonb_typeof(item->'payload_sha256') IS DISTINCT FROM 'string' OR item->>'payload_sha256' !~ '^[0-9a-f]{64}$'
    OR jsonb_typeof(item->'payload') IS DISTINCT FROM 'object'
    OR jsonb_typeof(item->'payload'->'kind') IS DISTINCT FROM 'string'
    OR jsonb_typeof(item->'status') IS DISTINCT FROM 'string'
    OR item->>'access' IS DISTINCT FROM 'private_owner_only' OR item->>'source_owner' IS DISTINCT FROM 'regional_knowledge'
    OR item->>'source_store' IS DISTINCT FROM 'legacy_rkb_sqlite'
    OR item->>'status' NOT IN ('legacy_unanchored','left_with_owner','excluded_non_geo')
    OR jsonb_typeof(item->'dependencies') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'dependencies')>256
    OR item->'payload'->>'kind' NOT IN ('place','alias','relation','provenance','source_geo_observation','owner_reference','excluded_non_geo')
    OR k IS DISTINCT FROM (item->'payload'->>'kind')||':'||(item->>'stable_id')
    OR (item->>'owner_person_id')::uuid::text IS DISTINCT FROM item->>'owner_person_id'
    OR EXISTS(SELECT 1 FROM rkb_v4.geo_manifest_items old WHERE old.manifest_id=manifest AND old.item_key=k AND old.item_sha256<>entry->>'sha256')
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(entry->'lookup_names') n WHERE jsonb_typeof(n)<>'string'
       OR octet_length(n#>>'{}') NOT BETWEEN 1 AND 16384) THEN RETURN false;END IF;
  END LOOP;
  RETURN true;
 END IF;
 IF act<>'activate' OR doc-ARRAY['codec','action']<>'{}'
 OR EXISTS(SELECT 1 FROM rkb_v4.geo_manifest_heads WHERE state='active')
 OR (SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest)<>jsonb_array_length(h.item_index)
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(h.item_index) e WHERE NOT EXISTS(
   SELECT 1 FROM rkb_v4.geo_manifest_items i WHERE i.manifest_id=manifest AND i.item_key=e->>'key' AND i.item_sha256=e->>'sha256'))
 OR EXISTS(SELECT 1 FROM rkb_v4.geo_manifest_items i CROSS JOIN LATERAL jsonb_array_elements_text(i.data->'dependencies') d
   WHERE i.manifest_id=manifest AND NOT EXISTS(SELECT 1 FROM rkb_v4.geo_manifest_items other WHERE other.manifest_id=manifest
    AND other.item_key=d AND other.owner_person_id=i.owner_person_id AND other.kind<>'excluded_non_geo')) THEN RETURN false;END IF;
 -- Independent input counts cannot be replaced by a partial accepted output.
 IF EXISTS(SELECT 1 FROM (VALUES
  ('entities',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND
    (kind='place' OR kind='excluded_non_geo' AND data->'payload'->>'source_table'='rkb_entities'))),
  ('geo_places',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND kind='place')),
  ('aliases',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND
    (kind='alias' OR kind='excluded_non_geo' AND data->'payload'->>'source_table'='rkb_entity_aliases'))),
  ('relations',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND
    (kind='relation' OR kind='excluded_non_geo' AND data->'payload'->>'source_table'='rkb_entity_relations'))),
  ('geo_intents',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND kind='source_geo_observation')),
  ('accepted_geo_metadata',(SELECT count(*) FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest AND kind='place'
    AND data->'payload'->'legacy_resolution' IS DISTINCT FROM 'null'::jsonb))
 ) actual(name,n) WHERE n<>(h.header->'inventory_counts'->>name)::bigint) THEN RETURN false;END IF;
 SELECT coalesce(sum(octet_length(wire)),0)+greatest(count(*)-1,0) INTO total FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest;
 IF total+octet_length(h.header_prefix)+octet_length(h.header_suffix)>4194304 THEN RETURN false;END IF;
 SELECT coalesce(string_agg(wire,convert_to(',','UTF8') ORDER BY item_key COLLATE "C"),''::bytea)
 INTO joined FROM rkb_v4.geo_manifest_items WHERE manifest_id=manifest;
 IF encode(sha256(h.header_prefix||joined||h.header_suffix),'hex')<>manifest THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_geo_manifest_install_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state;prior rkb_v4.read_frontier;doc jsonb;entry jsonb;item jsonb;
 body bytea;kind text;entity uuid;manifest text;name text;first_year integer;last_year integer;header jsonb;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_geo_manifest_install_v2(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'geographic manifest exact precondition failed';END IF;
 doc:=convert_from(p,'UTF8')::jsonb;manifest:=ids->>'manifest_id';
 IF doc->>'action'='open' THEN
  header:=convert_from(decode(doc->>'header_prefix_b64','base64')||decode(doc->>'header_suffix_b64','base64'),'UTF8')::jsonb;
  -- The fixed decoder binds this precondition to the accepted command actor;
  -- the receipt trigger checks that binding again beside the actual ledger.
  INSERT INTO rkb_v4.geo_manifest_heads VALUES(manifest,(expected->>'installer_person_id')::uuid,
   expected->>'manifest_index_sha256',decode(doc->>'index_b64','base64'),
   convert_from(decode(doc->>'index_b64','base64'),'UTF8')::jsonb,decode(doc->>'header_prefix_b64','base64'),
   decode(doc->>'header_suffix_b64','base64'),header,'staging',logical_time);
 ELSIF doc->>'action'='batch' THEN
  FOR entry IN SELECT value FROM jsonb_array_elements(doc->'items') LOOP
   body:=decode(entry->>'wire_b64','base64');item:=convert_from(body,'UTF8')::jsonb;kind:=item->'payload'->>'kind';
   INSERT INTO rkb_v4.geo_manifest_items VALUES(manifest,entry->>'key',entry->>'sha256',body,item,kind,
    (item->>'owner_person_id')::uuid,item->>'stable_id') ON CONFLICT DO NOTHING;
   entity:=CASE WHEN kind='place' THEN (item->>'stable_id')::uuid
    WHEN kind IN ('alias','source_geo_observation') THEN (item->'payload'->>'entity_id')::uuid ELSE NULL END;
   first_year:=CASE WHEN kind='source_geo_observation' THEN (item->'payload'->>'valid_year_from')::integer ELSE NULL END;
   last_year:=CASE WHEN kind='source_geo_observation' THEN (item->'payload'->>'valid_year_to')::integer ELSE NULL END;
   IF entity IS NOT NULL THEN
    FOR name IN SELECT value FROM jsonb_array_elements_text(entry->'lookup_names') LOOP
     INSERT INTO rkb_v4.geo_lookup_names VALUES(manifest,entry->>'key',entity,(item->>'owner_person_id')::uuid,
      name,encode(sha256(convert_to(name,'UTF8')),'hex'),first_year,last_year) ON CONFLICT DO NOTHING;
    END LOOP;
   END IF;
   IF kind='place' AND item->'payload'->>'canonical_poi_ref' IS NOT NULL THEN
    INSERT INTO rkb_v4.geo_poi_links VALUES(manifest,entry->>'key',(item->>'stable_id')::uuid,
     (item->>'owner_person_id')::uuid,item->'payload'->>'canonical_poi_ref') ON CONFLICT DO NOTHING;
   END IF;
  END LOOP;
 ELSE
  UPDATE rkb_v4.geo_manifest_heads SET state='active' WHERE manifest_id=manifest;
 END IF;
 -- Metadata effects advance the SAME canonical cursor while preserving actual
 -- source text/dense readiness. They never invent a projection readiness flag.
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,prior.visibility_epoch,state.applied_seq+1,
  prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
  prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
 RETURN convert_to(jsonb_build_object('manifest_id',manifest,'action',doc->>'action',
  'index_sha256',expected->>'manifest_index_sha256','active',doc->>'action'='activate')::text,'UTF8');
END $$;


CREATE FUNCTION rkb_v4.check_geo_dependencies_v2_transition(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb; s rkb_v4.recovery_state;
BEGIN
 doc:=convert_from(p,'UTF8')::jsonb;
 SELECT * INTO STRICT s FROM rkb_v4.recovery_state WHERE singleton;
 RETURN ids='{}'::jsonb
  AND expected=jsonb_build_object('schema','rkb-v4-sql001-011-v1',
    'function','rkb-v4-source-story-cases-geo-v6','executor','rkb-python-apply-v1','migration','')
  AND s.schema_revision='rkb-v4-sql001-011-v1'
  AND s.function_revision='rkb-v4-source-story-cases-geo-v6'
  AND s.executor_revision='rkb-python-apply-v1' AND s.last_migration_id IS NULL
  AND doc->>'migration_id'='geo-dependencies-v2'
  AND doc-'migration_id'-'artifact_sha256'-'predecessor'='{}'::jsonb
  AND doc->>'artifact_sha256' ~ '^[0-9a-f]{64}$' AND doc->'predecessor'='null'::jsonb;
END $$;
CREATE FUNCTION rkb_v4.apply_geo_dependencies_v2_transition(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF NOT rkb_v4.check_geo_dependencies_v2_transition(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'geographic dependency migration precondition failed';
 END IF;
 RETURN p;
END $$;
REVOKE ALL ON FUNCTION rkb_v4.check_geo_manifest_install_v2(bytea,jsonb,jsonb,text),
 rkb_v4.apply_geo_manifest_install_v2(bytea,jsonb,jsonb,text),
 rkb_v4.check_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text),
 rkb_v4.apply_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text) FROM PUBLIC;
