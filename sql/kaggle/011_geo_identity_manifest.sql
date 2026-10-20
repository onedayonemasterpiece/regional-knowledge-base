-- Explicit schema11 artifact, after exact009/010. No previous SQL is replaced.
-- Only RKB's minimal source GEO identity/provenance/reference whitelist enters
-- these tables. External canonical geometry/POI stores stay with their owners.
CREATE TABLE rkb_v4.geo_manifest_heads (
 manifest_id text PRIMARY KEY CHECK(manifest_id ~ '^[0-9a-f]{64}$'),
 installer_person_id uuid NOT NULL, index_sha256 text NOT NULL CHECK(index_sha256 ~ '^[0-9a-f]{64}$'),
 index_bytes bytea NOT NULL CHECK(octet_length(index_bytes)<=720896), item_index jsonb NOT NULL,
 header_prefix bytea NOT NULL CHECK(octet_length(header_prefix)<=8192),
 header_suffix bytea NOT NULL CHECK(octet_length(header_suffix)<=8192),
 header jsonb NOT NULL, state text NOT NULL CHECK(state IN ('staging','active')),
 logical_time text NOT NULL
);
CREATE UNIQUE INDEX geo_one_active_manifest ON rkb_v4.geo_manifest_heads(state) WHERE state='active';
CREATE TABLE rkb_v4.geo_manifest_items (
 manifest_id text NOT NULL REFERENCES rkb_v4.geo_manifest_heads,
 item_key text NOT NULL CHECK(octet_length(item_key) BETWEEN 1 AND 2048),
 item_sha256 text NOT NULL CHECK(item_sha256 ~ '^[0-9a-f]{64}$'),
 wire bytea NOT NULL CHECK(octet_length(wire) BETWEEN 1 AND 65536), data jsonb NOT NULL,
 kind text NOT NULL CHECK(kind IN ('place','alias','relation','provenance','source_geo_observation','owner_reference','excluded_non_geo')),
 owner_person_id uuid NOT NULL, stable_id text NOT NULL,
 PRIMARY KEY(manifest_id,item_key),UNIQUE(manifest_id,kind,stable_id)
);
CREATE INDEX geo_private_items ON rkb_v4.geo_manifest_items(owner_person_id,kind,stable_id,manifest_id);
CREATE TABLE rkb_v4.geo_lookup_names (
 manifest_id text NOT NULL, item_key text NOT NULL, entity_id uuid NOT NULL, owner_person_id uuid NOT NULL,
 normalized_name text NOT NULL CHECK(octet_length(normalized_name) BETWEEN 1 AND 16384),
 name_sha256 text NOT NULL CHECK(name_sha256=encode(sha256(convert_to(normalized_name,'UTF8')),'hex')),
 valid_year_from integer, valid_year_to integer,
 PRIMARY KEY(manifest_id,item_key,name_sha256),
 FOREIGN KEY(manifest_id,item_key) REFERENCES rkb_v4.geo_manifest_items,
 CHECK((valid_year_from IS NULL)=(valid_year_to IS NULL)),CHECK(valid_year_from<=valid_year_to)
);
CREATE INDEX geo_name_exact_private ON rkb_v4.geo_lookup_names(owner_person_id,name_sha256,entity_id);
CREATE INDEX geo_names_entity ON rkb_v4.geo_lookup_names(manifest_id,entity_id);
CREATE TABLE rkb_v4.geo_poi_links (
 manifest_id text NOT NULL,item_key text NOT NULL,entity_id uuid NOT NULL,owner_person_id uuid NOT NULL,
 poi_ref text NOT NULL,PRIMARY KEY(manifest_id,item_key),
 FOREIGN KEY(manifest_id,item_key) REFERENCES rkb_v4.geo_manifest_items
);
CREATE INDEX geo_poi_exact_private ON rkb_v4.geo_poi_links(owner_person_id,poi_ref,entity_id);
CREATE TABLE rkb_v4.geo_manifest_receipts (
 manifest_id text PRIMARY KEY REFERENCES rkb_v4.geo_manifest_heads,
 operation_key text NOT NULL REFERENCES rkb_v4.applied_ops,sequence bigint NOT NULL,
 command_sha256 text NOT NULL,person_id text NOT NULL,request_id text NOT NULL,logical_time text NOT NULL
);
CREATE TRIGGER immutable_geo_item BEFORE UPDATE OR DELETE ON rkb_v4.geo_manifest_items
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_geo_name BEFORE UPDATE OR DELETE ON rkb_v4.geo_lookup_names
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_geo_poi_link BEFORE UPDATE OR DELETE ON rkb_v4.geo_poi_links
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_geo_receipt BEFORE UPDATE OR DELETE ON rkb_v4.geo_manifest_receipts
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.validate_geo_manifest_head_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state<>'staging' OR NEW.state<>'active'
  OR (to_jsonb(OLD)-'state') IS DISTINCT FROM (to_jsonb(NEW)-'state') THEN
  RAISE EXCEPTION 'immutable geographic manifest head';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_geo_head BEFORE UPDATE OR DELETE ON rkb_v4.geo_manifest_heads
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.validate_geo_manifest_head_v1();

CREATE FUNCTION rkb_v4.check_geo_manifest_install_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
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
    OR jsonb_typeof(item->'dependencies') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'dependencies')>64
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

CREATE FUNCTION rkb_v4.apply_geo_manifest_install_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state;prior rkb_v4.read_frontier;doc jsonb;entry jsonb;item jsonb;
 body bytea;kind text;entity uuid;manifest text;name text;first_year integer;last_year integer;header jsonb;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_geo_manifest_install_v1(p,ids,expected,logical_time) THEN
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

CREATE FUNCTION rkb_v4.capture_geo_manifest_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;doc jsonb;
BEGIN
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF NEW.status='applied' AND c->>'route'='geo.manifest.install.v1' THEN
  doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.geo_manifest_heads h WHERE h.manifest_id=c->'ids'->>'manifest_id'
   AND h.installer_person_id::text=c->>'person_id') THEN RAISE EXCEPTION 'geographic accepted installer mismatch';END IF;
  IF doc->>'action'='activate' THEN
   INSERT INTO rkb_v4.geo_manifest_receipts VALUES(c->'ids'->>'manifest_id',NEW.operation_key,NEW.sequence,
    NEW.command_sha256,c->>'person_id',c->>'request_id',c->>'logical_time');
  END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_geo_manifest_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_geo_manifest_receipt_v1();

CREATE FUNCTION rkb_v4.read_geo_identity_v1(actor uuid,entity uuid,poi text,name_hash text,name_text text,
 query_year integer,after_id uuid,take integer)
RETURNS TABLE(entity_id uuid,item_key text,wire bytea,matched_wire bytea,manifest_id text,
 activation_sequence bigint,activation_operation_key text,activation_command_sha256 text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH candidates AS (
  SELECT i.stable_id::uuid entity_id,i.item_key,i.wire,NULL::bytea matched_wire,i.manifest_id
   FROM rkb_v4.geo_manifest_items i WHERE i.owner_person_id=actor AND i.kind='place'
    AND entity IS NOT NULL AND i.stable_id=entity::text
  UNION ALL
  SELECT i.stable_id::uuid,i.item_key,i.wire,NULL::bytea,i.manifest_id
   FROM rkb_v4.geo_poi_links p JOIN rkb_v4.geo_manifest_items i ON i.manifest_id=p.manifest_id AND i.item_key='place:'||p.entity_id::text
   WHERE p.owner_person_id=actor AND i.owner_person_id=actor AND poi IS NOT NULL AND p.poi_ref=poi
  UNION ALL
  SELECT n.entity_id,i.item_key,i.wire,m.wire,i.manifest_id
   FROM rkb_v4.geo_lookup_names n JOIN rkb_v4.geo_manifest_items i ON i.manifest_id=n.manifest_id AND i.item_key='place:'||n.entity_id::text
   JOIN rkb_v4.geo_manifest_items m ON m.manifest_id=n.manifest_id AND m.item_key=n.item_key
   WHERE n.owner_person_id=actor AND i.owner_person_id=actor AND m.owner_person_id=actor
    AND name_hash IS NOT NULL AND n.name_sha256=name_hash AND n.normalized_name=name_text
    AND (query_year IS NULL OR n.valid_year_from IS NULL OR query_year BETWEEN n.valid_year_from AND n.valid_year_to)
 ), chosen AS (
  SELECT DISTINCT ON (c.entity_id) c.* FROM candidates c JOIN rkb_v4.geo_manifest_heads h USING(manifest_id)
   WHERE h.state='active' AND (after_id IS NULL OR c.entity_id>after_id) ORDER BY c.entity_id,c.item_key
 ) SELECT c.entity_id,c.item_key,c.wire,c.matched_wire,c.manifest_id,r.sequence,r.operation_key,r.command_sha256
 FROM chosen c JOIN rkb_v4.geo_manifest_receipts r USING(manifest_id)
 ORDER BY c.entity_id LIMIT least(greatest(take,1),11)
$$;

CREATE FUNCTION rkb_v4.read_geo_members_v1(actor uuid,entity uuid,category text,after_key text,take integer)
RETURNS TABLE(item_key text,wire bytea,manifest_id text,activation_sequence bigint,
 activation_operation_key text,activation_command_sha256 text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT i.item_key,i.wire,i.manifest_id,r.sequence,r.operation_key,r.command_sha256
 FROM rkb_v4.geo_manifest_items i JOIN rkb_v4.geo_manifest_heads h USING(manifest_id)
 JOIN rkb_v4.geo_manifest_receipts r USING(manifest_id)
 JOIN rkb_v4.geo_manifest_items place ON place.manifest_id=i.manifest_id AND place.item_key='place:'||entity::text
 WHERE h.state='active' AND i.owner_person_id=actor AND place.owner_person_id=actor
 AND category IN ('owner_reference','provenance','alias','relation','source_geo_observation') AND i.kind=category
 AND (after_key IS NULL OR i.item_key>after_key COLLATE "C")
 AND (i.data->'dependencies' ? ('place:'||entity::text) OR
      category='provenance' AND place.data->'payload'->'provenance_keys' ? i.item_key)
 ORDER BY i.item_key COLLATE "C" LIMIT least(greatest(take,1),21)
$$;

REVOKE ALL ON rkb_v4.geo_manifest_heads,rkb_v4.geo_manifest_items,rkb_v4.geo_lookup_names,
 rkb_v4.geo_poi_links,rkb_v4.geo_manifest_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.validate_geo_manifest_head_v1(),
 rkb_v4.check_geo_manifest_install_v1(bytea,jsonb,jsonb,text),rkb_v4.apply_geo_manifest_install_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_geo_manifest_receipt_v1(),
 rkb_v4.read_geo_identity_v1(uuid,uuid,text,text,text,integer,uuid,integer),
 rkb_v4.read_geo_members_v1(uuid,uuid,text,text,integer) FROM PUBLIC;
