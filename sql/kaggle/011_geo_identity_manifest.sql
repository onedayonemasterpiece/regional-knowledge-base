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

-- New accepted sources retain their complete model-authored entity graph.
-- This is separate from the private legacy-unanchored geographic manifest.
CREATE TABLE rkb_v4.source_graph_documents (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,source_revision bigint NOT NULL CHECK(source_revision=1),
 owner_person_id uuid NOT NULL,data jsonb NOT NULL,payload_sha256 text NOT NULL CHECK(payload_sha256 ~ '^[0-9a-f]{64}$'),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.source_entity_ids (
 entity_id uuid PRIMARY KEY,owner_person_id uuid NOT NULL,
 kind text NOT NULL CHECK(kind IN ('person','organization','event','historical_thread','poi_ref'))
);
CREATE TABLE rkb_v4.source_entity_representations (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,entity_key text NOT NULL,
 entity_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,data jsonb NOT NULL,
 PRIMARY KEY(source_id,entity_key)
);
CREATE INDEX source_entity_active_lookup ON rkb_v4.source_entity_representations(entity_id,source_id,entity_key);
CREATE TABLE rkb_v4.source_entity_references (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,entity_key text NOT NULL,
 entity_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,data jsonb NOT NULL,
 PRIMARY KEY(source_id,entity_key)
);
CREATE TABLE rkb_v4.source_entity_mentions (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,mention_id uuid NOT NULL,
 entity_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,data jsonb NOT NULL,
 PRIMARY KEY(source_id,mention_id)
);
CREATE INDEX source_entity_mention_lookup ON rkb_v4.source_entity_mentions(entity_id,source_id,mention_id);
CREATE TABLE rkb_v4.source_entity_aliases (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,alias_id uuid NOT NULL,
 entity_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,normalized_value text NOT NULL,
 normalized_sha256 text NOT NULL CHECK(normalized_sha256=encode(sha256(convert_to(normalized_value,'UTF8')),'hex')),data jsonb NOT NULL,
 PRIMARY KEY(source_id,alias_id),UNIQUE(source_id,entity_id,normalized_sha256)
);
CREATE INDEX source_entity_alias_lookup ON rkb_v4.source_entity_aliases(normalized_sha256,entity_id,source_id);
CREATE INDEX source_entity_alias_members ON rkb_v4.source_entity_aliases(entity_id,source_id,alias_id);
CREATE TABLE rkb_v4.source_entity_relations (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,relation_id uuid NOT NULL,
 from_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,to_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,
 kind text NOT NULL,data jsonb NOT NULL,PRIMARY KEY(source_id,relation_id),CHECK(from_id<>to_id)
);
CREATE INDEX source_entity_outgoing ON rkb_v4.source_entity_relations(from_id,source_id,relation_id);
CREATE INDEX source_entity_incoming ON rkb_v4.source_entity_relations(to_id,source_id,relation_id);
CREATE TABLE rkb_v4.source_entity_finding_links (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,assertion_id uuid NOT NULL,
 entity_id uuid NOT NULL REFERENCES rkb_v4.source_entity_ids,entity_key text NOT NULL,assertion_revision bigint NOT NULL DEFAULT 1 CHECK(assertion_revision=1),
 PRIMARY KEY(source_id,assertion_id,entity_key),FOREIGN KEY(assertion_id,assertion_revision) REFERENCES rkb_v4.story_assertion_versions(assertion_id,revision)
);
CREATE TABLE rkb_v4.source_poi_hypotheses (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,candidate_id uuid NOT NULL,
 data jsonb NOT NULL,PRIMARY KEY(source_id,candidate_id)
);
CREATE TABLE rkb_v4.source_graph_receipts (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_graph_documents,
 operation_key text NOT NULL REFERENCES rkb_v4.applied_ops,sequence bigint NOT NULL,
 command_sha256 text NOT NULL,person_id uuid NOT NULL,request_id text NOT NULL
);
DO $$ DECLARE tab text;BEGIN
 FOREACH tab IN ARRAY ARRAY['source_graph_documents','source_entity_ids','source_entity_representations',
  'source_entity_references','source_entity_mentions','source_entity_aliases','source_entity_relations',
  'source_entity_finding_links','source_poi_hypotheses','source_graph_receipts'] LOOP
  EXECUTE format('CREATE TRIGGER immutable_source_graph BEFORE UPDATE OR DELETE ON rkb_v4.%I FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update()',tab);
 END LOOP;
END $$;

CREATE FUNCTION rkb_v4.check_source_graph_evidence_v1(doc jsonb,e jsonb) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE r jsonb;p jsonb;m jsonb;b integer;z integer;
BEGIN
 IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR NOT e ?& ARRAY['chunk_id','page_id','region_id','exact_quote','evidence_ref','start','end','source_region_sha256']
  OR e-ARRAY['chunk_id','page_id','region_id','exact_quote','evidence_ref','start','end','source_region_sha256']<>'{}'
  OR jsonb_typeof(e->'start') IS DISTINCT FROM 'number' OR jsonb_typeof(e->'end') IS DISTINCT FROM 'number'
  OR e->>'start' !~ '^(0|[1-9][0-9]{0,4})$' OR e->>'end' !~ '^[1-9][0-9]{0,4}$'
  OR jsonb_typeof(e->'exact_quote') IS DISTINCT FROM 'string' OR length(e->>'exact_quote') NOT BETWEEN 1 AND 2000
  OR e->>'evidence_ref' IS DISTINCT FROM 'knowledge://chunks/'||(e->>'chunk_id') THEN RETURN false;END IF;
 SELECT value INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') WHERE value->>'region_id'=e->>'region_id';
 SELECT value INTO STRICT p FROM jsonb_array_elements(doc->'passages') WHERE value->>'passage_id'=e->>'chunk_id';
 SELECT value INTO STRICT m FROM jsonb_array_elements(doc->'source_manifest'->'passages') WHERE value->>'passage_id'=e->>'chunk_id';
 b:=(e->>'start')::integer;z:=(e->>'end')::integer;
 RETURN r->>'page_id' IS NOT DISTINCT FROM e->>'page_id' AND r->>'source_text_sha256' IS NOT DISTINCT FROM e->>'source_region_sha256'
  AND m->'page_ids' @> jsonb_build_array(e->'page_id') AND m->'region_ids' @> jsonb_build_array(e->'region_id')
  AND b<z AND z<=length(r->>'source_text') AND substring(r->>'source_text' FROM b+1 FOR z-b)=e->>'exact_quote'
  AND position(e->>'exact_quote' IN p->>'body')>0;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.source_graph_endpoint_kind_v1(g jsonb,ident uuid) RETURNS text
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT coalesce((SELECT value->>'kind' FROM jsonb_array_elements(g->'entities') WHERE value->>'entity_id'=ident::text LIMIT 1),
  (SELECT h.kind FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND EXISTS(
   SELECT 1 FROM jsonb_array_elements(g->'references') WHERE value->>'entity_id'=ident::text)))
$$;

CREATE FUNCTION rkb_v4.check_source_graph_v1(doc jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE g jsonb:=doc->'source_graph';e jsonb;a jsonb;r jsonb;proof jsonb;f jsonb;name text;owner uuid;ident uuid;
 k text;from_kind text;to_kind text;ids uuid[]:='{}';keys text[]:='{}';binding jsonb:='{}';n integer;
BEGIN
 IF g IS NULL THEN RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->'entity_keys'<>'[]'::jsonb);END IF;
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR NOT g ?& ARRAY['codec','source_id','source_revision','owner_person_id','bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities']
  OR g-ARRAY['codec','source_id','source_revision','owner_person_id','bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities']<>'{}'
  OR g->>'codec' IS DISTINCT FROM 'rkb.source-graph.v1' OR g->>'source_id' IS DISTINCT FROM source::text
  OR g->'source_revision' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(g->'owner_person_id') IS DISTINCT FROM 'string'
  OR (g->>'owner_person_id')::uuid::text IS DISTINCT FROM g->>'owner_person_id' THEN RETURN false;END IF;
 owner:=(g->>'owner_person_id')::uuid;
 FOREACH k IN ARRAY ARRAY['bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities'] LOOP
  n:=CASE k WHEN 'bundles' THEN 32 WHEN 'poi_fact_inputs' THEN 32 WHEN 'entities' THEN 128 WHEN 'references' THEN 128
   WHEN 'relations' THEN 256 WHEN 'poi_facts' THEN 32 ELSE 100 END;
  IF jsonb_typeof(g->k) IS DISTINCT FROM 'array' OR jsonb_array_length(g->k)>n THEN RETURN false;END IF;
 END LOOP;
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR NOT e ?& ARRAY['key','entity_id','mode','kind','canonical_label','state','metadata','mention','aliases']
   OR e-ARRAY['key','entity_id','mode','kind','canonical_label','state','metadata','mention','aliases']<>'{}'
   OR jsonb_typeof(e->'key') IS DISTINCT FROM 'string' OR length(e->>'key') NOT BETWEEN 1 AND 100 OR e->>'key'=ANY(keys)
   OR jsonb_typeof(e->'entity_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(e->'mention'->'mention_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(e->'kind') IS DISTINCT FROM 'string' OR e->>'kind' NOT IN ('person','organization','event','historical_thread','poi_ref')
   OR e->>'mode' IS NULL OR e->>'mode' NOT IN ('create','reuse') OR e->>'state' IS NULL OR e->>'state' NOT IN ('candidate','reviewed')
   OR jsonb_typeof(e->'canonical_label') IS DISTINCT FROM 'string' OR length(e->>'canonical_label') NOT BETWEEN 1 AND 200
   OR jsonb_typeof(e->'metadata') IS DISTINCT FROM 'object'
   OR e->'metadata'->>'owner_reference_verification' IS DISTINCT FROM 'not_verified'
   OR e->'metadata'->>'historical_geometry' IS DISTINCT FROM 'not_verified'
   OR NOT rkb_v4.check_source_graph_evidence_v1(doc,e->'mention'->'evidence')
   OR jsonb_typeof(e->'mention'->'exact_source_spelling') IS DISTINCT FROM 'string'
   OR position(e->'mention'->>'exact_source_spelling' IN e->'mention'->'evidence'->>'exact_quote')<=0
   OR jsonb_typeof(e->'aliases') IS DISTINCT FROM 'array' OR jsonb_array_length(e->'aliases')>20 THEN RETURN false;END IF;
  ident:=(e->>'entity_id')::uuid;
  IF e->>'mode'='create' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids WHERE entity_id=ident)
   OR e->>'mode'='reuse' AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner
    AND h.kind=e->>'kind' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
     JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn)) THEN RETURN false;END IF;
  keys:=array_append(keys,e->>'key');ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(e->>'key',ident::text);
  FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   IF NOT rkb_v4.check_source_graph_evidence_v1(doc,a->'evidence') OR jsonb_typeof(a->'normalized_value') IS DISTINCT FROM 'string'
    OR length(a->>'normalized_value') NOT BETWEEN 1 AND 200 OR jsonb_typeof(a->'alias_id') IS DISTINCT FROM 'string' OR (a->>'alias_id')::uuid IS NULL THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  IF jsonb_typeof(r) IS DISTINCT FROM 'object' OR NOT r ?& ARRAY['key','entity_id'] OR r-ARRAY['key','entity_id']<>'{}'
   OR jsonb_typeof(r->'key') IS DISTINCT FROM 'string' OR length(r->>'key') NOT BETWEEN 1 AND 100 OR r->>'key'=ANY(keys) OR jsonb_typeof(r->'entity_id') IS DISTINCT FROM 'string' THEN RETURN false;END IF;
  ident:=(r->>'entity_id')::uuid;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner AND EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
    JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn)) THEN RETURN false;END IF;
  keys:=array_append(keys,r->>'key');ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(r->>'key',ident::text);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  IF NOT r ?& ARRAY['relation_id','source_id','target_id','kind','evidence','time_scope','state','review_note']
   OR jsonb_typeof(r->'relation_id') IS DISTINCT FROM 'string' OR jsonb_typeof(r->'source_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(r->'target_id') IS DISTINCT FROM 'string' OR (r->>'source_id')::uuid=(r->>'target_id')::uuid OR NOT ((r->>'source_id')::uuid=ANY(ids)) OR NOT ((r->>'target_id')::uuid=ANY(ids))
   OR jsonb_typeof(r->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(r->'evidence') NOT BETWEEN 1 AND 8 THEN RETURN false;END IF;
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
   IF NOT rkb_v4.check_source_graph_evidence_v1(doc,proof) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  IF jsonb_typeof(f->'candidate_id') IS DISTINCT FROM 'string' OR f->>'geometry_acceptance' IS DISTINCT FROM 'source_hypothesis_not_owner_geometry'
   OR jsonb_typeof(f->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(f->'evidence') NOT BETWEEN 1 AND 50
   OR jsonb_typeof(f->'text') IS DISTINCT FROM 'string' OR length(f->>'text') NOT BETWEEN 1 AND 500 THEN RETURN false;END IF;
  FOR proof IN SELECT value FROM jsonb_array_elements(f->'evidence') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->>'region_id'=proof->>'region_id'
    AND x->>'page_id'=proof->>'page_id' AND x->>'source_text_sha256'=proof->>'source_region_sha256') THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF (SELECT count(*) FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->'entity_keys'<>'[]'::jsonb)
    <>jsonb_array_length(g->'finding_entities') THEN RETURN false;END IF;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->>'assertion_id'=f->>'assertion_id'
    AND x->'entity_keys'=f->'entity_keys') OR jsonb_typeof(f->'entity_ids') IS DISTINCT FROM 'array'
    OR jsonb_typeof(f->'entity_keys') IS DISTINCT FROM 'array' OR jsonb_array_length(f->'entity_ids')<>jsonb_array_length(f->'entity_keys') THEN RETURN false;END IF;
  FOR name,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   IF binding->>name IS NULL OR binding->>name IS DISTINCT FROM f->'entity_ids'->>(n-1) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_import_graph_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 786432 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 RETURN rkb_v4.check_source_import_v4(convert_to((doc-'source_graph')::text,'UTF8'),ids,expected,logical_time)
   AND rkb_v4.check_source_graph_v1(doc,(ids->>'source_id')::uuid);
EXCEPTION WHEN data_exception THEN RETURN false;END $$;

CREATE FUNCTION rkb_v4.project_source_graph_v1(source uuid,g jsonb,payload_sha text) RETURNS void
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE e jsonb;a jsonb;r jsonb;link jsonb;k text;n integer;BEGIN
 IF g IS NULL THEN RETURN;END IF;
 INSERT INTO rkb_v4.source_graph_documents VALUES(source,1,(g->>'owner_person_id')::uuid,g,payload_sha);
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  INSERT INTO rkb_v4.source_entity_ids VALUES((e->>'entity_id')::uuid,(g->>'owner_person_id')::uuid,e->>'kind') ON CONFLICT DO NOTHING;
  INSERT INTO rkb_v4.source_entity_representations VALUES(source,e->>'key',(e->>'entity_id')::uuid,e);
  INSERT INTO rkb_v4.source_entity_mentions VALUES(source,(e->'mention'->>'mention_id')::uuid,(e->>'entity_id')::uuid,e->'mention') ON CONFLICT DO NOTHING;
  FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   INSERT INTO rkb_v4.source_entity_aliases VALUES(source,(a->>'alias_id')::uuid,(e->>'entity_id')::uuid,
    a->>'normalized_value',encode(sha256(convert_to(a->>'normalized_value','UTF8')),'hex'),a) ON CONFLICT DO NOTHING;
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  INSERT INTO rkb_v4.source_entity_references VALUES(source,r->>'key',(r->>'entity_id')::uuid,r);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  INSERT INTO rkb_v4.source_entity_relations VALUES(source,(r->>'relation_id')::uuid,(r->>'source_id')::uuid,(r->>'target_id')::uuid,r->>'kind',r);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  INSERT INTO rkb_v4.source_poi_hypotheses VALUES(source,(r->>'candidate_id')::uuid,r);
 END LOOP;
 FOR link IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  FOR k,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(link->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   INSERT INTO rkb_v4.source_entity_finding_links(source_id,assertion_id,entity_id,entity_key) VALUES(source,(link->>'assertion_id')::uuid,(link->'entity_ids'->>(n-1))::uuid,k);
  END LOOP;
 END LOOP;
END $$;

-- Exact full payload hashing and activation order are preserved at fresh11.
CREATE FUNCTION rkb_v4.apply_source_import_graph_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint; story jsonb; variant jsonb;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_import_graph_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'source import requires exact resolved new-source precondition';
    END IF;
    source:=(ids->>'source_id')::uuid; doc:=convert_from(p,'UTF8')::jsonb;
    research:=doc->'research'; original:=doc->'original';
    INSERT INTO rkb_v4.sources VALUES (source,doc->>'bibliography',true,false);
    INSERT INTO rkb_v4.revisions VALUES (source,1);
    INSERT INTO rkb_v4.source_import_origins VALUES (source,1,(doc->>'work_id')::uuid,
        (doc->>'edition_id')::uuid,original->>'sha256',(original->>'size')::bigint,
        original->>'ref',original->>'receipt_sha256',encode(sha256(p),'hex'),
        doc->'model'->>'revision',doc->'model'->>'space',doc->'model'->>'tokenizer_sha256',logical_time);
    INSERT INTO rkb_v4.source_research_frontier VALUES (source,1,(research->>'frontier_id')::uuid,
        research->>'state',research->'continuation');
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
        INSERT INTO rkb_v4.passage_search(passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
        VALUES ((item->>'passage_id')::uuid,source,1,(item->>'ordinal')::integer,item->>'locator',item->>'body',
            item->>'language',CASE item->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
                WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (item->'embedding')::text::public.vector);
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'regions') LOOP
        INSERT INTO rkb_v4.source_regions VALUES(source,1,(item->>'region_id')::uuid,
            (item->>'page_id')::uuid,item->>'source_text',item->>'source_text_sha256');
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
        INSERT INTO rkb_v4.story_assertion_versions VALUES((item->>'assertion_id')::uuid,1,
            source,1,item->>'candidate_key',item->>'proposition',item);
    END LOOP;
    FOR story IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'stories') LOOP
        variant:=story->'default_variant';
        INSERT INTO rkb_v4.stories VALUES((story->>'story_id')::uuid,1,source,1,
            (variant->>'variant_id')::uuid,1);
        INSERT INTO rkb_v4.story_versions VALUES((story->>'story_id')::uuid,1,
            story-'default_variant',logical_time,encode(sha256(p),'hex'));
        INSERT INTO rkb_v4.story_variant_versions VALUES((variant->>'variant_id')::uuid,1,
            (story->>'story_id')::uuid,variant->>'body',
            encode(sha256(convert_to(variant->>'body','UTF8')),'hex'),variant-'embedding');
        INSERT INTO rkb_v4.story_search(story_id,variant_id,variant_revision,source_id,source_revision,body,config,embedding)
        VALUES((story->>'story_id')::uuid,(variant->>'variant_id')::uuid,1,source,1,variant->>'body',
            CASE variant->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
            WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (variant->'embedding')::text::public.vector);
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'pending') LOOP
        INSERT INTO rkb_v4.source_story_pending VALUES(source,item->>'candidate_key',item);
    END LOOP;
    -- Activation happens after every exact passage and its research continuation
    -- exist. All source/projection/receipt/frontier work commits or rolls back.
    PERFORM rkb_v4.project_source_graph_v1(source,doc->'source_graph',encode(sha256(p),'hex'));
    INSERT INTO rkb_v4.source_manifests VALUES(source,1,doc->'source_manifest',encode(sha256(p),'hex'),
        decode(doc->'source_manifest'->>'original_receipt_b64','base64'));
    INSERT INTO rkb_v4.active_revision VALUES (source,1);
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
    SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=source;
    PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
        frontier.visibility_epoch,state.applied_seq+1,
        prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
        prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
    RETURN convert_to(jsonb_build_object('source_id',source,'content_revision',1,'accepted',true,
        'withdrawn',false,'eligibility_revision',eligibility,'visibility_epoch',frontier.visibility_epoch,
        'passage_count',jsonb_array_length(doc->'passages'),'story_count',jsonb_array_length(doc->'semantic'->'stories'),
        'finding_count',jsonb_array_length(doc->'semantic'->'findings'),'story_refs',(SELECT coalesce(jsonb_agg(jsonb_build_object(
            'story_id',s->>'story_id','variant_id',s->'default_variant'->>'variant_id','variant_revision',1,
            'body_sha256',encode(sha256(convert_to(s->'default_variant'->>'body','UTF8')),'hex'))),'[]'::jsonb)
            FROM jsonb_array_elements(doc->'semantic'->'stories') s),'payload_sha256',encode(sha256(p),'hex'),
        'research_frontier_id',research->>'frontier_id','research_state',research->>'state',
        'logical_time',logical_time)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.capture_source_graph_receipt_v1() RETURNS trigger
 LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;doc jsonb;source uuid;g rkb_v4.source_graph_documents;BEGIN
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF NEW.status<>'applied' OR c->>'route'<>'source.import.v4' THEN RETURN NULL;END IF;
 doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;source:=(c->'ids'->>'source_id')::uuid;
 SELECT * INTO g FROM rkb_v4.source_graph_documents WHERE source_id=source;
 IF doc ? 'source_graph' THEN
  IF g.source_id IS NULL OR g.owner_person_id::text IS DISTINCT FROM c->>'person_id' OR g.data IS DISTINCT FROM doc->'source_graph'
   OR g.payload_sha256 IS DISTINCT FROM encode(sha256(decode(c->>'payload_b64','base64')),'hex') THEN
   RAISE EXCEPTION 'source graph accepted actor/payload differs';END IF;
  INSERT INTO rkb_v4.source_graph_receipts VALUES(source,NEW.operation_key,NEW.sequence,NEW.command_sha256,(c->>'person_id')::uuid,c->>'request_id');
 ELSIF g.source_id IS NOT NULL THEN RAISE EXCEPTION 'source graph not in accepted source import';END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_source_graph_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_source_graph_receipt_v1();

CREATE FUNCTION rkb_v4.verify_source_graph_origins_v1(scope uuid[]) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE g rkb_v4.source_graph_documents;c jsonb;doc jsonb;e jsonb;a jsonb;r jsonb;f jsonb;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 FOR g IN SELECT * FROM rkb_v4.source_graph_documents WHERE source_id=ANY(scope) LOOP
  SELECT convert_from(op.command_bytes,'UTF8')::jsonb INTO STRICT c FROM rkb_v4.source_graph_receipts receipt
   JOIN rkb_v4.applied_ops op USING(operation_key) WHERE receipt.source_id=g.source_id AND op.status='applied'
    AND receipt.sequence=op.sequence AND receipt.command_sha256=op.command_sha256
    AND receipt.person_id=g.owner_person_id;
  doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
  IF c->>'route' IS DISTINCT FROM 'source.import.v4' OR c->'ids'->>'source_id' IS DISTINCT FROM g.source_id::text
   OR c->>'person_id' IS DISTINCT FROM g.owner_person_id::text OR doc->'source_graph' IS DISTINCT FROM g.data
   OR g.payload_sha256 IS DISTINCT FROM encode(sha256(decode(c->>'payload_b64','base64')),'hex')
   OR (SELECT count(*) FROM rkb_v4.source_entity_representations WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'entities')
   OR (SELECT count(*) FROM rkb_v4.source_entity_references WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'references')
   OR (SELECT count(*) FROM rkb_v4.source_entity_mentions WHERE source_id=g.source_id)<>
     (SELECT count(DISTINCT value->'mention'->>'mention_id') FROM jsonb_array_elements(g.data->'entities'))
   OR (SELECT count(*) FROM rkb_v4.source_entity_aliases WHERE source_id=g.source_id)<>
     (SELECT count(DISTINCT alias_row.value->>'alias_id') FROM jsonb_array_elements(g.data->'entities') AS node_row(value)
       CROSS JOIN LATERAL jsonb_array_elements(node_row.value->'aliases') AS alias_row(value))
   OR (SELECT count(*) FROM rkb_v4.source_entity_relations WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'relations')
   OR (SELECT count(*) FROM rkb_v4.source_poi_hypotheses WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'poi_facts')
   OR (SELECT count(*) FROM rkb_v4.source_entity_finding_links WHERE source_id=g.source_id)<>
     (SELECT coalesce(sum(jsonb_array_length(value->'entity_keys')),0) FROM jsonb_array_elements(g.data->'finding_entities')) THEN RETURN false;END IF;
  FOR e IN SELECT value FROM jsonb_array_elements(g.data->'entities') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.source_entity_ids h USING(entity_id)
    WHERE x.source_id=g.source_id AND x.entity_key=e->>'key' AND x.entity_id=(e->>'entity_id')::uuid AND x.data=e
     AND h.owner_person_id=g.owner_person_id AND h.kind=e->>'kind')
    OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_mentions x WHERE x.source_id=g.source_id
     AND x.mention_id=(e->'mention'->>'mention_id')::uuid AND x.entity_id=(e->>'entity_id')::uuid AND x.data=e->'mention') THEN RETURN false;END IF;
   FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
    IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_aliases x WHERE x.source_id=g.source_id AND x.alias_id=(a->>'alias_id')::uuid
     AND x.entity_id=(e->>'entity_id')::uuid AND x.normalized_value=a->>'normalized_value'
     AND x.normalized_sha256=encode(sha256(convert_to(a->>'normalized_value','UTF8')),'hex') AND x.data=a) THEN RETURN false;END IF;
   END LOOP;
  END LOOP;
  FOR r IN SELECT value FROM jsonb_array_elements(g.data->'references') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_references x JOIN rkb_v4.source_entity_ids h USING(entity_id)
    WHERE x.source_id=g.source_id AND x.entity_key=r->>'key' AND x.entity_id=(r->>'entity_id')::uuid
     AND h.owner_person_id=g.owner_person_id AND x.data=r) THEN RETURN false;END IF;
  END LOOP;
  FOR r IN SELECT value FROM jsonb_array_elements(g.data->'relations') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_relations x WHERE x.source_id=g.source_id AND x.relation_id=(r->>'relation_id')::uuid
    AND x.from_id=(r->>'source_id')::uuid AND x.to_id=(r->>'target_id')::uuid AND x.kind=r->>'kind' AND x.data=r) THEN RETURN false;END IF;
  END LOOP;
  FOR f IN SELECT value FROM jsonb_array_elements(g.data->'poi_facts') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_poi_hypotheses x WHERE x.source_id=g.source_id AND x.candidate_id=(f->>'candidate_id')::uuid AND x.data=f) THEN RETURN false;END IF;
  END LOOP;
  FOR f IN SELECT value FROM jsonb_array_elements(g.data->'finding_entities') LOOP
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY k(name,n) WHERE NOT EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_finding_links x WHERE x.source_id=g.source_id AND x.assertion_id=(f->>'assertion_id')::uuid
     AND x.entity_key=k.name AND x.entity_id=(f->'entity_ids'->>(k.n::integer-1))::uuid)) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 -- Accepted sources containing a graph must have a receipt; bare rows cannot
 -- create an origin proof, including orphan canonical identities.
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.applied_ops op CROSS JOIN LATERAL (SELECT convert_from(op.command_bytes,'UTF8')::jsonb command_doc) x
   WHERE op.status='applied' AND x.command_doc->>'route'='source.import.v4' AND (x.command_doc->'ids'->>'source_id')::uuid=ANY(scope)
    AND convert_from(decode(x.command_doc->>'payload_b64','base64'),'UTF8')::jsonb ? 'source_graph'
    AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_graph_documents z WHERE z.source_id=(x.command_doc->'ids'->>'source_id')::uuid))
  AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE NOT EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_representations x WHERE x.entity_id=h.entity_id));
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.read_source_graph_v1(wanted_entity uuid,wanted_source uuid,action text,
 after_source uuid,after_key text,take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,record_id text,
 data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH candidates AS (
  SELECT n.source_id,n.entity_key record_id,n.data,n.data->'mention'->'evidence'->>'exact_quote' evidence_text
   FROM rkb_v4.source_entity_representations n
   WHERE action IN ('entity','list') AND ((action='entity' AND wanted_entity IS NOT NULL AND n.entity_id=wanted_entity)
    OR (action='list' AND wanted_source IS NOT NULL AND n.source_id=wanted_source))
  UNION ALL
  SELECT m.source_id,m.mention_id::text,m.data,m.data->'evidence'->>'exact_quote'
   FROM rkb_v4.source_entity_mentions m WHERE action='mentions' AND wanted_entity IS NOT NULL AND m.entity_id=wanted_entity
  UNION ALL
  SELECT a.source_id,a.alias_id::text,a.data,a.data->'evidence'->>'exact_quote'
   FROM rkb_v4.source_entity_aliases a WHERE action='aliases' AND wanted_entity IS NOT NULL AND a.entity_id=wanted_entity
  UNION ALL
  SELECT r.source_id,r.relation_id::text,r.data,r.data->'evidence'->0->>'exact_quote'
   FROM rkb_v4.source_entity_relations r WHERE action='relations' AND wanted_entity IS NOT NULL AND (r.from_id=wanted_entity OR r.to_id=wanted_entity)
  UNION ALL
  SELECT f.source_id,f.candidate_id::text,f.data,f.data->>'text'
   FROM rkb_v4.source_poi_hypotheses f WHERE action='poi_facts' AND wanted_source IS NOT NULL AND f.source_id=wanted_source
 ) SELECT c.source_id,1::bigint,v.eligibility_revision,c.record_id,c.data,c.evidence_text,s.bibliography
 FROM candidates c JOIN rkb_v4.sources s USING(source_id) JOIN rkb_v4.active_revision active USING(source_id)
 JOIN rkb_v4.source_visibility v USING(source_id) JOIN rkb_v4.source_graph_receipts receipt USING(source_id)
 WHERE s.accepted AND NOT s.withdrawn AND active.revision=1 AND (wanted_source IS NULL OR c.source_id=wanted_source)
  AND (after_source IS NULL OR c.source_id>after_source OR c.source_id=after_source AND c.record_id COLLATE "C">after_key COLLATE "C")
 ORDER BY c.source_id,c.record_id COLLATE "C" LIMIT least(greatest(take,1),4)
$$;

REVOKE ALL ON rkb_v4.source_graph_documents,rkb_v4.source_entity_ids,rkb_v4.source_entity_representations,
 rkb_v4.source_entity_references,rkb_v4.source_entity_mentions,rkb_v4.source_entity_aliases,rkb_v4.source_entity_relations,
 rkb_v4.source_entity_finding_links,rkb_v4.source_poi_hypotheses,rkb_v4.source_graph_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_graph_evidence_v1(jsonb,jsonb),rkb_v4.source_graph_endpoint_kind_v1(jsonb,uuid),
 rkb_v4.check_source_graph_v1(jsonb,uuid),rkb_v4.check_source_import_graph_v1(bytea,jsonb,jsonb,text),
 rkb_v4.project_source_graph_v1(uuid,jsonb,text),rkb_v4.apply_source_import_graph_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_source_graph_receipt_v1(),rkb_v4.verify_source_graph_origins_v1(uuid[]),
 rkb_v4.read_source_graph_v1(uuid,uuid,text,uuid,text,integer) FROM PUBLIC;
