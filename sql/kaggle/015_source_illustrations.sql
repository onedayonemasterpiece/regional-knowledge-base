-- Additive metadata-only public illustrations. SQL001-014 are unchanged.
-- Text activation never waits for Telegram availability. Only the trusted native
-- archive adapter may call the private CAS function after an exact locator check.
-- No provider checksum, download gate, transport, queue, URL or credential here.
CREATE TABLE rkb_v4.source_illustration_candidates (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_activation_candidates,
 knowledge_sha256 text NOT NULL, media_bytes bytea NOT NULL CHECK(octet_length(media_bytes) BETWEEN 1 AND 33554432),
 media_sha256 text NOT NULL CHECK(media_sha256=encode(sha256(media_bytes),'hex')),
 media jsonb NOT NULL CHECK(media=convert_from(media_bytes,'UTF8')::jsonb)
);
CREATE TABLE rkb_v4.source_illustrations (
 source_id uuid NOT NULL REFERENCES rkb_v4.sources, illustration_id uuid NOT NULL,
 data jsonb NOT NULL, PRIMARY KEY(source_id,illustration_id)
);
CREATE TABLE rkb_v4.source_illustration_origins (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
 media_sha256 text NOT NULL, knowledge_sha256 text NOT NULL,
 operation_key text NOT NULL REFERENCES rkb_v4.applied_ops DEFERRABLE INITIALLY DEFERRED,
 sequence bigint NOT NULL, command_sha256 text NOT NULL, person_id uuid NOT NULL
);
CREATE TABLE rkb_v4.source_illustration_availability (
 source_id uuid NOT NULL, illustration_id uuid NOT NULL, operation_key text NOT NULL UNIQUE,
 binding_bytes bytea NOT NULL CHECK(octet_length(binding_bytes) BETWEEN 1 AND 8192),
 archive_chat_id bigint NOT NULL, archive_topic_id integer NOT NULL CHECK(archive_topic_id=4),
 version bigint NOT NULL CHECK(version>=0),
 status text NOT NULL CHECK(status IN ('pending','unknown','registered','ready','unavailable','failed')),
 photo jsonb, document jsonb,
 PRIMARY KEY(source_id,illustration_id),
 FOREIGN KEY(source_id,illustration_id) REFERENCES rkb_v4.source_illustrations
);
CREATE TRIGGER immutable_source_illustration_candidate BEFORE UPDATE OR DELETE ON rkb_v4.source_illustration_candidates
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_illustration BEFORE UPDATE OR DELETE ON rkb_v4.source_illustrations
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_illustration_origin BEFORE UPDATE OR DELETE ON rkb_v4.source_illustration_origins
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

-- Reconstruct only accepted link metadata, not text or embeddings. A changed
-- page invalidates old dependent links; an identical reissue is enrichment.
CREATE FUNCTION rkb_v4.source_illustration_stage_links_v1(source uuid) RETURNS jsonb
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE batch record;model jsonb;page jsonb;old_pages jsonb:='{}';chunks jsonb:='{}';media jsonb:='{}';item jsonb;changed text[];entry record;
BEGIN
 FOR batch IN SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=source ORDER BY batch_index LOOP
  model:=convert_from(batch.payload_bytes,'UTF8')::jsonb->'model_payload';changed:='{}';
  FOR page IN SELECT value FROM jsonb_array_elements(model->'pages') LOOP
   IF old_pages->(page->>'page_id') IS DISTINCT FROM page THEN changed:=array_append(changed,page->>'page_id');END IF;
   old_pages:=old_pages||jsonb_build_object(page->>'page_id',page);
  END LOOP;
  FOR entry IN SELECT key,value FROM jsonb_each(chunks) LOOP
   IF EXISTS(SELECT 1 FROM jsonb_array_elements(coalesce(entry.value->'region_refs','[]')||coalesce(entry.value->'footnote_refs','[]')||
       coalesce(entry.value->'span_refs','[]')||coalesce(entry.value->'illustration_refs','[]')) r WHERE r->>'page_id'=ANY(changed)) THEN chunks:=chunks-entry.key;END IF;
  END LOOP;
  FOR entry IN SELECT key,value FROM jsonb_each(media) LOOP
   IF entry.value->'illustration_ref'->>'page_id'=ANY(changed) THEN media:=media-entry.key;END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(model->'chunks') LOOP chunks:=chunks||jsonb_build_object(item->>'chunk_key',item);END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(coalesce(model->'poi_media_links','[]')) LOOP media:=media||jsonb_build_object(item->>'link_key',item);END LOOP;
 END LOOP;
 RETURN jsonb_build_object('chunks',chunks,'poi_media_links',media);
END $$;

CREATE FUNCTION rkb_v4.check_source_illustrations_v1(doc jsonb,media jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE item jsonb;page jsonb;region jsonb;proof jsonb;ref jsonb;link jsonb;caption text;
 ids uuid[]:='{}';links uuid[]:='{}';wanted uuid;raw_page jsonb;raw_figure jsonb;illustration_count integer:=0;
 staged jsonb;raw_link jsonb;expected_refs jsonb;entry record;
BEGIN
 IF media IS NULL OR jsonb_typeof(media) IS DISTINCT FROM 'object'
  OR NOT media ?& ARRAY['codec','source_id','source_revision','illustrations','passage_links','poi_media_links']
  OR media-ARRAY['codec','source_id','source_revision','illustrations','passage_links','poi_media_links']<>'{}'
  OR media->>'codec' IS DISTINCT FROM 'rkb.source-illustrations.v1'
  OR media->>'source_id' IS DISTINCT FROM source::text OR media->'source_revision' IS DISTINCT FROM '1'::jsonb
  OR jsonb_typeof(media->'illustrations') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'illustrations')>200*jsonb_array_length(doc->'image_manifest'->'pages')
  OR jsonb_typeof(media->'passage_links') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'passage_links')>jsonb_array_length(doc->'passages')
  OR jsonb_typeof(media->'poi_media_links') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'poi_media_links')>10000 THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(media->'illustrations') LOOP
  IF NOT item ?& ARRAY['illustration_key','illustration_id','page_id','source_region_id','bbox','kind','caption_region_ids','nearby_region_ids','visual_description','visual_description_provenance','visual_description_language','display_rotation_degrees','caption_text','crop_binding']
   OR item-ARRAY['illustration_key','illustration_id','page_id','source_region_id','bbox','kind','caption_region_ids','nearby_region_ids','visual_description','visual_description_provenance','visual_description_language','display_rotation_degrees','caption_text','crop_binding']<>'{}'
   OR item->>'visual_description_provenance' IS DISTINCT FROM 'model_observation'
   OR item->>'display_rotation_degrees' NOT IN ('0','90','180','270')
   OR jsonb_typeof(item->'caption_region_ids') IS DISTINCT FROM 'array'
   OR jsonb_array_length(item->'caption_region_ids')>50
   OR jsonb_typeof(item->'nearby_region_ids') IS DISTINCT FROM 'array'
   OR jsonb_array_length(item->'nearby_region_ids')>100 THEN RETURN false;END IF;
  wanted:=(item->>'illustration_id')::uuid;
  IF wanted IS NULL OR wanted::text IS DISTINCT FROM item->>'illustration_id' OR wanted=ANY(ids) THEN RETURN false;END IF;
  ids:=array_append(ids,wanted);
  SELECT value INTO STRICT page FROM jsonb_array_elements(doc->'image_manifest'->'pages') WHERE value->>'page_id'=item->>'page_id';
  SELECT value INTO STRICT region FROM jsonb_array_elements(doc->'image_manifest'->'regions') WHERE value->>'region_id'=item->>'source_region_id';
  IF region->>'page_id' IS DISTINCT FROM item->>'page_id' OR region->>'kind' IS DISTINCT FROM 'figure'
   OR region->'bbox' IS DISTINCT FROM item->'bbox' THEN RETURN false;END IF;
  raw_page:=rkb_v4.source_stage_latest_page_v1(source,(item->>'page_id')::uuid);
  SELECT value INTO STRICT raw_figure FROM jsonb_array_elements(raw_page->'illustrations') WHERE value->>'illustration_key'=item->>'illustration_key';
  IF raw_figure->>'source_region_key' IS DISTINCT FROM region->>'region_key'
   OR raw_figure->>'kind' IS DISTINCT FROM item->>'kind'
   OR coalesce(raw_figure->'visual_description','null') IS DISTINCT FROM item->'visual_description'
   OR coalesce(raw_figure->'visual_description_language','null') IS DISTINCT FROM item->'visual_description_language'
   OR coalesce(raw_figure->>'visual_description_provenance','model_observation') IS DISTINCT FROM item->>'visual_description_provenance'
   OR coalesce(raw_figure->'display_rotation_degrees','0') IS DISTINCT FROM item->'display_rotation_degrees' THEN RETURN false;END IF;
  IF (SELECT coalesce(jsonb_agg(r.value->'region_key' ORDER BY a.n),'[]') FROM jsonb_array_elements(item->'caption_region_ids') WITH ORDINALITY a(v,n)
      JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'region_id'=a.v)
      IS DISTINCT FROM coalesce(raw_figure->'caption_region_keys','[]')
   OR (SELECT coalesce(jsonb_agg(r.value->'region_key' ORDER BY a.n),'[]') FROM jsonb_array_elements(item->'nearby_region_ids') WITH ORDINALITY a(v,n)
      JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'region_id'=a.v)
      IS DISTINCT FROM coalesce(raw_figure->'nearby_region_keys','[]') THEN RETURN false;END IF;
  caption:='';
  FOR ref IN SELECT value FROM jsonb_array_elements(item->'caption_region_ids') LOOP
   SELECT value INTO STRICT region FROM jsonb_array_elements(doc->'image_manifest'->'regions') WHERE value->'region_id'=ref;
   IF region->>'page_id' IS DISTINCT FROM item->>'page_id' OR region->>'kind' IS DISTINCT FROM 'caption' THEN RETURN false;END IF;
   SELECT value INTO STRICT proof FROM jsonb_array_elements(doc->'semantic'->'regions') WHERE value->'region_id'=ref;
   caption:=caption||CASE WHEN caption='' THEN '' ELSE E'\n' END||(proof->>'source_text');
  END LOOP;
  IF item->>'caption_text' IS DISTINCT FROM caption THEN RETURN false;END IF;
  FOR ref IN SELECT value FROM jsonb_array_elements(item->'nearby_region_ids') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'image_manifest'->'regions') r WHERE r->'region_id'=ref AND r->>'page_id'=item->>'page_id') THEN RETURN false;END IF;
  END LOOP;
  IF item->'crop_binding' IS DISTINCT FROM jsonb_build_object('source_id',source::text,'source_revision',1,
   'illustration_id',item->'illustration_id','page_id',item->'page_id','source_region_id',item->'source_region_id',
   'bbox',item->'bbox','display_rotation_degrees',item->'display_rotation_degrees',
   'source_image_receipt_sha256',page->'source_image_receipt_sha256','source_raster_sha256',page->'image_sha256',
   'original_sha256',doc->'original'->'sha256','rendition','preview') THEN RETURN false;END IF;
 END LOOP;
 -- Every latest-page illustration remains present even when no native archive exists.
 FOR page IN SELECT value FROM jsonb_array_elements(doc->'image_manifest'->'pages') LOOP
  raw_page:=rkb_v4.source_stage_latest_page_v1(source,(page->>'page_id')::uuid);
  illustration_count:=illustration_count+jsonb_array_length(coalesce(raw_page->'illustrations','[]'));
  FOR raw_figure IN SELECT value FROM jsonb_array_elements(coalesce(raw_page->'illustrations','[]')) LOOP
   IF (SELECT count(*) FROM jsonb_array_elements(media->'illustrations') i WHERE i->'page_id'=page->'page_id' AND i->'illustration_key'=raw_figure->'illustration_key')<>1 THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF illustration_count<>cardinality(ids) THEN RETURN false;END IF;
 staged:=rkb_v4.source_illustration_stage_links_v1(source);
 IF jsonb_array_length(media->'passage_links')<>(SELECT count(*) FROM jsonb_each(staged->'chunks') c WHERE jsonb_array_length(coalesce(c.value->'illustration_refs','[]'))>0)
  OR jsonb_array_length(media->'poi_media_links')<>(SELECT count(*) FROM jsonb_each(staged->'poi_media_links')) THEN RETURN false;END IF;
 FOR link IN SELECT value FROM jsonb_array_elements(media->'passage_links') LOOP
  IF link-ARRAY['passage_id','chunk_key','illustration_ids']<>'{}' OR NOT link ?& ARRAY['passage_id','chunk_key','illustration_ids']
   OR jsonb_typeof(link->'illustration_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(link->'illustration_ids') NOT BETWEEN 1 AND 30
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'passages') p WHERE p->'passage_id'=link->'passage_id') THEN RETURN false;END IF;
  raw_link:=staged->'chunks'->(link->>'chunk_key');
  IF raw_link IS NULL THEN RETURN false;END IF;
  SELECT coalesce(jsonb_agg(i.value->'illustration_id' ORDER BY r.n),'[]') INTO expected_refs
   FROM jsonb_array_elements(coalesce(raw_link->'illustration_refs','[]')) WITH ORDINALITY r(value,n)
   JOIN LATERAL jsonb_array_elements(media->'illustrations') i(value) ON i.value->'page_id'=r.value->'page_id' AND i.value->'illustration_key'=r.value->'illustration_key';
  IF expected_refs IS DISTINCT FROM link->'illustration_ids'
   OR (SELECT count(*) FROM jsonb_array_elements(media->'passage_links') l WHERE l->'chunk_key'=link->'chunk_key')<>1 THEN RETURN false;END IF;
  SELECT value INTO STRICT proof FROM jsonb_array_elements(doc->'image_manifest'->'passages') WHERE value->'passage_id'=link->'passage_id';
  SELECT coalesce(jsonb_agg(refs.v ORDER BY refs.n),'[]') INTO expected_refs FROM (
   SELECT r.value->'region_id' v,min(a.n) n FROM jsonb_array_elements(coalesce(raw_link->'region_refs','[]')||coalesce(raw_link->'span_refs','[]')) WITH ORDINALITY a(value,n)
   JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'page_id'=a.value->'page_id' AND r.value->'region_key'=a.value->'region_key'
   GROUP BY r.value->'region_id') refs;
  IF expected_refs IS DISTINCT FROM proof->'region_ids' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_agg(refs.v ORDER BY refs.n),'[]') INTO expected_refs FROM (
   SELECT r.value->'region_id' v,min(a.n) n FROM jsonb_array_elements(coalesce(raw_link->'footnote_refs','[]')) WITH ORDINALITY a(value,n)
   JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'page_id'=a.value->'page_id' AND r.value->'region_key'=a.value->'region_key'
   GROUP BY r.value->'region_id') refs;
  IF expected_refs IS DISTINCT FROM proof->'footnote_region_ids' THEN RETURN false;END IF;
  wanted:=(link->>'passage_id')::uuid;IF wanted=ANY(links) THEN RETURN false;END IF;links:=array_append(links,wanted);
  IF (SELECT count(DISTINCT value) FROM jsonb_array_elements(link->'illustration_ids'))<>jsonb_array_length(link->'illustration_ids') THEN RETURN false;END IF;
  FOR ref IN SELECT value FROM jsonb_array_elements(link->'illustration_ids') LOOP
   IF NOT ((ref#>>'{}')::uuid=ANY(ids)) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 links:='{}';
 FOR link IN SELECT value FROM jsonb_array_elements(media->'poi_media_links') LOOP
  IF link-ARRAY['link_key','link_id','poi_locator','illustration_id','page_id','relation','time_scope']<>'{}'
   OR NOT link ?& ARRAY['link_key','link_id','poi_locator','illustration_id','page_id','relation','time_scope']
   OR link->>'relation' NOT IN ('depicts','illustrates','map_of','detail_of')
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(media->'illustrations') i WHERE i->'illustration_id'=link->'illustration_id' AND i->'page_id'=link->'page_id') THEN RETURN false;END IF;
  raw_link:=staged->'poi_media_links'->(link->>'link_key');
  IF raw_link IS NULL OR raw_link->'poi_locator' IS DISTINCT FROM link->'poi_locator'
   OR raw_link->'relation' IS DISTINCT FROM link->'relation' OR coalesce(raw_link->'time_scope','null') IS DISTINCT FROM link->'time_scope'
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(media->'illustrations') i WHERE i->'illustration_id'=link->'illustration_id'
      AND i->'page_id'=raw_link->'illustration_ref'->'page_id' AND i->'illustration_key'=raw_link->'illustration_ref'->'illustration_key')
   OR (SELECT count(*) FROM jsonb_array_elements(media->'poi_media_links') l WHERE l->'link_key'=link->'link_key')<>1 THEN RETURN false;END IF;
  wanted:=(link->>'link_id')::uuid;IF wanted IS NULL OR wanted=ANY(links) THEN RETURN false;END IF;links:=array_append(links,wanted);
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.prepare_source_activation_with_illustrations_v1(source uuid,ingestion uuid,batches integer,tail text,
 materializer text,knowledge_bytes bytea,expected_database text,expected_writer text,media_bytes bytea) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE h text;media jsonb;prior rkb_v4.source_illustration_candidates;
BEGIN
 IF octet_length(media_bytes) NOT BETWEEN 1 AND 33554432 THEN RAISE EXCEPTION 'bounded illustration metadata required';END IF;
 h:=rkb_v4.prepare_source_activation_v1(source,ingestion,batches,tail,materializer,knowledge_bytes,expected_database,expected_writer);
 media:=convert_from(media_bytes,'UTF8')::jsonb;
 IF NOT rkb_v4.check_source_illustrations_v1(convert_from(knowledge_bytes,'UTF8')::jsonb,media,source) THEN RAISE EXCEPTION 'illustration provenance differs';END IF;
 SELECT * INTO prior FROM rkb_v4.source_illustration_candidates WHERE source_id=source;
 IF FOUND THEN
  IF prior.knowledge_sha256<>h OR prior.media_bytes<>media_bytes THEN RAISE EXCEPTION 'immutable illustration candidate changed';END IF;
 ELSE INSERT INTO rkb_v4.source_illustration_candidates VALUES(source,h,media_bytes,encode(sha256(media_bytes),'hex'),media);END IF;
 RETURN h;
END $$;
CREATE FUNCTION rkb_v4.check_source_activate_with_illustrations_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 RETURN rkb_v4.check_source_activate_v1(p,ids,expected,logical_time) AND EXISTS(
  SELECT 1 FROM rkb_v4.source_illustration_candidates m JOIN rkb_v4.source_activation_candidates k USING(source_id)
  WHERE m.source_id=(ids->>'source_id')::uuid AND m.knowledge_sha256=k.knowledge_sha256
   AND m.media_sha256=encode(sha256(m.media_bytes),'hex') AND rkb_v4.check_source_illustrations_v1(k.knowledge,m.media,m.source_id));
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_activate_with_illustrations_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS bytea
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE result bytea;item jsonb;source uuid:=(ids->>'source_id')::uuid;
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_activate_with_illustrations_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'exact illustration candidate required';END IF;
 result:=rkb_v4.apply_source_activate_v1(p,ids,expected,logical_time);
 FOR item IN SELECT value FROM rkb_v4.source_illustration_candidates m CROSS JOIN LATERAL jsonb_array_elements(m.media->'illustrations') WHERE m.source_id=source LOOP
  INSERT INTO rkb_v4.source_illustrations VALUES(source,(item->>'illustration_id')::uuid,item);
 END LOOP;
 RETURN result;
END $$;
CREATE FUNCTION rkb_v4.capture_source_illustration_receipt_v1() RETURNS trigger
 LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;m rkb_v4.source_illustration_candidates;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route'<>'source.activate.v1' THEN RETURN NULL;END IF;
 SELECT * INTO m FROM rkb_v4.source_illustration_candidates WHERE source_id=(c->'ids'->>'source_id')::uuid;
 IF NOT FOUND THEN RETURN NULL;END IF;
 IF c->>'schema_revision' IS DISTINCT FROM 'rkb-v4-sql001-015-v1'
  OR c->>'function_revision' IS DISTINCT FROM 'rkb-v4-native-source-illustrations-v1'
  OR c->>'executor_revision' IS DISTINCT FROM 'rkb-python-apply-native-illustrations-v1' THEN RAISE EXCEPTION 'illustration activation revision differs';END IF;
 IF (SELECT count(*) FROM rkb_v4.source_illustrations WHERE source_id=m.source_id)<>jsonb_array_length(m.media->'illustrations')
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(m.media->'illustrations') i WHERE NOT EXISTS(
   SELECT 1 FROM rkb_v4.source_illustrations x WHERE x.source_id=m.source_id AND x.data=i)) THEN RAISE EXCEPTION 'illustration projection missing';END IF;
 INSERT INTO rkb_v4.source_illustration_origins VALUES(m.source_id,m.media_sha256,m.knowledge_sha256,NEW.operation_key,NEW.sequence,NEW.command_sha256,(c->>'person_id')::uuid);
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER capture_source_illustration_receipt AFTER INSERT ON rkb_v4.applied_ops
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_source_illustration_receipt_v1();

CREATE FUNCTION rkb_v4.verify_source_illustration_origins_v1(scope uuid[]) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m rkb_v4.source_illustration_candidates;o rkb_v4.source_illustration_origins;c jsonb;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 FOR m IN SELECT x.* FROM rkb_v4.source_illustration_candidates x JOIN rkb_v4.sources s USING(source_id) WHERE x.source_id=ANY(scope) LOOP
  SELECT * INTO STRICT o FROM rkb_v4.source_illustration_origins WHERE source_id=m.source_id;
  SELECT convert_from(command_bytes,'UTF8')::jsonb INTO STRICT c FROM rkb_v4.applied_ops
   WHERE operation_key=o.operation_key AND sequence=o.sequence AND status='applied'
    AND command_sha256=o.command_sha256 AND command_sha256=encode(sha256(command_bytes),'hex');
  IF c->>'route' IS DISTINCT FROM 'source.activate.v1' OR c->'ids'->>'source_id' IS DISTINCT FROM m.source_id::text
   OR c->>'person_id' IS DISTINCT FROM o.person_id::text OR o.media_sha256<>m.media_sha256
   OR m.media_sha256<>encode(sha256(m.media_bytes),'hex') OR o.knowledge_sha256<>m.knowledge_sha256
   OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_image_origins b WHERE b.source_id=m.source_id AND b.operation_key=o.operation_key
      AND b.command_sha256=o.command_sha256 AND b.knowledge_sha256=m.knowledge_sha256)
   OR (SELECT count(*) FROM rkb_v4.source_illustrations WHERE source_id=m.source_id)<>jsonb_array_length(m.media->'illustrations')
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(m.media->'illustrations') i WHERE NOT EXISTS(
      SELECT 1 FROM rkb_v4.source_illustrations x WHERE x.source_id=m.source_id AND x.data=i)) THEN RETURN false;END IF;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.source_illustrations x LEFT JOIN rkb_v4.source_illustration_origins origin_row USING(source_id)
  WHERE x.source_id=ANY(scope) AND origin_row.source_id IS NULL);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

-- This is a private receipt PROJECTION for the EXISTING native Telegram operation.
-- Durable UNKNOWN must already exist in the retained external journal authority;
-- these PG rows alone do not establish crash/restore or Dataset durability.
-- Binding bytes must be the Python canonical crop binding. SQL verifies their
-- parsed identity and byte hash; the adapter, not model JSON, supplies the state.
CREATE FUNCTION rkb_v4.cas_source_illustration_availability_v1(source uuid,illustration uuid,operation text,
 binding bytea,chat bigint,topic integer,expected_version bigint,new_status text,new_photo jsonb,new_document jsonb,
 expected_database text,expected_writer text) RETURNS bigint
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE old rkb_v4.source_illustration_availability;item jsonb;locator jsonb;kind text;state rkb_v4.recovery_state;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation IS DISTINCT FROM expected_database OR state.writer_generation IS DISTINCT FROM expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired)
  OR topic IS DISTINCT FROM 4 OR chat IS NULL OR chat>=0 OR expected_version IS NULL
  OR new_status IS NULL OR new_status NOT IN ('pending','unknown','registered','ready','unavailable','failed')
  OR binding IS NULL OR octet_length(binding) NOT BETWEEN 1 AND 8192
  OR operation IS DISTINCT FROM 'illustration:'||encode(sha256(binding),'hex') THEN RAISE EXCEPTION 'exact native illustration operation required';END IF;
 SELECT x.data INTO STRICT item FROM rkb_v4.source_illustrations x JOIN rkb_v4.sources s USING(source_id)
  JOIN rkb_v4.active_revision a USING(source_id) JOIN rkb_v4.source_illustration_origins o USING(source_id)
  WHERE x.source_id=source AND x.illustration_id=illustration AND s.accepted AND NOT s.withdrawn AND a.revision=1;
 IF convert_from(binding,'UTF8')::jsonb IS DISTINCT FROM item->'crop_binding' THEN RAISE EXCEPTION 'native crop binding changed';END IF;
 FOREACH kind IN ARRAY ARRAY['photo','document'] LOOP
  locator:=CASE kind WHEN 'photo' THEN new_photo ELSE new_document END;
  IF locator IS NULL THEN CONTINUE;END IF;
  IF jsonb_typeof(locator) IS DISTINCT FROM 'object'
   OR NOT locator ?& ARRAY['chat_id','topic_id','message_id','media_id','kind','width','height']
   OR locator-ARRAY['chat_id','topic_id','message_id','media_id','kind','width','height']<>'{}'
   OR locator->'chat_id' IS DISTINCT FROM to_jsonb(chat) OR locator->'topic_id' IS DISTINCT FROM to_jsonb(topic)
   OR locator->>'kind' IS DISTINCT FROM kind
   OR jsonb_typeof(locator->'message_id') IS DISTINCT FROM 'number' OR jsonb_typeof(locator->'media_id') IS DISTINCT FROM 'number'
   OR jsonb_typeof(locator->'width') IS DISTINCT FROM 'number' OR jsonb_typeof(locator->'height') IS DISTINCT FROM 'number'
   OR locator->>'message_id' !~ '^[1-9][0-9]*$'
   OR (locator->>'message_id')::bigint NOT BETWEEN 1 AND 2147483647
   OR locator->>'media_id' !~ '^-?[1-9][0-9]*$' OR (locator->>'media_id')::bigint=0
   OR locator->>'width' !~ '^[1-9][0-9]*$' OR (locator->>'width')::integer NOT BETWEEN 1 AND 20000
   OR locator->>'height' !~ '^[1-9][0-9]*$' OR (locator->>'height')::integer NOT BETWEEN 1 AND 20000
  THEN RAISE EXCEPTION 'exact registered native locator required';END IF;
 END LOOP;
 IF new_status IN ('registered','ready') AND new_photo IS NULL AND new_document IS NULL THEN RAISE EXCEPTION 'native locator required';END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(source::text||':'||illustration::text,0));
 SELECT * INTO old FROM rkb_v4.source_illustration_availability WHERE source_id=source AND illustration_id=illustration FOR UPDATE;
 IF NOT FOUND THEN
  IF expected_version<>-1 OR new_status<>'pending' OR new_photo IS NOT NULL OR new_document IS NOT NULL THEN RAISE EXCEPTION 'initial pending native operation required';END IF;
  INSERT INTO rkb_v4.source_illustration_availability VALUES(source,illustration,operation,binding,chat,topic,0,'pending',NULL,NULL);
  RETURN 0;
 END IF;
 IF old.operation_key<>operation OR old.binding_bytes<>binding OR old.archive_chat_id<>chat OR old.archive_topic_id<>topic THEN RAISE EXCEPTION 'immutable native operation changed';END IF;
 IF old.status=new_status AND old.photo IS NOT DISTINCT FROM new_photo AND old.document IS NOT DISTINCT FROM new_document
  AND expected_version IN (old.version,old.version-1) THEN RETURN old.version;END IF;
 IF old.version<>expected_version THEN RAISE EXCEPTION 'native availability CAS changed';END IF;
 -- Never erase a registered locator or silently replace it during ambiguity.
 IF old.photo IS NOT NULL AND old.photo IS DISTINCT FROM new_photo
  OR old.document IS NOT NULL AND old.document IS DISTINCT FROM new_document
  OR new_status='pending' THEN RAISE EXCEPTION 'registered native locator changed';END IF;
 UPDATE rkb_v4.source_illustration_availability SET version=old.version+1,status=new_status,photo=new_photo,document=new_document
  WHERE source_id=source AND illustration_id=illustration;
 RETURN old.version+1;
END $$;

CREATE FUNCTION rkb_v4.read_source_illustrations_v1(wanted_source uuid,after_id uuid,take integer)
 RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,illustration_id uuid,
 data jsonb,passage_links jsonb,poi_media_links jsonb,archive_status text)
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT i.source_id,1::bigint,v.eligibility_revision,i.illustration_id,
  i.data-'crop_binding',
  (SELECT coalesce(jsonb_agg(l),'[]') FROM jsonb_array_elements(m.media->'passage_links') l WHERE l->'illustration_ids' @> jsonb_build_array(i.illustration_id::text)),
  (SELECT coalesce(jsonb_agg(l),'[]') FROM jsonb_array_elements(m.media->'poi_media_links') l WHERE l->>'illustration_id'=i.illustration_id::text),
  coalesce(a.status,'pending')
 FROM rkb_v4.source_illustrations i JOIN rkb_v4.sources s USING(source_id)
 JOIN rkb_v4.active_revision r USING(source_id) JOIN rkb_v4.source_visibility v USING(source_id)
 JOIN rkb_v4.source_illustration_candidates m USING(source_id) JOIN rkb_v4.source_illustration_origins o USING(source_id)
 LEFT JOIN rkb_v4.source_illustration_availability a USING(source_id,illustration_id)
 WHERE i.source_id=wanted_source AND s.accepted AND NOT s.withdrawn AND r.revision=1
  AND m.media_sha256=o.media_sha256 AND m.knowledge_sha256=o.knowledge_sha256
  AND (after_id IS NULL OR i.illustration_id>after_id)
 ORDER BY i.illustration_id LIMIT least(greatest(take,1),4)
$$;
REVOKE ALL ON rkb_v4.source_illustration_candidates,rkb_v4.source_illustrations,
 rkb_v4.source_illustration_origins,rkb_v4.source_illustration_availability FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.source_illustration_stage_links_v1(uuid),rkb_v4.check_source_illustrations_v1(jsonb,jsonb,uuid),
 rkb_v4.prepare_source_activation_with_illustrations_v1(uuid,uuid,integer,text,text,bytea,text,text,bytea),
 rkb_v4.check_source_activate_with_illustrations_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_activate_with_illustrations_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_source_illustration_receipt_v1(),rkb_v4.verify_source_illustration_origins_v1(uuid[]),
 rkb_v4.cas_source_illustration_availability_v1(uuid,uuid,text,bytea,bigint,integer,bigint,text,jsonb,jsonb,text,text),
 rkb_v4.read_source_illustrations_v1(uuid,uuid,integer) FROM PUBLIC;
