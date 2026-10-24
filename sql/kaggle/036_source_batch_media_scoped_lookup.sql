-- SQL036: retain accepted page-correction semantics while reconstructing only
-- the packet's requested link keys. Historical SQL015/033 and command bytes
-- stay unchanged; no projection cache, provider work or materializer change.
CREATE FUNCTION rkb_v4.source_batch_media_stage_links_v1(
 source uuid,wanted_chunks text[],wanted_media text[]) RETURNS jsonb
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE batch record;model jsonb;page jsonb;old_pages jsonb:='{}';
 chunks jsonb:='{}';media jsonb:='{}';item jsonb;changed text[];entry record;
BEGIN
 -- Each accepted payload is decoded once. Keep exact page equality, including
 -- A -> B -> A corrections; comparing only final page values resurrects links.
 FOR batch IN SELECT payload_bytes FROM rkb_v4.source_stage_batches
  WHERE source_id=source ORDER BY batch_index LOOP
  model:=convert_from(batch.payload_bytes,'UTF8')::jsonb->'model_payload';changed:='{}';
  FOR page IN SELECT value FROM jsonb_array_elements(model->'pages') LOOP
   IF old_pages->(page->>'page_id') IS DISTINCT FROM page THEN
    changed:=array_append(changed,page->>'page_id');
   END IF;
   old_pages:=old_pages||jsonb_build_object(page->>'page_id',page);
  END LOOP;
  FOR entry IN SELECT key,value FROM jsonb_each(chunks) LOOP
   IF EXISTS(SELECT 1 FROM jsonb_array_elements(
       coalesce(entry.value->'region_refs','[]')||coalesce(entry.value->'footnote_refs','[]')||
       coalesce(entry.value->'span_refs','[]')||coalesce(entry.value->'illustration_refs','[]')) r
       WHERE r->>'page_id'=ANY(changed)) THEN chunks:=chunks-entry.key;END IF;
  END LOOP;
  FOR entry IN SELECT key,value FROM jsonb_each(media) LOOP
   IF entry.value->'illustration_ref'->>'page_id'=ANY(changed) THEN media:=media-entry.key;END IF;
  END LOOP;
  -- Invalidation precedes insertion, so a corrected same-batch replacement
  -- survives. Unrequested historical chunks/links never enter either map.
  FOR item IN SELECT value FROM jsonb_array_elements(model->'chunks')
   WHERE value->>'chunk_key'=ANY(wanted_chunks) LOOP
   chunks:=chunks||jsonb_build_object(item->>'chunk_key',item);
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(coalesce(model->'poi_media_links','[]'))
   WHERE value->>'link_key'=ANY(wanted_media) LOOP
   media:=media||jsonb_build_object(item->>'link_key',item);
  END LOOP;
 END LOOP;
 RETURN jsonb_build_object('chunks',chunks,'poi_media_links',media);
END $$;
REVOKE ALL ON FUNCTION rkb_v4.source_batch_media_stage_links_v1(uuid,text[],text[]) FROM PUBLIC;

CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_media_v1(doc jsonb,media jsonb,source uuid) RETURNS boolean
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
  raw_page:=rkb_v4.source_batch_page_v1(source,(item->>'page_id')::uuid);
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
  raw_page:=rkb_v4.source_batch_page_v1(source,(page->>'page_id')::uuid);
  illustration_count:=illustration_count+jsonb_array_length(coalesce(raw_page->'illustrations','[]'));
  FOR raw_figure IN SELECT value FROM jsonb_array_elements(coalesce(raw_page->'illustrations','[]')) LOOP
   IF (SELECT count(*) FROM jsonb_array_elements(media->'illustrations') i WHERE i->'page_id'=page->'page_id' AND i->'illustration_key'=raw_figure->'illustration_key')<>1 THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF illustration_count<>cardinality(ids) THEN RETURN false;END IF;
 -- With no submitted links both correlated cardinality checks below are zero
 -- and both link loops are empty. Keep every page/figure/crop check above;
 -- only reconstruct accepted link history when a link actually needs it.
 IF media->'passage_links'='[]'::jsonb AND media->'poi_media_links'='[]'::jsonb THEN
  staged:='{"chunks":{},"poi_media_links":{}}'::jsonb;
 ELSE
  staged:=rkb_v4.source_batch_media_stage_links_v1(source,
   ARRAY(SELECT value->>'chunk_key' FROM jsonb_array_elements(media->'passage_links')),
   ARRAY(SELECT value->>'link_key' FROM jsonb_array_elements(media->'poi_media_links')));
 END IF;
 IF jsonb_array_length(media->'passage_links')<>(SELECT count(*) FROM jsonb_each(staged->'chunks') c WHERE jsonb_array_length(coalesce(c.value->'illustration_refs','[]'))>0
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(media->'passage_links') l WHERE l->>'chunk_key'=c.key))
  OR jsonb_array_length(media->'poi_media_links')<>(SELECT count(*) FROM jsonb_each(staged->'poi_media_links') l WHERE EXISTS(
    SELECT 1 FROM jsonb_array_elements(media->'poi_media_links') p WHERE p->>'link_key'=l.key)) THEN RETURN false;END IF;
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
