-- Keep accepted batch parsing linear while checking the exact image manifest.
CREATE OR REPLACE FUNCTION rkb_v4.check_source_image_manifest_v1(doc jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'image_manifest'; session rkb_v4.source_stage_sessions;
 page jsonb; region jsonb; native rkb_v4.source_stage_pages; issued jsonb; raw_region jsonb;
 actual jsonb; original jsonb; model_page jsonb; latest_pages jsonb; idx integer:=0; seen uuid[]:='{}'; ident uuid; k text;
BEGIN
 SELECT * INTO STRICT session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 original:=convert_from(session.original_receipt,'UTF8')::jsonb;
 -- Decode each accepted batch once, preserving the latest explicit page correction.
 SELECT coalesce(jsonb_object_agg(selected.page_id,selected.model_page),'{}'::jsonb) INTO latest_pages
 FROM (SELECT DISTINCT ON (model.value->>'page_id') model.value->>'page_id' AS page_id,model.value AS model_page
       FROM rkb_v4.source_stage_batches batch_row CROSS JOIN LATERAL
        jsonb_array_elements(convert_from(batch_row.payload_bytes,'UTF8')::jsonb->'model_payload'->'pages') model(value)
       WHERE batch_row.source_id=source
       ORDER BY model.value->>'page_id',batch_row.batch_index DESC) selected;
 IF jsonb_typeof(m) IS DISTINCT FROM 'object'
  OR NOT m ?& ARRAY['codec','source_id','source_revision','original_receipt_b64','metadata','pages','regions','passages']
  OR m-ARRAY['codec','source_id','source_revision','original_receipt_b64','metadata','pages','regions','passages']<>'{}'
  OR m->>'codec' IS DISTINCT FROM 'rkb.source-image-manifest.v1' OR m->>'source_id' IS DISTINCT FROM source::text
  OR m->'source_revision' IS DISTINCT FROM '1'::jsonb
  OR decode(m->>'original_receipt_b64','base64') IS DISTINCT FROM session.original_receipt
  OR doc->'original' IS DISTINCT FROM jsonb_build_object('sha256',original->>'sha256','size',original->'size',
   'ref','archive:'||(original->>'key'),'receipt_sha256',encode(sha256(session.original_receipt),'hex'))
  OR doc->>'bibliography' IS DISTINCT FROM session.source_metadata->>'bibliography'
  OR doc->>'work_id' IS DISTINCT FROM session.source_metadata->>'work_id'
  OR doc->>'edition_id' IS DISTINCT FROM session.source_metadata->>'edition_id'
  OR doc->'public_rights' IS DISTINCT FROM session.source_metadata->'public_rights'
  OR m->'metadata' IS DISTINCT FROM (session.source_metadata->'metadata')-ARRAY['source_import','catalog_update','duplicate_policy']
  OR jsonb_typeof(m->'pages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'pages')<>session.page_count
  OR (SELECT count(*) FROM rkb_v4.source_stage_pages WHERE source_id=source)<>session.page_count
  OR jsonb_typeof(m->'regions') IS DISTINCT FROM 'array'
  OR jsonb_array_length(m->'regions')<>jsonb_array_length(doc->'semantic'->'regions')
  OR (SELECT coalesce(sum(jsonb_array_length(coalesce((latest_pages->page_id::text)->'regions','[]'::jsonb))),0)
      FROM rkb_v4.source_stage_pages WHERE source_id=source)<>jsonb_array_length(m->'regions') THEN RETURN false;END IF;
 FOR page IN SELECT value FROM jsonb_array_elements(m->'pages') LOOP
  SELECT * INTO STRICT native FROM rkb_v4.source_stage_pages WHERE source_id=source AND physical_page_index=idx;
  issued:=convert_from(native.image_receipt,'UTF8')::jsonb;
  model_page:=latest_pages->native.page_id::text;
  IF page-ARRAY['page_id','physical_page_index','printed_page_number','width','height','source_image_receipt_sha256',
      'issued_image_receipt_b64','image_sha256','preview_render','source_material','source_review_note']<>'{}'
   OR page->>'page_id' IS DISTINCT FROM native.page_id::text OR page->'physical_page_index' IS DISTINCT FROM to_jsonb(idx)
   OR page->>'source_image_receipt_sha256' IS DISTINCT FROM native.image_receipt_sha256
   OR decode(page->>'issued_image_receipt_b64','base64') IS DISTINCT FROM native.image_receipt
   OR native.image_receipt_sha256<>encode(sha256(native.image_receipt),'hex')
   OR page->'printed_page_number' IS DISTINCT FROM coalesce(model_page->'printed_page_number','null'::jsonb)
   OR page->>'source_material' IS DISTINCT FROM 'visual_reviewed'
   OR page->>'source_review_note' IS DISTINCT FROM model_page->>'source_review_note'
   OR NOT length(page->>'source_review_note') BETWEEN 1 AND 500 THEN RETURN false;END IF;
  FOREACH k IN ARRAY ARRAY['width','height','image_sha256','preview_render'] LOOP
   IF page->k IS DISTINCT FROM issued->k THEN RETURN false;END IF;
  END LOOP;
  idx:=idx+1;
 END LOOP;
 FOR region IN SELECT value FROM jsonb_array_elements(m->'regions') LOOP
  ident:=(region->>'region_id')::uuid;
  IF ident=ANY(seen) OR region-ARRAY['region_id','region_key','page_id','kind','bbox','reading_order','column_id','text_sha256','source_text_provenance']<>'{}'
   OR region->>'source_text_provenance' IS DISTINCT FROM 'model_transcription' THEN RETURN false;END IF;
  seen:=array_append(seen,ident);
  SELECT * INTO STRICT native FROM rkb_v4.source_stage_pages WHERE source_id=source AND page_id=(region->>'page_id')::uuid;
  model_page:=latest_pages->native.page_id::text;
  SELECT r INTO STRICT raw_region FROM jsonb_array_elements(model_page->'regions') r WHERE r->>'region_key'=region->>'region_key';
  SELECT r INTO STRICT actual FROM jsonb_array_elements(doc->'semantic'->'regions') r WHERE r->>'region_id'=region->>'region_id';
  IF actual->>'page_id' IS DISTINCT FROM native.page_id::text
   OR actual->>'source_text' IS DISTINCT FROM coalesce(raw_region->>'source_text','')
   OR actual->>'source_text_sha256' IS DISTINCT FROM region->>'text_sha256'
   OR region->>'text_sha256' IS DISTINCT FROM encode(sha256(convert_to(coalesce(raw_region->>'source_text',''),'UTF8')),'hex')
   OR region->'kind' IS DISTINCT FROM raw_region->'kind' OR region->'bbox' IS DISTINCT FROM raw_region->'bbox'
   OR region->'reading_order' IS DISTINCT FROM raw_region->'reading_order'
   OR region->'column_id' IS DISTINCT FROM coalesce(raw_region->'column_id','null'::jsonb) THEN RETURN false;END IF;
 END LOOP;
 RETURN rkb_v4.check_source_image_passages_v1(doc);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;
