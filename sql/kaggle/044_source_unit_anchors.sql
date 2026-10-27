-- P0.A #504: read-only, source-backed unit anchors from *published* stages.
-- Do not change source.stage.v1 / source.batch.v1 / source.finish.v1 replay.
-- No new section tables, mutable derived stores, or source author text copies.
CREATE FUNCTION rkb_v4.read_source_unit_pages_v1(wanted uuid)
RETURNS TABLE(physical_page_index integer, page jsonb,
              page_count integer, read_complete boolean)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 WITH accessible AS MATERIALIZED (
  SELECT st.page_count, EXISTS(
    SELECT 1 FROM rkb_v4.source_read_completions finish
    WHERE finish.source_id=st.source_id) AS read_complete
  FROM rkb_v4.source_stage_sessions st
  JOIN rkb_v4.sources s ON s.source_id=st.source_id
  JOIN rkb_v4.active_revision a ON a.source_id=s.source_id AND a.revision=1
  WHERE st.source_id=wanted AND s.accepted AND NOT s.withdrawn
 ),
 accepted AS MATERIALIZED (
  SELECT b.batch_index, model.value AS page
  FROM rkb_v4.source_stage_batches b
  JOIN rkb_v4.source_batch_receipts receipt
   ON receipt.source_id=b.source_id AND receipt.batch_index=b.batch_index
    AND receipt.component='text'
  CROSS JOIN LATERAL jsonb_array_elements(
   convert_from(b.payload_bytes,'UTF8')::jsonb->'model_payload'->'pages') model(value)
  WHERE b.source_id=wanted AND EXISTS(SELECT 1 FROM accessible)
 ),
 latest AS (
  SELECT DISTINCT ON ((a.page->>'physical_page_index')::integer)
   (a.page->>'physical_page_index')::integer AS physical_page_index, a.page
  FROM accepted a
  ORDER BY (a.page->>'physical_page_index')::integer,a.batch_index DESC
 )
 SELECT l.physical_page_index,
  jsonb_build_object(
   'page_id',l.page->>'page_id',
   'physical_page_index',l.physical_page_index,
   'source_unit_boundaries',coalesce(l.page->'source_unit_boundaries','[]'::jsonb),
   'regions',coalesce((
     SELECT jsonb_agg(jsonb_build_object(
      'region_key',r.value->>'region_key',
      'reading_order',(r.value->>'reading_order')::integer,
      'source_length',char_length(coalesce(r.value->>'source_text','')),
      'kind',r.value->>'kind')
      ORDER BY (r.value->>'reading_order')::integer)
     FROM jsonb_array_elements(l.page->'regions') r(value)
    ),'[]'::jsonb)),
  a.page_count,a.read_complete
 FROM latest l CROSS JOIN accessible a
 ORDER BY l.physical_page_index
$$;
REVOKE ALL ON FUNCTION rkb_v4.read_source_unit_pages_v1(uuid) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='rkb_reader') THEN
  GRANT EXECUTE ON FUNCTION rkb_v4.read_source_unit_pages_v1(uuid) TO rkb_reader;
 END IF;
END $$;
