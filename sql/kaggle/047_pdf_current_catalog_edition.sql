-- PDF bibliography uses the same latest accepted catalog metadata as book_find.
-- Original source-stage payload bytes and metadata remain immutable.
CREATE OR REPLACE FUNCTION rkb_v4.read_source_unit_export_pages_v1(
 wanted uuid, wanted_pages integer[])
RETURNS TABLE(page jsonb, edition jsonb)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 WITH accessible AS MATERIALIZED (
   SELECT coalesce(corrected.metadata,st.source_metadata->'metadata') AS edition
   FROM rkb_v4.source_stage_sessions st
   JOIN rkb_v4.sources s USING(source_id)
   JOIN rkb_v4.active_revision a ON a.source_id=s.source_id AND a.revision=1
   LEFT JOIN LATERAL (
     SELECT k.metadata FROM rkb_v4.source_catalog_corrections k
     WHERE k.source_id=st.source_id ORDER BY k.accepted_sequence DESC LIMIT 1
   ) corrected ON true
   WHERE st.source_id=wanted AND s.accepted AND NOT s.withdrawn
     AND cardinality(wanted_pages) BETWEEN 1 AND st.page_count
     AND NOT EXISTS(SELECT 1 FROM unnest(wanted_pages) p
         WHERE p IS NULL OR p<0 OR p>=st.page_count)
 ),
 accepted AS MATERIALIZED (
   SELECT b.batch_index, item.value AS page
   FROM rkb_v4.source_stage_batches b
   JOIN rkb_v4.source_batch_receipts receipt
     ON receipt.source_id=b.source_id AND receipt.batch_index=b.batch_index
      AND receipt.component='text'
   CROSS JOIN LATERAL jsonb_array_elements(
     convert_from(b.payload_bytes,'UTF8')::jsonb->'model_payload'->'pages') item(value)
   WHERE b.source_id=wanted AND EXISTS(SELECT 1 FROM accessible)
 ),
 latest AS (
   SELECT DISTINCT ON ((a.page->>'physical_page_index')::integer)
       (a.page->>'physical_page_index')::integer AS physical, a.page
   FROM accepted a
   WHERE (a.page->>'physical_page_index')::integer=ANY(wanted_pages)
   ORDER BY (a.page->>'physical_page_index')::integer,a.batch_index DESC
 )
 SELECT l.page,a.edition FROM latest l CROSS JOIN accessible a
 ORDER BY array_position(wanted_pages,l.physical)
$$;
REVOKE ALL ON FUNCTION rkb_v4.read_source_unit_export_pages_v1(uuid,integer[]) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='rkb_reader') THEN
  GRANT EXECUTE ON FUNCTION rkb_v4.read_source_unit_export_pages_v1(uuid,integer[]) TO rkb_reader;
 END IF;
END $$;
