-- P0.B #504: full accepted region text is read ONLY for one selected complete
-- unit. The existing bounded 16-span source_units listing stays unchanged.
CREATE FUNCTION rkb_v4.read_source_unit_export_pages_v1(
 wanted uuid, first_page integer, last_page integer)
RETURNS TABLE(page jsonb, edition jsonb)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 WITH accessible AS MATERIALIZED (
   SELECT st.source_metadata->'metadata' AS edition
   FROM rkb_v4.source_stage_sessions st
   JOIN rkb_v4.sources s USING(source_id)
   JOIN rkb_v4.active_revision a ON a.source_id=s.source_id AND a.revision=1
   WHERE st.source_id=wanted AND s.accepted AND NOT s.withdrawn
     AND first_page>=0 AND last_page>=first_page AND last_page<st.page_count
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
   WHERE (a.page->>'physical_page_index')::integer BETWEEN first_page AND last_page
   ORDER BY (a.page->>'physical_page_index')::integer,a.batch_index DESC
 )
 SELECT l.page,a.edition FROM latest l CROSS JOIN accessible a
 ORDER BY l.physical
$$;
REVOKE ALL ON FUNCTION rkb_v4.read_source_unit_export_pages_v1(uuid,integer,integer) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='rkb_reader') THEN
  GRANT EXECUTE ON FUNCTION rkb_v4.read_source_unit_export_pages_v1(uuid,integer,integer) TO rkb_reader;
 END IF;
END $$;
