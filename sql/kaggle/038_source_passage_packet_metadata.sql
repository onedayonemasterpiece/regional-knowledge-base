-- Read page and region provenance from all published source packets.
CREATE OR REPLACE FUNCTION rkb_v4.source_passage_metadata_v1(wanted uuid[])
RETURNS TABLE(passage_id uuid,metadata jsonb)
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT p.passage_id,jsonb_build_object('catalog',rkb_v4.read_source_manifest_v1(p.source_id)->'metadata','original_sha256',o.original_sha256,
  'source_revision',p.source_revision,'passage',p.data,
  'pages',(SELECT jsonb_agg(x) FROM jsonb_array_elements(rkb_v4.read_source_manifest_v1(p.source_id)->'pages') x WHERE p.data->'page_ids' @> jsonb_build_array(x->'page_id')),
  'regions',(SELECT jsonb_agg(x) FROM jsonb_array_elements(rkb_v4.read_source_manifest_v1(p.source_id)->'regions') x WHERE
    ((p.data->'region_ids')||(p.data->'footnote_region_ids')) @> jsonb_build_array(x->'region_id')))
 FROM rkb_v4.source_passage_provenance p
 JOIN rkb_v4.source_manifests m USING(source_id,source_revision)
 JOIN rkb_v4.source_import_origins o ON o.source_id=p.source_id AND o.revision=p.source_revision
 JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
 JOIN rkb_v4.sources s ON s.source_id=p.source_id
 WHERE p.passage_id=ANY(wanted[1:5]) AND s.accepted AND NOT s.withdrawn
$$;
