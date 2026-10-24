-- Separate accepted catalog corrections; original source-stage bytes and SQL026 stay immutable.
CREATE TABLE rkb_v4.source_catalog_corrections (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_stage_sessions(source_id),
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
 owner_person_id uuid NOT NULL,
 metadata jsonb NOT NULL,
 PRIMARY KEY(source_id,accepted_sequence)
);
-- Only accepted commands insert a new row; no update/delete mutation path or immutable trigger.

CREATE FUNCTION rkb_v4.check_source_catalog_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;source uuid;ingestion uuid;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 16384
    OR jsonb_typeof(ids) IS DISTINCT FROM 'object'
    OR ids-ARRAY['source_id']<>'{}'::jsonb
    OR expected IS DISTINCT FROM '{"source_revision":"1"}'::jsonb THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;
 ingestion:=(convert_from(p,'UTF8')::jsonb->>'ingestion_id')::uuid;
 -- The canonical codec, JSON shape and command actor are checked by source_catalog_command.py
 -- before acceptance; SQL checks only the referenced existing source and accepted stage.
 RETURN EXISTS (
  SELECT 1 FROM rkb_v4.source_stage_sessions session
  JOIN rkb_v4.source_stage_batches b ON b.source_id=session.source_id AND b.batch_index=0
  JOIN rkb_v4.applied_ops a ON a.sequence=b.accepted_sequence AND a.status='applied'
  JOIN rkb_v4.sources s ON s.source_id=session.source_id
  JOIN rkb_v4.active_revision ar ON ar.source_id=session.source_id AND ar.revision=1
  WHERE session.source_id=source AND session.ingestion_id=ingestion
    AND s.accepted AND NOT s.withdrawn
 );
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_catalog_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;patch jsonb;previous jsonb;merged jsonb;source uuid;seq bigint;
BEGIN
 -- Same transaction and writer fence as existing apply_accepted; exact-key replay
 -- resolves through applied_ops before this function is invoked again.
 SELECT applied_seq+1 INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_catalog_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'accepted catalog source integrity precondition required';END IF;
 d:=convert_from(p,'UTF8')::jsonb;source:=(ids->>'source_id')::uuid;
 patch:=d->'catalog_update'-'expected_revision';
 SELECT metadata INTO previous FROM rkb_v4.source_catalog_corrections
 WHERE source_id=source ORDER BY accepted_sequence DESC LIMIT 1;
 IF previous IS NULL THEN
  SELECT source_metadata->'metadata' INTO STRICT previous
  FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 END IF;
 merged:=previous||(patch-'catalog');
 IF jsonb_typeof(patch->'catalog')='object' THEN
  merged:=jsonb_set(merged,'{catalog}',
   (CASE WHEN jsonb_typeof(previous->'catalog')='object' THEN previous->'catalog'
         ELSE '{}'::jsonb END)||patch->'catalog',true);
 END IF;
 INSERT INTO rkb_v4.source_catalog_corrections(source_id,accepted_sequence,owner_person_id,metadata)
 VALUES(source,seq,(d->>'owner_person_id')::uuid,merged);
 RETURN convert_to(jsonb_build_object('source_id',source,'ingestion_id',d->>'ingestion_id',
  'catalog_revision',1,'payload_sha256',encode(sha256(p),'hex'),
  'accepted',true,'logical_time',logical_time)::text,'UTF8');
END $$;

-- Read the latest correction, not a historical row; original source_catalog/manifest remain unchanged.
CREATE OR REPLACE FUNCTION rkb_v4.find_source_catalog_v1(query_text text,budget integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,metadata jsonb)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT c.source_id,c.source_revision,v.eligibility_revision,picked.metadata
 FROM rkb_v4.source_catalog c
 JOIN rkb_v4.active_revision a ON a.source_id=c.source_id AND a.revision=c.source_revision
 JOIN rkb_v4.sources s ON s.source_id=c.source_id
 JOIN rkb_v4.source_visibility v ON v.source_id=c.source_id
 LEFT JOIN LATERAL (
  SELECT k.metadata FROM rkb_v4.source_catalog_corrections k
  WHERE k.source_id=c.source_id ORDER BY k.accepted_sequence DESC LIMIT 1
 ) corrected ON true
 CROSS JOIN LATERAL (SELECT coalesce(corrected.metadata,c.metadata) AS metadata) picked
 WHERE s.accepted AND NOT s.withdrawn AND picked.metadata->>'title' IS NOT NULL
 AND to_tsvector('pg_catalog.simple'::regconfig,
  coalesce(picked.metadata->>'title','')||E'\n'||
  coalesce((SELECT string_agg(x,E'\n' ORDER BY ordinal)
   FROM jsonb_array_elements_text(picked.metadata->'authors') WITH ORDINALITY names(x,ordinal)),'')) @@
  plainto_tsquery('pg_catalog.simple'::regconfig,query_text)
 ORDER BY c.source_id LIMIT greatest(0,least(budget,8))
$$;
REVOKE ALL ON rkb_v4.source_catalog_corrections FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_catalog_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_catalog_v1(bytea,jsonb,jsonb,text) FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='rkb_reader') THEN
 GRANT SELECT ON rkb_v4.source_catalog_corrections TO rkb_reader;
END IF;END $$;
