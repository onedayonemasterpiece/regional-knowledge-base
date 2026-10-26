-- Bibliography is a derived display projection; original accepted bytes stay immutable.
-- Existing correction envelopes allow 16 KiB, while passage reads paginate citations.
ALTER TABLE rkb_v4.sources DROP CONSTRAINT sources_bibliography_check;
ALTER TABLE rkb_v4.sources ADD CONSTRAINT sources_bibliography_check
 CHECK (length(bibliography)<=16384);

CREATE FUNCTION rkb_v4.catalog_bibliography_v1(metadata jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT concat_ws('; ',
  nullif((SELECT string_agg(author,', ' ORDER BY ordinal)
   FROM jsonb_array_elements_text(metadata->'authors') WITH ORDINALITY authors(author,ordinal)),''),
  metadata->>'title',metadata->>'publication_year')
$$;

CREATE FUNCTION rkb_v4.project_catalog_bibliography_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE citation text:=rkb_v4.catalog_bibliography_v1(NEW.metadata);
 state rkb_v4.recovery_state;frontier rkb_v4.read_frontier;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
 UPDATE rkb_v4.sources SET bibliography=citation
 WHERE source_id=NEW.source_id AND bibliography IS DISTINCT FROM citation;
 IF frontier.projection_seq=state.applied_seq AND frontier.pending_seq IS NULL THEN
  PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
   (SELECT visibility_epoch FROM rkb_v4.read_frontier WHERE singleton),state.applied_seq+1,
   frontier.text_ready,frontier.dense_ready);
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER project_catalog_bibliography AFTER INSERT ON rkb_v4.source_catalog_corrections
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.project_catalog_bibliography_v1();
REVOKE ALL ON FUNCTION rkb_v4.catalog_bibliography_v1(jsonb),
 rkb_v4.project_catalog_bibliography_v1() FROM PUBLIC;

-- Bring already accepted corrections into the same projection. Preserve prior
-- readiness: no passage/vector changes; retain the new visibility epoch/fences.
DO $$
DECLARE before_text boolean;before_dense boolean;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM rkb_v4.recovery_state WHERE singleton) THEN RETURN;END IF;
 SELECT text_ready,dense_ready INTO before_text,before_dense
 FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
 UPDATE rkb_v4.sources s SET bibliography=rkb_v4.catalog_bibliography_v1(c.metadata)
 FROM (SELECT DISTINCT ON(source_id) source_id,metadata
       FROM rkb_v4.source_catalog_corrections ORDER BY source_id,accepted_sequence DESC) c
 WHERE c.source_id=s.source_id
 AND s.bibliography IS DISTINCT FROM rkb_v4.catalog_bibliography_v1(c.metadata);
 IF before_text IS NOT NULL THEN
  UPDATE rkb_v4.read_frontier SET text_ready=before_text,dense_ready=before_dense WHERE singleton;
 END IF;
END $$;
