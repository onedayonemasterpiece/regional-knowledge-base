-- Integer schema metadata; SQL files and the ledger are the migration authority.
-- Historical command/receipt bytes in applied_ops remain immutable evidence.
ALTER TABLE rkb_v4.recovery_state ADD COLUMN schema_version integer NOT NULL DEFAULT 17
 CHECK(schema_version>0);

CREATE OR REPLACE FUNCTION rkb_v4.promote_applied_read_frontier() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE frontier rkb_v4.read_frontier; same_semantics boolean; recorded boolean;
BEGIN
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    -- Additive DDL changes the ledger, not the accepted data/search frontier.
    same_semantics := NEW.database_generation=OLD.database_generation;
    IF NEW.applied_seq<OLD.applied_seq THEN RAISE EXCEPTION 'applied cursor cannot regress'; END IF;
    IF NEW.applied_seq=OLD.applied_seq THEN
        IF NOT same_semantics THEN
            UPDATE rkb_v4.read_frontier SET text_ready=false,dense_ready=false WHERE singleton;
        END IF;
        RETURN NULL;
    END IF;
    SELECT EXISTS (SELECT 1 FROM rkb_v4.applied_ops op WHERE op.sequence=NEW.applied_seq
        AND op.database_generation=NEW.database_generation AND op.writer_generation=NEW.writer_generation)
        INTO recorded;
    IF frontier.pending_seq IS NOT NULL THEN
        IF NEW.applied_seq<>OLD.applied_seq+1 OR NOT recorded OR NOT same_semantics
           OR frontier.pending_seq<>NEW.applied_seq OR frontier.pending_txid<>pg_current_xact_id()
           OR frontier.pending_writer<>NEW.writer_generation
           OR frontier.pending_visibility<>frontier.visibility_epoch THEN
            RAISE EXCEPTION 'pending read frontier does not match atomic applied operation';
        END IF;
        UPDATE rkb_v4.read_frontier SET projection_seq=NEW.applied_seq,
            text_ready=pending_text_ready,dense_ready=pending_dense_ready,
            pending_seq=NULL,pending_txid=NULL,pending_visibility=NULL,pending_writer=NULL,
            pending_text_ready=NULL,pending_dense_ready=NULL WHERE singleton;
    ELSIF recorded AND same_semantics AND NEW.applied_seq=OLD.applied_seq+1
       AND frontier.projection_seq=OLD.applied_seq AND frontier.text_ready AND frontier.dense_ready
       AND frontier.invalidated_txid IS DISTINCT FROM pg_current_xact_id() THEN
        -- Exact recorded non-search/rejected command; source projections unchanged.
        UPDATE rkb_v4.read_frontier SET projection_seq=NEW.applied_seq WHERE singleton;
    ELSE
        -- Never infer readiness for dirty, lagged or semantically changed work.
        UPDATE rkb_v4.read_frontier SET text_ready=false,dense_ready=false WHERE singleton;
    END IF;
    RETURN NULL;
END $$;

DROP FUNCTION rkb_v4.apply_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text);
DROP FUNCTION rkb_v4.check_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text);
ALTER TABLE rkb_v4.recovery_state
 DROP COLUMN schema_revision,DROP COLUMN function_revision,DROP COLUMN executor_revision,
 DROP COLUMN last_migration_id,DROP COLUMN last_migration_hash;

CREATE OR REPLACE FUNCTION rkb_v4.capture_source_illustration_receipt_v1() RETURNS trigger
 LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;m rkb_v4.source_illustration_candidates;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route'<>'source.activate.v1' THEN RETURN NULL;END IF;
 SELECT * INTO m FROM rkb_v4.source_illustration_candidates WHERE source_id=(c->'ids'->>'source_id')::uuid;
 IF NOT FOUND THEN RETURN NULL;END IF;
 IF (SELECT schema_version FROM rkb_v4.recovery_state WHERE singleton)<15 THEN RAISE EXCEPTION 'illustration activation version differs';END IF;
 IF (SELECT count(*) FROM rkb_v4.source_illustrations WHERE source_id=m.source_id)<>jsonb_array_length(m.media->'illustrations')
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(m.media->'illustrations') i WHERE NOT EXISTS(
   SELECT 1 FROM rkb_v4.source_illustrations x WHERE x.source_id=m.source_id AND x.data=i)) THEN RAISE EXCEPTION 'illustration projection missing';END IF;
 INSERT INTO rkb_v4.source_illustration_origins VALUES(m.source_id,m.media_sha256,m.knowledge_sha256,NEW.operation_key,NEW.sequence,NEW.command_sha256,(c->>'person_id')::uuid);
 RETURN NULL;
END $$;
