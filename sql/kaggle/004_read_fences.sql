-- Immutable installation after 001..003, through existing ordered authority.
-- Defaults deliberately cannot serve QUERY: projection completion is not applied_seq.
CREATE TABLE rkb_v4.read_frontier (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    projection_seq bigint CHECK (projection_seq >= 0),
    text_ready boolean NOT NULL DEFAULT false,
    dense_ready boolean NOT NULL DEFAULT false,
    visibility_epoch bigint NOT NULL DEFAULT 0 CHECK (visibility_epoch >= 0),
    invalidated_txid xid8,
    pending_seq bigint,
    pending_txid xid8,
    pending_visibility bigint,
    pending_writer text,
    pending_text_ready boolean,
    pending_dense_ready boolean,
    CHECK ((pending_seq IS NULL AND pending_txid IS NULL AND pending_visibility IS NULL
            AND pending_writer IS NULL AND pending_text_ready IS NULL AND pending_dense_ready IS NULL)
        OR (pending_seq IS NOT NULL AND pending_seq>0 AND pending_txid IS NOT NULL
            AND pending_visibility IS NOT NULL AND pending_visibility>=0
            AND pending_writer IS NOT NULL AND pending_text_ready IS NOT NULL AND pending_dense_ready IS NOT NULL)),
    CHECK (NOT (text_ready OR dense_ready) OR projection_seq IS NOT NULL)
);
INSERT INTO rkb_v4.read_frontier(singleton) VALUES (true);
CREATE TABLE rkb_v4.source_visibility (
    source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources ON DELETE CASCADE,
    eligibility_revision bigint NOT NULL CHECK (eligibility_revision >= 1)
);
-- Explicit installation baseline, not historical revocation-delta reconstruction.
INSERT INTO rkb_v4.source_visibility SELECT source_id,1 FROM rkb_v4.sources;

CREATE FUNCTION rkb_v4.lock_read_fence_writer() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
    PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'read fence requires explicit database initialization'; END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER source_fence_lock BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.sources
    FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER active_fence_lock BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.active_revision
    FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER projection_fence_lock BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.passage_search
    FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER frontier_fence_lock BEFORE UPDATE ON rkb_v4.read_frontier
    FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();

CREATE FUNCTION rkb_v4.bump_source_visibility() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE old_id uuid; new_id uuid; id uuid; old_revision bigint; new_revision bigint;
BEGIN
    IF TG_OP='UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NULL; END IF;
    IF TG_OP<>'INSERT' THEN old_id:=OLD.source_id; END IF;
    IF TG_OP<>'DELETE' THEN new_id:=NEW.source_id; END IF;
    IF TG_TABLE_NAME='passage_search' THEN
        IF TG_OP<>'INSERT' THEN old_revision:=OLD.revision; END IF;
        IF TG_OP<>'DELETE' THEN new_revision:=NEW.revision; END IF;
        -- Staging a non-serving revision must preserve the old active reader.
        IF NOT EXISTS (SELECT 1 FROM rkb_v4.active_revision a
            JOIN rkb_v4.sources s USING(source_id)
            WHERE s.accepted AND NOT s.withdrawn AND
                ((a.source_id=old_id AND a.revision=old_revision)
                 OR (a.source_id=new_id AND a.revision=new_revision))) THEN
            RETURN NULL;
        END IF;
    END IF;
    FOR id IN SELECT DISTINCT x FROM unnest(ARRAY[old_id,new_id]) x WHERE x IS NOT NULL LOOP
        IF EXISTS (SELECT 1 FROM rkb_v4.sources s WHERE s.source_id=id) THEN
            INSERT INTO rkb_v4.source_visibility VALUES (id,1)
            ON CONFLICT(source_id) DO UPDATE
                SET eligibility_revision=source_visibility.eligibility_revision+1;
        END IF;
    END LOOP;
    UPDATE rkb_v4.read_frontier SET visibility_epoch=visibility_epoch+1,
        text_ready=false,dense_ready=false,invalidated_txid=pg_current_xact_id() WHERE singleton;
    RETURN NULL;
END $$;
CREATE TRIGGER source_visibility_change AFTER INSERT OR UPDATE OR DELETE ON rkb_v4.sources
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.bump_source_visibility();
CREATE TRIGGER active_visibility_change AFTER INSERT OR UPDATE OR DELETE ON rkb_v4.active_revision
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.bump_source_visibility();
CREATE TRIGGER projection_visibility_change AFTER INSERT OR UPDATE OR DELETE ON rkb_v4.passage_search
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.bump_source_visibility();

CREATE FUNCTION rkb_v4.validate_read_frontier() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE seq bigint;
BEGIN
    SELECT applied_seq INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    IF (OLD.projection_seq IS NOT NULL AND (NEW.projection_seq IS NULL OR NEW.projection_seq<OLD.projection_seq))
       OR NEW.projection_seq>seq OR NEW.visibility_epoch<OLD.visibility_epoch
       OR ((NEW.text_ready OR NEW.dense_ready) AND NEW.projection_seq IS DISTINCT FROM seq) THEN
        RAISE EXCEPTION 'read frontier cannot regress or overstate readiness';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER validate_read_frontier BEFORE UPDATE ON rkb_v4.read_frontier
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.validate_read_frontier();

CREATE FUNCTION rkb_v4.publish_read_frontier(expected_database text, expected_writer text,
    expected_visibility bigint, new_projection bigint, ready_text boolean, ready_dense boolean)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; frontier rkb_v4.read_frontier;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF expected_database IS DISTINCT FROM state.database_generation
       OR expected_writer IS DISTINCT FROM state.writer_generation
       OR expected_visibility IS DISTINCT FROM frontier.visibility_epoch THEN
        RAISE EXCEPTION 'read frontier generation/visibility fence failed';
    END IF;
    IF new_projection IS NULL OR new_projection<0 OR new_projection>state.applied_seq
       OR new_projection<coalesce(frontier.projection_seq,0)
       OR ready_text IS NULL OR ready_dense IS NULL
       OR ((ready_text OR ready_dense) AND new_projection<>state.applied_seq) THEN
        RAISE EXCEPTION 'invalid read frontier cursor/readiness';
    END IF;
    UPDATE rkb_v4.read_frontier SET projection_seq=new_projection,
        text_ready=ready_text,dense_ready=ready_dense WHERE singleton;
END $$;
CREATE FUNCTION rkb_v4.prepare_read_frontier(expected_database text, expected_writer text,
    expected_visibility bigint, target_seq bigint, ready_text boolean, ready_dense boolean)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; frontier rkb_v4.read_frontier;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF expected_database IS DISTINCT FROM state.database_generation
       OR expected_writer IS DISTINCT FROM state.writer_generation
       OR expected_visibility IS DISTINCT FROM frontier.visibility_epoch
       OR target_seq IS DISTINCT FROM state.applied_seq+1
       OR ready_text IS NULL OR ready_dense IS NULL OR frontier.pending_seq IS NOT NULL THEN
        RAISE EXCEPTION 'invalid pending read frontier proof';
    END IF;
    UPDATE rkb_v4.read_frontier SET pending_seq=target_seq,pending_txid=pg_current_xact_id(),
        pending_visibility=expected_visibility,pending_writer=expected_writer,
        pending_text_ready=ready_text,pending_dense_ready=ready_dense WHERE singleton;
END $$;

CREATE FUNCTION rkb_v4.promote_applied_read_frontier() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE frontier rkb_v4.read_frontier; same_semantics boolean; recorded boolean;
BEGIN
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    same_semantics := NEW.database_generation=OLD.database_generation
        AND NEW.schema_revision=OLD.schema_revision AND NEW.function_revision=OLD.function_revision
        AND NEW.executor_revision=OLD.executor_revision;
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
CREATE TRIGGER promote_applied_read_frontier AFTER UPDATE ON rkb_v4.recovery_state
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.promote_applied_read_frontier();

CREATE FUNCTION rkb_v4.reject_orphan_frontier_proof() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
    -- Read CURRENT state: the earlier preparation event's NEW image is obsolete
    -- after successful promotion clears the proof in the same transaction.
    IF EXISTS (SELECT 1 FROM rkb_v4.read_frontier WHERE pending_seq IS NOT NULL) THEN
        RAISE EXCEPTION 'pending read frontier must promote in its own apply transaction';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER reject_orphan_frontier_proof AFTER INSERT OR UPDATE ON rkb_v4.read_frontier
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_orphan_frontier_proof();

REVOKE ALL ON rkb_v4.read_frontier,rkb_v4.source_visibility FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.lock_read_fence_writer(),rkb_v4.bump_source_visibility(),
    rkb_v4.validate_read_frontier(),
    rkb_v4.prepare_read_frontier(text,text,bigint,bigint,boolean,boolean),
    rkb_v4.promote_applied_read_frontier(),rkb_v4.reject_orphan_frontier_proof(),
    rkb_v4.publish_read_frontier(text,text,bigint,bigint,boolean,boolean) FROM PUBLIC;
