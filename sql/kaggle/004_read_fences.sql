-- Immutable installation after 001..003, through existing ordered authority.
-- Defaults deliberately cannot serve QUERY: projection completion is not applied_seq.
CREATE TABLE rkb_v4.read_frontier (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    projection_seq bigint CHECK (projection_seq >= 0),
    text_ready boolean NOT NULL DEFAULT false,
    dense_ready boolean NOT NULL DEFAULT false,
    visibility_epoch bigint NOT NULL DEFAULT 0 CHECK (visibility_epoch >= 0),
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
        text_ready=false,dense_ready=false WHERE singleton;
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
REVOKE ALL ON rkb_v4.read_frontier,rkb_v4.source_visibility FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.lock_read_fence_writer(),rkb_v4.bump_source_visibility(),
    rkb_v4.validate_read_frontier(),
    rkb_v4.publish_read_frontier(text,text,bigint,bigint,boolean,boolean) FROM PUBLIC;
