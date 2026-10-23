-- Immutable ordered installation after 001..004. No public execute permission.
-- Source-only restriction intent: identity/capability checks happen at the
-- existing canonical controller intake, never through payload person fields.
CREATE FUNCTION rkb_v4.check_source_withdraw_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
    SELECT p=convert_to('{"withdrawn":true}','UTF8')
       AND ids=jsonb_build_object('source_id',ids->>'source_id')
       AND expected=jsonb_build_object('eligibility_revision',expected->>'eligibility_revision')
       AND EXISTS (SELECT 1 FROM rkb_v4.sources s
           JOIN rkb_v4.source_visibility v USING(source_id)
           WHERE s.source_id=(ids->>'source_id')::uuid
             AND v.eligibility_revision::text=expected->>'eligibility_revision')
$$;

CREATE FUNCTION rkb_v4.apply_source_withdraw_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier;
        frontier rkb_v4.read_frontier; source uuid; eligibility bigint; content bigint;
BEGIN
    -- Existing apply holds this same single writer row. Direct invocation is
    -- forbidden by deployment grants; the function cannot create a receipt.
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_withdraw_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'source withdrawal requires exact eligibility precondition';
    END IF;
    source:=(ids->>'source_id')::uuid;
    UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=source;
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
    SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=source;
    SELECT revision INTO content FROM rkb_v4.active_revision WHERE source_id=source;
    -- Withdrawal only removes an eligible source from both existing SQL paths.
    -- Preserve readiness ONLY when that exact pre-transaction projection was
    -- already current; stale/unready projections cannot become ready here.
    PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
        frontier.visibility_epoch,state.applied_seq+1,
        prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
        prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
    RETURN convert_to(jsonb_build_object('source_id',source,'withdrawn',true,
        'content_revision',content,'eligibility_revision',eligibility,
        'visibility_epoch',frontier.visibility_epoch,'logical_time',logical_time)::text,'UTF8');
END $$;

REVOKE ALL ON FUNCTION rkb_v4.check_source_withdraw_v1(bytea,jsonb,jsonb,text),
    rkb_v4.apply_source_withdraw_v1(bytea,jsonb,jsonb,text) FROM PUBLIC;
