-- Immutable, explicitly installed after 001_search.sql; never a silent upgrade.
CREATE TABLE rkb_v4.writer_generations (
    generation text PRIMARY KEY CHECK (octet_length(generation) BETWEEN 1 AND 256),
    predecessor text REFERENCES rkb_v4.writer_generations,
    retired boolean NOT NULL DEFAULT false
);
CREATE TABLE rkb_v4.recovery_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    database_generation text NOT NULL CHECK (octet_length(database_generation) BETWEEN 1 AND 256),
    writer_generation text NOT NULL REFERENCES rkb_v4.writer_generations,
    applied_seq bigint NOT NULL CHECK (applied_seq >= 0),
    schema_revision text NOT NULL CHECK (octet_length(schema_revision) BETWEEN 1 AND 256),
    function_revision text NOT NULL CHECK (octet_length(function_revision) BETWEEN 1 AND 256),
    executor_revision text NOT NULL CHECK (octet_length(executor_revision) BETWEEN 1 AND 256),
    last_migration_id text,
    last_migration_hash text CHECK (last_migration_hash ~ '^[0-9a-f]{64}$'),
    CHECK ((last_migration_id IS NULL) = (last_migration_hash IS NULL))
);
CREATE TABLE rkb_v4.applied_ops (
    operation_key text PRIMARY KEY CHECK (octet_length(operation_key) BETWEEN 1 AND 4096),
    sequence bigint NOT NULL UNIQUE CHECK (sequence > 0),
    command_bytes bytea NOT NULL CHECK (octet_length(command_bytes) BETWEEN 1 AND 1048576),
    command_sha256 text NOT NULL CHECK (command_sha256 ~ '^[0-9a-f]{64}$'),
    accepted_manifest_sha256 text NOT NULL CHECK (accepted_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    database_generation text NOT NULL,
    writer_generation text NOT NULL REFERENCES rkb_v4.writer_generations,
    status text NOT NULL CHECK (status IN ('applied', 'rejected_precondition')),
    outcome_bytes bytea NOT NULL CHECK (octet_length(outcome_bytes) BETWEEN 1 AND 1048576),
    outcome_sha256 text NOT NULL CHECK (outcome_sha256 ~ '^[0-9a-f]{64}$'),
    handler_sha256 text NOT NULL CHECK (handler_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE FUNCTION rkb_v4.reject_applied_op_change() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'applied operation receipts are immutable';
END
$$;
CREATE TRIGGER immutable_applied_ops BEFORE UPDATE OR DELETE ON rkb_v4.applied_ops
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_applied_op_change();
REVOKE ALL ON rkb_v4.recovery_state, rkb_v4.writer_generations, rkb_v4.applied_ops FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.reject_applied_op_change() FROM PUBLIC;
-- Installation/initialization and grants belong to the existing trusted controller.
-- Domain/check/migration functions are immutable versioned deployment artifacts.
