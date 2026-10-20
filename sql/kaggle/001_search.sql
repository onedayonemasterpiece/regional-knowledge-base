-- Immutable migration payload; applied by the existing ordered migration authority.
-- Requires PostgreSQL 17 and preinstalled pgvector 0.8.2 (extension public.vector).
CREATE SCHEMA rkb_v4;
CREATE TABLE rkb_v4.sources (
    source_id uuid PRIMARY KEY,
    bibliography text NOT NULL CHECK (length(bibliography) <= 8000),
    accepted boolean NOT NULL DEFAULT false,
    withdrawn boolean NOT NULL DEFAULT false
);
CREATE TABLE rkb_v4.revisions (
    source_id uuid NOT NULL REFERENCES rkb_v4.sources,
    revision bigint NOT NULL CHECK (revision >= 1),
    PRIMARY KEY (source_id, revision)
);
CREATE TABLE rkb_v4.active_revision (
    source_id uuid PRIMARY KEY,
    revision bigint NOT NULL,
    FOREIGN KEY (source_id, revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.passage_search (
    passage_id uuid PRIMARY KEY,
    source_id uuid NOT NULL,
    revision bigint NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    locator text NOT NULL CHECK (length(locator) <= 1000),
    body text NOT NULL CHECK (length(body) BETWEEN 1 AND 16000),
    language text NOT NULL CHECK (language IN ('ru', 'de', 'simple')),
    config regconfig NOT NULL CHECK (config IN ('pg_catalog.russian'::regconfig,
        'pg_catalog.german'::regconfig, 'pg_catalog.simple'::regconfig)),
    lexemes tsvector GENERATED ALWAYS AS (to_tsvector(config, body)) STORED,
    embedding public.vector(1024) NOT NULL CHECK (public.vector_norm(embedding) > 0),
    FOREIGN KEY (source_id, revision) REFERENCES rkb_v4.revisions,
    UNIQUE (source_id, revision, ordinal),
    CHECK (config = CASE language WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
        WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END)
);
CREATE INDEX passage_dense ON rkb_v4.passage_search USING hnsw (embedding public.vector_cosine_ops);
CREATE INDEX passage_lexical ON rkb_v4.passage_search USING gin (lexemes);
-- The unique source/revision/ordinal constraint supplies the scoped B-tree index.
CREATE FUNCTION rkb_v4.reject_revision_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'immutable revision/projection: create a new revision'; END
$$;
CREATE FUNCTION rkb_v4.reject_source_payload_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.source_id IS DISTINCT FROM OLD.source_id
       OR NEW.bibliography IS DISTINCT FROM OLD.bibliography THEN
        RAISE EXCEPTION 'immutable source payload';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER immutable_source_payload BEFORE UPDATE ON rkb_v4.sources
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_source_payload_update();
CREATE TRIGGER immutable_revision BEFORE UPDATE ON rkb_v4.revisions
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_passage BEFORE UPDATE ON rkb_v4.passage_search
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.lexical_candidates(query_text text, budget integer)
RETURNS TABLE (passage_id uuid, score double precision) LANGUAGE sql STABLE AS $$
    WITH configs AS (
        SELECT c, websearch_to_tsquery(c, query_text) q FROM unnest(ARRAY[
            'pg_catalog.russian'::regconfig, 'pg_catalog.german'::regconfig,
            'pg_catalog.simple'::regconfig]) c
    ), candidates AS (
        SELECT hit.* FROM configs CROSS JOIN LATERAL (
            SELECT p.passage_id, ts_rank_cd(p.lexemes, configs.q)::double precision score
            FROM rkb_v4.passage_search p
            JOIN rkb_v4.active_revision a USING (source_id, revision)
            JOIN rkb_v4.sources s USING (source_id)
            WHERE s.accepted AND NOT s.withdrawn AND p.config = configs.c
              AND p.lexemes @@ configs.q
            ORDER BY score DESC, p.passage_id
            LIMIT greatest(0, least(budget, 200))
        ) hit
    ) SELECT * FROM candidates ORDER BY score DESC, passage_id
      LIMIT greatest(0, least(budget, 200))
$$;
CREATE FUNCTION rkb_v4.dense_candidates(query_vector public.vector(1024), budget integer)
RETURNS TABLE (passage_id uuid, distance double precision) LANGUAGE sql STABLE AS $$
    SELECT p.passage_id, p.embedding <=> query_vector
    FROM rkb_v4.passage_search p
    JOIN rkb_v4.active_revision a USING (source_id, revision)
    JOIN rkb_v4.sources s USING (source_id)
    WHERE s.accepted AND NOT s.withdrawn
    ORDER BY p.embedding <=> query_vector
    LIMIT greatest(0, least(budget, 200))
$$;
CREATE FUNCTION rkb_v4.hydrate(candidate_ids uuid[])
RETURNS TABLE (passage_id uuid, source_id uuid, revision bigint, locator text,
               body text, bibliography text) LANGUAGE sql STABLE AS $$
    SELECT p.passage_id, p.source_id, p.revision, p.locator,
           left(p.body, 4000), left(s.bibliography, 1000)
    FROM unnest(candidate_ids[1:50]) WITH ORDINALITY ids(id, rank)
    JOIN rkb_v4.passage_search p ON p.passage_id = ids.id
    JOIN rkb_v4.active_revision a USING (source_id, revision)
    JOIN rkb_v4.sources s USING (source_id)
    WHERE s.accepted AND NOT s.withdrawn
    ORDER BY ids.rank
$$;
