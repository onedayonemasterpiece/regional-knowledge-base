-- Explicit ordered migration, not an implicit upgrade or a public activation.
-- Model inputs remain exact accepted bytes. No embeddings or invented v1
-- native-text provenance, and no provider/model work inside this transaction.
CREATE TABLE rkb_v4.source_stage_sessions (
    source_id uuid PRIMARY KEY,
    ingestion_id uuid NOT NULL UNIQUE,
    source_metadata jsonb NOT NULL,
    original_receipt bytea NOT NULL,
    page_count integer NOT NULL CHECK (page_count BETWEEN 1 AND 2500),
    logical_time text NOT NULL
);
CREATE TABLE rkb_v4.source_stage_batches (
    source_id uuid NOT NULL REFERENCES rkb_v4.source_stage_sessions,
    batch_index integer NOT NULL CHECK (batch_index BETWEEN 0 AND 2499),
    payload_bytes bytea NOT NULL CHECK (octet_length(payload_bytes) BETWEEN 1 AND 716800),
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    previous_batch_sha256 text NOT NULL CHECK (previous_batch_sha256 ~ '^[0-9a-f]{64}$'),
    logical_time text NOT NULL,
    accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (source_id,batch_index),
    UNIQUE (source_id,payload_sha256)
);
CREATE TABLE rkb_v4.source_stage_pages (
    source_id uuid NOT NULL,
    physical_page_index integer NOT NULL CHECK (physical_page_index BETWEEN 0 AND 2499),
    page_id uuid NOT NULL,
    image_receipt bytea NOT NULL CHECK (octet_length(image_receipt) BETWEEN 1 AND 8192),
    image_receipt_sha256 text NOT NULL CHECK (image_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    batch_index integer NOT NULL,
    model_page jsonb NOT NULL,
    PRIMARY KEY (source_id,physical_page_index),
    UNIQUE (source_id,page_id),
    FOREIGN KEY (source_id,batch_index) REFERENCES rkb_v4.source_stage_batches
);
CREATE TABLE rkb_v4.source_stage_candidates (
    source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_stage_sessions,
    ingestion_id uuid NOT NULL UNIQUE,
    batch_count integer NOT NULL CHECK (batch_count BETWEEN 1 AND 2500),
    last_batch_sha256 text NOT NULL CHECK (last_batch_sha256 ~ '^[0-9a-f]{64}$'),
    seal_bytes bytea NOT NULL CHECK (octet_length(seal_bytes) BETWEEN 1 AND 1024),
    seal_sha256 text NOT NULL CHECK (seal_sha256 ~ '^[0-9a-f]{64}$'),
    logical_time text NOT NULL,
    accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER immutable_source_stage_session BEFORE UPDATE OR DELETE ON rkb_v4.source_stage_sessions
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_stage_batch BEFORE UPDATE OR DELETE ON rkb_v4.source_stage_batches
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_stage_page BEFORE UPDATE OR DELETE ON rkb_v4.source_stage_pages
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_stage_candidate BEFORE UPDATE OR DELETE ON rkb_v4.source_stage_candidates
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.check_source_stage_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; src jsonb; original jsonb; original_bytes bytea; model jsonb; image jsonb; page jsonb;
    image_bytes bytea; encoded text; research jsonb; source uuid; ingestion uuid; idx integer; n integer;
    page_index integer; seen integer[]:='{}'; prev text; old rkb_v4.source_stage_sessions;
    batch rkb_v4.source_stage_batches; tail rkb_v4.source_stage_batches; prior_page rkb_v4.source_stage_pages;
BEGIN
    IF octet_length(p) NOT BETWEEN 1 AND 716800 OR logical_time IS NULL
       OR octet_length(logical_time) NOT BETWEEN 1 AND 256
       OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id') THEN RETURN false; END IF;
    source:=(ids->>'source_id')::uuid;
    IF source IS NULL OR source::text<>ids->>'source_id' THEN RETURN false; END IF;
    d:=convert_from(p,'UTF8')::jsonb;
    IF jsonb_typeof(d) IS DISTINCT FROM 'object'
       OR NOT d ?& ARRAY['codec','ingestion_id','batch_index','previous_batch_sha256','source','issued_images_b64','model_payload','research']
       OR d-ARRAY['codec','ingestion_id','batch_index','previous_batch_sha256','source','issued_images_b64','model_payload','research']<>'{}'::jsonb
       OR d->>'codec' IS DISTINCT FROM 'rkb.source-stage-batch.v1'
       OR jsonb_typeof(d->'batch_index') IS DISTINCT FROM 'number'
       OR d->>'batch_index' !~ '^(0|[1-9][0-9]{0,3})$'
       OR jsonb_typeof(d->'previous_batch_sha256') IS DISTINCT FROM 'string'
       OR d->>'previous_batch_sha256' !~ '^[0-9a-f]{64}$' THEN RETURN false; END IF;
    ingestion:=(d->>'ingestion_id')::uuid; idx:=(d->>'batch_index')::integer; prev:=d->>'previous_batch_sha256';
    IF ingestion IS NULL OR ingestion::text<>d->>'ingestion_id' OR idx NOT BETWEEN 0 AND 2499
       OR (idx=0) IS DISTINCT FROM (prev=repeat('0',64))
       OR expected IS DISTINCT FROM jsonb_build_object('stage_head',prev) THEN RETURN false; END IF;
    src:=d->'source'; model:=d->'model_payload'; research:=d->'research';
    IF jsonb_typeof(src) IS DISTINCT FROM 'object'
       OR NOT src ?& ARRAY['bibliography','work_id','edition_id','original_receipt_b64','public_rights','metadata']
       OR src-ARRAY['bibliography','work_id','edition_id','original_receipt_b64','public_rights','metadata']<>'{}'::jsonb
       OR jsonb_typeof(src->'metadata') IS DISTINCT FROM 'object'
       OR octet_length((src->'metadata')::text)>262144
       OR jsonb_typeof(src->'bibliography') IS DISTINCT FROM 'string'
       OR length(src->>'bibliography') NOT BETWEEN 1 AND 8000
       OR jsonb_typeof(src->'work_id') IS DISTINCT FROM 'string'
       OR jsonb_typeof(src->'edition_id') IS DISTINCT FROM 'string'
       OR (src->>'work_id')::uuid::text IS DISTINCT FROM src->>'work_id'
       OR (src->>'edition_id')::uuid::text IS DISTINCT FROM src->>'edition_id'
       OR jsonb_typeof(src->'public_rights') IS DISTINCT FROM 'object'
       OR src->'public_rights'='{}'::jsonb OR octet_length((src->'public_rights')::text)>262144
       OR jsonb_typeof(src->'original_receipt_b64') IS DISTINCT FROM 'string'
       OR length(src->>'original_receipt_b64')>21848 THEN RETURN false; END IF;
    original_bytes:=decode(src->>'original_receipt_b64','base64'); original:=convert_from(original_bytes,'UTF8')::jsonb;
    IF octet_length(original_bytes) NOT BETWEEN 1 AND 16384
       OR jsonb_typeof(original) IS DISTINCT FROM 'object'
       OR NOT original ?& ARRAY['codec','source_id','ingestion_id','request_id','revision','sha256','size','key','native_version','format','page_count','parser_sha256','acquisition_sha256','native_verification_sha256']
       OR original-ARRAY['codec','source_id','ingestion_id','request_id','revision','sha256','size','key','native_version','format','page_count','parser_sha256','acquisition_sha256','native_verification_sha256']<>'{}'::jsonb
       OR original->>'codec' IS DISTINCT FROM 'rkb.original-receipt.v1'
       OR original->>'source_id' IS DISTINCT FROM source::text
       OR original->>'ingestion_id' IS DISTINCT FROM ingestion::text
       OR original->'revision' IS DISTINCT FROM '1'::jsonb
       OR original->>'format' NOT IN ('pdf','djvu') OR original->>'format' IS NULL
       OR original->>'page_count' !~ '^[1-9][0-9]{0,3}$'
       OR jsonb_typeof(original->'page_count') IS DISTINCT FROM 'number'
       OR (original->>'page_count')::integer NOT BETWEEN 1 AND 2500
       OR original->>'sha256' !~ '^[0-9a-f]{64}$' OR original->>'sha256' IS NULL
       OR original->>'parser_sha256' !~ '^[0-9a-f]{64}$' OR original->>'parser_sha256' IS NULL
       OR jsonb_typeof(original->'request_id') IS DISTINCT FROM 'string'
       OR (original->>'request_id')::uuid::text IS DISTINCT FROM original->>'request_id'
       OR jsonb_typeof(original->'size') IS DISTINCT FROM 'number'
       OR original->>'size' !~ '^[1-9][0-9]{0,8}$' OR (original->>'size')::bigint>536870912
       OR jsonb_typeof(original->'key') IS DISTINCT FROM 'string'
       OR octet_length(original->>'key') NOT BETWEEN 1 AND 1024
       OR strpos(original->>'key','://')<>0 OR original->>'key' ~ '(^|/)(\.?\.?)(/|$)'
       OR jsonb_typeof(original->'native_version') IS DISTINCT FROM 'string'
       OR octet_length(original->>'native_version') NOT BETWEEN 1 AND 512
       OR original->>'acquisition_sha256' IS NULL OR original->>'acquisition_sha256' !~ '^[0-9a-f]{64}$'
       OR original->>'native_verification_sha256' IS NULL OR original->>'native_verification_sha256' !~ '^[0-9a-f]{64}$' THEN RETURN false; END IF;
    IF jsonb_typeof(model) IS DISTINCT FROM 'object' OR NOT model ?& ARRAY['pages','chunks']
       OR model-ARRAY['pages','chunks','story_candidates','entity_candidates','poi_facts','poi_media_links']<>'{}'::jsonb
       OR jsonb_typeof(model->'pages') IS DISTINCT FROM 'array'
       OR jsonb_array_length(model->'pages') NOT BETWEEN 1 AND 8
       OR jsonb_typeof(model->'chunks') IS DISTINCT FROM 'array' OR jsonb_array_length(model->'chunks')>100
       OR jsonb_typeof(d->'issued_images_b64') IS DISTINCT FROM 'array'
       OR jsonb_array_length(d->'issued_images_b64')<>jsonb_array_length(model->'pages')
       OR jsonb_path_exists(model,'$.**.embedding') OR jsonb_path_exists(model,'$.**.embeddings')
       OR jsonb_path_exists(model,'$.**.input_tokens') THEN RETURN false; END IF;
    IF model ? 'story_candidates' AND (jsonb_typeof(model->'story_candidates') IS DISTINCT FROM 'array'
            OR jsonb_array_length(model->'story_candidates')>12) THEN RETURN false; END IF;
    IF model ? 'poi_facts' AND (jsonb_typeof(model->'poi_facts') IS DISTINCT FROM 'array'
            OR jsonb_array_length(model->'poi_facts')>32) THEN RETURN false; END IF;
    IF model ? 'poi_media_links' AND (jsonb_typeof(model->'poi_media_links') IS DISTINCT FROM 'array'
            OR jsonb_array_length(model->'poi_media_links')>32) THEN RETURN false; END IF;
    IF model ? 'entity_candidates' AND (jsonb_typeof(model->'entity_candidates') IS DISTINCT FROM 'object'
            OR octet_length((model->'entity_candidates')::text)>1048576) THEN RETURN false; END IF;
    FOR n IN 0..jsonb_array_length(model->'pages')-1 LOOP
        page:=model->'pages'->n; encoded:=d->'issued_images_b64'->>n;
        IF jsonb_typeof(d->'issued_images_b64'->n) IS DISTINCT FROM 'string' OR length(encoded)>10924 THEN RETURN false; END IF;
        image_bytes:=decode(encoded,'base64'); image:=convert_from(image_bytes,'UTF8')::jsonb;
        IF octet_length(image_bytes) NOT BETWEEN 1 AND 8192 OR jsonb_typeof(image) IS DISTINCT FROM 'object'
           OR NOT image ?& ARRAY['codec','original_receipt_sha256','page_id','physical_page_index','original_sha256','parser_sha256','width','height','image_sha256','mime_type','preview_render']
           OR image-ARRAY['codec','original_receipt_sha256','page_id','physical_page_index','original_sha256','parser_sha256','width','height','image_sha256','mime_type','preview_render']<>'{}'::jsonb
           OR image->>'codec' IS DISTINCT FROM 'rkb.issued-source-image.v1'
           OR image->>'original_receipt_sha256' IS DISTINCT FROM encode(sha256(original_bytes),'hex')
           OR image->>'original_sha256' IS DISTINCT FROM original->>'sha256'
           OR image->>'parser_sha256' IS DISTINCT FROM original->>'parser_sha256'
           OR image->>'image_sha256' !~ '^[0-9a-f]{64}$'
           OR jsonb_typeof(image->'image_sha256') IS DISTINCT FROM 'string'
           OR image->>'mime_type' IS DISTINCT FROM 'image/jpeg'
           OR jsonb_typeof(image->'physical_page_index') IS DISTINCT FROM 'number'
           OR image->>'physical_page_index' !~ '^(0|[1-9][0-9]{0,3})$'
           OR (image->>'page_id')::uuid::text IS DISTINCT FROM image->>'page_id'
           OR jsonb_typeof(image->'page_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(image->'width') IS DISTINCT FROM 'number'
           OR jsonb_typeof(image->'height') IS DISTINCT FROM 'number'
           OR (image->>'width')::numeric<=0 OR (image->>'width')::numeric>100000
           OR (image->>'height')::numeric<=0 OR (image->>'height')::numeric>100000
           OR jsonb_typeof(image->'preview_render') IS DISTINCT FROM 'object'
           OR octet_length((image->'preview_render')::text)>16384
           OR jsonb_typeof(page) IS DISTINCT FROM 'object'
           OR page->>'page_id' IS DISTINCT FROM image->>'page_id'
           OR page->'physical_page_index' IS DISTINCT FROM image->'physical_page_index'
           OR page->>'source_image_receipt_sha256' IS DISTINCT FROM encode(sha256(image_bytes),'hex')
           OR page->>'source_material' IS DISTINCT FROM 'visual_reviewed'
           OR jsonb_typeof(page->'source_review_note') IS DISTINCT FROM 'string'
           OR length(page->>'source_review_note') NOT BETWEEN 1 AND 500 THEN RETURN false; END IF;
        page_index:=(image->>'physical_page_index')::integer;
        IF page_index<0 OR page_index>=(original->>'page_count')::integer OR page_index=ANY(seen) THEN RETURN false; END IF;
        seen:=array_append(seen,page_index);
        SELECT * INTO prior_page FROM rkb_v4.source_stage_pages
            WHERE source_id=source AND physical_page_index=page_index;
        -- The issuance is immutable, not the model's first interpretation.
        -- Later accepted batches can explicitly correct transcription or enrich
        -- stories. Native compilation consumes the complete accepted history in
        -- batch_index order; last page revision wins without deleting old bytes.
        -- model_page below is only the first snapshot, never final page truth.
        IF FOUND AND prior_page.image_receipt<>image_bytes THEN RETURN false; END IF;
    END LOOP;
    IF jsonb_typeof(research) IS DISTINCT FROM 'object'
       OR NOT research ?& ARRAY['frontier_id','state','continuation']
       OR research-ARRAY['frontier_id','state','continuation']<>'{}'::jsonb
       OR jsonb_typeof(research->'frontier_id') IS DISTINCT FROM 'string'
       OR (research->>'frontier_id')::uuid::text IS DISTINCT FROM research->>'frontier_id'
       OR research->>'state' IS NULL OR research->>'state' NOT IN ('pending','complete')
       OR jsonb_typeof(research->'continuation') IS DISTINCT FROM 'object'
       OR octet_length((research->'continuation')::text)>262144
       OR (research->>'state'='pending' AND research->'continuation'='{}'::jsonb) THEN RETURN false; END IF;
    SELECT * INTO old FROM rkb_v4.source_stage_sessions WHERE source_id=source;
    IF FOUND AND (old.ingestion_id<>ingestion OR old.source_metadata<>src OR old.original_receipt<>original_bytes) THEN RETURN false; END IF;
    SELECT * INTO batch FROM rkb_v4.source_stage_batches WHERE source_id=source AND batch_index=idx;
    IF FOUND THEN RETURN batch.payload_bytes=p; END IF;
    IF EXISTS (SELECT 1 FROM rkb_v4.sources WHERE source_id=source)
       OR EXISTS (SELECT 1 FROM rkb_v4.source_stage_candidates WHERE source_id=source)
       OR EXISTS (SELECT 1 FROM rkb_v4.source_stage_sessions WHERE ingestion_id=ingestion AND source_id<>source) THEN RETURN false; END IF;
    SELECT * INTO tail FROM rkb_v4.source_stage_batches WHERE source_id=source ORDER BY batch_index DESC LIMIT 1;
    RETURN (NOT FOUND AND idx=0) OR (FOUND AND idx=tail.batch_index+1 AND prev=tail.payload_sha256);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_stage_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; source uuid; idx integer; n integer; image_bytes bytea; image jsonb; seq bigint;
BEGIN
    -- Same owner lock as apply_accepted. Direct calls do not create accepted_ops.
    SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    IF NOT FOUND OR NOT rkb_v4.check_source_stage_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'exact inactive source stage precondition required'; END IF;
    d:=convert_from(p,'UTF8')::jsonb; source:=(ids->>'source_id')::uuid; idx:=(d->>'batch_index')::integer;
    IF NOT EXISTS (SELECT 1 FROM rkb_v4.source_stage_batches WHERE source_id=source AND batch_index=idx) THEN
        INSERT INTO rkb_v4.source_stage_sessions(source_id,ingestion_id,source_metadata,original_receipt,page_count,logical_time)
        VALUES(source,(d->>'ingestion_id')::uuid,d->'source',decode(d->'source'->>'original_receipt_b64','base64'),
            (convert_from(decode(d->'source'->>'original_receipt_b64','base64'),'UTF8')::jsonb->>'page_count')::integer,logical_time)
        ON CONFLICT(source_id) DO NOTHING;
        INSERT INTO rkb_v4.source_stage_batches VALUES(source,idx,p,encode(sha256(p),'hex'),d->>'previous_batch_sha256',logical_time,seq);
        FOR n IN 0..jsonb_array_length(d->'issued_images_b64')-1 LOOP
            image_bytes:=decode(d->'issued_images_b64'->>n,'base64'); image:=convert_from(image_bytes,'UTF8')::jsonb;
            INSERT INTO rkb_v4.source_stage_pages VALUES(source,(image->>'physical_page_index')::integer,
                (image->>'page_id')::uuid,image_bytes,encode(sha256(image_bytes),'hex'),idx,d->'model_payload'->'pages'->n)
                ON CONFLICT(source_id,physical_page_index) DO NOTHING;
        END LOOP;
    END IF;
    RETURN convert_to(jsonb_build_object('source_id',source,'ingestion_id',d->>'ingestion_id',
        'batch_index',idx,'batch_sha256',encode(sha256(p),'hex'),'candidate_state','staged_inactive',
        'logical_time',logical_time)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.check_source_stage_seal_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; source uuid; session rkb_v4.source_stage_sessions; tail rkb_v4.source_stage_batches;
    candidate rkb_v4.source_stage_candidates;
BEGIN
    IF octet_length(p) NOT BETWEEN 1 AND 1024 OR logical_time IS NULL
       OR octet_length(logical_time) NOT BETWEEN 1 AND 256
       OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id') THEN RETURN false; END IF;
    source:=(ids->>'source_id')::uuid; d:=convert_from(p,'UTF8')::jsonb;
    IF source IS NULL OR source::text<>ids->>'source_id' OR jsonb_typeof(d) IS DISTINCT FROM 'object'
       OR NOT d ?& ARRAY['codec','ingestion_id','batch_count','last_batch_sha256']
       OR d-ARRAY['codec','ingestion_id','batch_count','last_batch_sha256']<>'{}'::jsonb
       OR d->>'codec' IS DISTINCT FROM 'rkb.source-stage-seal.v1'
       OR jsonb_typeof(d->'batch_count') IS DISTINCT FROM 'number'
       OR d->>'batch_count' !~ '^[1-9][0-9]{0,3}$' OR (d->>'batch_count')::integer NOT BETWEEN 1 AND 2500
       OR d->>'last_batch_sha256' !~ '^[0-9a-f]{64}$' OR d->>'last_batch_sha256'=repeat('0',64)
       OR jsonb_typeof(d->'last_batch_sha256') IS DISTINCT FROM 'string'
       OR expected IS DISTINCT FROM jsonb_build_object('stage_head',d->>'last_batch_sha256') THEN RETURN false; END IF;
    SELECT * INTO session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
    IF NOT FOUND OR session.ingestion_id::text IS DISTINCT FROM d->>'ingestion_id' THEN RETURN false; END IF;
    SELECT * INTO candidate FROM rkb_v4.source_stage_candidates WHERE source_id=source;
    IF FOUND THEN RETURN candidate.seal_bytes=p; END IF;
    IF EXISTS (SELECT 1 FROM rkb_v4.sources WHERE source_id=source) THEN RETURN false; END IF;
    SELECT * INTO tail FROM rkb_v4.source_stage_batches WHERE source_id=source ORDER BY batch_index DESC LIMIT 1;
    -- This pins the exact current chain, NOT complete original-page coverage or
    -- completed research. A later activation must validate explicit coverage.
    RETURN FOUND AND tail.batch_index+1=(d->>'batch_count')::integer
        AND tail.payload_sha256=d->>'last_batch_sha256';
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_stage_seal_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; source uuid; seq bigint;
BEGIN
    SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    IF NOT FOUND OR NOT rkb_v4.check_source_stage_seal_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'exact inactive candidate seal precondition required'; END IF;
    d:=convert_from(p,'UTF8')::jsonb; source:=(ids->>'source_id')::uuid;
    INSERT INTO rkb_v4.source_stage_candidates VALUES(source,(d->>'ingestion_id')::uuid,
        (d->>'batch_count')::integer,d->>'last_batch_sha256',p,encode(sha256(p),'hex'),logical_time,seq)
    ON CONFLICT(source_id) DO NOTHING;
    RETURN convert_to(jsonb_build_object('source_id',source,'ingestion_id',d->>'ingestion_id',
        'batch_count',(d->>'batch_count')::integer,'last_batch_sha256',d->>'last_batch_sha256',
        'seal_sha256',encode(sha256(p),'hex'),'candidate_state','sealed_inactive',
        'logical_time',logical_time)::text,'UTF8');
END $$;

REVOKE ALL ON rkb_v4.source_stage_sessions,rkb_v4.source_stage_batches,
    rkb_v4.source_stage_pages,rkb_v4.source_stage_candidates FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_stage_v1(bytea,jsonb,jsonb,text),
    rkb_v4.apply_source_stage_v1(bytea,jsonb,jsonb,text),
    rkb_v4.check_source_stage_seal_v1(bytea,jsonb,jsonb,text),
    rkb_v4.apply_source_stage_seal_v1(bytea,jsonb,jsonb,text) FROM PUBLIC;
