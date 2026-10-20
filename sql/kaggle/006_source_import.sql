-- Immutable ordered expansion after 001..005. Fresh genesis pins all six files.
-- Existing initialized generations require their ordered maintenance barrier;
-- this artifact is never implicitly applied while adopting an owned database.
CREATE TABLE rkb_v4.source_import_origins (
    source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
    revision bigint NOT NULL CHECK (revision=1),
    work_id uuid NOT NULL,
    edition_id uuid NOT NULL,
    original_sha256 text NOT NULL CHECK (original_sha256 ~ '^[0-9a-f]{64}$'),
    original_size bigint NOT NULL CHECK (original_size>0),
    original_ref text NOT NULL CHECK (length(original_ref) BETWEEN 1 AND 2048),
    original_receipt_sha256 text NOT NULL CHECK (original_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    model_revision text NOT NULL,
    embedding_space text NOT NULL,
    tokenizer_sha256 text NOT NULL,
    imported_logical_time text NOT NULL,
    FOREIGN KEY (source_id,revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.source_research_frontier (
    source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
    revision bigint NOT NULL,
    frontier_id uuid NOT NULL UNIQUE,
    state text NOT NULL CHECK (state IN ('pending','complete')),
    continuation jsonb NOT NULL CHECK (jsonb_typeof(continuation)='object'
        AND octet_length(continuation::text)<=65536
        AND (state<>'pending' OR continuation<>'{}'::jsonb)),
    FOREIGN KEY (source_id,revision) REFERENCES rkb_v4.revisions
);
CREATE TRIGGER immutable_source_origin BEFORE UPDATE OR DELETE ON rkb_v4.source_import_origins
    FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.check_source_import_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb; original jsonb; model jsonb; passages jsonb; item jsonb; research jsonb;
        source uuid; ordinal_index integer:=0; seen uuid[]:='{}'; passage uuid; vec public.vector;
BEGIN
    IF octet_length(p) NOT BETWEEN 1 AND 786432 OR logical_time IS NULL
       OR octet_length(logical_time) NOT BETWEEN 1 AND 256
       OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id')
       OR expected IS DISTINCT FROM '{"absent_source":"true"}'::jsonb THEN RETURN false; END IF;
    source:=(ids->>'source_id')::uuid;
    IF source IS NULL OR source::text<>ids->>'source_id'
       OR EXISTS (SELECT 1 FROM rkb_v4.sources WHERE source_id=source) THEN RETURN false; END IF;
    doc:=convert_from(p,'UTF8')::jsonb;
    IF jsonb_typeof(doc) IS DISTINCT FROM 'object'
       OR NOT doc ?& ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','research']
       OR doc-ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','research']<>'{}'::jsonb
       OR doc->>'codec' IS DISTINCT FROM 'rkb.source-import.v1'
       OR doc->'revision' IS DISTINCT FROM '1'::jsonb
       OR jsonb_typeof(doc->'bibliography') IS DISTINCT FROM 'string'
       OR length(doc->>'bibliography') NOT BETWEEN 1 AND 8000
       OR (doc->>'work_id')::uuid::text<>doc->>'work_id'
       OR (doc->>'edition_id')::uuid::text<>doc->>'edition_id'
       OR jsonb_typeof(doc->'work_id') IS DISTINCT FROM 'string'
       OR jsonb_typeof(doc->'edition_id') IS DISTINCT FROM 'string' THEN RETURN false; END IF;
    original:=doc->'original'; model:=doc->'model'; passages:=doc->'passages'; research:=doc->'research';
    IF jsonb_typeof(original) IS DISTINCT FROM 'object'
       OR NOT original ?& ARRAY['sha256','size','ref','receipt_sha256']
       OR original-ARRAY['sha256','size','ref','receipt_sha256']<>'{}'::jsonb
       OR jsonb_typeof(original->'sha256') IS DISTINCT FROM 'string'
       OR jsonb_typeof(original->'receipt_sha256') IS DISTINCT FROM 'string'
       OR original->>'sha256' !~ '^[0-9a-f]{64}$' OR original->>'receipt_sha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(original->'size') IS DISTINCT FROM 'number'
       OR original->>'size' !~ '^[1-9][0-9]{0,18}$' OR (original->>'size')::bigint<1
       OR jsonb_typeof(original->'ref') IS DISTINCT FROM 'string'
       OR length(original->>'ref') NOT BETWEEN 1 AND 2048
       OR original->>'ref' !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*$'
       OR strpos(original->>'ref','://')<>0 THEN RETURN false; END IF;
    IF model IS DISTINCT FROM jsonb_build_object(
        'revision','5617a9f61b028005a4858fdac845db406aefb181',
        'space','bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1',
        'tokenizer_sha256','21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08',
        'dimension',1024,'max_tokens',512)
       OR jsonb_typeof(passages) IS DISTINCT FROM 'array'
       OR jsonb_array_length(passages) NOT BETWEEN 1 AND 100 THEN RETURN false; END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(passages) LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'object'
           OR NOT item ?& ARRAY['passage_id','ordinal','locator','body','language','embedding','input_tokens']
           OR item-ARRAY['passage_id','ordinal','locator','body','language','embedding','input_tokens']<>'{}'::jsonb
           OR jsonb_typeof(item->'passage_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'ordinal') IS DISTINCT FROM 'number'
           OR item->>'ordinal' !~ '^(0|[1-9][0-9]{0,2})$'
           OR (item->>'ordinal')::integer<>ordinal_index
           OR jsonb_typeof(item->'locator') IS DISTINCT FROM 'string'
           OR length(item->>'locator') NOT BETWEEN 1 AND 1000
           OR jsonb_typeof(item->'body') IS DISTINCT FROM 'string'
           OR length(item->>'body') NOT BETWEEN 1 AND 16000
           OR item->>'language' NOT IN ('ru','de','simple')
           OR jsonb_typeof(item->'language') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'input_tokens') IS DISTINCT FROM 'number'
           OR item->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
           OR (item->>'input_tokens')::integer NOT BETWEEN 1 AND 512
           OR jsonb_typeof(item->'embedding') IS DISTINCT FROM 'array'
           OR jsonb_array_length(item->'embedding')<>1024 THEN RETURN false; END IF;
        passage:=(item->>'passage_id')::uuid;
        IF passage::text<>item->>'passage_id' OR passage=ANY(seen)
           OR EXISTS (SELECT 1 FROM rkb_v4.passage_search WHERE passage_id=passage)
           OR EXISTS (SELECT 1 FROM jsonb_array_elements(item->'embedding') AS elements(value)
               WHERE jsonb_typeof(value) IS DISTINCT FROM 'number'
                  OR (value::text)::double precision IS DISTINCT FROM (value::text)::real::double precision)
           THEN RETURN false; END IF;
        vec:=(item->'embedding')::text::public.vector;
        IF public.vector_dims(vec)<>1024 OR abs(public.vector_norm(vec)-1.0)>0.002 THEN RETURN false; END IF;
        seen:=array_append(seen,passage); ordinal_index:=ordinal_index+1;
    END LOOP;
    IF jsonb_typeof(research) IS DISTINCT FROM 'object'
       OR NOT research ?& ARRAY['frontier_id','state','continuation']
       OR research-ARRAY['frontier_id','state','continuation']<>'{}'::jsonb
       OR jsonb_typeof(research->'frontier_id') IS DISTINCT FROM 'string'
       OR (research->>'frontier_id')::uuid::text<>research->>'frontier_id'
       OR research->>'state' NOT IN ('pending','complete')
       OR jsonb_typeof(research->'state') IS DISTINCT FROM 'string'
       OR jsonb_typeof(research->'continuation') IS DISTINCT FROM 'object'
       OR octet_length((research->'continuation')::text)>65536
       OR (research->>'state'='pending' AND research->'continuation'='{}'::jsonb)
       OR EXISTS (SELECT 1 FROM rkb_v4.source_research_frontier
           WHERE frontier_id=(research->>'frontier_id')::uuid) THEN RETURN false; END IF;
    RETURN true;
EXCEPTION WHEN data_exception THEN
    RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_import_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_import_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'source import requires exact resolved new-source precondition';
    END IF;
    source:=(ids->>'source_id')::uuid; doc:=convert_from(p,'UTF8')::jsonb;
    research:=doc->'research'; original:=doc->'original';
    INSERT INTO rkb_v4.sources VALUES (source,doc->>'bibliography',true,false);
    INSERT INTO rkb_v4.revisions VALUES (source,1);
    INSERT INTO rkb_v4.source_import_origins VALUES (source,1,(doc->>'work_id')::uuid,
        (doc->>'edition_id')::uuid,original->>'sha256',(original->>'size')::bigint,
        original->>'ref',original->>'receipt_sha256',encode(sha256(p),'hex'),
        doc->'model'->>'revision',doc->'model'->>'space',doc->'model'->>'tokenizer_sha256',logical_time);
    INSERT INTO rkb_v4.source_research_frontier VALUES (source,1,(research->>'frontier_id')::uuid,
        research->>'state',research->'continuation');
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
        INSERT INTO rkb_v4.passage_search(passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
        VALUES ((item->>'passage_id')::uuid,source,1,(item->>'ordinal')::integer,item->>'locator',item->>'body',
            item->>'language',CASE item->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
                WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (item->'embedding')::text::public.vector);
    END LOOP;
    -- Activation happens after every exact passage and its research continuation
    -- exist. All source/projection/receipt/frontier work commits or rolls back.
    INSERT INTO rkb_v4.active_revision VALUES (source,1);
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
    SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=source;
    PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
        frontier.visibility_epoch,state.applied_seq+1,
        prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
        prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
    RETURN convert_to(jsonb_build_object('source_id',source,'content_revision',1,'accepted',true,
        'withdrawn',false,'eligibility_revision',eligibility,'visibility_epoch',frontier.visibility_epoch,
        'passage_count',jsonb_array_length(doc->'passages'),'payload_sha256',encode(sha256(p),'hex'),
        'research_frontier_id',research->>'frontier_id','research_state',research->>'state',
        'logical_time',logical_time)::text,'UTF8');
END $$;

REVOKE ALL ON rkb_v4.source_import_origins,rkb_v4.source_research_frontier FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_import_v1(bytea,jsonb,jsonb,text),
    rkb_v4.apply_source_import_v1(bytea,jsonb,jsonb,text) FROM PUBLIC;
