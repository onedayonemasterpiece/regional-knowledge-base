-- English and other ISO-like source language codes retain provenance in
-- passage_search while existing RU/DE configurations and the simple fallback stay fixed.
-- Additive migration: SQL001-026 and already accepted commands are immutable.
ALTER TABLE rkb_v4.passage_search DROP CONSTRAINT passage_search_language_check;
ALTER TABLE rkb_v4.passage_search
 ADD CONSTRAINT passage_search_language_check
 CHECK (language ~ '^[a-z][a-z0-9-]{1,23}$');

CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_base_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb; original jsonb; model jsonb; passages jsonb; item jsonb; research jsonb;
        source uuid; ordinal_index integer:=0; seen uuid[]:='{}'; passage uuid; vec public.vector;
BEGIN
    IF octet_length(p) NOT BETWEEN 1 AND 67108864 OR logical_time IS NULL
       OR octet_length(logical_time) NOT BETWEEN 1 AND 256
       OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id')
       OR expected IS DISTINCT FROM '{"absent_source":"true"}'::jsonb THEN RETURN false; END IF;
    source:=(ids->>'source_id')::uuid;
    IF source IS NULL OR source::text<>ids->>'source_id' THEN RETURN false; END IF;
    doc:=convert_from(p,'UTF8')::jsonb;
    IF jsonb_typeof(doc) IS DISTINCT FROM 'object'
       OR NOT doc ?& ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','research']
       OR doc-ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','research']<>'{}'::jsonb
       OR doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-base.v1'
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
       OR jsonb_array_length(passages) NOT BETWEEN 0 AND 250000 THEN RETURN false; END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(passages) LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'object'
           OR NOT item ?& ARRAY['passage_id','ordinal','locator','body','language','input_tokens']
           OR item-ARRAY['passage_id','ordinal','locator','body','language','input_tokens']<>'{}'::jsonb
           OR jsonb_typeof(item->'passage_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'ordinal') IS DISTINCT FROM 'number'
           OR item->>'ordinal' !~ '^(0|[1-9][0-9]{0,5})$'
           OR (item->>'ordinal')::integer<>ordinal_index
           OR jsonb_typeof(item->'locator') IS DISTINCT FROM 'string'
           OR length(item->>'locator') NOT BETWEEN 1 AND 1000
           OR jsonb_typeof(item->'body') IS DISTINCT FROM 'string'
           OR length(item->>'body') NOT BETWEEN 1 AND 16000
           OR item->>'language' !~ '^[a-z][a-z0-9-]{1,23}
           OR jsonb_typeof(item->'language') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'input_tokens') IS DISTINCT FROM 'number'
           OR item->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
           OR (item->>'input_tokens')::integer NOT BETWEEN 1 AND 512
           THEN RETURN false; END IF;
        passage:=(item->>'passage_id')::uuid;
        IF passage::text<>item->>'passage_id' OR passage=ANY(seen)
           THEN RETURN false; END IF;
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
       OR (research->>'state'='pending' AND research->'continuation'='{}'::jsonb) THEN RETURN false; END IF;
    RETURN true;
EXCEPTION WHEN data_exception THEN
    RETURN false;
END $$;


           OR jsonb_typeof(item->'language') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'input_tokens') IS DISTINCT FROM 'number'
           OR item->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
           OR (item->>'input_tokens')::integer NOT BETWEEN 1 AND 512
           THEN RETURN false; END IF;
        passage:=(item->>'passage_id')::uuid;
        IF passage::text<>item->>'passage_id' OR passage=ANY(seen)
           THEN RETURN false; END IF;
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
       OR (research->>'state'='pending' AND research->'continuation'='{}'::jsonb) THEN RETURN false; END IF;
    RETURN true;
EXCEPTION WHEN data_exception THEN
    RETURN false;
END $$;
