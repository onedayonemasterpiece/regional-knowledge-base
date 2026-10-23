-- The current native compiler owns the JSON model. Keep persistence identities,
-- source/model integrity and text/vector limits here, not a second closed schema.
-- Frozen whole-book validators and migrations001-028 remain unchanged.
CREATE OR REPLACE FUNCTION rkb_v4.check_source_batch_base_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;original jsonb;model jsonb;item jsonb;source uuid;
 ident uuid;seen uuid[]:='{}';ordinal_index integer:=0;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 67108864 OR logical_time IS NULL
  OR octet_length(logical_time) NOT BETWEEN 1 AND 256
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id')
  OR expected IS DISTINCT FROM '{"absent_source":"true"}'::jsonb THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;
 IF source IS NULL OR source::text<>ids->>'source_id' THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-base.v1'
  OR doc->'revision' IS DISTINCT FROM '1'::jsonb
  OR jsonb_typeof(doc->'bibliography') IS DISTINCT FROM 'string'
  OR NOT coalesce(length(doc->>'bibliography') BETWEEN 1 AND 8000,false)
  OR (doc->>'work_id')::uuid IS NULL OR (doc->>'edition_id')::uuid IS NULL THEN RETURN false;END IF;
 original:=doc->'original';model:=doc->'model';
 -- These are the immutable original and model identities, not quote matching.
 IF NOT coalesce(original->>'sha256' ~ '^[0-9a-f]{64}$',false)
  OR NOT coalesce(original->>'receipt_sha256' ~ '^[0-9a-f]{64}$',false)
  OR NOT coalesce((original->>'size')::bigint>0,false)
  OR NOT coalesce(length(original->>'ref') BETWEEN 1 AND 2048,false)
  OR NOT coalesce(original->>'ref' ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*$',false)
  OR strpos(original->>'ref','://')<>0
  OR model->>'revision' IS DISTINCT FROM '5617a9f61b028005a4858fdac845db406aefb181'
  OR model->>'space' IS DISTINCT FROM 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'
  OR model->>'tokenizer_sha256' IS DISTINCT FROM '21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08'
  OR model->>'dimension' IS DISTINCT FROM '1024' OR model->>'max_tokens' IS DISTINCT FROM '512'
  OR jsonb_typeof(doc->'passages') IS DISTINCT FROM 'array'
  OR jsonb_array_length(doc->'passages')>250000 THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
  ident:=(item->>'passage_id')::uuid;
  IF ident IS NULL OR ident=ANY(seen)
   OR (item->>'ordinal')::integer IS DISTINCT FROM ordinal_index
   OR jsonb_typeof(item->'locator') IS DISTINCT FROM 'string'
   OR NOT coalesce(length(item->>'locator') BETWEEN 1 AND 1000,false)
   OR jsonb_typeof(item->'body') IS DISTINCT FROM 'string'
   OR NOT coalesce(length(item->>'body') BETWEEN 1 AND 16000,false)
   OR NOT coalesce(item->>'language' ~ '^[a-z][a-z0-9-]{1,23}$',false)
   OR NOT coalesce((item->>'input_tokens')::integer BETWEEN 1 AND 512,false)
   THEN RETURN false;END IF;
  seen:=array_append(seen,ident);ordinal_index:=ordinal_index+1;
 END LOOP;
 -- The caller already compares research byte-for-byte with the accepted stage;
 -- its SQL projection must still address a real, explicitly selected frontier.
 IF (doc->'research'->>'frontier_id')::uuid IS NULL THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
