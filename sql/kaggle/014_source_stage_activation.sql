-- Explicit native source-image activation revision. Existing v1-v4 import,
-- manifest, graph, receipt and replay functions remain byte-for-byte untouched.
-- Exact accepted batches are canonical inputs; PG-local derived vectors never
-- enter the activation command or canonical knowledge/outcome digest.
CREATE TABLE rkb_v4.source_activation_candidates (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_stage_sessions,
 ingestion_id uuid NOT NULL, batch_count integer NOT NULL, last_batch_sha256 text NOT NULL,
 materializer_sha256 text NOT NULL CHECK(materializer_sha256 ~ '^[0-9a-f]{64}$'),
 knowledge_bytes bytea NOT NULL CHECK(octet_length(knowledge_bytes) BETWEEN 1 AND 33554432),
 knowledge_sha256 text NOT NULL CHECK(knowledge_sha256=encode(sha256(knowledge_bytes),'hex')),
 knowledge jsonb NOT NULL CHECK(knowledge=convert_from(knowledge_bytes,'UTF8')::jsonb),
 total_inputs integer GENERATED ALWAYS AS (jsonb_array_length(knowledge->'passages')+jsonb_array_length(knowledge->'semantic'->'stories')) STORED,
 prepared_database text NOT NULL, prepared_writer text NOT NULL
);
CREATE TABLE rkb_v4.source_image_origins (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
 ingestion_id uuid NOT NULL, batch_count integer NOT NULL, last_batch_sha256 text NOT NULL,
 materializer_sha256 text NOT NULL, knowledge_sha256 text NOT NULL,
 activation_payload_sha256 text NOT NULL,
 operation_key text NOT NULL REFERENCES rkb_v4.applied_ops DEFERRABLE INITIALLY DEFERRED,
 sequence bigint NOT NULL, person_id uuid NOT NULL, request_id text NOT NULL,
 command_sha256 text NOT NULL
);
CREATE TRIGGER immutable_source_activation_candidate BEFORE UPDATE OR DELETE ON rkb_v4.source_activation_candidates
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_image_origin BEFORE UPDATE OR DELETE ON rkb_v4.source_image_origins
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.source_activation_knowledge_v1(doc jsonb) RETURNS jsonb
 LANGUAGE sql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT jsonb_set(jsonb_set(doc,'{passages}',(SELECT coalesce(jsonb_agg(x-'embedding' ORDER BY n),'[]'::jsonb)
  FROM jsonb_array_elements(doc->'passages') WITH ORDINALITY p(x,n))),'{semantic,stories}',
  (SELECT coalesce(jsonb_agg(jsonb_set(x,'{default_variant}',(x->'default_variant')-'embedding') ORDER BY n),'[]'::jsonb)
   FROM jsonb_array_elements(doc->'semantic'->'stories') WITH ORDINALITY s(x,n)))
$$;

CREATE FUNCTION rkb_v4.verify_source_stage_chain_v1(wanted uuid,ingestion uuid,batches integer,tail text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE seal rkb_v4.source_stage_candidates; row record; c jsonb; seal_command jsonb;
 n integer:=0; previous text:=repeat('0',64); previous_seq bigint:=0;
BEGIN
 SELECT * INTO STRICT seal FROM rkb_v4.source_stage_candidates WHERE source_id=wanted;
 IF seal.ingestion_id<>ingestion OR seal.batch_count<>batches OR seal.last_batch_sha256<>tail
  OR seal.seal_sha256<>encode(sha256(seal.seal_bytes),'hex') THEN RETURN false;END IF;
 SELECT convert_from(command_bytes,'UTF8')::jsonb INTO STRICT seal_command FROM rkb_v4.applied_ops
  WHERE sequence=seal.accepted_sequence AND status='applied' AND command_sha256=encode(sha256(command_bytes),'hex');
 IF seal_command->>'route'<>'source.stage.seal.v1' OR seal_command->'ids'->>'source_id'<>wanted::text
  OR decode(seal_command->>'payload_b64','base64')<>seal.seal_bytes THEN RETURN false;END IF;
 FOR row IN SELECT b.*,a.command_bytes,a.command_sha256,a.status FROM rkb_v4.source_stage_batches b
  JOIN rkb_v4.applied_ops a ON a.sequence=b.accepted_sequence WHERE b.source_id=wanted ORDER BY b.batch_index LOOP
  c:=convert_from(row.command_bytes,'UTF8')::jsonb;
  IF row.batch_index<>n OR row.previous_batch_sha256<>previous OR row.status<>'applied'
   OR row.payload_sha256<>encode(sha256(row.payload_bytes),'hex') OR row.command_sha256<>encode(sha256(row.command_bytes),'hex')
   OR c->>'route'<>'source.stage.v1' OR c->'ids'->>'source_id'<>wanted::text
   OR c->>'person_id' IS DISTINCT FROM seal_command->>'person_id'
   OR decode(c->>'payload_b64','base64')<>row.payload_bytes
   OR row.accepted_sequence<=previous_seq OR row.accepted_sequence>=seal.accepted_sequence THEN RETURN false;END IF;
  previous:=row.payload_sha256;previous_seq:=row.accepted_sequence;n:=n+1;
 END LOOP;
 RETURN n=batches AND previous=tail;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

-- Validates the exact current accepted chain before irreversible sealing.
-- Also works after a sealed candidate is restored into a new writer generation.
CREATE FUNCTION rkb_v4.verify_source_open_stage_chain_v1(wanted uuid,ingestion uuid,batches integer,tail text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE row record; c jsonb; actor text; n integer:=0; previous text:=repeat('0',64); previous_seq bigint:=0;
BEGIN
 IF batches NOT BETWEEN 1 AND 2500 OR tail !~ '^[0-9a-f]{64}$'
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_stage_sessions WHERE source_id=wanted AND ingestion_id=ingestion)
 THEN RETURN false;END IF;
 FOR row IN SELECT b.*,a.command_bytes,a.command_sha256,a.status FROM rkb_v4.source_stage_batches b
  JOIN rkb_v4.applied_ops a ON a.sequence=b.accepted_sequence WHERE b.source_id=wanted ORDER BY b.batch_index LOOP
  c:=convert_from(row.command_bytes,'UTF8')::jsonb;
  IF n=0 THEN actor:=c->>'person_id';END IF;
  IF row.batch_index<>n OR row.previous_batch_sha256<>previous OR row.status<>'applied'
   OR row.payload_sha256<>encode(sha256(row.payload_bytes),'hex') OR row.command_sha256<>encode(sha256(row.command_bytes),'hex')
   OR c->>'route'<>'source.stage.v1' OR c->'ids'->>'source_id'<>wanted::text OR c->>'person_id' IS DISTINCT FROM actor
   OR decode(c->>'payload_b64','base64')<>row.payload_bytes
   OR (convert_from(row.payload_bytes,'UTF8')::jsonb->>'ingestion_id') IS DISTINCT FROM ingestion::text
   OR row.accepted_sequence<=previous_seq THEN RETURN false;END IF;
  previous:=row.payload_sha256;previous_seq:=row.accepted_sequence;n:=n+1;
 END LOOP;
 RETURN n=batches AND previous=tail AND n=(SELECT count(*) FROM rkb_v4.source_stage_batches WHERE source_id=wanted);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

-- Schema14 freezes appends only after complete native validation succeeded.
-- Invalid compilation has no candidate side effect and remains correctable.
CREATE FUNCTION rkb_v4.check_source_stage_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE source uuid; d jsonb;
BEGIN
 IF NOT rkb_v4.check_source_stage_v1(p,ids,expected,logical_time) THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.source_activation_candidates WHERE source_id=source)
  OR EXISTS(SELECT 1 FROM rkb_v4.source_stage_batches WHERE source_id=source
      AND batch_index=(d->>'batch_index')::integer AND payload_bytes=p);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_stage_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_stage_v2(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'validated native candidate no longer accepts new stage batches';END IF;
 RETURN rkb_v4.apply_source_stage_v1(p,ids,expected,logical_time);
END $$;
CREATE FUNCTION rkb_v4.check_source_stage_seal_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE source uuid; d jsonb;
BEGIN
 IF NOT rkb_v4.check_source_stage_seal_v1(p,ids,expected,logical_time) THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 RETURN EXISTS(SELECT 1 FROM rkb_v4.source_activation_candidates k WHERE k.source_id=source
   AND k.ingestion_id::text=d->>'ingestion_id' AND k.batch_count=(d->>'batch_count')::integer
   AND k.last_batch_sha256=d->>'last_batch_sha256')
  AND rkb_v4.verify_source_open_stage_chain_v1(source,(d->>'ingestion_id')::uuid,
      (d->>'batch_count')::integer,d->>'last_batch_sha256');
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_stage_seal_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_stage_seal_v2(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'complete validated native knowledge required before seal';END IF;
 RETURN rkb_v4.apply_source_stage_seal_v1(p,ids,expected,logical_time);
END $$;

CREATE FUNCTION rkb_v4.check_source_activation_base_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb; original jsonb; model jsonb; passages jsonb; item jsonb; research jsonb;
        source uuid; ordinal_index integer:=0; seen uuid[]:='{}'; passage uuid; vec public.vector;
BEGIN
    IF octet_length(p) NOT BETWEEN 1 AND 67108864 OR logical_time IS NULL
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
       OR jsonb_array_length(passages) NOT BETWEEN 1 AND 250000 THEN RETURN false; END IF;
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
           OR item->>'language' NOT IN ('ru','de','simple')
           OR jsonb_typeof(item->'language') IS DISTINCT FROM 'string'
           OR jsonb_typeof(item->'input_tokens') IS DISTINCT FROM 'number'
           OR item->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
           OR (item->>'input_tokens')::integer NOT BETWEEN 1 AND 512
           THEN RETURN false; END IF;
        passage:=(item->>'passage_id')::uuid;
        IF passage::text<>item->>'passage_id' OR passage=ANY(seen)
           OR EXISTS (SELECT 1 FROM rkb_v4.passage_search WHERE passage_id=passage)
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
       OR (research->>'state'='pending' AND research->'continuation'='{}'::jsonb)
       OR EXISTS (SELECT 1 FROM rkb_v4.source_research_frontier
           WHERE frontier_id=(research->>'frontier_id')::uuid) THEN RETURN false; END IF;
    RETURN true;
EXCEPTION WHEN data_exception THEN
    RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_activation_semantic_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;sem jsonb;item jsonb;e jsonb;r jsonb;story jsonb;v jsonb;b jsonb;f jsonb;
 base bytea;regions uuid[]:='{}';findings uuid[]:='{}';stories uuid[]:='{}';variants uuid[]:='{}';
 candidates text[]:='{}';pending text[]:='{}';covered uuid[]:='{}';refs uuid[];
 ident uuid;region uuid;passage uuid;begin_offset integer;end_offset integer;body_end integer;vector public.vector;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 67108864 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-semantic.v1' OR NOT doc?'semantic' THEN RETURN false;END IF;
 base:=convert_to(((doc-'semantic')||'{"codec":"rkb.source-activation-base.v1"}'::jsonb)::text,'UTF8');
 IF NOT rkb_v4.check_source_activation_base_v1(base,ids,expected,logical_time) THEN RETURN false;END IF;
 sem:=doc->'semantic';
 IF jsonb_typeof(sem) IS DISTINCT FROM 'object' OR NOT sem ?& ARRAY['regions','findings','stories','pending']
    OR sem-ARRAY['regions','findings','stories','pending']<>'{}'::jsonb
    OR jsonb_typeof(sem->'regions') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'regions') NOT BETWEEN 1 AND 1250000
    OR jsonb_typeof(sem->'findings') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'findings')>30000
    OR jsonb_typeof(sem->'stories') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'stories')>30000
    OR jsonb_typeof(sem->'pending') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'pending')>30000 THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(sem->'regions') LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object' OR NOT item ?& ARRAY['region_id','page_id','source_text','source_text_sha256']
     OR item-ARRAY['region_id','page_id','source_text','source_text_sha256']<>'{}'::jsonb
     OR jsonb_typeof(item->'region_id') IS DISTINCT FROM 'string' OR jsonb_typeof(item->'page_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(item->'source_text') IS DISTINCT FROM 'string' OR length(item->>'source_text')>8000
     OR item->>'source_text_sha256' IS DISTINCT FROM encode(sha256(convert_to(item->>'source_text','UTF8')),'hex')
     OR (item->>'page_id')::uuid::text<>item->>'page_id' THEN RETURN false;END IF;
  ident:=(item->>'region_id')::uuid;
  IF ident::text<>item->>'region_id' OR ident=ANY(regions) OR EXISTS(SELECT 1 FROM rkb_v4.source_regions WHERE region_id=ident) THEN RETURN false;END IF;
  regions:=array_append(regions,ident);
 END LOOP;
 FOR item IN SELECT value FROM jsonb_array_elements(sem->'findings') LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object'
     OR NOT item ?& ARRAY['assertion_id','candidate_key','proposition','account_kind','attributed_to','reported_by','evidence','event_dates','observations','entity_keys']
     OR item-ARRAY['assertion_id','candidate_key','proposition','account_kind','attributed_to','reported_by','evidence','event_dates','observations','entity_keys']<>'{}'::jsonb
     OR item->>'candidate_key' !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$'
     OR item->>'candidate_key'=ANY(candidates) OR jsonb_typeof(item->'proposition') IS DISTINCT FROM 'string'
     OR length(item->>'proposition') NOT BETWEEN 1 AND 500
     OR item->>'account_kind' NOT IN ('legend','tradition','rumor','recollection','testimony','other')
     OR jsonb_typeof(item->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'evidence') NOT BETWEEN 1 AND 4
     OR jsonb_typeof(item->'event_dates') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'event_dates')>8
     OR jsonb_typeof(item->'observations') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'observations')>8
     OR jsonb_typeof(item->'entity_keys') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'entity_keys')>32 THEN RETURN false;END IF;
  ident:=(item->>'assertion_id')::uuid;
  IF ident::text<>item->>'assertion_id' OR ident=ANY(findings)
     OR EXISTS(SELECT 1 FROM rkb_v4.story_assertion_versions WHERE assertion_id=ident) THEN RETURN false;END IF;
  FOR e IN SELECT value FROM jsonb_array_elements(item->'evidence') LOOP
   IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR NOT e ?& ARRAY['region_id','passage_id','start','end','original_excerpt']
      OR e-ARRAY['region_id','passage_id','start','end','original_excerpt']<>'{}'::jsonb
      OR jsonb_typeof(e->'start') IS DISTINCT FROM 'number' OR e->>'start' !~ '^(0|[1-9][0-9]{0,7})$'
      OR jsonb_typeof(e->'end') IS DISTINCT FROM 'number' OR e->>'end' !~ '^[1-9][0-9]{0,7}$'
      OR jsonb_typeof(e->'original_excerpt') IS DISTINCT FROM 'string' OR length(e->>'original_excerpt') NOT BETWEEN 1 AND 500 THEN RETURN false;END IF;
   region:=(e->>'region_id')::uuid;passage:=(e->>'passage_id')::uuid;
   SELECT value INTO r FROM jsonb_array_elements(sem->'regions') WHERE value->>'region_id'=region::text;
   SELECT value INTO f FROM jsonb_array_elements(doc->'passages') WHERE value->>'passage_id'=passage::text;
   begin_offset:=(e->>'start')::integer;end_offset:=(e->>'end')::integer;
   IF region::text<>e->>'region_id' OR passage::text<>e->>'passage_id' OR r IS NULL OR f IS NULL
      OR begin_offset>=end_offset OR end_offset>length(r->>'source_text')
      OR substring(r->>'source_text' FROM begin_offset+1 FOR end_offset-begin_offset) IS DISTINCT FROM e->>'original_excerpt'
      OR strpos(f->>'body',e->>'original_excerpt')=0 THEN RETURN false;END IF;
  END LOOP;
  findings:=array_append(findings,ident);candidates:=array_append(candidates,item->>'candidate_key');
 END LOOP;
 FOR story IN SELECT value FROM jsonb_array_elements(sem->'stories') LOOP
  IF jsonb_typeof(story) IS DISTINCT FROM 'object' OR NOT story ?& ARRAY['story_id','finding_ids','title','summary','material_type','default_variant']
     OR story-ARRAY['story_id','finding_ids','title','summary','material_type','default_variant']<>'{}'::jsonb
     OR jsonb_typeof(story->'title') IS DISTINCT FROM 'string' OR length(story->>'title') NOT BETWEEN 1 AND 250
     OR jsonb_typeof(story->'summary') IS DISTINCT FROM 'string' OR length(story->>'summary') NOT BETWEEN 1 AND 1000
     OR story->>'material_type' NOT IN ('event_narrative','person_episode','place_biography','explanation','comparison','everyday_life','mystery','conflicting_accounts','other')
     OR jsonb_typeof(story->'finding_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(story->'finding_ids') NOT BETWEEN 1 AND 12 THEN RETURN false;END IF;
  ident:=(story->>'story_id')::uuid;
  IF ident::text<>story->>'story_id' OR ident=ANY(stories) OR EXISTS(SELECT 1 FROM rkb_v4.stories WHERE story_id=ident) THEN RETURN false;END IF;
  SELECT array_agg(value::uuid) INTO refs FROM jsonb_array_elements_text(story->'finding_ids');
  IF NOT refs<@findings OR cardinality(refs)<>(SELECT count(DISTINCT x) FROM unnest(refs) x) THEN RETURN false;END IF;
  covered:=covered||refs;v:=story->'default_variant';
  IF jsonb_typeof(v) IS DISTINCT FROM 'object' OR NOT v ?& ARRAY['variant_id','revision','audience','format','language','body','blocks','input_tokens']
     OR v-ARRAY['variant_id','revision','audience','format','language','body','blocks','input_tokens']<>'{}'::jsonb
     OR v->'revision' IS DISTINCT FROM '1'::jsonb OR v->>'format' IS DISTINCT FROM 'story'
     OR jsonb_typeof(v->'audience') IS DISTINCT FROM 'string' OR length(v->>'audience') NOT BETWEEN 1 AND 500
     OR jsonb_typeof(v->'language') IS DISTINCT FROM 'string' OR length(v->>'language') NOT BETWEEN 2 AND 24
     OR jsonb_typeof(v->'body') IS DISTINCT FROM 'string' OR length(v->>'body') NOT BETWEEN 1 AND 4000
     OR jsonb_typeof(v->'blocks') IS DISTINCT FROM 'array' OR jsonb_array_length(v->'blocks') NOT BETWEEN 1 AND 25
     OR jsonb_typeof(v->'input_tokens') IS DISTINCT FROM 'number' OR v->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
     OR (v->>'input_tokens')::integer NOT BETWEEN 1 AND 512 THEN RETURN false;END IF;
  passage:=(v->>'variant_id')::uuid;
  IF passage::text<>v->>'variant_id' OR passage=ANY(variants)
     OR EXISTS(SELECT 1 FROM rkb_v4.story_variant_versions WHERE variant_id=passage)
 THEN RETURN false;END IF;
  body_end:=0;
  FOR b IN SELECT value FROM jsonb_array_elements(v->'blocks') LOOP
   IF jsonb_typeof(b) IS DISTINCT FROM 'object' OR NOT b ?& ARRAY['start','end','assertion_ids']
      OR b-ARRAY['start','end','assertion_ids']<>'{}'::jsonb
      OR jsonb_typeof(b->'start') IS DISTINCT FROM 'number' OR b->>'start' !~ '^(0|[1-9][0-9]{0,3})$'
      OR jsonb_typeof(b->'end') IS DISTINCT FROM 'number' OR b->>'end' !~ '^[1-9][0-9]{0,3}$'
      OR (b->>'start')::integer<>body_end OR (b->>'end')::integer<=body_end OR (b->>'end')::integer>length(v->>'body')
      OR jsonb_typeof(b->'assertion_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(b->'assertion_ids') NOT BETWEEN 1 AND 12 THEN RETURN false;END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(b->'assertion_ids') x WHERE NOT(x::uuid=ANY(refs)))
      OR (SELECT count(*)<>count(DISTINCT x) FROM jsonb_array_elements_text(b->'assertion_ids') x) THEN RETURN false;END IF;
   body_end:=(b->>'end')::integer;
  END LOOP;
  IF body_end<>length(v->>'body') THEN RETURN false;END IF;
  stories:=array_append(stories,ident);variants:=array_append(variants,passage);
 END LOOP;
 FOR item IN SELECT value FROM jsonb_array_elements(sem->'pending') LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object' OR NOT item ?& ARRAY['candidate_key','finding_ids','reason']
     OR item-ARRAY['candidate_key','finding_ids','reason']<>'{}'::jsonb OR item->>'reason' IS DISTINCT FROM 'needs_context'
     OR NOT(item->>'candidate_key'=ANY(candidates)) OR item->>'candidate_key'=ANY(pending)
     OR jsonb_typeof(item->'finding_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'finding_ids') NOT BETWEEN 1 AND 12
     OR doc->'research'->>'state' IS DISTINCT FROM 'pending' OR doc->'research'->'continuation'='{}'::jsonb THEN RETURN false;END IF;
  SELECT array_agg(value::uuid) INTO refs FROM jsonb_array_elements_text(item->'finding_ids');
  IF NOT refs<@findings OR refs&&covered OR cardinality(refs)<>(SELECT count(DISTINCT x) FROM unnest(refs) x)
     OR EXISTS(SELECT 1 FROM jsonb_array_elements(sem->'findings') f WHERE f->>'assertion_id'=ANY(refs::text[]) AND f->>'candidate_key'<>item->>'candidate_key') THEN RETURN false;END IF;
  pending:=array_append(pending,item->>'candidate_key');
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_image_passages_v1(doc jsonb) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'image_manifest';item jsonb;span jsonb;r jsonb;p jsonb;refs jsonb;
 seen text[]:='{}';selected text[];page_ids text[];parts text[];key text;begin_offset integer;end_offset integer;last_offsets jsonb;
BEGIN
 IF jsonb_typeof(m->'passages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'passages') NOT BETWEEN 1 AND 250000
 OR jsonb_array_length(m->'passages')<>jsonb_array_length(doc->'passages') THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(m->'passages') LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object'
   OR NOT item ?& ARRAY['passage_id','region_ids','footnote_region_ids','page_ids','source_spans']
   OR item-ARRAY['passage_id','region_ids','footnote_region_ids','page_ids','source_spans']<>'{}'::jsonb
   OR item->>'passage_id'=ANY(seen) THEN RETURN false;END IF;
  SELECT x INTO STRICT p FROM jsonb_array_elements(doc->'passages') x WHERE x->>'passage_id'=item->>'passage_id';
  seen:=array_append(seen,item->>'passage_id');selected:='{}';page_ids:='{}';parts:='{}';last_offsets:='{}';
  FOREACH key IN ARRAY ARRAY['region_ids','footnote_region_ids','page_ids'] LOOP
   refs:=item->key;
   IF jsonb_typeof(refs) IS DISTINCT FROM 'array' OR jsonb_array_length(refs)>(CASE key WHEN 'region_ids' THEN 100
    WHEN 'footnote_region_ids' THEN 50 ELSE 20 END)
    OR (SELECT count(DISTINCT v) FROM jsonb_array_elements(refs) v)<>jsonb_array_length(refs) THEN RETURN false;END IF;
  END LOOP;
  IF jsonb_array_length(item->'region_ids')=0 OR jsonb_array_length(item->'page_ids')=0
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(item->'region_ids') x WHERE item->'footnote_region_ids' @> jsonb_build_array(x))
   THEN RETURN false;END IF;
  FOR refs IN SELECT value FROM jsonb_array_elements((item->'region_ids')||(item->'footnote_region_ids')) LOOP
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
   IF NOT item->'page_ids' @> jsonb_build_array(r->'page_id') THEN RETURN false;END IF;
   page_ids:=array_append(page_ids,r->>'page_id');
  END LOOP;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(item->'page_ids') x WHERE NOT x=ANY(page_ids)) THEN RETURN false;END IF;
  IF jsonb_typeof(item->'source_spans') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'source_spans')>100 THEN RETURN false;END IF;
  FOR span IN SELECT value FROM jsonb_array_elements(item->'source_spans') LOOP
   IF jsonb_typeof(span) IS DISTINCT FROM 'object' OR NOT span ?& ARRAY['region_id','start','end','source_text_sha256']
    OR span-ARRAY['region_id','start','end','source_text_sha256']<>'{}'::jsonb
    OR NOT item->'region_ids' @> jsonb_build_array(span->'region_id')
    OR jsonb_typeof(span->'start') IS DISTINCT FROM 'number' OR span->>'start' !~ '^(0|[1-9][0-9]*)$'
    OR jsonb_typeof(span->'end') IS DISTINCT FROM 'number' OR span->>'end' !~ '^[1-9][0-9]*$' THEN RETURN false;END IF;
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=span->'region_id';
   begin_offset:=(span->>'start')::integer;end_offset:=(span->>'end')::integer;
   IF span->>'source_text_sha256' IS DISTINCT FROM r->>'source_text_sha256' OR begin_offset>=end_offset
    OR end_offset>length(r->>'source_text') OR begin_offset<coalesce((last_offsets->>(span->>'region_id'))::integer,0) THEN RETURN false;END IF;
   last_offsets:=last_offsets||jsonb_build_object(span->>'region_id',end_offset);
   selected:=array_append(selected,span->>'region_id');
   parts:=array_append(parts,rkb_v4.source_text_trim_v1(substring(r->>'source_text' FROM begin_offset+1 FOR end_offset-begin_offset)));
  END LOOP;
  IF jsonb_array_length(item->'source_spans')>0 THEN
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(item->'region_ids') x WHERE NOT x=ANY(selected)) THEN RETURN false;END IF;
  ELSE
   FOR refs IN SELECT value FROM jsonb_array_elements(item->'region_ids') LOOP
    SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
    IF length(rkb_v4.source_text_trim_v1(r->>'source_text'))>0 THEN parts:=array_append(parts,rkb_v4.source_text_trim_v1(r->>'source_text'));END IF;
   END LOOP;
  END IF;
  FOR refs IN SELECT value FROM jsonb_array_elements(item->'footnote_region_ids') LOOP
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
   IF length(rkb_v4.source_text_trim_v1(r->>'source_text'))>0 THEN parts:=array_append(parts,'[Footnote] '||rkb_v4.source_text_trim_v1(r->>'source_text'));END IF;
  END LOOP;
  IF rkb_v4.source_text_trim_v1(array_to_string(parts,E'\n')) IS DISTINCT FROM p->>'body' THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_image_graph_evidence_v1(doc jsonb,e jsonb) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE r jsonb;p jsonb;m jsonb;b integer;z integer;
BEGIN
 IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR NOT e ?& ARRAY['chunk_id','page_id','region_id','exact_quote','evidence_ref','start','end','source_region_sha256']
  OR e-ARRAY['chunk_id','page_id','region_id','exact_quote','evidence_ref','start','end','source_region_sha256']<>'{}'
  OR jsonb_typeof(e->'start') IS DISTINCT FROM 'number' OR jsonb_typeof(e->'end') IS DISTINCT FROM 'number'
  OR e->>'start' !~ '^(0|[1-9][0-9]{0,4})$' OR e->>'end' !~ '^[1-9][0-9]{0,4}$'
  OR jsonb_typeof(e->'exact_quote') IS DISTINCT FROM 'string' OR length(e->>'exact_quote') NOT BETWEEN 1 AND 2000
  OR e->>'evidence_ref' IS DISTINCT FROM 'knowledge://chunks/'||(e->>'chunk_id') THEN RETURN false;END IF;
 SELECT value INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') WHERE value->>'region_id'=e->>'region_id';
 SELECT value INTO STRICT p FROM jsonb_array_elements(doc->'passages') WHERE value->>'passage_id'=e->>'chunk_id';
 SELECT value INTO STRICT m FROM jsonb_array_elements(doc->'image_manifest'->'passages') WHERE value->>'passage_id'=e->>'chunk_id';
 b:=(e->>'start')::integer;z:=(e->>'end')::integer;
 RETURN r->>'page_id' IS NOT DISTINCT FROM e->>'page_id' AND r->>'source_text_sha256' IS NOT DISTINCT FROM e->>'source_region_sha256'
  AND m->'page_ids' @> jsonb_build_array(e->'page_id') AND m->'region_ids' @> jsonb_build_array(e->'region_id')
  AND b<z AND z<=length(r->>'source_text') AND substring(r->>'source_text' FROM b+1 FOR z-b)=e->>'exact_quote'
  AND position(e->>'exact_quote' IN p->>'body')>0;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_image_graph_v1(doc jsonb,source uuid,batches integer) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE g jsonb:=doc->'source_graph';e jsonb;a jsonb;r jsonb;proof jsonb;f jsonb;name text;owner uuid;ident uuid;
 k text;from_kind text;to_kind text;ids uuid[]:='{}';keys text[]:='{}';binding jsonb:='{}';n integer;
 mention jsonb;mention_ids uuid[]:='{}';mention_id uuid;
BEGIN
 IF g IS NULL THEN RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->'entity_keys'<>'[]'::jsonb);END IF;
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR NOT g ?& ARRAY['codec','source_id','source_revision','owner_person_id','bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities']
  OR g-ARRAY['codec','source_id','source_revision','owner_person_id','bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities']<>'{}'
  OR g->>'codec' IS DISTINCT FROM 'rkb.source-graph.v2' OR g->>'source_id' IS DISTINCT FROM source::text
  OR g->'source_revision' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(g->'owner_person_id') IS DISTINCT FROM 'string'
  OR (g->>'owner_person_id')::uuid::text IS DISTINCT FROM g->>'owner_person_id' THEN RETURN false;END IF;
 owner:=(g->>'owner_person_id')::uuid;
 FOREACH k IN ARRAY ARRAY['bundles','poi_fact_inputs','entities','references','relations','poi_facts','finding_entities'] LOOP
  n:=batches*CASE k WHEN 'bundles' THEN 1 WHEN 'poi_fact_inputs' THEN 32 WHEN 'entities' THEN 32 WHEN 'references' THEN 32
   WHEN 'relations' THEN 64 WHEN 'poi_facts' THEN 32 ELSE 12 END;
  IF jsonb_typeof(g->k) IS DISTINCT FROM 'array' OR jsonb_array_length(g->k)>n THEN RETURN false;END IF;
 END LOOP;
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR NOT e ?& ARRAY['key','entity_id','mode','kind','canonical_label','state','metadata','mentions','aliases']
   OR e-ARRAY['key','entity_id','mode','kind','canonical_label','state','metadata','mentions','aliases']<>'{}'
   OR jsonb_typeof(e->'key') IS DISTINCT FROM 'string' OR length(e->>'key') NOT BETWEEN 1 AND 100 OR e->>'key'=ANY(keys)
   OR jsonb_typeof(e->'entity_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(e->'kind') IS DISTINCT FROM 'string' OR e->>'kind' NOT IN ('person','organization','event','historical_thread','poi_ref')
   OR e->>'mode' IS NULL OR e->>'mode' NOT IN ('create','reuse') OR e->>'state' IS NULL OR e->>'state' NOT IN ('candidate','reviewed')
   OR jsonb_typeof(e->'canonical_label') IS DISTINCT FROM 'string' OR length(e->>'canonical_label') NOT BETWEEN 1 AND 200
   OR jsonb_typeof(e->'metadata') IS DISTINCT FROM 'object'
   OR e->'metadata'->>'owner_reference_verification' IS DISTINCT FROM 'not_verified'
   OR e->'metadata'->>'historical_geometry' IS DISTINCT FROM 'not_verified'
   OR jsonb_typeof(e->'mentions') IS DISTINCT FROM 'array'
   OR jsonb_array_length(e->'mentions') NOT BETWEEN 1 AND 32*batches
   OR jsonb_typeof(e->'aliases') IS DISTINCT FROM 'array' OR jsonb_array_length(e->'aliases')>640*batches THEN RETURN false;END IF;
  ident:=(e->>'entity_id')::uuid;
  IF e->>'mode'='create' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids WHERE entity_id=ident)
   OR e->>'mode'='reuse' AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner
    AND h.kind=e->>'kind' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
     JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn)) THEN RETURN false;END IF;
  keys:=array_append(keys,e->>'key');ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(e->>'key',ident::text);
  FOR mention IN SELECT value FROM jsonb_array_elements(e->'mentions') LOOP
   IF jsonb_typeof(mention) IS DISTINCT FROM 'object'
    OR NOT mention ?& ARRAY['mention_id','exact_source_spelling','evidence']
    OR mention-ARRAY['mention_id','exact_source_spelling','evidence']<>'{}'
    OR jsonb_typeof(mention->'mention_id') IS DISTINCT FROM 'string'
    OR jsonb_typeof(mention->'exact_source_spelling') IS DISTINCT FROM 'string'
    OR length(mention->>'exact_source_spelling') NOT BETWEEN 1 AND 200
    OR NOT rkb_v4.check_source_image_graph_evidence_v1(doc,mention->'evidence')
    OR position(mention->>'exact_source_spelling' IN mention->'evidence'->>'exact_quote')<=0 THEN RETURN false;END IF;
   mention_id:=(mention->>'mention_id')::uuid;
   IF mention_id::text IS DISTINCT FROM mention->>'mention_id' OR mention_id=ANY(mention_ids) THEN RETURN false;END IF;
   mention_ids:=array_append(mention_ids,mention_id);
  END LOOP;
  FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   IF jsonb_typeof(a->'evidences') IS DISTINCT FROM 'array'
    OR jsonb_array_length(a->'evidences') NOT BETWEEN 1 AND 640*batches
    OR a->'evidence' IS DISTINCT FROM a->'evidences'->0
    OR NOT rkb_v4.check_source_image_graph_evidence_v1(doc,a->'evidence') OR jsonb_typeof(a->'normalized_value') IS DISTINCT FROM 'string'
    OR length(a->>'normalized_value') NOT BETWEEN 1 AND 200 OR jsonb_typeof(a->'alias_id') IS DISTINCT FROM 'string' OR (a->>'alias_id')::uuid IS NULL THEN RETURN false;END IF;
   IF (SELECT count(DISTINCT value) FROM jsonb_array_elements(a->'evidences'))<>jsonb_array_length(a->'evidences') THEN RETURN false;END IF;
   FOR proof IN SELECT value FROM jsonb_array_elements(a->'evidences') LOOP
    IF NOT rkb_v4.check_source_image_graph_evidence_v1(doc,proof) THEN RETURN false;END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  IF jsonb_typeof(r) IS DISTINCT FROM 'object' OR NOT r ?& ARRAY['key','entity_id'] OR r-ARRAY['key','entity_id']<>'{}'
   OR jsonb_typeof(r->'key') IS DISTINCT FROM 'string' OR length(r->>'key') NOT BETWEEN 1 AND 100 OR r->>'key'=ANY(keys) OR jsonb_typeof(r->'entity_id') IS DISTINCT FROM 'string' THEN RETURN false;END IF;
  ident:=(r->>'entity_id')::uuid;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE h.entity_id=ident AND h.owner_person_id=owner AND EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.active_revision act USING(source_id)
    JOIN rkb_v4.sources s USING(source_id) WHERE x.entity_id=ident AND act.revision=1 AND s.accepted AND NOT s.withdrawn)) THEN RETURN false;END IF;
  keys:=array_append(keys,r->>'key');ids:=array_append(ids,ident);binding:=binding||jsonb_build_object(r->>'key',ident::text);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  IF NOT r ?& ARRAY['relation_id','source_id','target_id','kind','evidence','time_scope','state','review_note']
   OR jsonb_typeof(r->'relation_id') IS DISTINCT FROM 'string' OR jsonb_typeof(r->'source_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(r->'target_id') IS DISTINCT FROM 'string' OR (r->>'source_id')::uuid=(r->>'target_id')::uuid OR NOT ((r->>'source_id')::uuid=ANY(ids)) OR NOT ((r->>'target_id')::uuid=ANY(ids))
   OR jsonb_typeof(r->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(r->'evidence') NOT BETWEEN 1 AND 8 THEN RETURN false;END IF;
  from_kind:=rkb_v4.source_graph_endpoint_kind_v1(g,(r->>'source_id')::uuid);
  to_kind:=rkb_v4.source_graph_endpoint_kind_v1(g,(r->>'target_id')::uuid);
  IF NOT coalesce(CASE r->>'kind'
   WHEN 'participated_in' THEN from_kind IN ('person','organization') AND to_kind='event'
   WHEN 'occurred_at' THEN from_kind='event' AND to_kind='poi_ref'
   WHEN 'member_of' THEN from_kind IN ('person','organization','event','poi_ref') AND to_kind='historical_thread'
   WHEN 'affiliated_with' THEN from_kind='person' AND to_kind='organization'
   WHEN 'operated_at' THEN from_kind='organization' AND to_kind='poi_ref'
   WHEN 'predecessor_of' THEN from_kind='organization' AND to_kind='organization'
   WHEN 'founded_by' THEN from_kind='organization' AND to_kind='person'
   WHEN 'located_in' THEN from_kind='poi_ref' AND to_kind='poi_ref' ELSE false END,false) THEN RETURN false;END IF;
  FOR proof IN SELECT value FROM jsonb_array_elements(r->'evidence') LOOP
   IF NOT rkb_v4.check_source_image_graph_evidence_v1(doc,proof) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  IF jsonb_typeof(f->'candidate_id') IS DISTINCT FROM 'string' OR f->>'geometry_acceptance' IS DISTINCT FROM 'source_hypothesis_not_owner_geometry'
   OR jsonb_typeof(f->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(f->'evidence') NOT BETWEEN 1 AND 50
   OR jsonb_typeof(f->'text') IS DISTINCT FROM 'string' OR length(f->>'text') NOT BETWEEN 1 AND 500 THEN RETURN false;END IF;
  FOR proof IN SELECT value FROM jsonb_array_elements(f->'evidence') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->>'region_id'=proof->>'region_id'
    AND x->>'page_id'=proof->>'page_id' AND x->>'source_text_sha256'=proof->>'source_region_sha256') THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF (SELECT count(*) FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->'entity_keys'<>'[]'::jsonb)
    <>jsonb_array_length(g->'finding_entities') THEN RETURN false;END IF;
 FOR f IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') x WHERE x->>'assertion_id'=f->>'assertion_id'
    AND x->'entity_keys'=f->'entity_keys') OR jsonb_typeof(f->'entity_ids') IS DISTINCT FROM 'array'
    OR jsonb_typeof(f->'entity_keys') IS DISTINCT FROM 'array' OR jsonb_array_length(f->'entity_ids')<>jsonb_array_length(f->'entity_keys') THEN RETURN false;END IF;
  FOR name,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   IF binding->>name IS NULL OR binding->>name IS DISTINCT FROM f->'entity_ids'->>(n-1) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF cardinality(mention_ids)>32*batches THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;


CREATE FUNCTION rkb_v4.source_stage_latest_page_v1(source uuid,wanted uuid) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT page FROM rkb_v4.source_stage_batches b CROSS JOIN LATERAL
  jsonb_array_elements(convert_from(b.payload_bytes,'UTF8')::jsonb->'model_payload'->'pages') page
 WHERE b.source_id=source AND page->>'page_id'=wanted::text ORDER BY b.batch_index DESC LIMIT 1
$$;

CREATE FUNCTION rkb_v4.check_source_image_manifest_v1(doc jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'image_manifest'; session rkb_v4.source_stage_sessions;
 page jsonb; region jsonb; native rkb_v4.source_stage_pages; issued jsonb; raw_region jsonb;
 actual jsonb; original jsonb; model_page jsonb; idx integer:=0; seen uuid[]:='{}'; ident uuid; k text;
BEGIN
 SELECT * INTO STRICT session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 original:=convert_from(session.original_receipt,'UTF8')::jsonb;
 IF jsonb_typeof(m) IS DISTINCT FROM 'object'
  OR NOT m ?& ARRAY['codec','source_id','source_revision','original_receipt_b64','metadata','pages','regions','passages']
  OR m-ARRAY['codec','source_id','source_revision','original_receipt_b64','metadata','pages','regions','passages']<>'{}'
  OR m->>'codec' IS DISTINCT FROM 'rkb.source-image-manifest.v1' OR m->>'source_id' IS DISTINCT FROM source::text
  OR m->'source_revision' IS DISTINCT FROM '1'::jsonb
  OR decode(m->>'original_receipt_b64','base64') IS DISTINCT FROM session.original_receipt
  OR doc->'original' IS DISTINCT FROM jsonb_build_object('sha256',original->>'sha256','size',original->'size',
   'ref','archive:'||(original->>'key'),'receipt_sha256',encode(sha256(session.original_receipt),'hex'))
  OR doc->>'bibliography' IS DISTINCT FROM session.source_metadata->>'bibliography'
  OR doc->>'work_id' IS DISTINCT FROM session.source_metadata->>'work_id'
  OR doc->>'edition_id' IS DISTINCT FROM session.source_metadata->>'edition_id'
  OR doc->'public_rights' IS DISTINCT FROM session.source_metadata->'public_rights'
  OR m->'metadata' IS DISTINCT FROM (session.source_metadata->'metadata')-ARRAY['source_import','catalog_update','duplicate_policy']
  OR jsonb_typeof(m->'pages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'pages')<>session.page_count
  OR (SELECT count(*) FROM rkb_v4.source_stage_pages WHERE source_id=source)<>session.page_count
  OR jsonb_typeof(m->'regions') IS DISTINCT FROM 'array'
  OR jsonb_array_length(m->'regions')<>jsonb_array_length(doc->'semantic'->'regions')
  OR (SELECT coalesce(sum(jsonb_array_length(coalesce(rkb_v4.source_stage_latest_page_v1(source,page_id)->'regions','[]'::jsonb))),0)
      FROM rkb_v4.source_stage_pages WHERE source_id=source)<>jsonb_array_length(m->'regions') THEN RETURN false;END IF;
 FOR page IN SELECT value FROM jsonb_array_elements(m->'pages') LOOP
  SELECT * INTO STRICT native FROM rkb_v4.source_stage_pages WHERE source_id=source AND physical_page_index=idx;
  issued:=convert_from(native.image_receipt,'UTF8')::jsonb;
  model_page:=rkb_v4.source_stage_latest_page_v1(source,native.page_id);
  IF page-ARRAY['page_id','physical_page_index','printed_page_number','width','height','source_image_receipt_sha256',
      'issued_image_receipt_b64','image_sha256','preview_render','source_material','source_review_note']<>'{}'
   OR page->>'page_id' IS DISTINCT FROM native.page_id::text OR page->'physical_page_index' IS DISTINCT FROM to_jsonb(idx)
   OR page->>'source_image_receipt_sha256' IS DISTINCT FROM native.image_receipt_sha256
   OR decode(page->>'issued_image_receipt_b64','base64') IS DISTINCT FROM native.image_receipt
   OR native.image_receipt_sha256<>encode(sha256(native.image_receipt),'hex')
   OR page->'printed_page_number' IS DISTINCT FROM coalesce(model_page->'printed_page_number','null'::jsonb)
   OR page->>'source_material' IS DISTINCT FROM 'visual_reviewed'
   OR page->>'source_review_note' IS DISTINCT FROM model_page->>'source_review_note'
   OR NOT length(page->>'source_review_note') BETWEEN 1 AND 500 THEN RETURN false;END IF;
  FOREACH k IN ARRAY ARRAY['width','height','image_sha256','preview_render'] LOOP
   IF page->k IS DISTINCT FROM issued->k THEN RETURN false;END IF;
  END LOOP;
  idx:=idx+1;
 END LOOP;
 FOR region IN SELECT value FROM jsonb_array_elements(m->'regions') LOOP
  ident:=(region->>'region_id')::uuid;
  IF ident=ANY(seen) OR region-ARRAY['region_id','region_key','page_id','kind','bbox','reading_order','column_id','text_sha256','source_text_provenance']<>'{}'
   OR region->>'source_text_provenance' IS DISTINCT FROM 'model_transcription' THEN RETURN false;END IF;
  seen:=array_append(seen,ident);
  SELECT * INTO STRICT native FROM rkb_v4.source_stage_pages WHERE source_id=source AND page_id=(region->>'page_id')::uuid;
  model_page:=rkb_v4.source_stage_latest_page_v1(source,native.page_id);
  SELECT r INTO STRICT raw_region FROM jsonb_array_elements(model_page->'regions') r WHERE r->>'region_key'=region->>'region_key';
  SELECT r INTO STRICT actual FROM jsonb_array_elements(doc->'semantic'->'regions') r WHERE r->>'region_id'=region->>'region_id';
  IF actual->>'page_id' IS DISTINCT FROM native.page_id::text
   OR actual->>'source_text' IS DISTINCT FROM coalesce(raw_region->>'source_text','')
   OR actual->>'source_text_sha256' IS DISTINCT FROM region->>'text_sha256'
   OR region->>'text_sha256' IS DISTINCT FROM encode(sha256(convert_to(coalesce(raw_region->>'source_text',''),'UTF8')),'hex')
   OR region->'kind' IS DISTINCT FROM raw_region->'kind' OR region->'bbox' IS DISTINCT FROM raw_region->'bbox'
   OR region->'reading_order' IS DISTINCT FROM raw_region->'reading_order'
   OR region->'column_id' IS DISTINCT FROM coalesce(raw_region->'column_id','null'::jsonb) THEN RETURN false;END IF;
 END LOOP;
 RETURN rkb_v4.check_source_image_passages_v1(doc);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.prepare_source_activation_v1(source uuid,ingestion uuid,batches integer,tail text,
 materializer text,knowledge_bytes bytea,expected_database text,expected_writer text) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; session rkb_v4.source_stage_sessions; doc jsonb; base jsonb; finding jsonb;
 prior rkb_v4.source_activation_candidates; knowledge_hash text; research jsonb;
BEGIN
 IF octet_length(knowledge_bytes) NOT BETWEEN 1 AND 33554432 OR (materializer IS NULL OR materializer !~ '^[0-9a-f]{64}$')
  OR batches NOT BETWEEN 1 AND 2500 THEN RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='bounded native source candidate required';END IF;
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation IS DISTINCT FROM expected_database OR state.writer_generation IS DISTINCT FROM expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired) THEN
  RAISE EXCEPTION 'native source candidate writer changed';END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(source::text,0));
 IF NOT rkb_v4.verify_source_open_stage_chain_v1(source,ingestion,batches,tail) THEN
  RAISE EXCEPTION 'exact accepted native source chain required';END IF;
 knowledge_hash:=encode(sha256(knowledge_bytes),'hex');doc:=convert_from(knowledge_bytes,'UTF8')::jsonb;
 SELECT * INTO prior FROM rkb_v4.source_activation_candidates WHERE source_id=source;
 IF FOUND THEN
  IF prior.ingestion_id<>ingestion OR prior.batch_count<>batches OR prior.last_batch_sha256<>tail
   OR prior.materializer_sha256<>materializer OR prior.knowledge_bytes<>knowledge_bytes THEN
   RAISE EXCEPTION 'native source candidate knowledge changed';END IF;
  RETURN prior.knowledge_sha256;
 END IF;
 SELECT * INTO STRICT session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-projection.v1'
  OR doc-ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','semantic','research','public_rights','image_manifest','source_graph']<>'{}'
  OR NOT doc ?& ARRAY['codec','bibliography','revision','work_id','edition_id','original','model','passages','semantic','research','public_rights','image_manifest']
  OR jsonb_array_length(doc->'passages')>100*batches
  OR jsonb_array_length(doc->'semantic'->'stories')>12*batches
  OR jsonb_array_length(doc->'semantic'->'findings')>12*batches
  OR jsonb_array_length(doc->'semantic'->'regions')>500*session.page_count THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='native source candidate envelope differs';END IF;
 base:=(doc-ARRAY['image_manifest','source_graph','public_rights'])||'{"codec":"rkb.source-activation-semantic.v1"}'::jsonb;
 IF NOT rkb_v4.check_source_activation_semantic_v1(convert_to(base::text,'UTF8'),
     jsonb_build_object('source_id',source),'{"absent_source":"true"}'::jsonb,session.logical_time)
  OR NOT rkb_v4.check_source_image_manifest_v1(doc,source)
  OR NOT rkb_v4.check_source_image_graph_v1(doc,source,batches) THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='native source candidate provenance or domain validation failed';END IF;
 FOR finding IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
  IF NOT rkb_v4.check_source_facts_v1(finding) THEN RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='source finding evidence invalid';END IF;
 END LOOP;
 SELECT convert_from(payload_bytes,'UTF8')::jsonb->'research' INTO STRICT research
  FROM rkb_v4.source_stage_batches WHERE source_id=source AND batch_index=batches-1;
 IF doc->'research' IS DISTINCT FROM research THEN RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='model continuation changed';END IF;
 INSERT INTO rkb_v4.source_activation_candidates(source_id,ingestion_id,batch_count,last_batch_sha256,materializer_sha256,knowledge_bytes,knowledge_sha256,knowledge,prepared_database,prepared_writer) VALUES(source,ingestion,batches,tail,materializer,
  knowledge_bytes,knowledge_hash,doc,expected_database,expected_writer);
 RETURN knowledge_hash;
END $$;

CREATE TABLE rkb_v4.source_activation_vectors (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_activation_candidates,
 ordinal integer NOT NULL CHECK(ordinal>=0), input_id uuid NOT NULL, input_sha256 text NOT NULL,
 embedding public.vector(1024) NOT NULL CHECK(abs(public.vector_norm(embedding)-1.0)<=0.002),
 PRIMARY KEY(source_id,ordinal), UNIQUE(source_id,input_id)
);
CREATE TABLE rkb_v4.source_activation_materialized (
 source_id uuid NOT NULL, ordinal integer NOT NULL, knowledge_sha256 text NOT NULL, input_sha256 text NOT NULL,
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
 PRIMARY KEY(source_id,ordinal), FOREIGN KEY(source_id,ordinal) REFERENCES rkb_v4.source_activation_vectors
);
CREATE TRIGGER immutable_source_activation_vector BEFORE UPDATE OR DELETE ON rkb_v4.source_activation_vectors
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_activation_materialized BEFORE UPDATE OR DELETE ON rkb_v4.source_activation_materialized
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.source_activation_input_v1(source uuid,wanted integer) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT CASE WHEN wanted<jsonb_array_length(knowledge->'passages') THEN
  jsonb_build_object('input_id',knowledge->'passages'->wanted->'passage_id',
   'body',knowledge->'passages'->wanted->'body','input_tokens',knowledge->'passages'->wanted->'input_tokens') ELSE
  jsonb_build_object('input_id',knowledge->'semantic'->'stories'->(wanted-jsonb_array_length(knowledge->'passages'))->'default_variant'->'variant_id',
   'body',knowledge->'semantic'->'stories'->(wanted-jsonb_array_length(knowledge->'passages'))->'default_variant'->'body',
   'input_tokens',knowledge->'semantic'->'stories'->(wanted-jsonb_array_length(knowledge->'passages'))->'default_variant'->'input_tokens') END
 FROM rkb_v4.source_activation_candidates WHERE source_id=source AND wanted>=0
  AND wanted<jsonb_array_length(knowledge->'passages')+jsonb_array_length(knowledge->'semantic'->'stories')
$$;

CREATE FUNCTION rkb_v4.install_source_activation_vector_v1(source uuid,knowledge_hash text,wanted integer,
 vector public.vector,expected_database text,expected_writer text) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; candidate rkb_v4.source_activation_candidates; input jsonb; prior rkb_v4.source_activation_vectors;
 input_hash text;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation IS DISTINCT FROM expected_database OR state.writer_generation IS DISTINCT FROM expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired) THEN
  RAISE EXCEPTION 'native source vector writer changed';END IF;
 SELECT * INTO STRICT candidate FROM rkb_v4.source_activation_candidates WHERE source_id=source;
 IF candidate.knowledge_sha256 IS DISTINCT FROM knowledge_hash OR vector IS NULL OR public.vector_dims(vector)<>1024
  OR abs(public.vector_norm(vector)-1.0)>0.002 THEN RAISE EXCEPTION 'native source vector binding differs';END IF;
 input:=rkb_v4.source_activation_input_v1(source,wanted);
 IF input IS NULL THEN RAISE EXCEPTION 'native source input ordinal differs';END IF;
 input_hash:=encode(sha256(convert_to(input->>'body','UTF8')),'hex');
 INSERT INTO rkb_v4.source_activation_vectors VALUES(source,wanted,(input->>'input_id')::uuid,input_hash,vector)
  ON CONFLICT(source_id,ordinal) DO NOTHING;
 SELECT * INTO STRICT prior FROM rkb_v4.source_activation_vectors WHERE source_id=source AND ordinal=wanted;
 IF prior.input_id<>(input->>'input_id')::uuid OR prior.input_sha256<>input_hash THEN
  RAISE EXCEPTION 'native source vector input changed';END IF;
 -- First valid derived vector wins; deterministic knowledge is immutable, but
 -- harmless floating-point drift on an exact retry must not strand the source.
 RETURN input_hash;
END $$;

CREATE FUNCTION rkb_v4.check_source_materialize_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; candidate rkb_v4.source_activation_candidates; input jsonb; wanted integer; source uuid;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 OR expected IS DISTINCT FROM '{"absent_source":"true"}'::jsonb
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id') THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF d-ARRAY['codec','ingestion_id','batch_count','last_batch_sha256','materializer_sha256','ordinal']<>'{}'
  OR NOT d ?& ARRAY['codec','ingestion_id','batch_count','last_batch_sha256','materializer_sha256','ordinal']
  OR d->>'codec' IS DISTINCT FROM 'rkb.source-materialize.v1' OR jsonb_typeof(d->'ordinal') IS DISTINCT FROM 'number'
  OR d->>'ordinal' !~ '^(0|[1-9][0-9]{0,5})$' OR EXISTS(SELECT 1 FROM rkb_v4.sources WHERE source_id=source) THEN RETURN false;END IF;
 wanted:=(d->>'ordinal')::integer;
 SELECT * INTO STRICT candidate FROM rkb_v4.source_activation_candidates WHERE source_id=source;
 IF d->>'ingestion_id' IS DISTINCT FROM candidate.ingestion_id::text
  OR d->'batch_count' IS DISTINCT FROM to_jsonb(candidate.batch_count)
  OR d->>'last_batch_sha256' IS DISTINCT FROM candidate.last_batch_sha256
  OR d->>'materializer_sha256' IS DISTINCT FROM candidate.materializer_sha256
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_stage_candidates sc JOIN rkb_v4.applied_ops a ON a.sequence=sc.accepted_sequence
   WHERE sc.source_id=source AND sc.ingestion_id=candidate.ingestion_id AND sc.batch_count=candidate.batch_count
    AND sc.last_batch_sha256=candidate.last_batch_sha256 AND a.status='applied') THEN RETURN false;END IF;
 input:=rkb_v4.source_activation_input_v1(source,wanted);
 RETURN input IS NOT NULL AND EXISTS(SELECT 1 FROM rkb_v4.source_activation_vectors v WHERE v.source_id=source
  AND v.ordinal=wanted AND v.input_id=(input->>'input_id')::uuid
  AND v.input_sha256=encode(sha256(convert_to(input->>'body','UTF8')),'hex'));
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_materialize_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS bytea
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; source uuid; wanted integer; candidate rkb_v4.source_activation_candidates; input jsonb; seq bigint; h text;
BEGIN
 SELECT applied_seq+1 INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_materialize_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'exact prepared native input required';END IF;
 d:=convert_from(p,'UTF8')::jsonb;source:=(ids->>'source_id')::uuid;wanted:=(d->>'ordinal')::integer;
 SELECT * INTO STRICT candidate FROM rkb_v4.source_activation_candidates WHERE source_id=source;
 input:=rkb_v4.source_activation_input_v1(source,wanted);h:=encode(sha256(convert_to(input->>'body','UTF8')),'hex');
 INSERT INTO rkb_v4.source_activation_materialized VALUES(source,wanted,candidate.knowledge_sha256,h,seq)
  ON CONFLICT(source_id,ordinal) DO NOTHING;
 RETURN convert_to(jsonb_build_object('source_id',source,'ordinal',wanted,
  'total_inputs',jsonb_array_length(candidate.knowledge->'passages')+jsonb_array_length(candidate.knowledge->'semantic'->'stories'),
  'knowledge_sha256',candidate.knowledge_sha256,'input_sha256',h,'input_id',input->>'input_id','logical_time',logical_time)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.source_activation_doc_v1(source uuid) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT jsonb_set(jsonb_set(c.knowledge,'{passages}',(SELECT coalesce(jsonb_agg(
  p.x||jsonb_build_object('embedding',to_jsonb(v.embedding::real[])) ORDER BY p.n),'[]'::jsonb)
  FROM jsonb_array_elements(c.knowledge->'passages') WITH ORDINALITY p(x,n)
  JOIN rkb_v4.source_activation_vectors v ON v.source_id=c.source_id AND v.ordinal=p.n-1)),
  '{semantic,stories}',(SELECT coalesce(jsonb_agg(jsonb_set(s.x,'{default_variant}',
   (s.x->'default_variant')||jsonb_build_object('embedding',to_jsonb(v.embedding::real[]))) ORDER BY s.n),'[]'::jsonb)
   FROM jsonb_array_elements(c.knowledge->'semantic'->'stories') WITH ORDINALITY s(x,n)
   JOIN rkb_v4.source_activation_vectors v ON v.source_id=c.source_id
    AND v.ordinal=jsonb_array_length(c.knowledge->'passages')+s.n-1))
 FROM rkb_v4.source_activation_candidates c WHERE c.source_id=source
$$;

CREATE FUNCTION rkb_v4.check_source_activate_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; candidate rkb_v4.source_activation_candidates; source uuid; row record; c jsonb; input jsonb; n integer:=0;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 OR expected IS DISTINCT FROM '{"absent_source":"true"}'::jsonb
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id') THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF d-ARRAY['codec','ingestion_id','batch_count','last_batch_sha256','materializer_sha256']<>'{}'
  OR NOT d ?& ARRAY['codec','ingestion_id','batch_count','last_batch_sha256','materializer_sha256']
  OR d->>'codec' IS DISTINCT FROM 'rkb.source-activate.v1'
  OR EXISTS(SELECT 1 FROM rkb_v4.sources WHERE source_id=source) THEN RETURN false;END IF;
 SELECT * INTO STRICT candidate FROM rkb_v4.source_activation_candidates WHERE source_id=source;
 IF d->>'ingestion_id' IS DISTINCT FROM candidate.ingestion_id::text
  OR d->'batch_count' IS DISTINCT FROM to_jsonb(candidate.batch_count)
  OR d->>'last_batch_sha256' IS DISTINCT FROM candidate.last_batch_sha256
  OR d->>'materializer_sha256' IS DISTINCT FROM candidate.materializer_sha256
  OR NOT rkb_v4.verify_source_stage_chain_v1(source,candidate.ingestion_id,candidate.batch_count,candidate.last_batch_sha256)
  OR candidate.knowledge_sha256<>encode(sha256(candidate.knowledge_bytes),'hex')
  OR NOT rkb_v4.check_source_image_graph_v1(candidate.knowledge,source,candidate.batch_count) THEN RETURN false;END IF;
 FOR row IN SELECT v.*,m.knowledge_sha256,m.input_sha256 materialized_hash,m.accepted_sequence,
    a.command_bytes,a.command_sha256,a.status FROM rkb_v4.source_activation_vectors v
   JOIN rkb_v4.source_activation_materialized m USING(source_id,ordinal)
   JOIN rkb_v4.applied_ops a ON a.sequence=m.accepted_sequence WHERE v.source_id=source ORDER BY v.ordinal LOOP
  c:=convert_from(row.command_bytes,'UTF8')::jsonb;input:=rkb_v4.source_activation_input_v1(source,n);
  IF row.ordinal<>n OR row.status<>'applied' OR row.command_sha256<>encode(sha256(row.command_bytes),'hex')
   OR row.knowledge_sha256<>candidate.knowledge_sha256 OR row.materialized_hash<>row.input_sha256
   OR row.input_id::text IS DISTINCT FROM input->>'input_id'
   OR row.input_sha256<>encode(sha256(convert_to(input->>'body','UTF8')),'hex')
   OR c->>'route'<>'source.materialize.v1' OR c->'ids'->>'source_id'<>source::text
   OR convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb IS DISTINCT FROM
    (d||jsonb_build_object('codec','rkb.source-materialize.v1','ordinal',n)) THEN RETURN false;END IF;
  n:=n+1;
 END LOOP;
 RETURN n=jsonb_array_length(candidate.knowledge->'passages')+jsonb_array_length(candidate.knowledge->'semantic'->'stories')
  AND n=(SELECT count(*) FROM rkb_v4.source_activation_vectors WHERE source_id=source)
  AND n=(SELECT count(*) FROM rkb_v4.source_activation_materialized WHERE source_id=source);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.project_source_graph_v2(source uuid,g jsonb,payload_sha text) RETURNS void
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE e jsonb;a jsonb;r jsonb;link jsonb;mention jsonb;k text;n integer;BEGIN
 IF g IS NULL THEN RETURN;END IF;
 INSERT INTO rkb_v4.source_graph_documents VALUES(source,1,(g->>'owner_person_id')::uuid,g,payload_sha);
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  INSERT INTO rkb_v4.source_entity_ids VALUES((e->>'entity_id')::uuid,(g->>'owner_person_id')::uuid,e->>'kind') ON CONFLICT DO NOTHING;
  INSERT INTO rkb_v4.source_entity_representations VALUES(source,e->>'key',(e->>'entity_id')::uuid,e);
  FOR mention IN SELECT value FROM jsonb_array_elements(e->'mentions') LOOP
   INSERT INTO rkb_v4.source_entity_mentions VALUES(source,(mention->>'mention_id')::uuid,(e->>'entity_id')::uuid,mention);
  END LOOP;
  FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   INSERT INTO rkb_v4.source_entity_aliases VALUES(source,(a->>'alias_id')::uuid,(e->>'entity_id')::uuid,
    a->>'normalized_value',encode(sha256(convert_to(a->>'normalized_value','UTF8')),'hex'),a);
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  INSERT INTO rkb_v4.source_entity_references VALUES(source,r->>'key',(r->>'entity_id')::uuid,r);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  INSERT INTO rkb_v4.source_entity_relations VALUES(source,(r->>'relation_id')::uuid,(r->>'source_id')::uuid,(r->>'target_id')::uuid,r->>'kind',r);
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  INSERT INTO rkb_v4.source_poi_hypotheses VALUES(source,(r->>'candidate_id')::uuid,r);
 END LOOP;
 FOR link IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  FOR k,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(link->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   INSERT INTO rkb_v4.source_entity_finding_links(source_id,assertion_id,entity_id,entity_key) VALUES(source,(link->>'assertion_id')::uuid,(link->'entity_ids'->>(n-1))::uuid,k);
  END LOOP;
 END LOOP;
END $$;

CREATE FUNCTION rkb_v4.apply_source_activate_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint; story jsonb; variant jsonb;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_activate_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'source import requires exact resolved new-source precondition';
    END IF;
    source:=(ids->>'source_id')::uuid; doc:=rkb_v4.source_activation_doc_v1(source);
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
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'regions') LOOP
        INSERT INTO rkb_v4.source_regions VALUES(source,1,(item->>'region_id')::uuid,
            (item->>'page_id')::uuid,item->>'source_text',item->>'source_text_sha256');
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
        INSERT INTO rkb_v4.story_assertion_versions VALUES((item->>'assertion_id')::uuid,1,
            source,1,item->>'candidate_key',item->>'proposition',item);
    END LOOP;
    FOR story IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'stories') LOOP
        variant:=story->'default_variant';
        INSERT INTO rkb_v4.stories VALUES((story->>'story_id')::uuid,1,source,1,
            (variant->>'variant_id')::uuid,1);
        INSERT INTO rkb_v4.story_versions VALUES((story->>'story_id')::uuid,1,
            story-'default_variant',logical_time,encode(sha256(p),'hex'));
        INSERT INTO rkb_v4.story_variant_versions VALUES((variant->>'variant_id')::uuid,1,
            (story->>'story_id')::uuid,variant->>'body',
            encode(sha256(convert_to(variant->>'body','UTF8')),'hex'),variant-'embedding');
        INSERT INTO rkb_v4.story_search(story_id,variant_id,variant_revision,source_id,source_revision,body,config,embedding)
        VALUES((story->>'story_id')::uuid,(variant->>'variant_id')::uuid,1,source,1,variant->>'body',
            CASE variant->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
            WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (variant->'embedding')::text::public.vector);
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'pending') LOOP
        INSERT INTO rkb_v4.source_story_pending VALUES(source,item->>'candidate_key',item);
    END LOOP;
    -- Activation happens after every exact passage and its research continuation
    -- exist. All source/projection/receipt/frontier work commits or rolls back.
    PERFORM rkb_v4.project_source_graph_v2(source,doc->'source_graph',encode(sha256(p),'hex'));
    INSERT INTO rkb_v4.source_manifests VALUES(source,1,doc->'image_manifest',encode(sha256(p),'hex'),
        decode(doc->'image_manifest'->>'original_receipt_b64','base64'));
    INSERT INTO rkb_v4.active_revision VALUES (source,1);
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
    SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=source;
    PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
        frontier.visibility_epoch,state.applied_seq+1,
        prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
        prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
    RETURN convert_to(jsonb_build_object('source_id',source,'content_revision',1,'accepted',true,
        'withdrawn',false,'eligibility_revision',eligibility,'visibility_epoch',frontier.visibility_epoch,
        'passage_count',jsonb_array_length(doc->'passages'),'story_count',jsonb_array_length(doc->'semantic'->'stories'),
        'finding_count',jsonb_array_length(doc->'semantic'->'findings'),'knowledge_sha256',(SELECT knowledge_sha256 FROM rkb_v4.source_activation_candidates WHERE source_id=source),
        'payload_sha256',encode(sha256(p),'hex'),
        'research_frontier_id',research->>'frontier_id','research_state',research->>'state',
        'logical_time',logical_time)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.capture_source_image_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;wanted uuid;story record;v jsonb;refs jsonb;new_revision bigint; candidate rkb_v4.source_activation_candidates; payload jsonb; seal_actor text; g rkb_v4.source_graph_documents;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route' NOT IN ('source.activate.v1','source.materialize.v1') THEN RETURN NULL;END IF;
 SELECT * INTO STRICT candidate FROM rkb_v4.source_activation_candidates WHERE source_id=(c->'ids'->>'source_id')::uuid;
 SELECT convert_from(a.command_bytes,'UTF8')::jsonb->>'person_id' INTO STRICT seal_actor
  FROM rkb_v4.source_stage_candidates sc JOIN rkb_v4.applied_ops a ON a.sequence=sc.accepted_sequence
  WHERE sc.source_id=candidate.source_id AND a.status='applied';
 IF seal_actor IS DISTINCT FROM c->>'person_id' THEN RAISE EXCEPTION 'native source accepted actor changed';END IF;
 IF c->>'route'='source.materialize.v1' THEN RETURN NULL;END IF;
 payload:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
 INSERT INTO rkb_v4.source_image_origins VALUES(candidate.source_id,candidate.ingestion_id,candidate.batch_count,
  candidate.last_batch_sha256,candidate.materializer_sha256,candidate.knowledge_sha256,
  encode(sha256(decode(c->>'payload_b64','base64')),'hex'),NEW.operation_key,NEW.sequence,
  (c->>'person_id')::uuid,c->>'request_id',NEW.command_sha256);
 SELECT * INTO g FROM rkb_v4.source_graph_documents WHERE source_id=candidate.source_id;
 IF candidate.knowledge ? 'source_graph' THEN
  IF g.source_id IS NULL OR g.owner_person_id::text IS DISTINCT FROM c->>'person_id'
   OR g.data IS DISTINCT FROM candidate.knowledge->'source_graph'
   OR g.payload_sha256 IS DISTINCT FROM encode(sha256(decode(c->>'payload_b64','base64')),'hex') THEN
   RAISE EXCEPTION 'source image graph differs from accepted actor';END IF;
  INSERT INTO rkb_v4.source_graph_receipts VALUES(candidate.source_id,NEW.operation_key,NEW.sequence,
   NEW.command_sha256,(c->>'person_id')::uuid,c->>'request_id');
 ELSIF g.source_id IS NOT NULL THEN RAISE EXCEPTION 'unexpected image source graph';END IF;
 IF c->>'route'='source.activate.v1' THEN
  FOR story IN SELECT s.*,sv.data version_data,b.data variant_data,b.body,b.body_sha256,
     p.embedding,p.config FROM rkb_v4.stories s JOIN rkb_v4.story_versions sv USING(story_id)
     JOIN rkb_v4.story_variant_versions b ON b.variant_id=s.default_variant_id AND b.revision=s.default_variant_revision
     JOIN rkb_v4.story_search p ON p.story_id=s.story_id
     WHERE s.source_id=(c->'ids'->>'source_id')::uuid LOOP
   INSERT INTO rkb_v4.story_history_receipts VALUES(story.story_id,1,NEW.operation_key,NEW.sequence,
    c->>'person_id',c->>'request_id',c->>'logical_time',NEW.command_sha256);
   INSERT INTO rkb_v4.editorial_story_heads VALUES(story.story_id,1,(c->>'person_id')::uuid,
    story.source_id,story.source_revision,story.default_variant_id,story.default_variant_revision);
   v:=(story.variant_data-'blocks'-'input_tokens')||jsonb_build_object('blocks','[]'::jsonb,'attributions','[]'::jsonb,
    'media_refs','[]'::jsonb,'channel',NULL,'state','drafting','approved_revision',NULL,'approval_fingerprint',NULL);
   -- Preserve the original exact source block map separately, without changing
   -- its accepted interpretation into legacy free-text editorial block objects.
   v:=v||jsonb_build_object('source_blocks',story.variant_data->'blocks');
   INSERT INTO rkb_v4.editorial_variant_versions VALUES(story.default_variant_id,1,story.story_id,
    story.body,story.body_sha256,v,story.embedding,(story.variant_data->>'input_tokens')::integer,
    (c->>'person_id')::uuid,c->>'logical_time');
   INSERT INTO rkb_v4.editorial_variant_heads VALUES(story.default_variant_id,story.story_id,1);
   refs:=jsonb_build_array(jsonb_build_object('variant_id',story.default_variant_id,'revision',1));
   INSERT INTO rkb_v4.editorial_story_versions VALUES(story.story_id,1,jsonb_build_object(
    'default_variant_ref',refs->0,'variants',refs,'state','drafting'),(c->>'person_id')::uuid,c->>'logical_time','source_import');
   INSERT INTO rkb_v4.editorial_story_search VALUES(story.story_id,story.default_variant_id,1,
    story.source_id,story.source_revision,story.body,story.config,story.embedding,DEFAULT);
   INSERT INTO rkb_v4.editorial_story_receipts VALUES(story.story_id,1,NEW.operation_key,NEW.sequence,
    (c->>'person_id')::uuid,c->>'request_id',NEW.command_sha256);
  END LOOP;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_source_image_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_source_image_receipt_v1();


CREATE FUNCTION rkb_v4.read_source_activation_input_v1(source uuid,knowledge_hash text,wanted integer)
 RETURNS TABLE(input_id uuid,input_sha256 text,body text,installed boolean)
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT (input->>'input_id')::uuid,encode(sha256(convert_to(input->>'body','UTF8')),'hex'),input->>'body',
  EXISTS(SELECT 1 FROM rkb_v4.source_activation_vectors v WHERE v.source_id=source AND v.ordinal=wanted
   AND v.input_id=(input->>'input_id')::uuid AND v.input_sha256=encode(sha256(convert_to(input->>'body','UTF8')),'hex'))
 FROM rkb_v4.source_activation_candidates c CROSS JOIN LATERAL
  (SELECT rkb_v4.source_activation_input_v1(source,wanted) input) x
 WHERE c.source_id=source AND c.knowledge_sha256=knowledge_hash AND input IS NOT NULL
$$;

CREATE FUNCTION rkb_v4.verify_source_image_manifest_v1(wanted uuid,accepted jsonb,accepted_sha256 text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=accepted->'image_manifest';item jsonb;author_text text;
BEGIN
 IF NOT rkb_v4.check_source_image_manifest_v1(accepted,wanted)
  OR (SELECT count(*) FROM rkb_v4.source_manifests WHERE source_id=wanted)<>1
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_manifests z WHERE z.source_id=wanted AND z.source_revision=1
    AND z.manifest=m AND z.payload_sha256=accepted_sha256
    AND z.original_receipt=decode(m->>'original_receipt_b64','base64'))
  OR (SELECT count(*) FROM rkb_v4.source_catalog WHERE source_id=wanted)<>1
  OR (SELECT count(*) FROM rkb_v4.source_passage_provenance WHERE source_id=wanted)<>jsonb_array_length(m->'passages') THEN RETURN false;END IF;
 SELECT string_agg(x,E'\n' ORDER BY ordinal) INTO author_text
  FROM jsonb_array_elements_text(m->'metadata'->'authors') WITH ORDINALITY a(x,ordinal);
 IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_catalog z WHERE z.source_id=wanted AND z.source_revision=1
   AND z.metadata=m->'metadata' AND z.search_text=coalesce(m->'metadata'->>'title','')||E'\n'||coalesce(author_text,'')) THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(m->'passages') LOOP
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_passage_provenance z WHERE z.source_id=wanted AND z.source_revision=1
   AND z.passage_id=(item->>'passage_id')::uuid AND z.data=item) THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.verify_editorial_story_origins_v2(scope uuid[])
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE head record;version record;c jsonb;doc jsonb;op jsonb;base jsonb;variant jsonb;
 expected_data jsonb;snapshot jsonb;refs jsonb;grants jsonb;actual jsonb;default_ref jsonb;
 variants jsonb;actor uuid;role text;wanted_variant uuid;wanted_revision bigint;
 previous_revision bigint;previous_sequence bigint;variant_rows integer;projection_rows integer;seen integer:=0;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 IF (SELECT count(*) FROM rkb_v4.editorial_story_heads WHERE source_id=ANY(scope))
  <>(SELECT count(*) FROM rkb_v4.stories WHERE source_id=ANY(scope)) THEN RETURN false;END IF;
 FOR head IN SELECT h.* FROM rkb_v4.editorial_story_heads h WHERE source_id=ANY(scope) ORDER BY story_id LIMIT 10001 LOOP
  IF head.source_revision<>1 THEN RETURN false;END IF;
  previous_revision:=0;previous_sequence:=0;variant_rows:=0;projection_rows:=0;
  refs:='{}'::jsonb;grants:='{}'::jsonb;snapshot:=NULL;default_ref:=NULL;
  FOR version IN SELECT v.*,r.operation_key,r.sequence,r.person_id,r.request_id,r.command_sha256,
     a.command_bytes,a.command_sha256 actual_command_sha256,a.sequence actual_sequence,a.status
   FROM rkb_v4.editorial_story_versions v LEFT JOIN rkb_v4.editorial_story_receipts r
      ON r.story_id=v.story_id AND r.revision=v.revision
   LEFT JOIN rkb_v4.applied_ops a ON a.operation_key=r.operation_key
   WHERE v.story_id=head.story_id ORDER BY v.revision LIMIT 10001 LOOP
   seen:=seen+1;
   IF seen>10000 OR version.revision<>previous_revision+1 OR version.status IS DISTINCT FROM 'applied'
    OR version.sequence IS NULL OR version.sequence<=previous_sequence OR version.sequence<>version.actual_sequence
    OR version.command_sha256 IS DISTINCT FROM version.actual_command_sha256
    OR version.command_sha256 IS DISTINCT FROM encode(sha256(version.command_bytes),'hex') THEN RETURN false;END IF;
   c:=convert_from(version.command_bytes,'UTF8')::jsonb;
   actor:=(c->>'person_id')::uuid;doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
   IF version.actor_id<>actor OR version.person_id<>actor OR version.logical_time IS DISTINCT FROM c->>'logical_time'
    OR version.operation_key IS DISTINCT FROM c->>'operation_key' OR version.request_id IS DISTINCT FROM c->>'request_id'
    OR version.sequence<>(c->>'sequence')::bigint OR (c->'ids'->>'source_id')::uuid<>head.source_id THEN RETURN false;END IF;
   IF version.revision=1 THEN
    IF c->>'route'='source.activate.v1' THEN
     IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_image_origins o JOIN rkb_v4.source_activation_candidates k USING(source_id)
       WHERE o.source_id=head.source_id AND o.operation_key=version.operation_key AND o.command_sha256=version.command_sha256
        AND o.knowledge_sha256=k.knowledge_sha256 AND o.person_id=actor) THEN RETURN false;END IF;
     doc:=rkb_v4.source_activation_doc_v1(head.source_id);
    ELSIF c->>'route' IS DISTINCT FROM 'source.import.v4' THEN RETURN false;END IF;
    IF version.action<>'source_import' OR head.owner_id<>actor THEN RETURN false;END IF;
    SELECT x INTO STRICT base FROM jsonb_array_elements(doc->'semantic'->'stories') x WHERE (x->>'story_id')::uuid=head.story_id;
    variant:=base->'default_variant';wanted_variant:=(variant->>'variant_id')::uuid;wanted_revision:=1;
    expected_data:=(variant-'embedding'-'blocks'-'input_tokens')||jsonb_build_object('blocks','[]'::jsonb,
      'attributions','[]'::jsonb,'media_refs','[]'::jsonb,'channel',NULL,'state','drafting',
      'approved_revision',NULL,'approval_fingerprint',NULL,'source_blocks',variant->'blocks');
    refs:=jsonb_build_object(wanted_variant::text,1);
    default_ref:=jsonb_build_object('variant_id',wanted_variant,'revision',1);
    snapshot:=jsonb_build_object('default_variant_ref',default_ref,
      'variants',jsonb_build_array(default_ref),'state','drafting');
   ELSE
    IF (c->'ids'->>'story_id')::uuid<>head.story_id OR (c->'ids'->>'actor_id')::uuid<>actor
     OR (c->'expected_revisions'->>'story_revision')::bigint<>previous_revision
     OR (c->'expected_revisions'->>'source_revision')::bigint<>head.source_revision THEN RETURN false;END IF;
    role:=CASE WHEN actor=head.owner_id THEN 'manager' ELSE grants->actor::text->>'capability' END;
    IF c->>'route'='story.edit.v1' THEN
     IF version.action<>'upsert_variant' OR role IS NULL OR role NOT IN ('editor','publisher','manager') THEN RETURN false;END IF;
     op:=doc->'operation';variant:=doc;wanted_variant:=(doc->>'resolved_variant_id')::uuid;
     wanted_revision:=(c->'expected_revisions'->>'variant_revision')::bigint+1;
     IF wanted_revision<>coalesce((refs->>wanted_variant::text)::bigint,0)+1 THEN RETURN false;END IF;
     expected_data:=(op-ARRAY['op','expected_variant_revision'])||jsonb_build_object('variant_id',wanted_variant,
       'revision',wanted_revision,'state','drafting','approved_revision',NULL,'approval_fingerprint',NULL);
     refs:=refs||jsonb_build_object(wanted_variant::text,wanted_revision);
     IF default_ref->>'variant_id'=wanted_variant::text THEN
      default_ref:=jsonb_build_object('variant_id',wanted_variant,'revision',wanted_revision);
     END IF;
     SELECT coalesce(jsonb_agg(jsonb_build_object('variant_id',key,'revision',value) ORDER BY key),'[]'::jsonb)
      INTO variants FROM jsonb_each(refs);
     snapshot:=jsonb_build_object('default_variant_ref',default_ref,'variants',variants,'state','drafting');
    ELSIF c->>'route'='story.access.v1' THEN
     IF version.action<>'access_change' OR role IS DISTINCT FROM 'manager' THEN RETURN false;END IF;
     IF doc->>'capability'='revoke' THEN grants:=grants-(doc->>'grantee_user_id');
     ELSE grants:=grants||jsonb_build_object(doc->>'grantee_user_id',jsonb_build_object('capability',doc->>'capability',
       'granted_by',actor,'story_revision',version.revision));END IF;
    ELSE RETURN false;END IF;
   END IF;
   IF version.data IS DISTINCT FROM snapshot THEN RETURN false;END IF;
   IF version.action IN ('source_import','upsert_variant') THEN
    variant_rows:=variant_rows+1;
    IF NOT EXISTS(SELECT 1 FROM rkb_v4.editorial_variant_versions v WHERE v.story_id=head.story_id
      AND v.variant_id=wanted_variant AND v.revision=wanted_revision AND v.data=expected_data
      AND v.body=expected_data->>'body' AND v.body_sha256=encode(sha256(convert_to(v.body,'UTF8')),'hex')
      AND v.embedding OPERATOR(public.=) (variant->'embedding')::text::public.vector
      AND v.input_tokens=(variant->>'input_tokens')::integer AND v.actor_id=actor AND v.logical_time=c->>'logical_time') THEN RETURN false;END IF;
    IF default_ref->>'variant_id'=wanted_variant::text THEN
     projection_rows:=projection_rows+1;
     IF NOT EXISTS(SELECT 1 FROM rkb_v4.editorial_story_search p WHERE p.story_id=head.story_id
       AND p.variant_id=wanted_variant AND p.variant_revision=wanted_revision
       AND p.source_id=head.source_id AND p.source_revision=head.source_revision AND p.body=expected_data->>'body'
       AND p.config=CASE expected_data->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
        WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END
       AND p.embedding OPERATOR(public.=) (variant->'embedding')::text::public.vector) THEN RETURN false;END IF;
    END IF;
   END IF;
   previous_revision:=version.revision;previous_sequence:=version.sequence;
  END LOOP;
  IF head.revision<>previous_revision OR head.default_variant_id::text IS DISTINCT FROM default_ref->>'variant_id'
   OR head.default_variant_revision IS DISTINCT FROM (default_ref->>'revision')::bigint
   OR (SELECT count(*) FROM rkb_v4.editorial_variant_versions WHERE story_id=head.story_id)<>variant_rows
   OR (SELECT count(*) FROM rkb_v4.editorial_story_search WHERE story_id=head.story_id)<>projection_rows
   OR (SELECT count(*) FROM rkb_v4.editorial_story_receipts WHERE story_id=head.story_id)<>previous_revision THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(variant_id::text,revision),'{}'::jsonb) INTO actual
   FROM rkb_v4.editorial_variant_heads WHERE story_id=head.story_id;
  IF actual IS DISTINCT FROM refs THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(grantee_id::text,jsonb_build_object('capability',capability,
    'granted_by',granted_by,'story_revision',story_revision)),'{}'::jsonb) INTO actual
   FROM rkb_v4.story_editor_grants WHERE story_id=head.story_id;
  IF actual IS DISTINCT FROM grants THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.verify_source_image_origins_v1(scope uuid[])
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE op record;c jsonb;doc jsonb;sem jsonb;source uuid;item jsonb;v jsonb;
 verified uuid[]:='{}';seen integer:=0;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 IF cardinality(scope)=0 THEN RETURN true;END IF;
 FOR op IN SELECT a.command_bytes FROM rkb_v4.source_image_origins origin JOIN rkb_v4.applied_ops a USING(operation_key) WHERE a.status='applied' AND origin.source_id=ANY(scope) ORDER BY a.sequence LIMIT 10001 LOOP
  seen:=seen+1;IF seen>10000 THEN RETURN false;END IF;
  c:=convert_from(op.command_bytes,'UTF8')::jsonb;
  IF c->>'route' IS DISTINCT FROM 'source.activate.v1' OR NOT((c->'ids'->>'source_id')::uuid=ANY(scope)) THEN CONTINUE;END IF;
  source:=(c->'ids'->>'source_id')::uuid;
  doc:=rkb_v4.source_activation_doc_v1(source);sem:=doc->'semantic';
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_image_origins o JOIN rkb_v4.source_activation_candidates k USING(source_id)
    JOIN rkb_v4.applied_ops a ON a.operation_key=o.operation_key WHERE o.source_id=source
    AND o.operation_key=c->>'operation_key' AND o.sequence=a.sequence AND o.sequence=(c->>'sequence')::bigint
    AND o.command_sha256=a.command_sha256 AND o.command_sha256=encode(sha256(a.command_bytes),'hex')
    AND o.person_id::text=c->>'person_id' AND o.request_id=c->>'request_id'
    AND o.ingestion_id=k.ingestion_id AND o.batch_count=k.batch_count AND o.last_batch_sha256=k.last_batch_sha256
    AND o.materializer_sha256=k.materializer_sha256 AND o.knowledge_sha256=k.knowledge_sha256
    AND k.knowledge_sha256=encode(sha256(k.knowledge_bytes),'hex')
    AND o.activation_payload_sha256=encode(sha256(decode(c->>'payload_b64','base64')),'hex')
    AND convert_from(decode(convert_from(a.outcome_bytes,'UTF8')::jsonb->>'result_b64','base64'),'UTF8')::jsonb->>'knowledge_sha256'=k.knowledge_sha256
    AND convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb=jsonb_build_object(
      'codec','rkb.source-activate.v1','ingestion_id',k.ingestion_id,'batch_count',k.batch_count,
      'last_batch_sha256',k.last_batch_sha256,'materializer_sha256',k.materializer_sha256)
    AND rkb_v4.verify_source_stage_chain_v1(source,k.ingestion_id,k.batch_count,k.last_batch_sha256)) THEN RETURN false;END IF;
  verified:=array_append(verified,source);
  IF NOT rkb_v4.verify_source_image_manifest_v1(source,doc,
    encode(sha256(decode(c->>'payload_b64','base64')),'hex')) THEN RETURN false;END IF;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_manifests m
   WHERE m.source_id=source AND m.source_revision=1 AND m.manifest=doc->'image_manifest'
    AND m.payload_sha256=encode(sha256(decode(c->>'payload_b64','base64')),'hex')
    AND m.original_receipt=decode(doc->'image_manifest'->>'original_receipt_b64','base64')) THEN RETURN false;END IF;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_import_origins o WHERE o.source_id=source
    AND o.payload_sha256=encode(sha256(decode(c->>'payload_b64','base64')),'hex'))
   OR (SELECT count(*) FROM rkb_v4.source_regions r WHERE r.source_id=source)<>jsonb_array_length(sem->'regions')
   OR (SELECT count(*) FROM rkb_v4.story_assertion_versions a WHERE a.source_id=source)<>jsonb_array_length(sem->'findings')
   OR (SELECT count(*) FROM rkb_v4.stories s WHERE s.source_id=source)<>jsonb_array_length(sem->'stories')
   OR (SELECT count(*) FROM rkb_v4.source_story_pending p WHERE p.source_id=source)<>jsonb_array_length(sem->'pending')
   OR (SELECT count(*) FROM rkb_v4.passage_search p WHERE p.source_id=source)<>jsonb_array_length(doc->'passages') THEN RETURN false;END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(sem->'regions') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_regions r WHERE r.source_id=source AND r.source_revision=1
      AND r.region_id=(item->>'region_id')::uuid AND r.page_id=(item->>'page_id')::uuid
      AND r.source_text=item->>'source_text' AND r.source_text_sha256=item->>'source_text_sha256') THEN RETURN false;END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(sem->'findings') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.story_assertion_versions a WHERE a.source_id=source AND a.source_revision=1
      AND a.assertion_id=(item->>'assertion_id')::uuid AND a.revision=1 AND a.data=item
      AND a.candidate_key=item->>'candidate_key' AND a.proposition=item->>'proposition') THEN RETURN false;END IF;
   IF (SELECT count(*) FROM rkb_v4.source_event_dates WHERE assertion_id=(item->>'assertion_id')::uuid)<>jsonb_array_length(item->'event_dates')
    OR (SELECT count(*) FROM rkb_v4.source_observations WHERE assertion_id=(item->>'assertion_id')::uuid)<>jsonb_array_length(item->'observations')
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(item->'event_dates') WITH ORDINALITY x(d,n)
     WHERE NOT EXISTS(SELECT 1 FROM rkb_v4.source_event_dates z WHERE z.assertion_id=(item->>'assertion_id')::uuid
      AND z.assertion_revision=1 AND z.ordinal=x.n-1 AND z.source_id=source AND z.data=x.d
      AND z.proofs=rkb_v4.source_fact_proofs_v1(item,x.d->'evidence_indexes')
      AND z.calendar=x.d->>'calendar' AND z.precision=x.d->>'precision' AND z.date_role=x.d->>'date_role'
      AND z.year IS NOT DISTINCT FROM (x.d->>'year')::integer AND z.month IS NOT DISTINCT FROM (x.d->>'month')::integer
      AND z.day IS NOT DISTINCT FROM (x.d->>'day')::integer))
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(item->'observations') WITH ORDINALITY x(d,n)
     WHERE NOT EXISTS(SELECT 1 FROM rkb_v4.source_observations z WHERE z.assertion_id=(item->>'assertion_id')::uuid
      AND z.assertion_revision=1 AND z.ordinal=x.n-1 AND z.source_id=source AND z.data=x.d
      AND z.proofs=rkb_v4.source_fact_proofs_v1(item,x.d->'evidence_indexes')
      AND z.metric_key=x.d->>'metric_key' AND z.unit_code=x.d->>'unit_code' AND z.value_decimal=(x.d->>'value')::numeric))
   THEN RETURN false;END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.passage_search p WHERE p.source_id=source AND p.revision=1
     AND p.passage_id=(item->>'passage_id')::uuid AND p.ordinal=(item->>'ordinal')::integer
     AND p.locator=item->>'locator' AND p.body=item->>'body' AND p.language=item->>'language'
     AND p.embedding OPERATOR(public.=) (item->'embedding')::text::public.vector) THEN RETURN false;END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(sem->'stories') LOOP
   v:=item->'default_variant';
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.stories s JOIN rkb_v4.story_versions r USING(story_id)
     JOIN rkb_v4.story_variant_versions b ON b.story_id=s.story_id AND b.variant_id=s.default_variant_id
       AND b.revision=s.default_variant_revision
     JOIN rkb_v4.story_search p ON p.story_id=s.story_id
     JOIN rkb_v4.story_history_receipts h ON h.story_id=s.story_id AND h.revision=1
     WHERE s.source_id=source AND s.source_revision=1 AND s.story_id=(item->>'story_id')::uuid
       AND s.current_revision=1 AND s.default_variant_id=(v->>'variant_id')::uuid AND s.default_variant_revision=1
       AND r.revision=1 AND r.data=item-'default_variant' AND r.logical_time=c->>'logical_time'
       AND r.payload_sha256=encode(sha256(decode(c->>'payload_b64','base64')),'hex')
       AND b.body=v->>'body' AND b.data=v-'embedding'
       AND b.body_sha256=encode(sha256(convert_to(v->>'body','UTF8')),'hex')
       AND p.variant_id=b.variant_id AND p.variant_revision=1 AND p.source_id=source AND p.source_revision=1
       AND p.body=b.body AND p.embedding OPERATOR(public.=) (v->'embedding')::text::public.vector
       AND h.operation_key=c->>'operation_key' AND h.sequence=(c->>'sequence')::bigint
       AND h.person_id=c->>'person_id' AND h.request_id=c->>'request_id' AND h.logical_time=c->>'logical_time') THEN RETURN false;END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(sem->'pending') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_story_pending p WHERE p.source_id=source
     AND p.candidate_key=item->>'candidate_key' AND p.data=item) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.source_regions WHERE source_id=ANY(scope) AND NOT(source_id=ANY(verified)))
   AND NOT EXISTS(SELECT 1 FROM rkb_v4.story_assertion_versions WHERE source_id=ANY(scope) AND NOT(source_id=ANY(verified)))
   AND NOT EXISTS(SELECT 1 FROM rkb_v4.stories WHERE source_id=ANY(scope) AND NOT(source_id=ANY(verified)))
   AND rkb_v4.verify_editorial_story_origins_v2(scope);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.verify_source_story_origins_v4(scope uuid[]) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE legacy uuid[]; images uuid[];
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 SELECT coalesce(array_agg(x),'{}'::uuid[]) INTO images FROM unnest(scope) x
  WHERE EXISTS(SELECT 1 FROM rkb_v4.source_image_origins WHERE source_id=x);
 SELECT coalesce(array_agg(x),'{}'::uuid[]) INTO legacy FROM unnest(scope) x WHERE NOT(x=ANY(images));
 RETURN rkb_v4.verify_source_story_origins_v3(legacy) AND rkb_v4.verify_source_image_origins_v1(images);
END $$;

CREATE FUNCTION rkb_v4.verify_source_graph_origins_v2(scope uuid[]) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE g rkb_v4.source_graph_documents;c jsonb;doc jsonb;e jsonb;a jsonb;r jsonb;f jsonb;mention jsonb;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 FOR g IN SELECT * FROM rkb_v4.source_graph_documents WHERE source_id=ANY(scope) LOOP
  SELECT convert_from(op.command_bytes,'UTF8')::jsonb INTO STRICT c FROM rkb_v4.source_graph_receipts receipt
   JOIN rkb_v4.applied_ops op USING(operation_key) WHERE receipt.source_id=g.source_id AND op.status='applied'
    AND receipt.sequence=op.sequence AND receipt.command_sha256=op.command_sha256
    AND receipt.person_id=g.owner_person_id;
  IF c->>'route'='source.activate.v1' THEN
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_image_origins o JOIN rkb_v4.source_activation_candidates k USING(source_id)
      WHERE o.source_id=g.source_id AND o.operation_key=c->>'operation_key'
       AND o.knowledge_sha256=k.knowledge_sha256 AND o.person_id=g.owner_person_id) THEN RETURN false;END IF;
   doc:=rkb_v4.source_activation_doc_v1(g.source_id);
  ELSE doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;END IF;
  IF c->>'route' NOT IN ('source.import.v4','source.activate.v1') OR c->'ids'->>'source_id' IS DISTINCT FROM g.source_id::text
   OR c->>'person_id' IS DISTINCT FROM g.owner_person_id::text OR doc->'source_graph' IS DISTINCT FROM g.data
   OR g.payload_sha256 IS DISTINCT FROM encode(sha256(decode(c->>'payload_b64','base64')),'hex')
   OR (SELECT count(*) FROM rkb_v4.source_entity_representations WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'entities')
   OR (SELECT count(*) FROM rkb_v4.source_entity_references WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'references')
   OR (SELECT count(*) FROM rkb_v4.source_entity_mentions WHERE source_id=g.source_id)<>
     (SELECT count(DISTINCT mention_row.value->>'mention_id') FROM jsonb_array_elements(g.data->'entities') node_row(value)
       CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN g.data->>'codec'='rkb.source-graph.v2'
         THEN node_row.value->'mentions' ELSE jsonb_build_array(node_row.value->'mention') END) mention_row(value))
   OR (SELECT count(*) FROM rkb_v4.source_entity_aliases WHERE source_id=g.source_id)<>
     (SELECT count(DISTINCT alias_row.value->>'alias_id') FROM jsonb_array_elements(g.data->'entities') AS node_row(value)
       CROSS JOIN LATERAL jsonb_array_elements(node_row.value->'aliases') AS alias_row(value))
   OR (SELECT count(*) FROM rkb_v4.source_entity_relations WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'relations')
   OR (SELECT count(*) FROM rkb_v4.source_poi_hypotheses WHERE source_id=g.source_id)<>jsonb_array_length(g.data->'poi_facts')
   OR (SELECT count(*) FROM rkb_v4.source_entity_finding_links WHERE source_id=g.source_id)<>
     (SELECT coalesce(sum(jsonb_array_length(value->'entity_keys')),0) FROM jsonb_array_elements(g.data->'finding_entities')) THEN RETURN false;END IF;
  FOR e IN SELECT value FROM jsonb_array_elements(g.data->'entities') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_representations x JOIN rkb_v4.source_entity_ids h USING(entity_id)
    WHERE x.source_id=g.source_id AND x.entity_key=e->>'key' AND x.entity_id=(e->>'entity_id')::uuid AND x.data=e
     AND h.owner_person_id=g.owner_person_id AND h.kind=e->>'kind') THEN RETURN false;END IF;
   FOR mention IN SELECT value FROM jsonb_array_elements(CASE WHEN g.data->>'codec'='rkb.source-graph.v2'
      THEN e->'mentions' ELSE jsonb_build_array(e->'mention') END) LOOP
    IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_mentions x WHERE x.source_id=g.source_id
      AND x.mention_id=(mention->>'mention_id')::uuid AND x.entity_id=(e->>'entity_id')::uuid AND x.data=mention) THEN RETURN false;END IF;
   END LOOP;
   FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
    IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_aliases x WHERE x.source_id=g.source_id AND x.alias_id=(a->>'alias_id')::uuid
     AND x.entity_id=(e->>'entity_id')::uuid AND x.normalized_value=a->>'normalized_value'
     AND x.normalized_sha256=encode(sha256(convert_to(a->>'normalized_value','UTF8')),'hex') AND x.data=a) THEN RETURN false;END IF;
   END LOOP;
  END LOOP;
  FOR r IN SELECT value FROM jsonb_array_elements(g.data->'references') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_references x JOIN rkb_v4.source_entity_ids h USING(entity_id)
    WHERE x.source_id=g.source_id AND x.entity_key=r->>'key' AND x.entity_id=(r->>'entity_id')::uuid
     AND h.owner_person_id=g.owner_person_id AND x.data=r) THEN RETURN false;END IF;
  END LOOP;
  FOR r IN SELECT value FROM jsonb_array_elements(g.data->'relations') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_relations x WHERE x.source_id=g.source_id AND x.relation_id=(r->>'relation_id')::uuid
    AND x.from_id=(r->>'source_id')::uuid AND x.to_id=(r->>'target_id')::uuid AND x.kind=r->>'kind' AND x.data=r) THEN RETURN false;END IF;
  END LOOP;
  FOR f IN SELECT value FROM jsonb_array_elements(g.data->'poi_facts') LOOP
   IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_poi_hypotheses x WHERE x.source_id=g.source_id AND x.candidate_id=(f->>'candidate_id')::uuid AND x.data=f) THEN RETURN false;END IF;
  END LOOP;
  FOR f IN SELECT value FROM jsonb_array_elements(g.data->'finding_entities') LOOP
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(f->'entity_keys') WITH ORDINALITY k(name,n) WHERE NOT EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_finding_links x WHERE x.source_id=g.source_id AND x.assertion_id=(f->>'assertion_id')::uuid
     AND x.entity_key=k.name AND x.entity_id=(f->'entity_ids'->>(k.n::integer-1))::uuid)) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 -- Accepted sources containing a graph must have a receipt; bare rows cannot
 -- create an origin proof, including orphan canonical identities.
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.applied_ops op CROSS JOIN LATERAL (SELECT convert_from(op.command_bytes,'UTF8')::jsonb command_doc) x
   WHERE op.status='applied' AND x.command_doc->>'route'='source.import.v4' AND (x.command_doc->'ids'->>'source_id')::uuid=ANY(scope)
    AND convert_from(decode(x.command_doc->>'payload_b64','base64'),'UTF8')::jsonb ? 'source_graph'
    AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_graph_documents z WHERE z.source_id=(x.command_doc->'ids'->>'source_id')::uuid))
  AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_image_origins o JOIN rkb_v4.source_activation_candidates k USING(source_id)
   WHERE o.source_id=ANY(scope) AND k.knowledge ? 'source_graph'
    AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_graph_documents z WHERE z.source_id=o.source_id))
  AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids h WHERE NOT EXISTS(
    SELECT 1 FROM rkb_v4.source_entity_representations x WHERE x.entity_id=h.entity_id));
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

REVOKE ALL ON rkb_v4.source_activation_candidates,rkb_v4.source_activation_vectors,
 rkb_v4.source_activation_materialized,rkb_v4.source_image_origins FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.project_source_graph_v2(uuid,jsonb,text),
 rkb_v4.source_activation_knowledge_v1(jsonb),
 rkb_v4.verify_source_stage_chain_v1(uuid,uuid,integer,text),
 rkb_v4.check_source_activation_base_v1(bytea,jsonb,jsonb,text),
 rkb_v4.check_source_activation_semantic_v1(bytea,jsonb,jsonb,text),
 rkb_v4.check_source_image_passages_v1(jsonb),
 rkb_v4.check_source_image_graph_evidence_v1(jsonb,jsonb),
 rkb_v4.check_source_image_graph_v1(jsonb,uuid,integer),
 rkb_v4.check_source_image_manifest_v1(jsonb,uuid),
 rkb_v4.prepare_source_activation_v1(uuid,uuid,integer,text,text,bytea,text,text),
 rkb_v4.source_activation_input_v1(uuid,integer),
 rkb_v4.install_source_activation_vector_v1(uuid,text,integer,public.vector,text,text),
 rkb_v4.check_source_materialize_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_materialize_v1(bytea,jsonb,jsonb,text),
 rkb_v4.source_activation_doc_v1(uuid),
 rkb_v4.check_source_activate_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_activate_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_source_image_receipt_v1(),
 rkb_v4.read_source_activation_input_v1(uuid,text,integer),
 rkb_v4.verify_source_image_manifest_v1(uuid,jsonb,text),
 rkb_v4.verify_editorial_story_origins_v2(uuid[]),
 rkb_v4.verify_source_image_origins_v1(uuid[]),
 rkb_v4.verify_source_story_origins_v4(uuid[]),
 rkb_v4.verify_source_graph_origins_v2(uuid[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION rkb_v4.source_stage_latest_page_v1(uuid,uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION rkb_v4.verify_source_open_stage_chain_v1(uuid,uuid,integer,text),
 rkb_v4.check_source_stage_v2(bytea,jsonb,jsonb,text),rkb_v4.apply_source_stage_v2(bytea,jsonb,jsonb,text),
 rkb_v4.check_source_stage_seal_v2(bytea,jsonb,jsonb,text),rkb_v4.apply_source_stage_seal_v2(bytea,jsonb,jsonb,text) FROM PUBLIC;
