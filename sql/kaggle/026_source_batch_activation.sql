-- Packet-scoped publication. Historical SQL001-025/accepted bytes stay unchanged.
-- Knowledge and provider outcomes remain immutable; public projections are
-- derived per packet. No whole-book seal or activation candidate is required.
CREATE TABLE rkb_v4.source_batch_prepared (
 source_id uuid NOT NULL REFERENCES rkb_v4.source_stage_sessions,
 batch_index integer NOT NULL CHECK(batch_index BETWEEN 0 AND 2499),
 command_sha256 text NOT NULL CHECK(command_sha256 ~ '^[0-9a-f]{64}$'),
 command_bytes bytea NOT NULL CHECK(command_sha256=encode(sha256(command_bytes),'hex')),
 knowledge_bytes bytea NOT NULL, knowledge_sha256 text NOT NULL,
 knowledge jsonb NOT NULL, media jsonb NOT NULL,
 PRIMARY KEY(source_id,batch_index),
 CHECK(knowledge_sha256=encode(sha256(knowledge_bytes),'hex')),
 CHECK(knowledge=convert_from(knowledge_bytes,'UTF8')::jsonb)
);
CREATE TABLE rkb_v4.source_batch_vectors (
 source_id uuid NOT NULL, batch_index integer NOT NULL, input_id uuid NOT NULL,
 input_sha256 text NOT NULL, embedding public.vector(1024) NOT NULL,
 PRIMARY KEY(source_id,batch_index,input_id),
 FOREIGN KEY(source_id,batch_index) REFERENCES rkb_v4.source_batch_prepared,
 CHECK(abs(public.vector_norm(embedding)-1.0)<=0.002)
);
CREATE TABLE rkb_v4.source_batch_receipts (
 source_id uuid NOT NULL, batch_index integer NOT NULL,
 operation_key text NOT NULL UNIQUE REFERENCES rkb_v4.applied_ops DEFERRABLE INITIALLY DEFERRED,
 sequence bigint NOT NULL, command_sha256 text NOT NULL, knowledge_sha256 text NOT NULL,
 PRIMARY KEY(source_id,batch_index),
 FOREIGN KEY(source_id,batch_index) REFERENCES rkb_v4.source_batch_prepared
);
CREATE TABLE rkb_v4.source_batch_publications (
 source_id uuid NOT NULL, batch_index integer NOT NULL,manifest jsonb NOT NULL,media jsonb NOT NULL,
 figure_ids uuid[] NOT NULL,PRIMARY KEY(source_id,batch_index),
 FOREIGN KEY(source_id,batch_index) REFERENCES rkb_v4.source_batch_receipts
);
CREATE INDEX source_batch_public_figures ON rkb_v4.source_batch_publications USING gin(figure_ids);
CREATE TRIGGER immutable_source_batch_publications BEFORE UPDATE OR DELETE ON rkb_v4.source_batch_publications
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_batch_prepared BEFORE UPDATE OR DELETE ON rkb_v4.source_batch_prepared
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_batch_vectors BEFORE UPDATE OR DELETE ON rkb_v4.source_batch_vectors
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_batch_receipts BEFORE UPDATE OR DELETE ON rkb_v4.source_batch_receipts
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.source_batch_inputs_v1(doc jsonb)
RETURNS TABLE(input_id uuid,body text,input_tokens integer)
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT (p->>'passage_id')::uuid,p->>'body',(p->>'input_tokens')::integer
 FROM jsonb_array_elements(doc->'passages') p
 UNION ALL
 SELECT (f->>'fragment_id')::uuid,
 substring(s->'default_variant'->>'body' FROM (f->>'start')::integer+1
  FOR (f->>'end')::integer-(f->>'start')::integer),(f->>'input_tokens')::integer
 FROM jsonb_array_elements(doc->'semantic'->'stories') s
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN s->'default_variant' ? 'fragments'
  THEN s->'default_variant'->'fragments' ELSE jsonb_build_array(jsonb_build_object(
  'fragment_id',s->'default_variant'->>'variant_id','start',0,
  'end',length(s->'default_variant'->>'body'),'input_tokens',s->'default_variant'->'input_tokens')) END) f
$$;

CREATE FUNCTION rkb_v4.source_batch_conflicts_v1(source uuid,doc jsonb) RETURNS boolean
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'passages') p
  JOIN rkb_v4.passage_search old ON old.passage_id=(p->>'passage_id')::uuid
  WHERE old.source_id<>source OR old.revision<>1 OR old.body IS DISTINCT FROM p->>'body'
   OR old.language IS DISTINCT FROM p->>'language' OR old.locator IS DISTINCT FROM p->>'locator')
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'regions') r
  JOIN rkb_v4.source_regions old ON old.region_id=(r->>'region_id')::uuid
  WHERE old.source_id<>source OR old.page_id<>(r->>'page_id')::uuid
   OR old.source_text IS DISTINCT FROM r->>'source_text' OR old.source_text_sha256 IS DISTINCT FROM r->>'source_text_sha256')
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'findings') f
  JOIN rkb_v4.story_assertion_versions old ON old.assertion_id=(f->>'assertion_id')::uuid
  WHERE old.source_id<>source OR old.data IS DISTINCT FROM f)
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'stories') s
  JOIN rkb_v4.story_versions old ON old.story_id=(s->>'story_id')::uuid
  WHERE old.data IS DISTINCT FROM s-'default_variant')
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'semantic'->'stories') s
  JOIN rkb_v4.story_variant_versions old ON old.variant_id=(s->'default_variant'->>'variant_id')::uuid
  WHERE old.story_id<>(s->>'story_id')::uuid OR old.data IS DISTINCT FROM s->'default_variant')
$$;

CREATE FUNCTION rkb_v4.prepare_source_batch_v1(command_bytes bytea,knowledge_bytes bytea,media jsonb,
 expected_database text,expected_writer text) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;d jsonb;doc jsonb;source uuid;session rkb_v4.source_stage_sessions;
 state rkb_v4.recovery_state;prior rkb_v4.source_batch_prepared;last_stage jsonb;f jsonb;h text;idx integer;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation IS DISTINCT FROM expected_database OR state.writer_generation IS DISTINCT FROM expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired)
  THEN RAISE EXCEPTION 'source packet writer changed';END IF;
 IF octet_length(command_bytes) NOT BETWEEN 1 AND 8192 OR octet_length(knowledge_bytes) NOT BETWEEN 1 AND 33554432
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='source packet envelope invalid';END IF;
 c:=convert_from(command_bytes,'UTF8')::jsonb;d:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
 source:=(c->'ids'->>'source_id')::uuid;idx:=(d->>'batch_index')::integer;
 IF c->>'route' IS DISTINCT FROM 'source.batch.v1' OR d->>'codec' IS DISTINCT FROM 'rkb.source-batch.v1'
  OR d->>'materializer_sha256' !~ '^[0-9a-f]{64}$' OR idx IS DISTINCT FROM (d->>'batch_count')::integer-1
  OR c->'expected_revisions' IS DISTINCT FROM jsonb_build_object('batch_head',d->>'last_batch_sha256')
  OR NOT rkb_v4.verify_source_open_stage_chain_v1(source,(d->>'ingestion_id')::uuid,
   (d->>'batch_count')::integer,d->>'last_batch_sha256') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='exact current accepted packet required';END IF;
 SELECT * INTO STRICT session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 SELECT convert_from(a.command_bytes,'UTF8')::jsonb INTO STRICT last_stage
  FROM rkb_v4.source_stage_batches b JOIN rkb_v4.applied_ops a ON a.sequence=b.accepted_sequence
  WHERE b.source_id=source AND b.batch_index=idx AND a.status='applied';
 IF last_stage->>'person_id' IS DISTINCT FROM c->>'person_id' OR EXISTS(
  SELECT 1 FROM rkb_v4.sources WHERE source_id=source AND (NOT accepted OR withdrawn)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='owned current source required';END IF;
 h:=encode(sha256(knowledge_bytes),'hex');doc:=convert_from(knowledge_bytes,'UTF8')::jsonb;
 SELECT * INTO prior FROM rkb_v4.source_batch_prepared WHERE source_id=source AND batch_index=idx;
 IF FOUND THEN
  IF prior.command_sha256<>encode(sha256(command_bytes),'hex') OR prior.knowledge_bytes<>knowledge_bytes
   OR prior.media<>media THEN RAISE EXCEPTION 'source packet preparation changed';END IF;
  RETURN h;
 END IF;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-projection.v1'
  OR NOT rkb_v4.check_source_batch_semantic_v1(convert_to(((doc-ARRAY['image_manifest','source_graph','public_rights'])||
   '{"codec":"rkb.source-activation-semantic.v1"}'::jsonb)::text,'UTF8'),jsonb_build_object('source_id',source),
   '{"absent_source":"true"}'::jsonb,c->>'logical_time')
  OR NOT rkb_v4.check_source_batch_manifest_v1(doc,source)
  OR NOT rkb_v4.check_source_batch_graph_v1(doc,source,(d->>'batch_count')::integer)
  OR NOT rkb_v4.check_source_batch_media_v1(doc,media,source)
  OR rkb_v4.source_batch_conflicts_v1(source,doc)
  OR doc->'research' IS DISTINCT FROM convert_from(decode(last_stage->>'payload_b64','base64'),'UTF8')::jsonb->'research'
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='source packet validation failed';END IF;
 FOR f IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
  IF NOT rkb_v4.check_source_facts_v1(f) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='source packet fact evidence invalid';END IF;
 END LOOP;
 INSERT INTO rkb_v4.source_batch_prepared VALUES(source,idx,encode(sha256(command_bytes),'hex'),command_bytes,knowledge_bytes,h,doc,media);
 RETURN h;
END $$;

CREATE FUNCTION rkb_v4.install_source_batch_vector_v1(source uuid,idx integer,ident uuid,input_hash text,
 embedding public.vector,expected_database text,expected_writer text) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state;input record;prior rkb_v4.source_batch_vectors;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation IS DISTINCT FROM expected_database OR state.writer_generation IS DISTINCT FROM expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired)
  THEN RAISE EXCEPTION 'source packet vector writer changed';END IF;
 SELECT i.* INTO STRICT input FROM rkb_v4.source_batch_prepared p
  CROSS JOIN LATERAL rkb_v4.source_batch_inputs_v1(p.knowledge) i
  WHERE p.source_id=source AND p.batch_index=idx AND i.input_id=ident;
 IF input_hash IS DISTINCT FROM encode(sha256(convert_to(input.body,'UTF8')),'hex')
  OR input.input_tokens NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'source packet exact vector input required';END IF;
 SELECT * INTO prior FROM rkb_v4.source_batch_vectors WHERE source_id=source AND batch_index=idx AND input_id=ident;
 IF FOUND THEN
  IF prior.input_sha256<>input_hash THEN RAISE EXCEPTION 'source packet vector input changed';END IF;
  RETURN input_hash;
 END IF;
 INSERT INTO rkb_v4.source_batch_vectors VALUES(source,idx,ident,input_hash,embedding);
 RETURN input_hash;
END $$;
CREATE FUNCTION rkb_v4.check_source_batch_base_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
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
           OR item->>'language' NOT IN ('ru','de','simple')
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

CREATE FUNCTION rkb_v4.check_source_batch_semantic_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;sem jsonb;item jsonb;e jsonb;r jsonb;story jsonb;v jsonb;b jsonb;f jsonb;
 base bytea;regions uuid[]:='{}';findings uuid[]:='{}';stories uuid[]:='{}';variants uuid[]:='{}';
 candidates text[]:='{}';pending text[]:='{}';covered uuid[]:='{}';refs uuid[];
 ident uuid;region uuid;passage uuid;begin_offset integer;end_offset integer;body_end integer;vector public.vector;
 fragment_end integer;fragment_id uuid;fragment_ids uuid[]:='{}';
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 67108864 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-activation-semantic.v1' OR NOT doc?'semantic' THEN RETURN false;END IF;
 base:=convert_to(((doc-'semantic')||'{"codec":"rkb.source-activation-base.v1"}'::jsonb)::text,'UTF8');
 IF NOT rkb_v4.check_source_batch_base_v1(base,ids,expected,logical_time) THEN RETURN false;END IF;
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
  IF ident::text<>item->>'region_id' OR ident=ANY(regions) THEN RETURN false;END IF;
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
  IF ident::text<>item->>'assertion_id' OR ident=ANY(findings) THEN RETURN false;END IF;
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
  IF ident::text<>story->>'story_id' OR ident=ANY(stories) THEN RETURN false;END IF;
  SELECT array_agg(value::uuid) INTO refs FROM jsonb_array_elements_text(story->'finding_ids');
  IF NOT refs<@findings OR cardinality(refs)<>(SELECT count(DISTINCT x) FROM unnest(refs) x) THEN RETURN false;END IF;
  covered:=covered||refs;v:=story->'default_variant';
  IF jsonb_typeof(v) IS DISTINCT FROM 'object' OR NOT v ?& ARRAY['variant_id','revision','audience','format','language','body','blocks','input_tokens']
     OR v-ARRAY['variant_id','revision','audience','format','language','body','blocks','input_tokens','fragments']<>'{}'::jsonb
     OR v->'revision' IS DISTINCT FROM '1'::jsonb OR v->>'format' IS DISTINCT FROM 'story'
     OR jsonb_typeof(v->'audience') IS DISTINCT FROM 'string' OR length(v->>'audience') NOT BETWEEN 1 AND 500
     OR jsonb_typeof(v->'language') IS DISTINCT FROM 'string' OR length(v->>'language') NOT BETWEEN 2 AND 24
     OR jsonb_typeof(v->'body') IS DISTINCT FROM 'string' OR length(v->>'body') NOT BETWEEN 1 AND 4000
     OR jsonb_typeof(v->'blocks') IS DISTINCT FROM 'array' OR jsonb_array_length(v->'blocks') NOT BETWEEN 1 AND 25
     OR jsonb_typeof(v->'input_tokens') IS DISTINCT FROM 'number' OR v->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
     OR (v->>'input_tokens')::integer NOT BETWEEN 1 AND 512 THEN RETURN false;END IF;
  passage:=(v->>'variant_id')::uuid;
  IF passage::text<>v->>'variant_id' OR passage=ANY(variants)
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
  -- Full immutable body remains unchanged; these only bound derived offsets.
  IF v ? 'fragments' THEN
   IF jsonb_typeof(v->'fragments') IS DISTINCT FROM 'array'
      OR jsonb_array_length(v->'fragments') NOT BETWEEN 2 AND 4000 THEN RETURN false;END IF;
   fragment_end:=0;fragment_ids:='{}';
   FOR f IN SELECT value FROM jsonb_array_elements(v->'fragments') LOOP
    IF jsonb_typeof(f) IS DISTINCT FROM 'object'
      OR NOT f ?& ARRAY['fragment_id','start','end','input_tokens']
      OR f-ARRAY['fragment_id','start','end','input_tokens']<>'{}'::jsonb
      OR jsonb_typeof(f->'fragment_id') IS DISTINCT FROM 'string'
      OR jsonb_typeof(f->'start') IS DISTINCT FROM 'number'
      OR f->>'start' !~ '^(0|[1-9][0-9]{0,3})$'
      OR jsonb_typeof(f->'end') IS DISTINCT FROM 'number'
      OR f->>'end' !~ '^[1-9][0-9]{0,3}$'
      OR jsonb_typeof(f->'input_tokens') IS DISTINCT FROM 'number'
      OR f->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
      OR (f->>'input_tokens')::integer NOT BETWEEN 1 AND 512
      OR (f->>'start')::integer<>fragment_end
      OR (f->>'end')::integer<=fragment_end
      OR (f->>'end')::integer>length(v->>'body') THEN RETURN false;END IF;
    fragment_id:=(f->>'fragment_id')::uuid;
    IF fragment_id::text IS DISTINCT FROM f->>'fragment_id'
       OR fragment_id=ANY(fragment_ids) THEN RETURN false;END IF;
    fragment_ids:=array_append(fragment_ids,fragment_id);
    fragment_end:=(f->>'end')::integer;
   END LOOP;
   IF fragment_end<>length(v->>'body')
      OR (v->>'input_tokens')::integer<>(v->'fragments'->0->>'input_tokens')::integer THEN RETURN false;END IF;
  END IF;
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
     OR EXISTS(SELECT 1 FROM jsonb_array_elements(sem->'findings') AS finding_row(value) WHERE finding_row.value->>'assertion_id'=ANY(refs::text[]) AND finding_row.value->>'candidate_key'<>item->>'candidate_key') THEN RETURN false;END IF;
  pending:=array_append(pending,item->>'candidate_key');
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_batch_manifest_v1(doc jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'image_manifest'; session rkb_v4.source_stage_sessions;
 page jsonb; region jsonb; native rkb_v4.source_stage_pages; issued jsonb; raw_region jsonb;
 actual jsonb; original jsonb; model_page jsonb; latest_pages jsonb; semantic_regions jsonb; total bigint; unique_ids bigint; idx integer:=0; seen uuid[]:='{}'; ident uuid; k text;
BEGIN
 SELECT * INTO STRICT session FROM rkb_v4.source_stage_sessions WHERE source_id=source;
 original:=convert_from(session.original_receipt,'UTF8')::jsonb;
 -- Decode each accepted batch once, preserving the latest explicit page correction.
 SELECT coalesce(jsonb_object_agg(selected.page_id,selected.model_page),'{}'::jsonb) INTO latest_pages
 FROM (SELECT DISTINCT ON (model.value->>'page_id') model.value->>'page_id' AS page_id,model.value AS model_page
       FROM rkb_v4.source_stage_batches batch_row CROSS JOIN LATERAL
        jsonb_array_elements(convert_from(batch_row.payload_bytes,'UTF8')::jsonb->'model_payload'->'pages') model(value)
       WHERE batch_row.source_id=source
       ORDER BY model.value->>'page_id',batch_row.batch_index DESC) selected;
 SELECT jsonb_object_agg(value->>'region_id',value),count(*),count(DISTINCT value->>'region_id')
 INTO semantic_regions,total,unique_ids FROM jsonb_array_elements(doc->'semantic'->'regions');
 IF total<>unique_ids THEN RETURN false;END IF;
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
  OR jsonb_typeof(m->'pages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'pages') NOT BETWEEN 1 AND session.page_count
  OR jsonb_typeof(m->'regions') IS DISTINCT FROM 'array'
  OR jsonb_array_length(m->'regions')<>jsonb_array_length(doc->'semantic'->'regions')
  OR (SELECT coalesce(sum(jsonb_array_length(coalesce((latest_pages->page_id::text)->'regions','[]'::jsonb))),0)
      FROM rkb_v4.source_stage_pages WHERE source_id=source
       AND page_id::text IN (SELECT value->>'page_id' FROM jsonb_array_elements(m->'pages')))<>jsonb_array_length(m->'regions') THEN RETURN false;END IF;
 FOR page IN SELECT value FROM jsonb_array_elements(m->'pages') LOOP
  idx:=(page->>'physical_page_index')::integer;
  SELECT * INTO STRICT native FROM rkb_v4.source_stage_pages WHERE source_id=source AND physical_page_index=idx;
  issued:=convert_from(native.image_receipt,'UTF8')::jsonb;
  model_page:=latest_pages->native.page_id::text;
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
  model_page:=latest_pages->native.page_id::text;
  SELECT r INTO STRICT raw_region FROM jsonb_array_elements(model_page->'regions') r WHERE r->>'region_key'=region->>'region_key';
  actual:=semantic_regions->(region->>'region_id');IF actual IS NULL THEN RETURN false;END IF;
  IF actual->>'page_id' IS DISTINCT FROM native.page_id::text
   OR actual->>'source_text' IS DISTINCT FROM coalesce(raw_region->>'source_text','')
   OR actual->>'source_text_sha256' IS DISTINCT FROM region->>'text_sha256'
   OR region->>'text_sha256' IS DISTINCT FROM encode(sha256(convert_to(coalesce(raw_region->>'source_text',''),'UTF8')),'hex')
   OR region->'kind' IS DISTINCT FROM raw_region->'kind' OR region->'bbox' IS DISTINCT FROM raw_region->'bbox'
   OR region->'reading_order' IS DISTINCT FROM raw_region->'reading_order'
   OR region->'column_id' IS DISTINCT FROM coalesce(raw_region->'column_id','null'::jsonb) THEN RETURN false;END IF;
 END LOOP;
 RETURN rkb_v4.(jsonb_array_length(doc->'passages')=0 AND m->'passages'='[]'::jsonb OR rkb_v4.check_source_image_passages_v1(doc));
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_batch_graph_v1(doc jsonb,source uuid,batches integer) RETURNS boolean
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
  IF e->>'mode'='create' AND EXISTS(SELECT 1 FROM rkb_v4.source_entity_ids WHERE entity_id=ident
     AND (owner_person_id<>owner OR kind<>e->>'kind'))
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

CREATE FUNCTION rkb_v4.check_source_batch_media_v1(doc jsonb,media jsonb,source uuid) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE item jsonb;page jsonb;region jsonb;proof jsonb;ref jsonb;link jsonb;caption text;
 ids uuid[]:='{}';links uuid[]:='{}';wanted uuid;raw_page jsonb;raw_figure jsonb;illustration_count integer:=0;
 staged jsonb;raw_link jsonb;expected_refs jsonb;entry record;
BEGIN
 IF media IS NULL OR jsonb_typeof(media) IS DISTINCT FROM 'object'
  OR NOT media ?& ARRAY['codec','source_id','source_revision','illustrations','passage_links','poi_media_links']
  OR media-ARRAY['codec','source_id','source_revision','illustrations','passage_links','poi_media_links']<>'{}'
  OR media->>'codec' IS DISTINCT FROM 'rkb.source-illustrations.v1'
  OR media->>'source_id' IS DISTINCT FROM source::text OR media->'source_revision' IS DISTINCT FROM '1'::jsonb
  OR jsonb_typeof(media->'illustrations') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'illustrations')>200*jsonb_array_length(doc->'image_manifest'->'pages')
  OR jsonb_typeof(media->'passage_links') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'passage_links')>jsonb_array_length(doc->'passages')
  OR jsonb_typeof(media->'poi_media_links') IS DISTINCT FROM 'array'
  OR jsonb_array_length(media->'poi_media_links')>10000 THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(media->'illustrations') LOOP
  IF NOT item ?& ARRAY['illustration_key','illustration_id','page_id','source_region_id','bbox','kind','caption_region_ids','nearby_region_ids','visual_description','visual_description_provenance','visual_description_language','display_rotation_degrees','caption_text','crop_binding']
   OR item-ARRAY['illustration_key','illustration_id','page_id','source_region_id','bbox','kind','caption_region_ids','nearby_region_ids','visual_description','visual_description_provenance','visual_description_language','display_rotation_degrees','caption_text','crop_binding']<>'{}'
   OR item->>'visual_description_provenance' IS DISTINCT FROM 'model_observation'
   OR item->>'display_rotation_degrees' NOT IN ('0','90','180','270')
   OR jsonb_typeof(item->'caption_region_ids') IS DISTINCT FROM 'array'
   OR jsonb_array_length(item->'caption_region_ids')>50
   OR jsonb_typeof(item->'nearby_region_ids') IS DISTINCT FROM 'array'
   OR jsonb_array_length(item->'nearby_region_ids')>100 THEN RETURN false;END IF;
  wanted:=(item->>'illustration_id')::uuid;
  IF wanted IS NULL OR wanted::text IS DISTINCT FROM item->>'illustration_id' OR wanted=ANY(ids) THEN RETURN false;END IF;
  ids:=array_append(ids,wanted);
  SELECT value INTO STRICT page FROM jsonb_array_elements(doc->'image_manifest'->'pages') WHERE value->>'page_id'=item->>'page_id';
  SELECT value INTO STRICT region FROM jsonb_array_elements(doc->'image_manifest'->'regions') WHERE value->>'region_id'=item->>'source_region_id';
  IF region->>'page_id' IS DISTINCT FROM item->>'page_id' OR region->>'kind' IS DISTINCT FROM 'figure'
   OR region->'bbox' IS DISTINCT FROM item->'bbox' THEN RETURN false;END IF;
  raw_page:=rkb_v4.source_stage_latest_page_v1(source,(item->>'page_id')::uuid);
  SELECT value INTO STRICT raw_figure FROM jsonb_array_elements(raw_page->'illustrations') WHERE value->>'illustration_key'=item->>'illustration_key';
  IF raw_figure->>'source_region_key' IS DISTINCT FROM region->>'region_key'
   OR raw_figure->>'kind' IS DISTINCT FROM item->>'kind'
   OR coalesce(raw_figure->'visual_description','null') IS DISTINCT FROM item->'visual_description'
   OR coalesce(raw_figure->'visual_description_language','null') IS DISTINCT FROM item->'visual_description_language'
   OR coalesce(raw_figure->>'visual_description_provenance','model_observation') IS DISTINCT FROM item->>'visual_description_provenance'
   OR coalesce(raw_figure->'display_rotation_degrees','0') IS DISTINCT FROM item->'display_rotation_degrees' THEN RETURN false;END IF;
  IF (SELECT coalesce(jsonb_agg(r.value->'region_key' ORDER BY a.n),'[]') FROM jsonb_array_elements(item->'caption_region_ids') WITH ORDINALITY a(v,n)
      JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'region_id'=a.v)
      IS DISTINCT FROM coalesce(raw_figure->'caption_region_keys','[]')
   OR (SELECT coalesce(jsonb_agg(r.value->'region_key' ORDER BY a.n),'[]') FROM jsonb_array_elements(item->'nearby_region_ids') WITH ORDINALITY a(v,n)
      JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'region_id'=a.v)
      IS DISTINCT FROM coalesce(raw_figure->'nearby_region_keys','[]') THEN RETURN false;END IF;
  caption:='';
  FOR ref IN SELECT value FROM jsonb_array_elements(item->'caption_region_ids') LOOP
   SELECT value INTO STRICT region FROM jsonb_array_elements(doc->'image_manifest'->'regions') WHERE value->'region_id'=ref;
   IF region->>'page_id' IS DISTINCT FROM item->>'page_id' OR region->>'kind' IS DISTINCT FROM 'caption' THEN RETURN false;END IF;
   SELECT value INTO STRICT proof FROM jsonb_array_elements(doc->'semantic'->'regions') WHERE value->'region_id'=ref;
   caption:=caption||CASE WHEN caption='' THEN '' ELSE E'\n' END||(proof->>'source_text');
  END LOOP;
  IF item->>'caption_text' IS DISTINCT FROM caption THEN RETURN false;END IF;
  FOR ref IN SELECT value FROM jsonb_array_elements(item->'nearby_region_ids') LOOP
   IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'image_manifest'->'regions') r WHERE r->'region_id'=ref AND r->>'page_id'=item->>'page_id') THEN RETURN false;END IF;
  END LOOP;
  IF item->'crop_binding' IS DISTINCT FROM jsonb_build_object('source_id',source::text,'source_revision',1,
   'illustration_id',item->'illustration_id','page_id',item->'page_id','source_region_id',item->'source_region_id',
   'bbox',item->'bbox','display_rotation_degrees',item->'display_rotation_degrees',
   'source_image_receipt_sha256',page->'source_image_receipt_sha256','source_raster_sha256',page->'image_sha256',
   'original_sha256',doc->'original'->'sha256','rendition','preview') THEN RETURN false;END IF;
 END LOOP;
 -- Every latest-page illustration remains present even when no native archive exists.
 FOR page IN SELECT value FROM jsonb_array_elements(doc->'image_manifest'->'pages') LOOP
  raw_page:=rkb_v4.source_stage_latest_page_v1(source,(page->>'page_id')::uuid);
  illustration_count:=illustration_count+jsonb_array_length(coalesce(raw_page->'illustrations','[]'));
  FOR raw_figure IN SELECT value FROM jsonb_array_elements(coalesce(raw_page->'illustrations','[]')) LOOP
   IF (SELECT count(*) FROM jsonb_array_elements(media->'illustrations') i WHERE i->'page_id'=page->'page_id' AND i->'illustration_key'=raw_figure->'illustration_key')<>1 THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF illustration_count<>cardinality(ids) THEN RETURN false;END IF;
 staged:=rkb_v4.source_illustration_stage_links_v1(source);
 IF jsonb_array_length(media->'passage_links')<>(SELECT count(*) FROM jsonb_each(staged->'chunks') c WHERE jsonb_array_length(coalesce(c.value->'illustration_refs','[]'))>0
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(media->'passage_links') l WHERE l->>'chunk_key'=c.key))
  OR jsonb_array_length(media->'poi_media_links')<>(SELECT count(*) FROM jsonb_each(staged->'poi_media_links') l WHERE EXISTS(
    SELECT 1 FROM jsonb_array_elements(media->'poi_media_links') p WHERE p->>'link_key'=l.key)) THEN RETURN false;END IF;
 FOR link IN SELECT value FROM jsonb_array_elements(media->'passage_links') LOOP
  IF link-ARRAY['passage_id','chunk_key','illustration_ids']<>'{}' OR NOT link ?& ARRAY['passage_id','chunk_key','illustration_ids']
   OR jsonb_typeof(link->'illustration_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(link->'illustration_ids') NOT BETWEEN 1 AND 30
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'passages') p WHERE p->'passage_id'=link->'passage_id') THEN RETURN false;END IF;
  raw_link:=staged->'chunks'->(link->>'chunk_key');
  IF raw_link IS NULL THEN RETURN false;END IF;
  SELECT coalesce(jsonb_agg(i.value->'illustration_id' ORDER BY r.n),'[]') INTO expected_refs
   FROM jsonb_array_elements(coalesce(raw_link->'illustration_refs','[]')) WITH ORDINALITY r(value,n)
   JOIN LATERAL jsonb_array_elements(media->'illustrations') i(value) ON i.value->'page_id'=r.value->'page_id' AND i.value->'illustration_key'=r.value->'illustration_key';
  IF expected_refs IS DISTINCT FROM link->'illustration_ids'
   OR (SELECT count(*) FROM jsonb_array_elements(media->'passage_links') l WHERE l->'chunk_key'=link->'chunk_key')<>1 THEN RETURN false;END IF;
  SELECT value INTO STRICT proof FROM jsonb_array_elements(doc->'image_manifest'->'passages') WHERE value->'passage_id'=link->'passage_id';
  SELECT coalesce(jsonb_agg(refs.v ORDER BY refs.n),'[]') INTO expected_refs FROM (
   SELECT r.value->'region_id' v,min(a.n) n FROM jsonb_array_elements(coalesce(raw_link->'region_refs','[]')||coalesce(raw_link->'span_refs','[]')) WITH ORDINALITY a(value,n)
   JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'page_id'=a.value->'page_id' AND r.value->'region_key'=a.value->'region_key'
   GROUP BY r.value->'region_id') refs;
  IF expected_refs IS DISTINCT FROM proof->'region_ids' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_agg(refs.v ORDER BY refs.n),'[]') INTO expected_refs FROM (
   SELECT r.value->'region_id' v,min(a.n) n FROM jsonb_array_elements(coalesce(raw_link->'footnote_refs','[]')) WITH ORDINALITY a(value,n)
   JOIN LATERAL jsonb_array_elements(doc->'image_manifest'->'regions') r(value) ON r.value->'page_id'=a.value->'page_id' AND r.value->'region_key'=a.value->'region_key'
   GROUP BY r.value->'region_id') refs;
  IF expected_refs IS DISTINCT FROM proof->'footnote_region_ids' THEN RETURN false;END IF;
  wanted:=(link->>'passage_id')::uuid;IF wanted=ANY(links) THEN RETURN false;END IF;links:=array_append(links,wanted);
  IF (SELECT count(DISTINCT value) FROM jsonb_array_elements(link->'illustration_ids'))<>jsonb_array_length(link->'illustration_ids') THEN RETURN false;END IF;
  FOR ref IN SELECT value FROM jsonb_array_elements(link->'illustration_ids') LOOP
   IF NOT ((ref#>>'{}')::uuid=ANY(ids)) THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 links:='{}';
 FOR link IN SELECT value FROM jsonb_array_elements(media->'poi_media_links') LOOP
  IF link-ARRAY['link_key','link_id','poi_locator','illustration_id','page_id','relation','time_scope']<>'{}'
   OR NOT link ?& ARRAY['link_key','link_id','poi_locator','illustration_id','page_id','relation','time_scope']
   OR link->>'relation' NOT IN ('depicts','illustrates','map_of','detail_of')
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(media->'illustrations') i WHERE i->'illustration_id'=link->'illustration_id' AND i->'page_id'=link->'page_id') THEN RETURN false;END IF;
  raw_link:=staged->'poi_media_links'->(link->>'link_key');
  IF raw_link IS NULL OR raw_link->'poi_locator' IS DISTINCT FROM link->'poi_locator'
   OR raw_link->'relation' IS DISTINCT FROM link->'relation' OR coalesce(raw_link->'time_scope','null') IS DISTINCT FROM link->'time_scope'
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(media->'illustrations') i WHERE i->'illustration_id'=link->'illustration_id'
      AND i->'page_id'=raw_link->'illustration_ref'->'page_id' AND i->'illustration_key'=raw_link->'illustration_ref'->'illustration_key')
   OR (SELECT count(*) FROM jsonb_array_elements(media->'poi_media_links') l WHERE l->'link_key'=link->'link_key')<>1 THEN RETURN false;END IF;
  wanted:=(link->>'link_id')::uuid;IF wanted IS NULL OR wanted=ANY(links) THEN RETURN false;END IF;links:=array_append(links,wanted);
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.project_source_batch_graph_v1(source uuid,g jsonb,payload_sha text) RETURNS void
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE e jsonb;a jsonb;r jsonb;link jsonb;mention jsonb;k text;n integer;BEGIN
 IF g IS NULL THEN RETURN;END IF;
 INSERT INTO rkb_v4.source_graph_documents VALUES(source,1,(g->>'owner_person_id')::uuid,g,payload_sha) ON CONFLICT DO NOTHING;
 FOR e IN SELECT value FROM jsonb_array_elements(g->'entities') LOOP
  INSERT INTO rkb_v4.source_entity_ids VALUES((e->>'entity_id')::uuid,(g->>'owner_person_id')::uuid,e->>'kind') ON CONFLICT DO NOTHING;
  INSERT INTO rkb_v4.source_entity_representations VALUES(source,e->>'key',(e->>'entity_id')::uuid,e) ON CONFLICT DO NOTHING;
  FOR mention IN SELECT value FROM jsonb_array_elements(e->'mentions') LOOP
   INSERT INTO rkb_v4.source_entity_mentions VALUES(source,(mention->>'mention_id')::uuid,(e->>'entity_id')::uuid,mention) ON CONFLICT DO NOTHING;
  END LOOP;
  FOR a IN SELECT value FROM jsonb_array_elements(e->'aliases') LOOP
   INSERT INTO rkb_v4.source_entity_aliases VALUES(source,(a->>'alias_id')::uuid,(e->>'entity_id')::uuid,
    a->>'normalized_value',encode(sha256(convert_to(a->>'normalized_value','UTF8')),'hex'),a) ON CONFLICT DO NOTHING;
  END LOOP;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'references') LOOP
  INSERT INTO rkb_v4.source_entity_references VALUES(source,r->>'key',(r->>'entity_id')::uuid,r) ON CONFLICT DO NOTHING;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'relations') LOOP
  INSERT INTO rkb_v4.source_entity_relations VALUES(source,(r->>'relation_id')::uuid,(r->>'source_id')::uuid,(r->>'target_id')::uuid,r->>'kind',r) ON CONFLICT DO NOTHING;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(g->'poi_facts') LOOP
  INSERT INTO rkb_v4.source_poi_hypotheses VALUES(source,(r->>'candidate_id')::uuid,r) ON CONFLICT DO NOTHING;
 END LOOP;
 FOR link IN SELECT value FROM jsonb_array_elements(g->'finding_entities') LOOP
  FOR k,n IN SELECT value,ordinal::integer FROM jsonb_array_elements_text(link->'entity_keys') WITH ORDINALITY x(value,ordinal) LOOP
   INSERT INTO rkb_v4.source_entity_finding_links(source_id,assertion_id,entity_id,entity_key) VALUES(source,(link->>'assertion_id')::uuid,(link->'entity_ids'->>(n-1))::uuid,k) ON CONFLICT DO NOTHING;
  END LOOP;
 END LOOP;
END $$;
CREATE FUNCTION rkb_v4.apply_source_batch_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint; story jsonb; variant jsonb; packet jsonb; idx integer; ordinal_base integer;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_batch_v1(p,ids,expected,logical_time) THEN
        RAISE EXCEPTION 'source import requires exact resolved new-source precondition';
    END IF;
    source:=(ids->>'source_id')::uuid;packet:=convert_from(p,'UTF8')::jsonb;idx:=(packet->>'batch_index')::integer;
    doc:=rkb_v4.source_batch_doc_v1(source,idx);
    SELECT coalesce(max(ordinal)+1,0) INTO ordinal_base FROM rkb_v4.passage_search WHERE source_id=source AND revision=1;
    research:=doc->'research'; original:=doc->'original';
    INSERT INTO rkb_v4.sources VALUES (source,doc->>'bibliography',true,false) ON CONFLICT DO NOTHING;
    INSERT INTO rkb_v4.revisions VALUES (source,1) ON CONFLICT DO NOTHING;
    INSERT INTO rkb_v4.source_import_origins VALUES (source,1,(doc->>'work_id')::uuid,
        (doc->>'edition_id')::uuid,original->>'sha256',(original->>'size')::bigint,
        original->>'ref',original->>'receipt_sha256',encode(sha256(p),'hex'),
        doc->'model'->>'revision',doc->'model'->>'space',doc->'model'->>'tokenizer_sha256',logical_time) ON CONFLICT DO NOTHING;
    INSERT INTO rkb_v4.source_research_frontier VALUES (source,1,(research->>'frontier_id')::uuid,
        research->>'state',research->'continuation') ON CONFLICT(source_id) DO UPDATE SET state=EXCLUDED.state,continuation=EXCLUDED.continuation;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
        INSERT INTO rkb_v4.passage_search(passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
        VALUES ((item->>'passage_id')::uuid,source,1,ordinal_base+(item->>'ordinal')::integer,item->>'locator',item->>'body',
            item->>'language',CASE item->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
                WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (item->'embedding')::text::public.vector) ON CONFLICT DO NOTHING;
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'regions') LOOP
        INSERT INTO rkb_v4.source_regions VALUES(source,1,(item->>'region_id')::uuid,
            (item->>'page_id')::uuid,item->>'source_text',item->>'source_text_sha256') ON CONFLICT DO NOTHING;
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'findings') LOOP
        INSERT INTO rkb_v4.story_assertion_versions VALUES((item->>'assertion_id')::uuid,1,
            source,1,item->>'candidate_key',item->>'proposition',item) ON CONFLICT DO NOTHING;
    END LOOP;
    FOR story IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'stories') LOOP
        variant:=story->'default_variant';
        INSERT INTO rkb_v4.stories VALUES((story->>'story_id')::uuid,1,source,1,
            (variant->>'variant_id')::uuid,1) ON CONFLICT DO NOTHING;
        INSERT INTO rkb_v4.story_versions VALUES((story->>'story_id')::uuid,1,
            story-'default_variant',logical_time,encode(sha256(p),'hex')) ON CONFLICT DO NOTHING;
        INSERT INTO rkb_v4.story_variant_versions VALUES((variant->>'variant_id')::uuid,1,
            (story->>'story_id')::uuid,variant->>'body',
            encode(sha256(convert_to(variant->>'body','UTF8')),'hex'),variant-'embedding') ON CONFLICT DO NOTHING;
        INSERT INTO rkb_v4.story_search(story_id,variant_id,variant_revision,source_id,source_revision,body,config,embedding)
        VALUES((story->>'story_id')::uuid,(variant->>'variant_id')::uuid,1,source,1,variant->>'body',
            CASE variant->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
            WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
            (variant->'embedding')::text::public.vector) ON CONFLICT DO NOTHING;
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'semantic'->'pending') LOOP
        INSERT INTO rkb_v4.source_story_pending VALUES(source,item->>'candidate_key',item) ON CONFLICT DO NOTHING;
    END LOOP;
    INSERT INTO rkb_v4.source_manifests VALUES(source,1,doc->'image_manifest',encode(sha256(p),'hex'),
        decode(doc->'image_manifest'->>'original_receipt_b64','base64')) ON CONFLICT DO NOTHING;
    FOR item IN SELECT value FROM jsonb_array_elements(doc->'image_manifest'->'passages') LOOP
        INSERT INTO rkb_v4.source_passage_provenance VALUES((item->>'passage_id')::uuid,source,1,item) ON CONFLICT DO NOTHING;
    END LOOP;
    FOR item IN SELECT value FROM rkb_v4.source_batch_prepared b CROSS JOIN LATERAL jsonb_array_elements(b.media->'illustrations')
        WHERE b.source_id=source AND b.batch_index=idx LOOP
        INSERT INTO rkb_v4.source_illustrations VALUES(source,(item->>'illustration_id')::uuid,item) ON CONFLICT DO NOTHING;
    END LOOP;
    -- Activation happens after every exact passage and its research continuation
    -- exist. All source/projection/receipt/frontier work commits or rolls back.
    PERFORM rkb_v4.project_source_batch_graph_v1(source,doc->'source_graph',encode(sha256(p),'hex'));

    INSERT INTO rkb_v4.active_revision VALUES (source,1) ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
    SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=source;
    PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
        frontier.visibility_epoch,state.applied_seq+1,
        prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
        prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
    RETURN convert_to(jsonb_build_object('source_id',source,'content_revision',1,'accepted',true,
        'withdrawn',false,'eligibility_revision',eligibility,'visibility_epoch',frontier.visibility_epoch,
        'passage_count',jsonb_array_length(doc->'passages'),'story_count',jsonb_array_length(doc->'semantic'->'stories'),
        'finding_count',jsonb_array_length(doc->'semantic'->'findings'),'batch_index',idx,'ingestion_id',packet->>'ingestion_id','candidate_state','published','knowledge_sha256',(SELECT knowledge_sha256 FROM rkb_v4.source_batch_prepared WHERE source_id=source AND batch_index=idx),
        'payload_sha256',encode(sha256(p),'hex'),
        'research_frontier_id',research->>'frontier_id','research_state',research->>'state',
        'logical_time',logical_time)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.source_batch_doc_v1(source uuid,idx integer) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT jsonb_set(jsonb_set(p.knowledge,'{passages}',
  (SELECT coalesce(jsonb_agg(x.value||jsonb_build_object('embedding',to_jsonb(v.embedding::real[])) ORDER BY x.n),'[]')
   FROM jsonb_array_elements(p.knowledge->'passages') WITH ORDINALITY x(value,n)
   JOIN rkb_v4.source_batch_vectors v ON v.source_id=p.source_id AND v.batch_index=p.batch_index
    AND v.input_id=(x.value->>'passage_id')::uuid)),
  '{semantic,stories}',(SELECT coalesce(jsonb_agg(jsonb_set(s.value,'{default_variant}',s.value->'default_variant'||
   jsonb_build_object('embedding',to_jsonb(v.embedding::real[]))) ORDER BY s.n),'[]')
   FROM jsonb_array_elements(p.knowledge->'semantic'->'stories') WITH ORDINALITY s(value,n)
   JOIN rkb_v4.source_batch_vectors v ON v.source_id=p.source_id AND v.batch_index=p.batch_index
    AND v.input_id=coalesce((s.value->'default_variant'->'fragments'->0->>'fragment_id')::uuid,
      (s.value->'default_variant'->>'variant_id')::uuid)))
 FROM rkb_v4.source_batch_prepared p WHERE p.source_id=source AND p.batch_index=idx
$$;

CREATE FUNCTION rkb_v4.check_source_batch_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;prepared rkb_v4.source_batch_prepared;c jsonb;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 THEN RETURN false;END IF;
 d:=convert_from(p,'UTF8')::jsonb;
 IF d->>'codec' IS DISTINCT FROM 'rkb.source-batch.v1' OR d->'batch_index' IS NULL
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id')
  OR expected IS DISTINCT FROM jsonb_build_object('batch_head',d->>'last_batch_sha256') THEN RETURN false;END IF;
 SELECT * INTO STRICT prepared FROM rkb_v4.source_batch_prepared
  WHERE source_id=(ids->>'source_id')::uuid AND batch_index=(d->>'batch_index')::integer;
 c:=convert_from(prepared.command_bytes,'UTF8')::jsonb;
 IF decode(c->>'payload_b64','base64') IS DISTINCT FROM p OR c->'ids' IS DISTINCT FROM ids
  OR c->>'logical_time' IS DISTINCT FROM logical_time OR c->'expected_revisions' IS DISTINCT FROM expected
  OR EXISTS(SELECT 1 FROM rkb_v4.sources s WHERE s.source_id=prepared.source_id AND (NOT s.accepted OR s.withdrawn))
  OR rkb_v4.source_batch_conflicts_v1(prepared.source_id,prepared.knowledge)
  OR EXISTS(SELECT 1 FROM rkb_v4.source_batch_inputs_v1(prepared.knowledge) i
   LEFT JOIN rkb_v4.source_batch_vectors v ON v.source_id=prepared.source_id AND v.batch_index=prepared.batch_index AND v.input_id=i.input_id
   WHERE v.input_id IS NULL OR v.input_sha256<>encode(sha256(convert_to(i.body,'UTF8')),'hex')) THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.capture_source_batch_receipt_v1() RETURNS trigger
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;d jsonb;source uuid;idx integer;prepared rkb_v4.source_batch_prepared;
 story record;v jsonb;refs jsonb;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route'<>'source.batch.v1' THEN RETURN NULL;END IF;
 d:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;
 source:=(c->'ids'->>'source_id')::uuid;idx:=(d->>'batch_index')::integer;
 SELECT * INTO STRICT prepared FROM rkb_v4.source_batch_prepared WHERE source_id=source AND batch_index=idx;
 IF prepared.command_bytes<>NEW.command_bytes THEN RAISE EXCEPTION 'source packet receipt identity differs';END IF;
 INSERT INTO rkb_v4.source_batch_receipts VALUES(source,idx,NEW.operation_key,NEW.sequence,NEW.command_sha256,prepared.knowledge_sha256);
 INSERT INTO rkb_v4.source_batch_publications VALUES(source,idx,prepared.knowledge->'image_manifest',prepared.media,
  ARRAY(SELECT (value->>'illustration_id')::uuid FROM jsonb_array_elements(prepared.media->'illustrations')));
 IF prepared.knowledge?'source_graph' THEN
  INSERT INTO rkb_v4.source_graph_receipts VALUES(source,NEW.operation_key,NEW.sequence,NEW.command_sha256,
   (c->>'person_id')::uuid,c->>'request_id') ON CONFLICT DO NOTHING;
 END IF;
  FOR story IN SELECT s.*,sv.data version_data,b.data variant_data,b.body,b.body_sha256,
     p.embedding,p.config FROM rkb_v4.stories s JOIN rkb_v4.story_versions sv USING(story_id)
     JOIN rkb_v4.story_variant_versions b ON b.variant_id=s.default_variant_id AND b.revision=s.default_variant_revision
     JOIN rkb_v4.story_search p ON p.story_id=s.story_id
     WHERE s.source_id=(c->'ids'->>'source_id')::uuid
      AND EXISTS(SELECT 1 FROM jsonb_array_elements(prepared.knowledge->'semantic'->'stories') x WHERE x->>'story_id'=s.story_id::text)
      AND NOT EXISTS(SELECT 1 FROM rkb_v4.editorial_story_heads h WHERE h.story_id=s.story_id) LOOP
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
 RETURN NULL;
END $$;
CREATE TRIGGER capture_source_batch_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_source_batch_receipt_v1();

CREATE TABLE rkb_v4.source_read_completions (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,ingestion_id uuid NOT NULL,
 logical_time text NOT NULL,accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER immutable_source_read_completion BEFORE UPDATE OR DELETE ON rkb_v4.source_read_completions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE FUNCTION rkb_v4.check_source_finish_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;BEGIN
 d:=convert_from(p,'UTF8')::jsonb;
 RETURN octet_length(p) BETWEEN 1 AND 256 AND d->>'codec'='rkb.source-finish.v1'
  AND d-ARRAY['codec','ingestion_id']='{}'::jsonb AND expected='{"source_revision":"1"}'::jsonb
  AND EXISTS(SELECT 1 FROM rkb_v4.source_stage_sessions st JOIN rkb_v4.sources s USING(source_id)
   WHERE st.source_id=(ids->>'source_id')::uuid AND st.ingestion_id=(d->>'ingestion_id')::uuid AND s.accepted AND NOT s.withdrawn)
  AND EXISTS(SELECT 1 FROM rkb_v4.source_batch_receipts WHERE source_id=(ids->>'source_id')::uuid);
EXCEPTION WHEN data_exception THEN RETURN false;END $$;
CREATE FUNCTION rkb_v4.apply_source_finish_v1(p bytea,ids jsonb,expected jsonb,logical_time text) RETURNS bytea
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;seq bigint;source uuid:=(ids->>'source_id')::uuid;BEGIN
 SELECT applied_seq+1 INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_finish_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'owned active source required';END IF;
 d:=convert_from(p,'UTF8')::jsonb;
 INSERT INTO rkb_v4.source_read_completions VALUES(source,(d->>'ingestion_id')::uuid,logical_time,seq) ON CONFLICT DO NOTHING;
 -- Reading completion does not claim that pending narrative research is complete.
 RETURN convert_to(jsonb_build_object('source_id',source,'ingestion_id',d->>'ingestion_id','read_complete',true)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.source_batch_manifest_v1(wanted uuid) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH batches AS MATERIALIZED (SELECT batch_index,manifest FROM rkb_v4.source_batch_publications WHERE source_id=wanted),
 base AS (SELECT manifest FROM batches ORDER BY batch_index DESC LIMIT 1)
 SELECT base.manifest||jsonb_build_object(
  'pages',(SELECT coalesce(jsonb_agg(p.value ORDER BY (p.value->>'physical_page_index')::integer),'[]')
   FROM (SELECT DISTINCT ON (value->>'page_id') value FROM batches CROSS JOIN LATERAL jsonb_array_elements(manifest->'pages')
    ORDER BY value->>'page_id',batch_index DESC) p),
  'regions',(SELECT coalesce(jsonb_agg(p.value ORDER BY p.value->>'region_id'),'[]')
   FROM (SELECT DISTINCT ON (value->>'region_id') value FROM batches CROSS JOIN LATERAL jsonb_array_elements(manifest->'regions')
    ORDER BY value->>'region_id',batch_index DESC) p),
  'passages',(SELECT coalesce(jsonb_agg(p.value ORDER BY p.value->>'passage_id'),'[]')
   FROM (SELECT DISTINCT ON (value->>'passage_id') value FROM batches CROSS JOIN LATERAL jsonb_array_elements(manifest->'passages')
    ORDER BY value->>'passage_id',batch_index DESC) p))
 FROM base
$$;
CREATE OR REPLACE FUNCTION rkb_v4.read_source_manifest_v1(wanted uuid) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT coalesce(rkb_v4.source_batch_manifest_v1(wanted),m.manifest)
 FROM rkb_v4.source_manifests m JOIN rkb_v4.sources s USING(source_id)
 JOIN rkb_v4.active_revision a ON a.source_id=m.source_id AND a.revision=m.source_revision
 WHERE m.source_id=wanted AND s.accepted AND NOT s.withdrawn
$$;

CREATE FUNCTION rkb_v4.check_source_stage_packet_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
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
    IF EXISTS (SELECT 1 FROM rkb_v4.sources WHERE source_id=source AND (NOT accepted OR withdrawn))
       OR EXISTS (SELECT 1 FROM rkb_v4.source_stage_candidates WHERE source_id=source)
       OR EXISTS (SELECT 1 FROM rkb_v4.source_stage_sessions WHERE ingestion_id=ingestion AND source_id<>source) THEN RETURN false; END IF;
    SELECT * INTO tail FROM rkb_v4.source_stage_batches WHERE source_id=source ORDER BY batch_index DESC LIMIT 1;
    RETURN (NOT FOUND AND idx=0) OR (FOUND AND idx=tail.batch_index+1 AND prev=tail.payload_sha256);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_stage_packet_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; source uuid; idx integer; n integer; image_bytes bytea; image jsonb; seq bigint;
BEGIN
    -- Same owner lock as apply_accepted. Direct calls do not create accepted_ops.
    SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    IF NOT FOUND OR NOT rkb_v4.check_source_stage_packet_v1(p,ids,expected,logical_time) THEN
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
CREATE OR REPLACE FUNCTION rkb_v4.check_source_stage_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE source uuid; d jsonb;
BEGIN
 IF NOT rkb_v4.check_source_stage_packet_v1(p,ids,expected,logical_time) THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 RETURN NOT EXISTS(SELECT 1 FROM rkb_v4.source_activation_candidates WHERE source_id=source)
  OR EXISTS(SELECT 1 FROM rkb_v4.source_stage_batches WHERE source_id=source
      AND batch_index=(d->>'batch_index')::integer AND payload_bytes=p);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE OR REPLACE FUNCTION rkb_v4.apply_source_stage_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
 RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_stage_v2(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'validated native candidate no longer accepts new stage batches';END IF;
 RETURN rkb_v4.apply_source_stage_packet_v1(p,ids,expected,logical_time);
END $$;

REVOKE ALL ON rkb_v4.source_batch_prepared,rkb_v4.source_batch_vectors,rkb_v4.source_batch_receipts,
 rkb_v4.source_batch_publications,rkb_v4.source_read_completions FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rkb_v4 FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
  GRANT SELECT ON rkb_v4.source_batch_publications TO rkb_reader;
  GRANT EXECUTE ON FUNCTION rkb_v4.source_batch_manifest_v1(uuid) TO rkb_reader;
 END IF;
END $$;
