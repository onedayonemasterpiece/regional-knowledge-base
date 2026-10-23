-- Fresh-generation7 artifact; never silently installed/adopted into generation6.
-- Source v1 functions remain exact. Version2 source activation includes actual
-- model-authored facts and default narrative versions in its same transaction.
CREATE TABLE rkb_v4.source_regions (
 source_id uuid NOT NULL,source_revision bigint NOT NULL CHECK(source_revision=1),
 region_id uuid PRIMARY KEY,page_id uuid NOT NULL,source_text text NOT NULL CHECK(length(source_text)<=8000),
 source_text_sha256 text NOT NULL CHECK(source_text_sha256=encode(sha256(convert_to(source_text,'UTF8')),'hex')),
 UNIQUE(source_id,source_revision,region_id),FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.story_assertion_versions (
 assertion_id uuid NOT NULL,revision bigint NOT NULL CHECK(revision=1),source_id uuid NOT NULL,
 source_revision bigint NOT NULL CHECK(source_revision=1),candidate_key text NOT NULL,
 proposition text NOT NULL CHECK(length(proposition) BETWEEN 1 AND 500),
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),PRIMARY KEY(assertion_id,revision),
 UNIQUE(source_id,candidate_key),FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE INDEX story_assertions_source ON rkb_v4.story_assertion_versions(source_id,source_revision);
CREATE TABLE rkb_v4.stories (
 story_id uuid PRIMARY KEY,current_revision bigint NOT NULL CHECK(current_revision=1),
 source_id uuid NOT NULL,source_revision bigint NOT NULL CHECK(source_revision=1),
 default_variant_id uuid NOT NULL,default_variant_revision bigint NOT NULL CHECK(default_variant_revision=1),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE INDEX stories_source ON rkb_v4.stories(source_id,source_revision,story_id);
CREATE TABLE rkb_v4.story_versions (
 story_id uuid NOT NULL REFERENCES rkb_v4.stories,revision bigint NOT NULL CHECK(revision=1),
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),logical_time text NOT NULL,
 payload_sha256 text NOT NULL CHECK(payload_sha256 ~ '^[0-9a-f]{64}$'),PRIMARY KEY(story_id,revision)
);
CREATE TABLE rkb_v4.story_variant_versions (
 variant_id uuid NOT NULL,revision bigint NOT NULL CHECK(revision=1),story_id uuid NOT NULL REFERENCES rkb_v4.stories,
 body text NOT NULL CHECK(length(body) BETWEEN 1 AND 4000),
 body_sha256 text NOT NULL CHECK(body_sha256=encode(sha256(convert_to(body,'UTF8')),'hex')),
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),PRIMARY KEY(variant_id,revision),
 UNIQUE(story_id,variant_id,revision)
);
ALTER TABLE rkb_v4.stories ADD CONSTRAINT story_default_exact_variant
 FOREIGN KEY(story_id,default_variant_id,default_variant_revision)
 REFERENCES rkb_v4.story_variant_versions(story_id,variant_id,revision) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE rkb_v4.story_search (
 story_id uuid PRIMARY KEY REFERENCES rkb_v4.stories,variant_id uuid NOT NULL,variant_revision bigint NOT NULL,
 source_id uuid NOT NULL,source_revision bigint NOT NULL,
 body text NOT NULL CHECK(length(body) BETWEEN 1 AND 4000),
 config regconfig NOT NULL,embedding public.vector(1024) NOT NULL,
 fts tsvector GENERATED ALWAYS AS(to_tsvector(config,body)) STORED,
 FOREIGN KEY(story_id,variant_id,variant_revision) REFERENCES rkb_v4.story_variant_versions(story_id,variant_id,revision),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE INDEX story_search_fts ON rkb_v4.story_search USING gin(fts);
CREATE INDEX story_search_dense ON rkb_v4.story_search USING hnsw(embedding public.vector_cosine_ops);
CREATE INDEX story_search_source ON rkb_v4.story_search(source_id,source_revision,story_id);
CREATE TABLE rkb_v4.source_story_pending (
 source_id uuid NOT NULL REFERENCES rkb_v4.sources,candidate_key text NOT NULL,
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),PRIMARY KEY(source_id,candidate_key)
);
CREATE TABLE rkb_v4.story_history_receipts (
 story_id uuid NOT NULL,revision bigint NOT NULL,operation_key text NOT NULL REFERENCES rkb_v4.applied_ops,
 sequence bigint NOT NULL,person_id text NOT NULL,request_id text NOT NULL,logical_time text NOT NULL,
 command_sha256 text NOT NULL,PRIMARY KEY(story_id,revision),
 FOREIGN KEY(story_id,revision) REFERENCES rkb_v4.story_versions
);
CREATE FUNCTION rkb_v4.capture_story_import_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE command jsonb;
BEGIN
 command:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF NEW.status='applied' AND command->>'route'='source.import.v2' THEN
  INSERT INTO rkb_v4.story_history_receipts
  SELECT s.story_id,1,NEW.operation_key,NEW.sequence,command->>'person_id',command->>'request_id',
   command->>'logical_time',NEW.command_sha256 FROM rkb_v4.stories s
  WHERE s.source_id=(command->'ids'->>'source_id')::uuid;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_story_import_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_story_import_receipt_v1();
CREATE TRIGGER immutable_story_receipt BEFORE UPDATE OR DELETE ON rkb_v4.story_history_receipts
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_story_current BEFORE UPDATE OR DELETE ON rkb_v4.stories
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_story_projection BEFORE UPDATE OR DELETE ON rkb_v4.story_search
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_region BEFORE UPDATE OR DELETE ON rkb_v4.source_regions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_story_assertion BEFORE UPDATE OR DELETE ON rkb_v4.story_assertion_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_story_version BEFORE UPDATE OR DELETE ON rkb_v4.story_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_story_variant BEFORE UPDATE OR DELETE ON rkb_v4.story_variant_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER story_source_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.stories
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER story_projection_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.story_search
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();

CREATE FUNCTION rkb_v4.check_source_import_v2(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;sem jsonb;item jsonb;e jsonb;r jsonb;story jsonb;v jsonb;b jsonb;f jsonb;
 base bytea;regions uuid[]:='{}';findings uuid[]:='{}';stories uuid[]:='{}';variants uuid[]:='{}';
 candidates text[]:='{}';pending text[]:='{}';covered uuid[]:='{}';refs uuid[];
 ident uuid;region uuid;passage uuid;begin_offset integer;end_offset integer;body_end integer;vector public.vector;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 786432 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-import.v2' OR NOT doc?'semantic' THEN RETURN false;END IF;
 base:=convert_to(((doc-'semantic')||'{"codec":"rkb.source-import.v1"}'::jsonb)::text,'UTF8');
 IF NOT rkb_v4.check_source_import_v1(base,ids,expected,logical_time) THEN RETURN false;END IF;
 sem:=doc->'semantic';
 IF jsonb_typeof(sem) IS DISTINCT FROM 'object' OR NOT sem ?& ARRAY['regions','findings','stories','pending']
    OR sem-ARRAY['regions','findings','stories','pending']<>'{}'::jsonb
    OR jsonb_typeof(sem->'regions') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'regions') NOT BETWEEN 1 AND 800
    OR jsonb_typeof(sem->'findings') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'findings')>100
    OR jsonb_typeof(sem->'stories') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'stories')>12
    OR jsonb_typeof(sem->'pending') IS DISTINCT FROM 'array' OR jsonb_array_length(sem->'pending')>12 THEN RETURN false;END IF;
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
  IF jsonb_typeof(v) IS DISTINCT FROM 'object' OR NOT v ?& ARRAY['variant_id','revision','audience','format','language','body','blocks','embedding','input_tokens']
     OR v-ARRAY['variant_id','revision','audience','format','language','body','blocks','embedding','input_tokens']<>'{}'::jsonb
     OR v->'revision' IS DISTINCT FROM '1'::jsonb OR v->>'format' IS DISTINCT FROM 'story'
     OR jsonb_typeof(v->'audience') IS DISTINCT FROM 'string' OR length(v->>'audience') NOT BETWEEN 1 AND 500
     OR jsonb_typeof(v->'language') IS DISTINCT FROM 'string' OR length(v->>'language') NOT BETWEEN 2 AND 24
     OR jsonb_typeof(v->'body') IS DISTINCT FROM 'string' OR length(v->>'body') NOT BETWEEN 1 AND 4000
     OR jsonb_typeof(v->'blocks') IS DISTINCT FROM 'array' OR jsonb_array_length(v->'blocks') NOT BETWEEN 1 AND 25
     OR jsonb_typeof(v->'embedding') IS DISTINCT FROM 'array' OR jsonb_array_length(v->'embedding')<>1024
     OR jsonb_typeof(v->'input_tokens') IS DISTINCT FROM 'number' OR v->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
     OR (v->>'input_tokens')::integer NOT BETWEEN 1 AND 512 THEN RETURN false;END IF;
  passage:=(v->>'variant_id')::uuid;
  IF passage::text<>v->>'variant_id' OR passage=ANY(variants)
     OR EXISTS(SELECT 1 FROM rkb_v4.story_variant_versions WHERE variant_id=passage)
     OR EXISTS(SELECT 1 FROM jsonb_array_elements(v->'embedding') x WHERE jsonb_typeof(x) IS DISTINCT FROM 'number'
        OR (x::text)::double precision IS DISTINCT FROM (x::text)::real::double precision) THEN RETURN false;END IF;
  vector:=(v->'embedding')::text::public.vector;
  IF abs(public.vector_norm(vector)-1.0)>0.002 THEN RETURN false;END IF;
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

CREATE FUNCTION rkb_v4.apply_source_import_v2(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint; story jsonb; variant jsonb;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_import_v2(p,ids,expected,logical_time) THEN
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
        'finding_count',jsonb_array_length(doc->'semantic'->'findings'),'story_refs',(SELECT coalesce(jsonb_agg(jsonb_build_object(
            'story_id',s->>'story_id','variant_id',s->'default_variant'->>'variant_id','variant_revision',1,
            'body_sha256',encode(sha256(convert_to(s->'default_variant'->>'body','UTF8')),'hex'))),'[]'::jsonb)
            FROM jsonb_array_elements(doc->'semantic'->'stories') s),'payload_sha256',encode(sha256(p),'hex'),
        'research_frontier_id',research->>'frontier_id','research_state',research->>'state',
        'logical_time',logical_time)::text,'UTF8');
END $$;


CREATE FUNCTION rkb_v4.read_story_v1(wanted uuid,view_name text,wanted_revision bigint,after_revision bigint,page_limit integer)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE story rkb_v4.stories;source rkb_v4.sources;eligibility bigint;version rkb_v4.story_versions;
 variant rkb_v4.story_variant_versions;entries jsonb;result jsonb;
BEGIN
 IF view_name NOT IN ('get','history','variants') OR page_limit NOT BETWEEN 1 AND 10
    OR after_revision<0 OR wanted_revision IS NOT NULL AND wanted_revision<1 THEN
    RAISE EXCEPTION 'bounded story read required';END IF;
 SELECT st.* INTO story FROM rkb_v4.stories st JOIN rkb_v4.active_revision a
  ON a.source_id=st.source_id AND a.revision=st.source_revision
  JOIN rkb_v4.sources s ON s.source_id=a.source_id
  WHERE st.story_id=wanted AND s.accepted AND NOT s.withdrawn;
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO STRICT source FROM rkb_v4.sources WHERE source_id=story.source_id;
 SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=story.source_id;
 SELECT * INTO version FROM rkb_v4.story_versions WHERE story_id=wanted AND revision=coalesce(wanted_revision,story.current_revision);
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO STRICT variant FROM rkb_v4.story_variant_versions
  WHERE variant_id=story.default_variant_id AND revision=story.default_variant_revision AND story_id=wanted;
 IF view_name='history' THEN
  SELECT coalesce(jsonb_agg(row),'[]'::jsonb) INTO entries FROM(
   SELECT jsonb_build_object('story_id',v.story_id,'revision',v.revision,'logical_time',v.logical_time,
    'action','source_import','payload_sha256',v.payload_sha256,'actor_id',r.person_id,'request_id',r.request_id,
    'operation_key',r.operation_key,'sequence',r.sequence,'command_sha256',r.command_sha256) row
   FROM rkb_v4.story_versions v JOIN rkb_v4.story_history_receipts r USING(story_id,revision)
   WHERE v.story_id=wanted AND v.revision>after_revision ORDER BY v.revision LIMIT page_limit) bounded;
 ELSIF view_name='variants' THEN
  SELECT coalesce(jsonb_agg(row),'[]'::jsonb) INTO entries FROM(
   SELECT data||jsonb_build_object('body_sha256',body_sha256) row
   FROM rkb_v4.story_variant_versions WHERE story_id=wanted AND revision>after_revision
   ORDER BY revision,variant_id LIMIT page_limit) bounded;
 ELSE entries:='[]'::jsonb;END IF;
 result:=jsonb_build_object('story_id',wanted,'revision',version.revision,'title',version.data->>'title',
  'summary',version.data->>'summary','material_type',version.data->>'material_type',
  'body',variant.body,'body_sha256',variant.body_sha256,'narrative_availability','available',
  'default_variant_ref',jsonb_build_object('variant_id',variant.variant_id,'revision',variant.revision),
  'variant',variant.data,'bibliography',source.bibliography,
  'source',jsonb_build_object('source_id',source.source_id,'content_revision',story.source_revision,'eligibility_revision',eligibility),
  'finding_ids',version.data->'finding_ids','view',view_name,'entries',entries,
  'next_revision',CASE WHEN jsonb_array_length(entries)=page_limit THEN (entries->-1->>'revision')::bigint ELSE NULL END);
 IF octet_length(result::text)>131072 THEN RAISE EXCEPTION 'story response bound';END IF;
 RETURN result;
END $$;

CREATE FUNCTION rkb_v4.story_lexical_candidates_v1(q text,n integer,scope uuid[])
RETURNS TABLE(story_id uuid,score real) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH configs AS (
  SELECT c,websearch_to_tsquery(c,q) query FROM unnest(ARRAY[
   'pg_catalog.russian'::regconfig,'pg_catalog.german'::regconfig,'pg_catalog.simple'::regconfig]) c
 ),candidates AS (
  SELECT hit.* FROM configs CROSS JOIN LATERAL (
   SELECT p.story_id,ts_rank_cd(p.fts,configs.query) score
   FROM rkb_v4.story_search p JOIN rkb_v4.sources s USING(source_id)
   JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
   WHERE s.accepted AND NOT s.withdrawn AND p.config=configs.c AND p.fts@@configs.query
    AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
   ORDER BY score DESC,p.story_id LIMIT least(greatest(n,1),200)
  ) hit
 ) SELECT * FROM candidates ORDER BY score DESC,story_id LIMIT least(greatest(n,1),200)
$$;
CREATE FUNCTION rkb_v4.story_dense_candidates_v1(v public.vector,n integer,scope uuid[])
RETURNS TABLE(story_id uuid,score double precision) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT p.story_id,1-(p.embedding OPERATOR(public.<=>) v) score
 FROM rkb_v4.story_search p JOIN rkb_v4.sources s USING(source_id)
 JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
 WHERE s.accepted AND NOT s.withdrawn AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
 ORDER BY p.embedding OPERATOR(public.<=>) v LIMIT least(greatest(n,1),200)
$$;
CREATE FUNCTION rkb_v4.story_hydrate_v1(ids uuid[],scope uuid[])
RETURNS TABLE(story_id uuid,record jsonb) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT s.story_id,rkb_v4.read_story_v1(s.story_id,'get',NULL,0,1)
 FROM unnest(ids[1:5]) WITH ORDINALITY requested(id,position)
 JOIN rkb_v4.stories s ON s.story_id=requested.id
 WHERE scope IS NULL OR s.source_id=ANY(rkb_v4.checked_source_scope(scope))
 ORDER BY requested.position
$$;

REVOKE ALL ON rkb_v4.source_regions,rkb_v4.story_assertion_versions,rkb_v4.stories,rkb_v4.story_versions,
 rkb_v4.story_variant_versions,rkb_v4.story_search,rkb_v4.source_story_pending,rkb_v4.story_history_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_import_v2(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_import_v2(bytea,jsonb,jsonb,text),rkb_v4.read_story_v1(uuid,text,bigint,bigint,integer),
 rkb_v4.story_lexical_candidates_v1(text,integer,uuid[]),rkb_v4.story_dense_candidates_v1(public.vector,integer,uuid[]),
 rkb_v4.story_hydrate_v1(uuid[],uuid[]),rkb_v4.capture_story_import_receipt_v1() FROM PUBLIC;

CREATE FUNCTION rkb_v4.verify_source_story_origins_v1(scope uuid[])
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE op record;c jsonb;doc jsonb;sem jsonb;source uuid;item jsonb;v jsonb;
 verified uuid[]:='{}';seen integer:=0;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 IF cardinality(scope)=0 THEN RETURN true;END IF;
 FOR op IN SELECT command_bytes FROM rkb_v4.applied_ops WHERE status='applied' ORDER BY sequence LIMIT 10001 LOOP
  seen:=seen+1;IF seen>10000 THEN RETURN false;END IF;
  c:=convert_from(op.command_bytes,'UTF8')::jsonb;
  IF c->>'route'<>'source.import.v2' OR NOT((c->'ids'->>'source_id')::uuid=ANY(scope)) THEN CONTINUE;END IF;
  source:=(c->'ids'->>'source_id')::uuid;
  doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;sem:=doc->'semantic';
  verified:=array_append(verified,source);
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
   AND NOT EXISTS(SELECT 1 FROM rkb_v4.stories WHERE source_id=ANY(scope) AND NOT(source_id=ANY(verified)));
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
REVOKE ALL ON FUNCTION rkb_v4.verify_source_story_origins_v1(uuid[]) FROM PUBLIC;
