-- Qualify the finding value independently of the PL/pgSQL variable f.
CREATE OR REPLACE FUNCTION rkb_v4.check_source_activation_semantic_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
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
