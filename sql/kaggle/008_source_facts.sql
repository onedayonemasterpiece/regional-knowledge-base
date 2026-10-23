-- Explicit fresh schema8 only. Never append to an existing generation7.
-- Original source.import.v2 functions/accepted semantics remain pinned artifacts;
-- this trigger adds atomic, immutable native projections within their transaction.
CREATE TABLE rkb_v4.source_event_dates (
 assertion_id uuid NOT NULL, assertion_revision bigint NOT NULL, ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 7),
 source_id uuid NOT NULL, calendar text NOT NULL, precision text NOT NULL, date_role text NOT NULL,
 year integer, month integer, day integer, data jsonb NOT NULL, proofs jsonb NOT NULL,
 PRIMARY KEY(assertion_id,assertion_revision,ordinal),
 FOREIGN KEY(assertion_id,assertion_revision) REFERENCES rkb_v4.story_assertion_versions
);
CREATE INDEX source_event_day ON rkb_v4.source_event_dates(calendar,month,day,year,assertion_id,ordinal)
 WHERE precision='day' AND date_role='event';
CREATE INDEX source_event_source ON rkb_v4.source_event_dates(source_id,assertion_id,ordinal);
CREATE TABLE rkb_v4.source_observations (
 assertion_id uuid NOT NULL, assertion_revision bigint NOT NULL, ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 7),
 source_id uuid NOT NULL, metric_key text NOT NULL, unit_code text NOT NULL,
 value_decimal numeric(20,6) NOT NULL, data jsonb NOT NULL, proofs jsonb NOT NULL,
 PRIMARY KEY(assertion_id,assertion_revision,ordinal),
 FOREIGN KEY(assertion_id,assertion_revision) REFERENCES rkb_v4.story_assertion_versions
);
CREATE INDEX source_observation_metric ON rkb_v4.source_observations(metric_key,unit_code,assertion_id,ordinal);
CREATE INDEX source_observation_source ON rkb_v4.source_observations(source_id,assertion_id,ordinal);
CREATE TRIGGER immutable_source_date BEFORE UPDATE OR DELETE ON rkb_v4.source_event_dates
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_observation BEFORE UPDATE OR DELETE ON rkb_v4.source_observations
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE FUNCTION rkb_v4.source_fact_proofs_v1(finding jsonb,indexes jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE i jsonb; n integer; seen integer[]:='{}'; proofs jsonb:='[]'; e jsonb;
BEGIN
 IF jsonb_typeof(indexes) IS DISTINCT FROM 'array' OR jsonb_array_length(indexes) NOT BETWEEN 1 AND 4
 OR jsonb_typeof(finding->'evidence') IS DISTINCT FROM 'array' OR jsonb_array_length(finding->'evidence') NOT BETWEEN 1 AND 4
 THEN RAISE EXCEPTION 'invalid fact evidence'; END IF;
 FOR i IN SELECT value FROM jsonb_array_elements(indexes) LOOP
  IF jsonb_typeof(i) IS DISTINCT FROM 'number' OR i::text !~ '^[0-3]$' THEN RAISE EXCEPTION 'invalid fact evidence'; END IF;
  n:=(i::text)::integer; e:=finding->'evidence'->n;
  IF n=ANY(seen) OR e IS NULL THEN RAISE EXCEPTION 'invalid fact evidence'; END IF;
  seen:=array_append(seen,n); proofs:=proofs||jsonb_build_array(e);
 END LOOP;
 RETURN proofs;
END $$;

-- Pure schema8 precondition; malformed facts never become an accepted apply
-- failure after a source activation has begun. Existing v2 checks are unchanged.
CREATE FUNCTION rkb_v4.check_source_facts_v1(finding jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; o jsonb; proofs jsonb; n integer; y integer; m integer; dy integer;
 leap boolean; max_day integer; number numeric; first_year integer; last_year integer;
BEGIN
 IF jsonb_typeof(finding->'event_dates') IS DISTINCT FROM 'array' OR jsonb_array_length(finding->'event_dates')>8
 OR jsonb_typeof(finding->'observations') IS DISTINCT FROM 'array' OR jsonb_array_length(finding->'observations')>8
 THEN RAISE EXCEPTION 'invalid fact projection'; END IF;
 n:=0;
 FOR d IN SELECT value FROM jsonb_array_elements(finding->'event_dates') LOOP
  IF jsonb_typeof(d) IS DISTINCT FROM 'object'
   OR NOT d ?& ARRAY['original_date_text','precision','calendar','date_role','year','month','day','rationale','evidence_indexes']
   OR d-ARRAY['original_date_text','precision','calendar','date_role','year','month','day','rationale','evidence_indexes']<>'{}'
   OR jsonb_typeof(d->'original_date_text') IS DISTINCT FROM 'string' OR length(d->>'original_date_text') NOT BETWEEN 1 AND 150
   OR jsonb_typeof(d->'rationale') IS DISTINCT FROM 'string' OR length(d->>'rationale') NOT BETWEEN 10 AND 500
   OR jsonb_typeof(d->'precision') IS DISTINCT FROM 'string' OR d->>'precision' NOT IN ('day','month','year','approximate_day','range','unknown')
   OR jsonb_typeof(d->'calendar') IS DISTINCT FROM 'string' OR d->>'calendar' NOT IN ('gregorian','julian','unspecified')
   OR jsonb_typeof(d->'date_role') IS DISTINCT FROM 'string' OR d->>'date_role' NOT IN ('event','publication','source_recorded')
   THEN RAISE EXCEPTION 'invalid source date'; END IF;
  FOR o IN SELECT d->k FROM unnest(ARRAY['year','month','day']) k LOOP
   IF o<>'null'::jsonb AND (jsonb_typeof(o) IS DISTINCT FROM 'number' OR o::text !~ '^[1-9][0-9]{0,3}$')
   THEN RAISE EXCEPTION 'invalid source date'; END IF;
  END LOOP;
  y:=(d->>'year')::integer; m:=(d->>'month')::integer; dy:=(d->>'day')::integer;
  IF (y IS NOT NULL AND y NOT BETWEEN 1 AND 9999) OR (m IS NOT NULL AND m NOT BETWEEN 1 AND 12)
   OR (dy IS NOT NULL AND dy NOT BETWEEN 1 AND 31)
   OR (d->>'precision' IN ('day','approximate_day') AND (y IS NULL OR m IS NULL OR dy IS NULL))
   OR (d->>'precision'='month' AND (y IS NULL OR m IS NULL OR dy IS NOT NULL))
   OR (d->>'precision'='year' AND (y IS NULL OR m IS NOT NULL OR dy IS NOT NULL))
   OR (d->>'precision' IN ('range','unknown') AND (y IS NOT NULL OR m IS NOT NULL OR dy IS NOT NULL))
   THEN RAISE EXCEPTION 'invalid source date'; END IF;
  IF dy IS NOT NULL THEN
   leap:=(y%4=0 AND (d->>'calendar' IN ('julian','unspecified') OR y%100<>0 OR y%400=0));
   max_day:=CASE WHEN m=2 THEN CASE WHEN leap THEN 29 ELSE 28 END WHEN m IN (4,6,9,11) THEN 30 ELSE 31 END;
   IF dy>max_day THEN RAISE EXCEPTION 'invalid source date'; END IF;
  END IF;
  proofs:=rkb_v4.source_fact_proofs_v1(finding,d->'evidence_indexes');
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e WHERE strpos(e->>'original_excerpt',d->>'original_date_text')>0)
   THEN RAISE EXCEPTION 'date wording not in selected evidence'; END IF;
  n:=n+1;
 END LOOP;
 n:=0;
 FOR d IN SELECT value FROM jsonb_array_elements(finding->'observations') LOOP
  IF jsonb_typeof(d) IS DISTINCT FROM 'object'
   OR NOT d ?& ARRAY['original_value_text','value','metric_key','unit_code','subject_label','period_text',
     'period_start_year','period_end_year','method','method_note','precision','rationale','evidence_indexes']
   OR d-ARRAY['original_value_text','value','metric_key','unit_code','subject_label','period_text',
     'period_start_year','period_end_year','method','method_note','precision','rationale','evidence_indexes']<>'{}'
   OR jsonb_typeof(d->'original_value_text') IS DISTINCT FROM 'string' OR length(d->>'original_value_text') NOT BETWEEN 1 AND 100
   OR jsonb_typeof(d->'value') IS DISTINCT FROM 'string' OR length(d->>'value')>64
   OR d->>'value' !~ '^[+-]?[0-9]+(\.[0-9]+)?([Ee][+-]?[0-9]+)?$'
   OR jsonb_typeof(d->'metric_key') IS DISTINCT FROM 'string' OR d->>'metric_key' !~ '^[a-z][a-z0-9_]{1,63}$'
   OR jsonb_typeof(d->'unit_code') IS DISTINCT FROM 'string' OR d->>'unit_code' !~ '^[a-z][a-z0-9_]{1,63}$'
   OR jsonb_typeof(d->'subject_label') IS DISTINCT FROM 'string' OR length(d->>'subject_label') NOT BETWEEN 1 AND 200
   OR jsonb_typeof(d->'period_text') IS DISTINCT FROM 'string' OR length(d->>'period_text') NOT BETWEEN 1 AND 150
   OR jsonb_typeof(d->'method') IS DISTINCT FROM 'string' OR d->>'method' NOT IN ('reported','counted','measured','estimated','derived','unknown')
   OR jsonb_typeof(d->'precision') IS DISTINCT FROM 'string' OR d->>'precision' NOT IN ('as_reported','approximate','lower_bound','upper_bound')
   OR jsonb_typeof(d->'method_note') IS DISTINCT FROM 'string' OR length(d->>'method_note') NOT BETWEEN 10 AND 350
   OR jsonb_typeof(d->'rationale') IS DISTINCT FROM 'string' OR length(d->>'rationale') NOT BETWEEN 10 AND 500
   THEN RAISE EXCEPTION 'invalid source observation'; END IF;
  number:=(d->>'value')::numeric;
  IF number<>round(number,6) OR abs(number)>=100000000000000 THEN RAISE EXCEPTION 'invalid source observation'; END IF;
  FOR o IN SELECT d->k FROM unnest(ARRAY['period_start_year','period_end_year']) k LOOP
   IF o<>'null'::jsonb AND (jsonb_typeof(o) IS DISTINCT FROM 'number' OR o::text !~ '^[1-9][0-9]{0,3}$')
   THEN RAISE EXCEPTION 'invalid source observation'; END IF;
  END LOOP;
  first_year:=(d->>'period_start_year')::integer; last_year:=(d->>'period_end_year')::integer;
  IF (first_year IS NULL)<>(last_year IS NULL) OR first_year>last_year
   OR (d->>'period_text'='unknown' AND first_year IS NOT NULL) THEN RAISE EXCEPTION 'invalid observation period'; END IF;
  proofs:=rkb_v4.source_fact_proofs_v1(finding,d->'evidence_indexes');
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e WHERE strpos(e->>'original_excerpt',d->>'original_value_text')>0)
   OR (d->>'period_text'<>'unknown' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e
     WHERE strpos(e->>'original_excerpt',d->>'period_text')>0)) THEN RAISE EXCEPTION 'observation wording not in selected evidence'; END IF;
  n:=n+1;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR raise_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.project_source_facts_v1()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; n integer:=0;
BEGIN
 IF NOT rkb_v4.check_source_facts_v1(NEW.data) THEN RAISE EXCEPTION 'invalid source fact projection'; END IF;
 FOR d IN SELECT value FROM jsonb_array_elements(NEW.data->'event_dates') LOOP
  INSERT INTO rkb_v4.source_event_dates VALUES(NEW.assertion_id,NEW.revision,n,NEW.source_id,
   d->>'calendar',d->>'precision',d->>'date_role',(d->>'year')::integer,(d->>'month')::integer,(d->>'day')::integer,
   d,rkb_v4.source_fact_proofs_v1(NEW.data,d->'evidence_indexes')); n:=n+1;
 END LOOP;
 n:=0;
 FOR d IN SELECT value FROM jsonb_array_elements(NEW.data->'observations') LOOP
  INSERT INTO rkb_v4.source_observations VALUES(NEW.assertion_id,NEW.revision,n,NEW.source_id,
   d->>'metric_key',d->>'unit_code',(d->>'value')::numeric,d,
   rkb_v4.source_fact_proofs_v1(NEW.data,d->'evidence_indexes')); n:=n+1;
 END LOOP;
 RETURN NEW;
END $$;
CREATE TRIGGER project_source_facts AFTER INSERT ON rkb_v4.story_assertion_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.project_source_facts_v1();

CREATE FUNCTION rkb_v4.read_source_calendar_v1(cal text,mo integer,dy integer,scope uuid,
 after_year integer,after_assertion uuid,after_ordinal integer,take integer)
RETURNS TABLE(assertion_id uuid,source_id uuid,revision bigint,eligibility_revision bigint,proposition text,
 account_kind text,attributed_to text,reported_by text,ordinal integer,fact jsonb,proofs jsonb,bibliography text,sort_year integer)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT a.assertion_id,a.source_id,a.source_revision,v.eligibility_revision,a.proposition,
 a.data->>'account_kind',a.data->>'attributed_to',a.data->>'reported_by',d.ordinal,d.data,d.proofs,s.bibliography,d.year
 FROM rkb_v4.source_event_dates d JOIN rkb_v4.story_assertion_versions a
 ON (a.assertion_id,a.revision)=(d.assertion_id,d.assertion_revision)
 JOIN rkb_v4.sources s ON s.source_id=a.source_id JOIN rkb_v4.active_revision r ON (r.source_id,r.revision)=(a.source_id,a.source_revision)
 JOIN rkb_v4.source_visibility v ON v.source_id=a.source_id
 WHERE d.precision='day' AND d.date_role='event' AND d.calendar=cal AND d.month=mo AND d.day=dy
 AND (scope IS NULL OR d.source_id=scope) AND s.accepted AND NOT s.withdrawn
 AND (d.year,d.assertion_id,d.ordinal)>(after_year,after_assertion,after_ordinal)
 ORDER BY d.year,d.assertion_id,d.ordinal LIMIT least(greatest(take,1),11)
$$;

CREATE FUNCTION rkb_v4.read_source_observations_v1(metric text,unit text,scope uuid,
 after_assertion uuid,after_ordinal integer,take integer)
RETURNS TABLE(assertion_id uuid,source_id uuid,revision bigint,eligibility_revision bigint,proposition text,
 account_kind text,attributed_to text,reported_by text,ordinal integer,fact jsonb,proofs jsonb,bibliography text,sort_year integer)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT a.assertion_id,a.source_id,a.source_revision,v.eligibility_revision,a.proposition,
 a.data->>'account_kind',a.data->>'attributed_to',a.data->>'reported_by',d.ordinal,d.data,d.proofs,s.bibliography,0
 FROM rkb_v4.source_observations d JOIN rkb_v4.story_assertion_versions a
 ON (a.assertion_id,a.revision)=(d.assertion_id,d.assertion_revision)
 JOIN rkb_v4.sources s ON s.source_id=a.source_id JOIN rkb_v4.active_revision r ON (r.source_id,r.revision)=(a.source_id,a.source_revision)
 JOIN rkb_v4.source_visibility v ON v.source_id=a.source_id
 WHERE d.metric_key=metric AND (unit IS NULL OR d.unit_code=unit) AND (scope IS NULL OR d.source_id=scope)
 AND s.accepted AND NOT s.withdrawn AND (d.assertion_id,d.ordinal)>(after_assertion,after_ordinal)
 ORDER BY d.assertion_id,d.ordinal LIMIT least(greatest(take,1),11)
$$;

CREATE FUNCTION rkb_v4.read_source_assertions_v1(scope uuid,after_assertion uuid,take integer)
RETURNS TABLE(assertion_id uuid,source_id uuid,revision bigint,eligibility_revision bigint,proposition text,
 account_kind text,attributed_to text,reported_by text,ordinal integer,fact jsonb,proofs jsonb,bibliography text,sort_year integer)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT a.assertion_id,a.source_id,a.source_revision,v.eligibility_revision,a.proposition,
 a.data->>'account_kind',a.data->>'attributed_to',a.data->>'reported_by',0,
 jsonb_build_object('event_date_count',jsonb_array_length(a.data->'event_dates'),
 'observation_count',jsonb_array_length(a.data->'observations'),'entity_keys',a.data->'entity_keys'),a.data->'evidence',s.bibliography,0
 FROM rkb_v4.story_assertion_versions a JOIN rkb_v4.sources s USING(source_id)
 JOIN rkb_v4.active_revision r ON (r.source_id,r.revision)=(a.source_id,a.source_revision)
 JOIN rkb_v4.source_visibility v ON v.source_id=a.source_id
 WHERE a.source_id=scope AND s.accepted AND NOT s.withdrawn AND a.assertion_id>after_assertion
 ORDER BY a.assertion_id LIMIT least(greatest(take,1),11)
$$;

CREATE FUNCTION rkb_v4.check_source_import_v3(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE finding jsonb;
BEGIN
 IF NOT rkb_v4.check_source_import_v2(p,ids,expected,logical_time) THEN RETURN false; END IF;
 FOR finding IN SELECT value FROM jsonb_array_elements(convert_from(p,'UTF8')::jsonb->'semantic'->'findings') LOOP
  IF NOT rkb_v4.check_source_facts_v1(finding) THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_import_v3(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF NOT rkb_v4.check_source_import_v3(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'source facts require exact resolved evidence precondition';
 END IF;
 RETURN rkb_v4.apply_source_import_v2(p,ids,expected,logical_time);
END $$;

CREATE FUNCTION rkb_v4.capture_story_facts_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE command jsonb;
BEGIN
 command:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF NEW.status='applied' AND command->>'route'='source.import.v3' THEN
  INSERT INTO rkb_v4.story_history_receipts
  SELECT s.story_id,1,NEW.operation_key,NEW.sequence,command->>'person_id',command->>'request_id',
   command->>'logical_time',NEW.command_sha256 FROM rkb_v4.stories s
  WHERE s.source_id=(command->'ids'->>'source_id')::uuid;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_story_facts_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_story_facts_receipt_v1();
CREATE FUNCTION rkb_v4.verify_source_story_origins_v2(scope uuid[])
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE op record;c jsonb;doc jsonb;sem jsonb;source uuid;item jsonb;v jsonb;
 verified uuid[]:='{}';seen integer:=0;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 IF cardinality(scope)=0 THEN RETURN true;END IF;
 FOR op IN SELECT command_bytes FROM rkb_v4.applied_ops WHERE status='applied' ORDER BY sequence LIMIT 10001 LOOP
  seen:=seen+1;IF seen>10000 THEN RETURN false;END IF;
  c:=convert_from(op.command_bytes,'UTF8')::jsonb;
  IF c->>'route' NOT IN ('source.import.v2','source.import.v3') OR NOT((c->'ids'->>'source_id')::uuid=ANY(scope)) THEN CONTINUE;END IF;
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
   AND NOT EXISTS(SELECT 1 FROM rkb_v4.stories WHERE source_id=ANY(scope) AND NOT(source_id=ANY(verified)));
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

REVOKE ALL ON rkb_v4.source_event_dates,rkb_v4.source_observations FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.source_fact_proofs_v1(jsonb,jsonb),rkb_v4.project_source_facts_v1(),
 rkb_v4.check_source_facts_v1(jsonb),rkb_v4.check_source_import_v3(bytea,jsonb,jsonb,text),
 rkb_v4.apply_source_import_v3(bytea,jsonb,jsonb,text),rkb_v4.capture_story_facts_receipt_v1(),
 rkb_v4.verify_source_story_origins_v2(uuid[]),
 rkb_v4.read_source_calendar_v1(text,integer,integer,uuid,integer,uuid,integer,integer),
 rkb_v4.read_source_observations_v1(text,text,uuid,uuid,integer,integer),
 rkb_v4.read_source_assertions_v1(uuid,uuid,integer) FROM PUBLIC;
