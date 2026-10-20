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

CREATE FUNCTION rkb_v4.project_source_facts_v1()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; o jsonb; proofs jsonb; n integer; y integer; m integer; dy integer;
 leap boolean; max_day integer; number numeric; first_year integer; last_year integer;
BEGIN
 IF jsonb_typeof(NEW.data->'event_dates') IS DISTINCT FROM 'array' OR jsonb_array_length(NEW.data->'event_dates')>8
 OR jsonb_typeof(NEW.data->'observations') IS DISTINCT FROM 'array' OR jsonb_array_length(NEW.data->'observations')>8
 THEN RAISE EXCEPTION 'invalid fact projection'; END IF;
 n:=0;
 FOR d IN SELECT value FROM jsonb_array_elements(NEW.data->'event_dates') LOOP
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
  proofs:=rkb_v4.source_fact_proofs_v1(NEW.data,d->'evidence_indexes');
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e WHERE strpos(e->>'original_excerpt',d->>'original_date_text')>0)
   THEN RAISE EXCEPTION 'date wording not in selected evidence'; END IF;
  INSERT INTO rkb_v4.source_event_dates VALUES(NEW.assertion_id,NEW.revision,n,NEW.source_id,
   d->>'calendar',d->>'precision',d->>'date_role',y,m,dy,d,proofs); n:=n+1;
 END LOOP;
 n:=0;
 FOR d IN SELECT value FROM jsonb_array_elements(NEW.data->'observations') LOOP
  IF jsonb_typeof(d) IS DISTINCT FROM 'object'
   OR NOT d ?& ARRAY['original_value_text','value','metric_key','unit_code','subject_label','period_text',
     'period_start_year','period_end_year','method','method_note','precision','rationale','evidence_indexes']
   OR d-ARRAY['original_value_text','value','metric_key','unit_code','subject_label','period_text',
     'period_start_year','period_end_year','method','method_note','precision','rationale','evidence_indexes']<>'{}'
   OR jsonb_typeof(d->'original_value_text') IS DISTINCT FROM 'string' OR length(d->>'original_value_text') NOT BETWEEN 1 AND 100
   OR jsonb_typeof(d->'value') IS DISTINCT FROM 'string' OR d->>'value' !~ '^-?[0-9]+(\.[0-9]+)?$'
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
  proofs:=rkb_v4.source_fact_proofs_v1(NEW.data,d->'evidence_indexes');
  IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e WHERE strpos(e->>'original_excerpt',d->>'original_value_text')>0)
   OR (d->>'period_text'<>'unknown' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proofs) e
     WHERE strpos(e->>'original_excerpt',d->>'period_text')>0)) THEN RAISE EXCEPTION 'observation wording not in selected evidence'; END IF;
  INSERT INTO rkb_v4.source_observations VALUES(NEW.assertion_id,NEW.revision,n,NEW.source_id,
   d->>'metric_key',d->>'unit_code',number,d,proofs); n:=n+1;
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
