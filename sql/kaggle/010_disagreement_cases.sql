-- Immutable ordered schema010. Native case authority; no migrated old corpus.
CREATE TABLE rkb_v4.disagreement_cases (
 case_id uuid PRIMARY KEY,owner_id uuid NOT NULL,revision bigint NOT NULL CHECK(revision>0),
 acl_revision bigint NOT NULL CHECK(acl_revision>0),event_seq bigint NOT NULL CHECK(event_seq>=0),
 state text NOT NULL CHECK(state IN ('open','paused','archived')),current_conclusion_id uuid,
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object')
);
CREATE TABLE rkb_v4.disagreement_case_versions (
 case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,revision bigint NOT NULL CHECK(revision>0),
 state text NOT NULL,current_conclusion_id uuid,data jsonb NOT NULL,actor_id uuid NOT NULL,
 logical_time text NOT NULL,payload_sha256 text NOT NULL,PRIMARY KEY(case_id,revision)
);
CREATE TABLE rkb_v4.disagreement_grants (
 case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,grantee_id uuid NOT NULL,
 capability text NOT NULL CHECK(capability IN ('viewer','contributor','curator','manager')),
 revision bigint NOT NULL CHECK(revision>0),PRIMARY KEY(case_id,grantee_id)
);
CREATE TABLE rkb_v4.disagreement_evidence (
 evidence_id uuid NOT NULL,version bigint NOT NULL CHECK(version>0),case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,
 source_id uuid NOT NULL,source_revision bigint NOT NULL,page_id uuid NOT NULL,region_id uuid NOT NULL,
 data jsonb NOT NULL,actor_id uuid NOT NULL,event_seq bigint NOT NULL,logical_time text NOT NULL,payload_sha256 text NOT NULL,
 PRIMARY KEY(evidence_id,version),FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions,
 FOREIGN KEY(region_id) REFERENCES rkb_v4.source_regions
);
CREATE INDEX disagreement_evidence_case ON rkb_v4.disagreement_evidence(case_id,event_seq,evidence_id,version);
CREATE INDEX disagreement_evidence_source ON rkb_v4.disagreement_evidence(source_id,source_revision,case_id);
CREATE TABLE rkb_v4.disagreement_contributions (
 contribution_id uuid NOT NULL,version bigint NOT NULL CHECK(version>0),case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,
 actor_id uuid NOT NULL,recorded_by text NOT NULL,data jsonb NOT NULL,event_seq bigint NOT NULL,
 logical_time text NOT NULL,payload_sha256 text NOT NULL,PRIMARY KEY(contribution_id,version)
);
CREATE INDEX disagreement_contribution_case ON rkb_v4.disagreement_contributions(case_id,event_seq,contribution_id,version);
CREATE TABLE rkb_v4.disagreement_conclusions (
 conclusion_id uuid PRIMARY KEY,case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,
 ordinal bigint NOT NULL CHECK(ordinal>0),actor_id uuid NOT NULL,data jsonb NOT NULL,based_on_seq bigint NOT NULL,
 event_seq bigint NOT NULL,logical_time text NOT NULL,payload_sha256 text NOT NULL,UNIQUE(case_id,ordinal)
);
CREATE INDEX disagreement_conclusion_case ON rkb_v4.disagreement_conclusions(case_id,event_seq,conclusion_id);
CREATE TABLE rkb_v4.disagreement_events (
 case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,seq bigint NOT NULL CHECK(seq>0),actor_id uuid NOT NULL,
 kind text NOT NULL,object_id uuid NOT NULL,version bigint NOT NULL,data jsonb NOT NULL,
 logical_time text NOT NULL,payload_sha256 text NOT NULL,PRIMARY KEY(case_id,seq)
);
CREATE TABLE rkb_v4.disagreement_drafts (
 draft_id uuid PRIMARY KEY,case_id uuid NOT NULL REFERENCES rkb_v4.disagreement_cases,
 actor_id uuid NOT NULL,version bigint NOT NULL CHECK(version>0)
);
CREATE INDEX disagreement_own_drafts ON rkb_v4.disagreement_drafts(actor_id,case_id,draft_id);
CREATE TABLE rkb_v4.disagreement_draft_versions (
 draft_id uuid NOT NULL REFERENCES rkb_v4.disagreement_drafts,version bigint NOT NULL CHECK(version>0),
 data jsonb NOT NULL,source_dependencies jsonb NOT NULL,logical_time text NOT NULL,payload_sha256 text NOT NULL,
 PRIMARY KEY(draft_id,version)
);
CREATE TABLE rkb_v4.disagreement_draft_publications (
 draft_id uuid NOT NULL,version bigint NOT NULL,contribution_id uuid NOT NULL,case_id uuid NOT NULL,
 actor_id uuid NOT NULL,contribution_version bigint NOT NULL DEFAULT 1 CHECK(contribution_version=1),PRIMARY KEY(draft_id,version,contribution_id),
 FOREIGN KEY(draft_id,version) REFERENCES rkb_v4.disagreement_draft_versions,
 FOREIGN KEY(contribution_id,contribution_version) REFERENCES rkb_v4.disagreement_contributions(contribution_id,version)
);
CREATE TABLE rkb_v4.disagreement_receipts (
 operation_key text PRIMARY KEY REFERENCES rkb_v4.applied_ops,case_id uuid NOT NULL,
 object_id uuid NOT NULL,action text NOT NULL,sequence bigint NOT NULL,person_id text NOT NULL,
 request_id text NOT NULL,command_sha256 text NOT NULL,payload_sha256 text NOT NULL,
 result jsonb NOT NULL
);
CREATE INDEX disagreement_receipt_case ON rkb_v4.disagreement_receipts(case_id,sequence);

CREATE FUNCTION rkb_v4.case_role_v1(wanted uuid,actor uuid) RETURNS integer
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT CASE WHEN c.owner_id=actor THEN 4 ELSE CASE g.capability
  WHEN 'viewer' THEN 1 WHEN 'contributor' THEN 2 WHEN 'curator' THEN 3 WHEN 'manager' THEN 4 ELSE 0 END END
 FROM rkb_v4.disagreement_cases c LEFT JOIN rkb_v4.disagreement_grants g
 ON g.case_id=c.case_id AND g.grantee_id=actor WHERE c.case_id=wanted
$$;
CREATE FUNCTION rkb_v4.case_sources_v1(wanted uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE refs jsonb;
BEGIN
 IF EXISTS(SELECT 1 FROM rkb_v4.disagreement_evidence e
  LEFT JOIN rkb_v4.sources s ON s.source_id=e.source_id
  LEFT JOIN rkb_v4.active_revision a ON a.source_id=e.source_id AND a.revision=e.source_revision
  LEFT JOIN rkb_v4.source_visibility v ON v.source_id=e.source_id
  WHERE e.case_id=wanted AND (s.source_id IS NULL OR NOT s.accepted OR s.withdrawn OR a.source_id IS NULL OR v.source_id IS NULL)) THEN RETURN NULL;END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('source_id',r.source_id,'content_revision',r.source_revision,
  'eligibility_revision',r.eligibility_revision) ORDER BY r.source_id),'[]'::jsonb) INTO refs FROM(
  SELECT DISTINCT e.source_id,e.source_revision,v.eligibility_revision
  FROM rkb_v4.disagreement_evidence e JOIN rkb_v4.source_visibility v USING(source_id) WHERE e.case_id=wanted) r;
 RETURN refs;
END $$;
CREATE FUNCTION rkb_v4.case_refs_v1(wanted uuid,evidence jsonb,positions jsonb) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE e jsonb;position text;
BEGIN
 IF jsonb_typeof(evidence) IS DISTINCT FROM 'array' OR jsonb_typeof(positions) IS DISTINCT FROM 'array' THEN RETURN false;END IF;
 IF (SELECT count(*) FROM jsonb_array_elements(evidence))<>(SELECT count(DISTINCT value) FROM jsonb_array_elements(evidence))
 OR (SELECT count(*) FROM jsonb_array_elements(positions))<>(SELECT count(DISTINCT value) FROM jsonb_array_elements(positions)) THEN RETURN false;END IF;
 FOR e IN SELECT value FROM jsonb_array_elements(evidence) LOOP
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.disagreement_evidence WHERE case_id=wanted AND evidence_id=(e#>>'{}')::uuid) THEN RETURN false;END IF;
 END LOOP;
 FOR position IN SELECT value FROM jsonb_array_elements_text(positions) LOOP
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.disagreement_cases c CROSS JOIN LATERAL jsonb_array_elements(c.data->'positions') p
   WHERE c.case_id=wanted AND p->>'id'=position) THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN invalid_text_representation THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.case_exact_keys_v1(value jsonb,keys text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(value)='object' AND value ?& keys AND value-keys='{}'::jsonb,false)
$$;

CREATE FUNCTION rkb_v4.case_command_types_v1(action text,op jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE types jsonb;pair record;actual text;
BEGIN
 types:=CASE action
 WHEN 'create' THEN '{"question":"string","context":"string","classification":"string","rationale":"string","positions":"array"}'::jsonb
 WHEN 'revise' THEN '{"question":"string","context":"string","classification":"string","rationale":"string","reason":"string"}'::jsonb
 WHEN 'state' THEN '{"state":"string","reason":"string"}'::jsonb
 WHEN 'grant' THEN '{"grantee":"string","capability":"nullable_string","reason":"string"}'::jsonb
 WHEN 'add_evidence' THEN '{"document_id":"string","source_revision":"integer","source_sha256":"string","page_id":"string","region_id":"string","text_start":"integer","quote":"string","context":"string","position_ids":"array","relation":"string","speaker":"string","attribution":"string"}'::jsonb
 WHEN 'contribute' THEN '{"kind":"string","original_text":"string","interpretation":"string","evidence_ids":"array","position_ids":"array","reply_to":"nullable_string"}'::jsonb
 WHEN 'correct_contribution' THEN '{"original_text":"string","interpretation":"string","reason":"string","withdrawn":"boolean"}'::jsonb
 WHEN 'conclude' THEN '{"kind":"string","text":"string","outcome":"string","allowed_wording":"string","limitations":"string","open_questions":"array","based_on_seq":"integer","evidence_ids":"array","contribution_versions":"object","weight_analysis":"string","make_current":"boolean"}'::jsonb
 WHEN 'draft_save' THEN '{"text":"string","based_on_seq":"integer","evidence_ids":"array","position_ids":"array"}'::jsonb
 WHEN 'draft_publish' THEN '{"text_start":"integer","text_end":"integer","kind":"string","evidence_ids":"array","position_ids":"array"}'::jsonb
 ELSE NULL END;
 IF types IS NULL OR jsonb_typeof(op) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(op))<>(SELECT count(*) FROM jsonb_object_keys(types)) THEN RETURN false;END IF;
 FOR pair IN SELECT key,value FROM jsonb_each_text(types) LOOP
  actual:=jsonb_typeof(op->pair.key);
  IF pair.value='integer' THEN
   IF actual IS DISTINCT FROM 'number' OR (op->>pair.key) !~ '^(0|[1-9][0-9]{0,18})$' THEN RETURN false;END IF;
  ELSIF pair.value='nullable_string' THEN
   IF actual IS NULL OR actual NOT IN ('string','null') THEN RETURN false;END IF;
  ELSIF actual IS DISTINCT FROM pair.value THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
END $$;

CREATE FUNCTION rkb_v4.check_disagreement_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;op jsonb;action text;wanted uuid;actor uuid;object_id uuid;head rkb_v4.disagreement_cases;
 role integer;case_revision bigint;object_version bigint;refs jsonb;item jsonb;region rkb_v4.source_regions;
 contribution rkb_v4.disagreement_contributions;draft rkb_v4.disagreement_drafts;material record;position jsonb;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 65536 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF NOT rkb_v4.case_exact_keys_v1(doc,ARRAY['codec','action','command','source_dependencies','attribution'])
 OR doc->>'codec' IS DISTINCT FROM 'rkb.disagreement-command.v1' THEN RETURN false;END IF;
 action:=doc->>'action';op:=doc->'command';
 IF NOT rkb_v4.case_command_types_v1(action,op) THEN RETURN false;END IF;
 IF action NOT IN ('create','revise','state','grant','add_evidence','contribute','correct_contribution','conclude','draft_save','draft_publish')
 OR NOT rkb_v4.case_exact_keys_v1(expected,ARRAY['case_revision','object_version'])
 OR NOT rkb_v4.case_exact_keys_v1(ids,CASE WHEN action='draft_publish' THEN ARRAY['case_id','object_id','actor_id','result_id'] ELSE ARRAY['case_id','object_id','actor_id'] END)
 OR (expected->>'case_revision') !~ '^(0|[1-9][0-9]{0,18})$' OR (expected->>'object_version') !~ '^(0|[1-9][0-9]{0,18})$' THEN RETURN false;END IF;
 wanted:=(ids->>'case_id')::uuid;actor:=(ids->>'actor_id')::uuid;object_id:=(ids->>'object_id')::uuid;
 case_revision:=(expected->>'case_revision')::bigint;object_version:=(expected->>'object_version')::bigint;
 IF wanted::text<>ids->>'case_id' OR actor::text<>ids->>'actor_id' OR object_id::text<>ids->>'object_id'
 OR NOT rkb_v4.case_exact_keys_v1(doc->'attribution',ARRAY['person_id','recorded_by','route','link_id','link_revision','human_reading_confirmed'])
 OR doc->'attribution'->>'person_id' IS DISTINCT FROM actor::text OR doc->'attribution'->'human_reading_confirmed'<>'false'::jsonb
 OR doc->'attribution'->>'route' IS NULL OR doc->'attribution'->>'route' NOT IN ('owner','personal','expert')
 OR jsonb_typeof(doc->'attribution'->'recorded_by') IS DISTINCT FROM 'string' OR length(doc->'attribution'->>'recorded_by') NOT BETWEEN 1 AND 256
 OR jsonb_typeof(doc->'source_dependencies') IS DISTINCT FROM 'array' OR jsonb_array_length(doc->'source_dependencies')>32 THEN RETURN false;END IF;
 SELECT * INTO head FROM rkb_v4.disagreement_cases WHERE case_id=wanted;
 IF action='create' THEN
  IF head.case_id IS NOT NULL OR case_revision<>0 OR object_version<>0 OR object_id<>wanted OR doc->'source_dependencies'<>'[]'::jsonb
   OR NOT rkb_v4.case_exact_keys_v1(op,ARRAY['question','context','classification','rationale','positions'])
   OR length(op->>'question') NOT BETWEEN 1 AND 3000 OR length(op->>'context') NOT BETWEEN 1 AND 8000
   OR length(op->>'classification') NOT BETWEEN 1 AND 200 OR length(op->>'rationale') NOT BETWEEN 1 AND 8000
   OR jsonb_typeof(op->'positions')<>'array' OR jsonb_array_length(op->'positions') NOT BETWEEN 1 AND 40
   OR (SELECT count(DISTINCT x->>'id') FROM jsonb_array_elements(op->'positions') x)<>jsonb_array_length(op->'positions') THEN RETURN false;END IF;
  FOR position IN SELECT value FROM jsonb_array_elements(op->'positions') LOOP
   IF NOT rkb_v4.case_exact_keys_v1(position,ARRAY['id','text','speaker','modality']) OR position->>'id' !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$'
    OR EXISTS(SELECT 1 FROM jsonb_each(position) e WHERE jsonb_typeof(e.value)<>'string')
    OR length(position->>'text') NOT BETWEEN 1 AND 8000 OR length(position->>'speaker') NOT BETWEEN 1 AND 500
    OR length(position->>'modality') NOT BETWEEN 1 AND 500 THEN RETURN false;END IF;
  END LOOP;
  RETURN true;
 END IF;
 IF head.case_id IS NULL THEN RETURN false;END IF;
 role:=coalesce(rkb_v4.case_role_v1(wanted,actor),0);refs:=rkb_v4.case_sources_v1(wanted);
 IF role<1 OR refs IS NULL OR case_revision>0 AND case_revision<>head.revision THEN RETURN false;END IF;
 IF action='add_evidence' THEN
  IF NOT rkb_v4.case_exact_keys_v1(op,ARRAY['document_id','source_revision','source_sha256','page_id','region_id','text_start','quote','context','position_ids','relation','speaker','attribution'])
   OR role<2 OR EXISTS(SELECT 1 FROM rkb_v4.disagreement_evidence WHERE evidence_id=object_id)
   OR NOT rkb_v4.case_refs_v1(wanted,'[]'::jsonb,op->'position_ids')
   OR op->>'relation' NOT IN ('supports','challenges','context','attributed_criticism')
   OR op->>'attribution' NOT IN ('direct_statement','attributed_report','unknown_speaker')
   OR length(op->>'quote') NOT BETWEEN 1 AND 8000 OR length(op->>'context') NOT BETWEEN 1 AND 8000 THEN RETURN false;END IF;
  SELECT r.* INTO region FROM rkb_v4.source_regions r JOIN rkb_v4.source_import_origins o ON o.source_id=r.source_id
   JOIN rkb_v4.sources s ON s.source_id=r.source_id JOIN rkb_v4.active_revision a ON a.source_id=r.source_id AND a.revision=r.source_revision
   WHERE r.region_id=(op->>'region_id')::uuid AND r.source_id=(op->>'document_id')::uuid AND r.source_revision=(op->>'source_revision')::bigint
    AND r.page_id=(op->>'page_id')::uuid AND o.original_sha256=op->>'source_sha256' AND s.accepted AND NOT s.withdrawn;
  IF region.region_id IS NULL OR (op->>'text_start')::integer<0 OR substring(region.source_text FROM (op->>'text_start')::integer+1 FOR length(op->>'quote')) IS DISTINCT FROM op->>'quote' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_agg(x ORDER BY x->>'source_id'),'[]'::jsonb) INTO refs FROM(
   SELECT value x FROM jsonb_array_elements(refs) UNION SELECT jsonb_build_object('source_id',region.source_id,
    'content_revision',region.source_revision,'eligibility_revision',v.eligibility_revision) FROM rkb_v4.source_visibility v WHERE v.source_id=region.source_id) all_refs;
 ELSIF action IN ('revise','state','grant') THEN
  IF case_revision<1 OR role<(CASE WHEN action='grant' THEN 4 ELSE 3 END) THEN RETURN false;END IF;
  IF action='revise' AND (NOT rkb_v4.case_exact_keys_v1(op,ARRAY['question','context','classification','rationale','reason']) OR length(op->>'question') NOT BETWEEN 1 AND 3000 OR length(op->>'context') NOT BETWEEN 1 AND 8000) THEN RETURN false;END IF;
  IF action='state' AND (NOT rkb_v4.case_exact_keys_v1(op,ARRAY['state','reason']) OR op->>'state' NOT IN ('open','paused','archived')) THEN RETURN false;END IF;
  IF action='grant' AND (NOT rkb_v4.case_exact_keys_v1(op,ARRAY['grantee','capability','reason']) OR (op->>'grantee')::uuid::text<>op->>'grantee'
   OR op->'capability'<>'null'::jsonb AND op->>'capability' NOT IN ('viewer','contributor','curator','manager')) THEN RETURN false;END IF;
 ELSIF action IN ('contribute','correct_contribution') THEN
  IF role<2 THEN RETURN false;END IF;
  IF action='contribute' THEN
   IF NOT rkb_v4.case_exact_keys_v1(op,ARRAY['kind','original_text','interpretation','evidence_ids','position_ids','reply_to'])
    OR op->>'kind' NOT IN ('argument','opinion','question','correction','reply')
    OR EXISTS(SELECT 1 FROM rkb_v4.disagreement_contributions WHERE contribution_id=object_id)
    OR NOT rkb_v4.case_refs_v1(wanted,op->'evidence_ids',op->'position_ids')
    OR op->'reply_to'<>'null'::jsonb AND NOT EXISTS(SELECT 1 FROM rkb_v4.disagreement_contributions WHERE case_id=wanted AND contribution_id=(op->>'reply_to')::uuid) THEN RETURN false;END IF;
  ELSE
   SELECT * INTO contribution FROM rkb_v4.disagreement_contributions WHERE contribution_id=object_id AND case_id=wanted ORDER BY version DESC LIMIT 1;
   IF NOT rkb_v4.case_exact_keys_v1(op,ARRAY['original_text','interpretation','reason','withdrawn'])
    OR contribution.actor_id IS DISTINCT FROM actor OR contribution.version IS DISTINCT FROM object_version OR object_version<1 THEN RETURN false;END IF;
  END IF;
  IF length(op->>'original_text') NOT BETWEEN 1 AND 12000 OR length(op->>'interpretation')>8000 THEN RETURN false;END IF;
 ELSIF action IN ('conclude','draft_save') THEN
  IF action='conclude' THEN
   IF NOT rkb_v4.case_exact_keys_v1(op,ARRAY['kind','text','outcome','allowed_wording','limitations','open_questions','based_on_seq','evidence_ids','contribution_versions','weight_analysis','make_current'])
    OR case_revision<1 OR role<(CASE WHEN op->'make_current'='true'::jsonb THEN 3 ELSE 2 END)
    OR op->>'kind' NOT IN ('model_synthesis','individual_opinion') OR op->>'kind'='individual_opinion' AND op->'make_current'='true'::jsonb
    OR op->>'outcome' NOT IN ('context_explains','insufficient_support','preferred_variant','multiple_interpretations','insufficient_material')
    OR EXISTS(SELECT 1 FROM rkb_v4.disagreement_conclusions WHERE conclusion_id=object_id)
    OR NOT rkb_v4.case_refs_v1(wanted,op->'evidence_ids','[]'::jsonb) THEN RETURN false;END IF;
   FOR material IN SELECT key,value FROM jsonb_each_text(op->'contribution_versions') LOOP
    IF NOT EXISTS(SELECT 1 FROM rkb_v4.disagreement_contributions WHERE case_id=wanted AND contribution_id=material.key::uuid
     AND version=material.value::bigint AND event_seq<=(op->>'based_on_seq')::bigint) THEN RETURN false;END IF;
   END LOOP;
  ELSE
   IF NOT rkb_v4.case_exact_keys_v1(op,ARRAY['text','based_on_seq','evidence_ids','position_ids']) OR NOT rkb_v4.case_refs_v1(wanted,op->'evidence_ids',op->'position_ids') THEN RETURN false;END IF;
   SELECT * INTO draft FROM rkb_v4.disagreement_drafts WHERE draft_id=object_id;
   IF draft.draft_id IS NOT NULL AND (draft.actor_id<>actor OR draft.case_id<>wanted OR draft.version<>object_version)
    OR draft.draft_id IS NULL AND object_version<>0 THEN RETURN false;END IF;
  END IF;
  IF length(op->>'text') NOT BETWEEN 1 AND 16000 OR (op->>'based_on_seq')::bigint NOT BETWEEN 0 AND head.event_seq
   OR EXISTS(SELECT 1 FROM rkb_v4.disagreement_evidence e WHERE e.case_id=wanted AND e.evidence_id IN
     (SELECT value::uuid FROM jsonb_array_elements_text(op->'evidence_ids')) AND e.event_seq>(op->>'based_on_seq')::bigint) THEN RETURN false;END IF;
 ELSIF action='draft_publish' THEN
  IF role<2 OR NOT rkb_v4.case_exact_keys_v1(op,ARRAY['text_start','text_end','kind','evidence_ids','position_ids'])
   OR op->>'kind' NOT IN ('argument','opinion','question') OR NOT rkb_v4.case_refs_v1(wanted,op->'evidence_ids',op->'position_ids')
   OR EXISTS(SELECT 1 FROM rkb_v4.disagreement_contributions WHERE contribution_id=(ids->>'result_id')::uuid) THEN RETURN false;END IF;
  SELECT * INTO draft FROM rkb_v4.disagreement_drafts WHERE draft_id=object_id AND case_id=wanted AND actor_id=actor;
  IF draft.draft_id IS NULL OR draft.version<>object_version OR object_version<1 THEN RETURN false;END IF;
  SELECT data INTO item FROM rkb_v4.disagreement_draft_versions WHERE draft_id=object_id AND version=object_version AND source_dependencies=refs;
  IF item IS NULL OR (op->>'text_start')::integer<0 OR (op->>'text_end')::integer>(length(item->>'text'))
   OR (op->>'text_end')::integer-(op->>'text_start')::integer NOT BETWEEN 1 AND 12000
   OR length(btrim(substring(item->>'text' FROM (op->>'text_start')::integer+1 FOR (op->>'text_end')::integer-(op->>'text_start')::integer)))=0
   OR NOT (item->'evidence_ids') @> (op->'evidence_ids') OR NOT (item->'position_ids') @> (op->'position_ids') THEN RETURN false;END IF;
 END IF;
 IF doc->'source_dependencies' IS DISTINCT FROM refs OR action NOT IN ('draft_save','draft_publish','correct_contribution') AND object_version<>0 THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_disagreement_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb:=convert_from(p,'UTF8')::jsonb;op jsonb:=doc->'command';action text:=doc->>'action';
 wanted uuid:=(ids->>'case_id')::uuid;actor uuid:=(ids->>'actor_id')::uuid;object_id uuid:=(ids->>'object_id')::uuid;
 head rkb_v4.disagreement_cases;new_data jsonb;result jsonb;version bigint:=1;seq bigint;ordinal bigint;
 contribution uuid;payload_hash text:=encode(sha256(p),'hex');draft_data jsonb;page_proof jsonb;region_proof jsonb;
BEGIN
 IF NOT rkb_v4.check_disagreement_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'native case precondition differs';END IF;
 IF action='create' THEN
  INSERT INTO rkb_v4.disagreement_cases VALUES(wanted,actor,1,1,0,'open',NULL,op);
 END IF;
 SELECT * INTO STRICT head FROM rkb_v4.disagreement_cases WHERE case_id=wanted FOR UPDATE;
 IF action='draft_save' THEN
  version:=(expected->>'object_version')::bigint+1;
  INSERT INTO rkb_v4.disagreement_drafts VALUES(object_id,wanted,actor,version)
   ON CONFLICT(draft_id) DO UPDATE SET version=excluded.version;
  INSERT INTO rkb_v4.disagreement_draft_versions VALUES(object_id,version,op,doc->'source_dependencies',logical_time,payload_hash);
  RETURN convert_to(jsonb_build_object('case_id',wanted,'draft_id',object_id,'version',version,
   'case_revision',head.revision,'event_seq',head.event_seq,'visibility','personal')::text,'UTF8');
 END IF;
 seq:=head.event_seq+1;
 UPDATE rkb_v4.disagreement_cases SET event_seq=seq WHERE case_id=wanted;
 IF action='create' THEN
  INSERT INTO rkb_v4.disagreement_case_versions VALUES(wanted,1,'open',NULL,op,actor,logical_time,payload_hash);
 ELSIF action IN ('revise','state','grant') THEN
  new_data:=head.data;
  IF action='revise' THEN new_data:=new_data||(op-'reason');END IF;
  IF action='grant' THEN
   IF op->'capability'='null'::jsonb THEN DELETE FROM rkb_v4.disagreement_grants WHERE case_id=wanted AND grantee_id=(op->>'grantee')::uuid;
   ELSE INSERT INTO rkb_v4.disagreement_grants VALUES(wanted,(op->>'grantee')::uuid,op->>'capability',head.acl_revision+1)
    ON CONFLICT(case_id,grantee_id) DO UPDATE SET capability=excluded.capability,revision=excluded.revision;END IF;
  END IF;
  UPDATE rkb_v4.disagreement_cases SET revision=head.revision+1,data=new_data,
   state=CASE WHEN action='state' THEN op->>'state' ELSE head.state END,
   acl_revision=head.acl_revision+CASE WHEN action='grant' THEN 1 ELSE 0 END WHERE case_id=wanted;
  INSERT INTO rkb_v4.disagreement_case_versions SELECT wanted,h.revision,h.state,h.current_conclusion_id,h.data,actor,logical_time,payload_hash
   FROM rkb_v4.disagreement_cases h WHERE h.case_id=wanted;
 ELSIF action='add_evidence' THEN
  SELECT page.value,region.value INTO STRICT page_proof,region_proof
   FROM rkb_v4.source_manifests m
   CROSS JOIN LATERAL jsonb_array_elements(m.manifest->'pages') page(value)
   CROSS JOIN LATERAL jsonb_array_elements(m.manifest->'regions') region(value)
   WHERE m.source_id=(op->>'document_id')::uuid AND m.source_revision=(op->>'source_revision')::bigint
    AND page.value->>'page_id'=op->>'page_id' AND region.value->>'region_id'=op->>'region_id'
    AND region.value->>'page_id'=page.value->>'page_id';
  new_data:=op||jsonb_build_object('text_end',(op->>'text_start')::integer+length(op->>'quote'),
   'verification','accepted_text_match','visual_confirmation',NULL,
   'physical_page_index',(page_proof->>'physical_page_index')::integer,
   'page_provenance',page_proof,'region_provenance',region_proof);
  INSERT INTO rkb_v4.disagreement_evidence VALUES(object_id,1,wanted,(op->>'document_id')::uuid,
   (op->>'source_revision')::bigint,(op->>'page_id')::uuid,(op->>'region_id')::uuid,new_data,actor,seq,logical_time,payload_hash);
 ELSIF action IN ('contribute','correct_contribution','draft_publish') THEN
  contribution:=object_id;
  IF action='correct_contribution' THEN
   SELECT c.data INTO new_data FROM rkb_v4.disagreement_contributions c WHERE c.contribution_id=object_id
    ORDER BY c.version DESC LIMIT 1;
   new_data:=new_data||op;version:=(expected->>'object_version')::bigint+1;
  ELSIF action='draft_publish' THEN
   SELECT v.data INTO STRICT draft_data FROM rkb_v4.disagreement_draft_versions v
    WHERE v.draft_id=object_id AND v.version=(expected->>'object_version')::bigint;
   contribution:=(ids->>'result_id')::uuid;
   new_data:=jsonb_build_object('kind',op->>'kind','original_text',substring(draft_data->>'text'
     FROM (op->>'text_start')::integer+1 FOR (op->>'text_end')::integer-(op->>'text_start')::integer),
    'interpretation','','evidence_ids',op->'evidence_ids','position_ids',op->'position_ids','reply_to',NULL,'withdrawn',false);
  ELSE new_data:=op||jsonb_build_object('withdrawn',false);END IF;
  new_data:=new_data||jsonb_build_object('attribution',doc->'attribution','human_reading_confirmed',false,
   'origin',CASE WHEN doc->'attribution'->>'route'='expert' THEN 'verified_external_account' ELSE 'authenticated_mcp_account' END);
  INSERT INTO rkb_v4.disagreement_contributions VALUES(contribution,version,wanted,actor,
   doc->'attribution'->>'recorded_by',new_data,seq,logical_time,payload_hash);
  IF action='draft_publish' THEN
   INSERT INTO rkb_v4.disagreement_draft_publications(draft_id,version,contribution_id,case_id,actor_id)
    VALUES(object_id,(expected->>'object_version')::bigint,contribution,wanted,actor);
  END IF;
 ELSIF action='conclude' THEN
  SELECT coalesce(max(c.ordinal),0)+1 INTO ordinal FROM rkb_v4.disagreement_conclusions c WHERE c.case_id=wanted;
  INSERT INTO rkb_v4.disagreement_conclusions VALUES(object_id,wanted,ordinal,actor,op||jsonb_build_object('human_support','[]'::jsonb),
   (op->>'based_on_seq')::bigint,seq,logical_time,payload_hash);
  IF op->'make_current'='true'::jsonb THEN
   UPDATE rkb_v4.disagreement_cases SET revision=head.revision+1,current_conclusion_id=object_id WHERE case_id=wanted;
   INSERT INTO rkb_v4.disagreement_case_versions SELECT wanted,h.revision,h.state,h.current_conclusion_id,h.data,actor,logical_time,payload_hash
    FROM rkb_v4.disagreement_cases h WHERE h.case_id=wanted;
  END IF;
 END IF;
 -- Shared history contains references and explicit shared grant changes only.
 -- No private draft body, identity list or unselected span enters this table.
 INSERT INTO rkb_v4.disagreement_events VALUES(wanted,seq,actor,action,coalesce(contribution,object_id),version,
  CASE WHEN action='grant' THEN op ELSE jsonb_build_object('object_id',coalesce(contribution,object_id),'version',version) END,
  logical_time,payload_hash);
 SELECT * INTO STRICT head FROM rkb_v4.disagreement_cases WHERE case_id=wanted;
 result:=jsonb_build_object('case_id',wanted,'case_revision',head.revision,'event_seq',seq,
  'acl_revision',head.acl_revision,'object_id',coalesce(contribution,object_id),'version',version,'action',action);
 IF action='add_evidence' THEN result:=result||jsonb_build_object('evidence_id',object_id);END IF;
 IF action IN ('contribute','correct_contribution','draft_publish') THEN result:=result||jsonb_build_object('contribution_id',contribution);END IF;
 IF action='conclude' THEN result:=result||jsonb_build_object('conclusion_id',object_id,'conclusion_ordinal',ordinal);END IF;
 IF action='draft_publish' THEN result:=result||jsonb_build_object('draft_id',object_id,'draft_version',(expected->>'object_version')::bigint);END IF;
 RETURN convert_to(result::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.capture_disagreement_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE command jsonb;doc jsonb;outcome jsonb;result jsonb;
BEGIN
 command:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF NEW.status='applied' AND command->>'route' IN ('disagreement.edit.v1','disagreement.access.v1','disagreement.draft.v1','disagreement.publish.v1') THEN
  doc:=convert_from(decode(command->>'payload_b64','base64'),'UTF8')::jsonb;
  outcome:=convert_from(NEW.outcome_bytes,'UTF8')::jsonb;
  result:=convert_from(decode(outcome->>'result_b64','base64'),'UTF8')::jsonb;
  IF command->>'person_id' IS DISTINCT FROM command->'ids'->>'actor_id' THEN RAISE EXCEPTION 'case receipt actor differs';END IF;
  INSERT INTO rkb_v4.disagreement_receipts VALUES(NEW.operation_key,(command->'ids'->>'case_id')::uuid,
   (command->'ids'->>'object_id')::uuid,doc->>'action',NEW.sequence,command->>'person_id',command->>'request_id',
   NEW.command_sha256,encode(sha256(decode(command->>'payload_b64','base64')),'hex'),result);
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_disagreement_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_disagreement_receipt_v1();

-- Immutable bodies and retained evidence cannot be erased to launder a summary.
CREATE TRIGGER immutable_case_version BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_case_versions FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_evidence BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_evidence FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_contribution BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_contributions FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_conclusion BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_conclusions FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_event BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_events FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_draft_version BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_draft_versions FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_publication BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_draft_publications FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_case_receipt BEFORE UPDATE OR DELETE ON rkb_v4.disagreement_receipts FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER case_head_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_cases FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER case_grant_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_grants FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER case_evidence_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_evidence FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER case_contribution_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_contributions FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER case_conclusion_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_conclusions FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER case_draft_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.disagreement_drafts FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();

CREATE FUNCTION rkb_v4.read_disagreement_v1(wanted uuid,actor uuid,section text,after_seq bigint,
 take integer,wanted_revision bigint,wanted_draft uuid,wanted_version bigint,after_draft uuid)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE head rkb_v4.disagreement_cases;version rkb_v4.disagreement_case_versions;
 refs jsonb;header jsonb;entries jsonb:='[]'::jsonb;item record;more boolean:=false;last_seq bigint:=after_seq;
 last_draft uuid:=after_draft;draft rkb_v4.disagreement_drafts;draft_version rkb_v4.disagreement_draft_versions;
 current_conclusion jsonb;latest_material bigint;
BEGIN
 IF section IS NULL OR section NOT IN ('case','evidence','contributions','conclusions','history','draft_get','draft_list')
 OR take IS NULL OR after_seq IS NULL OR take NOT BETWEEN 1 AND 20 OR after_seq<0 OR wanted_revision IS NOT NULL AND wanted_revision<1
 OR wanted_version IS NOT NULL AND wanted_version<1 THEN RAISE EXCEPTION 'bounded native case read required';END IF;
 SELECT * INTO head FROM rkb_v4.disagreement_cases WHERE case_id=wanted;
 IF head.case_id IS NULL OR coalesce(rkb_v4.case_role_v1(wanted,actor),0)<1 THEN RETURN NULL;END IF;
 refs:=rkb_v4.case_sources_v1(wanted);IF refs IS NULL THEN RETURN NULL;END IF;
 header:=jsonb_build_object('case_id',wanted,'case_revision',head.revision,'acl_revision',head.acl_revision,
  'event_seq',head.event_seq,'state',head.state,'current_conclusion_id',head.current_conclusion_id,
  'source_dependencies',refs,'case_role',rkb_v4.case_role_v1(wanted,actor),'section',section);
 IF section='case' THEN
  SELECT * INTO version FROM rkb_v4.disagreement_case_versions WHERE case_id=wanted AND revision=coalesce(wanted_revision,head.revision);
  IF version.case_id IS NULL THEN RETURN NULL;END IF;
  SELECT c.data||jsonb_build_object('conclusion_id',c.conclusion_id,'ordinal',c.ordinal,'based_on_seq',c.based_on_seq,
   'actor_id',c.actor_id,'event_seq',c.event_seq) INTO current_conclusion
   FROM rkb_v4.disagreement_conclusions c WHERE c.case_id=wanted AND c.conclusion_id=version.current_conclusion_id;
  SELECT coalesce(max(e.seq),0) INTO latest_material FROM rkb_v4.disagreement_events e
   WHERE e.case_id=wanted AND e.kind IN ('add_evidence','contribute','correct_contribution','draft_publish');
  RETURN header||version.data||jsonb_build_object('case_revision',version.revision,'state',version.state,
   'current_conclusion_id',version.current_conclusion_id,'current_conclusion',current_conclusion,
   'new_material_since_conclusion',CASE WHEN current_conclusion IS NULL THEN latest_material>0
     ELSE latest_material>(current_conclusion->>'based_on_seq')::bigint END,'has_more',false);
 ELSIF section='draft_get' THEN
  SELECT * INTO draft FROM rkb_v4.disagreement_drafts WHERE draft_id=wanted_draft AND case_id=wanted AND actor_id=actor;
  IF draft.draft_id IS NULL THEN RETURN NULL;END IF;
  SELECT * INTO draft_version FROM rkb_v4.disagreement_draft_versions
   WHERE draft_id=draft.draft_id AND disagreement_draft_versions.version=coalesce(wanted_version,draft.version);
  IF draft_version.draft_id IS NULL OR draft_version.source_dependencies IS DISTINCT FROM refs THEN RETURN NULL;END IF;
  RETURN header||jsonb_build_object('draft_id',draft.draft_id,'version',draft_version.version,'current_version',draft.version,
   'visibility','personal','payload',draft_version.data,'has_more',false,
   'publication_ids',coalesce((SELECT jsonb_agg(p.contribution_id ORDER BY p.contribution_id)
      FROM rkb_v4.disagreement_draft_publications p WHERE p.draft_id=draft.draft_id AND p.version=draft_version.version),'[]'::jsonb));
 ELSIF section='draft_list' THEN
  FOR item IN SELECT d.draft_id,d.version FROM rkb_v4.disagreement_drafts d
   JOIN rkb_v4.disagreement_draft_versions v ON v.draft_id=d.draft_id AND v.version=d.version
   WHERE d.case_id=wanted AND d.actor_id=actor AND (after_draft IS NULL OR d.draft_id>after_draft)
    AND v.source_dependencies=refs ORDER BY d.draft_id LIMIT take+1 LOOP
   IF jsonb_array_length(entries)>=take THEN more:=true;EXIT;END IF;
   entries:=entries||jsonb_build_array(jsonb_build_object('draft_id',item.draft_id,'version',item.version,'visibility','personal'));
   last_draft:=item.draft_id;
  END LOOP;
  RETURN header||jsonb_build_object('items',entries,'has_more',more,'next_after_id',CASE WHEN more THEN last_draft ELSE NULL END);
 END IF;
 -- Whole immutable records are paginated by actual case event sequence. The
 -- byte bound stops before the next record; no body/quote is clipped.
 FOR item IN
  SELECT * FROM (
   SELECT e.event_seq AS seq,jsonb_build_object('evidence_id',e.evidence_id,'version',e.version,'actor_id',e.actor_id,
    'event_seq',e.event_seq,'payload',e.data,'logical_time',e.logical_time) AS row
    FROM rkb_v4.disagreement_evidence e WHERE section='evidence' AND e.case_id=wanted AND e.event_seq>after_seq
   UNION ALL
   SELECT c.event_seq,jsonb_build_object('contribution_id',c.contribution_id,'version',c.version,'actor_id',c.actor_id,
    'recorded_by',c.recorded_by,'event_seq',c.event_seq,'payload',c.data,'logical_time',c.logical_time)
    FROM rkb_v4.disagreement_contributions c WHERE section='contributions' AND c.case_id=wanted AND c.event_seq>after_seq
   UNION ALL
   SELECT c.event_seq,jsonb_build_object('conclusion_id',c.conclusion_id,'ordinal',c.ordinal,'actor_id',c.actor_id,
    'based_on_seq',c.based_on_seq,'event_seq',c.event_seq,'payload',c.data,'logical_time',c.logical_time)
    FROM rkb_v4.disagreement_conclusions c WHERE section='conclusions' AND c.case_id=wanted AND c.event_seq>after_seq
   UNION ALL
   SELECT e.seq,jsonb_build_object('seq',e.seq,'actor_id',e.actor_id,'kind',e.kind,'object_id',e.object_id,
    'version',e.version,'payload',e.data,'logical_time',e.logical_time)
    FROM rkb_v4.disagreement_events e WHERE section='history' AND e.case_id=wanted AND e.seq>after_seq
  ) all_rows ORDER BY seq LIMIT take+1 LOOP
  IF jsonb_array_length(entries)>=take OR octet_length((entries||jsonb_build_array(item.row))::text)>98304 THEN more:=true;EXIT;END IF;
  entries:=entries||jsonb_build_array(item.row);last_seq:=item.seq;
 END LOOP;
 RETURN header||jsonb_build_object('items',entries,'has_more',more,'next_after_seq',CASE WHEN more THEN last_seq ELSE NULL END);
END $$;

CREATE FUNCTION rkb_v4.read_disagreement_operation_v1(wanted uuid,actor uuid,operation_key text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE receipt rkb_v4.disagreement_receipts;refs jsonb;
BEGIN
 IF coalesce(rkb_v4.case_role_v1(wanted,actor),0)<1 THEN RETURN NULL;END IF;
 refs:=rkb_v4.case_sources_v1(wanted);IF refs IS NULL THEN RETURN NULL;END IF;
 SELECT * INTO receipt FROM rkb_v4.disagreement_receipts r WHERE r.operation_key=read_disagreement_operation_v1.operation_key
  AND r.case_id=wanted AND r.person_id=actor::text;
 IF receipt.operation_key IS NULL THEN RETURN NULL;END IF;
 -- A manager cannot turn someone else's private write receipt into a draft
 -- read. Receipts are actor scoped even for otherwise shared operations.
 IF receipt.action IN ('draft_save','draft_publish') AND NOT EXISTS(SELECT 1 FROM rkb_v4.disagreement_drafts d
  WHERE d.draft_id=receipt.object_id AND d.case_id=wanted AND d.actor_id=actor) THEN RETURN NULL;END IF;
 RETURN jsonb_build_object('case_id',wanted,'source_dependencies',refs,'result',receipt.result,
  'operation_key',receipt.operation_key,'sequence',receipt.sequence,'command_sha256',receipt.command_sha256,
  'request_id',receipt.request_id,'state','applied','outcome_durability','native_only');
END $$;

CREATE FUNCTION rkb_v4.reject_case_identity_update_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_TABLE_NAME='disagreement_cases' THEN
  IF NEW.case_id<>OLD.case_id OR NEW.owner_id<>OLD.owner_id THEN RAISE EXCEPTION 'immutable case identity';END IF;
 ELSE
  IF NEW.draft_id<>OLD.draft_id OR NEW.case_id<>OLD.case_id OR NEW.actor_id<>OLD.actor_id THEN RAISE EXCEPTION 'immutable draft author';END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_case_identity BEFORE UPDATE ON rkb_v4.disagreement_cases FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_case_identity_update_v1();
CREATE TRIGGER immutable_draft_identity BEFORE UPDATE ON rkb_v4.disagreement_drafts FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_case_identity_update_v1();

REVOKE ALL ON rkb_v4.disagreement_cases,rkb_v4.disagreement_case_versions,rkb_v4.disagreement_grants,
 rkb_v4.disagreement_evidence,rkb_v4.disagreement_contributions,rkb_v4.disagreement_conclusions,
 rkb_v4.disagreement_events,rkb_v4.disagreement_drafts,rkb_v4.disagreement_draft_versions,
 rkb_v4.disagreement_draft_publications,rkb_v4.disagreement_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.case_role_v1(uuid,uuid),rkb_v4.case_sources_v1(uuid),rkb_v4.case_refs_v1(uuid,jsonb,jsonb),
 rkb_v4.case_exact_keys_v1(jsonb,text[]),rkb_v4.case_command_types_v1(text,jsonb),
 rkb_v4.check_disagreement_v1(bytea,jsonb,jsonb,text),rkb_v4.apply_disagreement_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_disagreement_receipt_v1(),rkb_v4.read_disagreement_v1(uuid,uuid,text,bigint,integer,bigint,uuid,bigint,uuid),
 rkb_v4.read_disagreement_operation_v1(uuid,uuid,text),rkb_v4.reject_case_identity_update_v1() FROM PUBLIC;

-- Reconstruct every Case projection from exact applied command bytes. This is
-- independent of mutable heads/grants and does not treat receipt presence as
-- proof of body fidelity. Used inside the same native read snapshot.
CREATE FUNCTION rkb_v4.verify_disagreement_origins_v1(wanted uuid[]) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE cid uuid;receipt record;c jsonb;doc jsonb;op jsonb;ids jsonb;expected jsonb;actor uuid;
 action text;object_id uuid;contribution uuid;payload_hash text;logical_time text;
 head jsonb;maps jsonb;data jsonb;prior jsonb;version bigint;seq bigint;ordinal bigint;key text;
 page_proof jsonb;region_proof jsonb;actual jsonb;relation text;
BEGIN
 IF wanted IS NULL OR cardinality(wanted)>2048 OR cardinality(wanted)<>(SELECT count(DISTINCT x) FROM unnest(wanted) x)
 THEN RETURN false;END IF;
 FOREACH cid IN ARRAY wanted LOOP
  maps:='{"heads":{},"versions":{},"grants":{},"evidence":{},"contributions":{},"conclusions":{},"events":{},"drafts":{},"draft_versions":{},"publications":{},"receipts":{}}'::jsonb;
  head:=NULL;ordinal:=0;
  FOR receipt IN SELECT a.* FROM rkb_v4.applied_ops a WHERE a.status='applied'
   AND convert_from(a.command_bytes,'UTF8')::jsonb->>'route' IN
    ('disagreement.edit.v1','disagreement.access.v1','disagreement.draft.v1','disagreement.publish.v1')
   AND convert_from(a.command_bytes,'UTF8')::jsonb->'ids'->>'case_id'=cid::text ORDER BY a.sequence LOOP
   c:=convert_from(receipt.command_bytes,'UTF8')::jsonb;ids:=c->'ids';expected:=c->'expected_revisions';
   doc:=convert_from(decode(c->>'payload_b64','base64'),'UTF8')::jsonb;op:=doc->'command';action:=doc->>'action';
   actor:=(c->>'person_id')::uuid;object_id:=(ids->>'object_id')::uuid;contribution:=NULL;
   logical_time:=c->>'logical_time';payload_hash:=encode(sha256(decode(c->>'payload_b64','base64')),'hex');version:=1;
   IF ids->>'actor_id' IS DISTINCT FROM actor::text OR doc->'attribution'->>'person_id' IS DISTINCT FROM actor::text
    OR receipt.command_sha256<>encode(sha256(receipt.command_bytes),'hex') THEN RETURN false;END IF;
   IF action='create' THEN
    IF head IS NOT NULL THEN RETURN false;END IF;
    head:=jsonb_build_object('case_id',cid,'owner_id',actor,'revision',1,'acl_revision',1,'event_seq',0,
     'state','open','current_conclusion_id',NULL,'data',op);
   END IF;
   IF head IS NULL THEN RETURN false;END IF;
   IF action='draft_save' THEN
    version:=(expected->>'object_version')::bigint+1;
    data:=jsonb_build_object('draft_id',object_id,'case_id',cid,'actor_id',actor,'version',version);
    maps:=jsonb_set(maps,ARRAY['drafts',object_id::text],data);
    maps:=jsonb_set(maps,ARRAY['draft_versions',object_id::text||':'||version],jsonb_build_object(
     'draft_id',object_id,'version',version,'data',op,'source_dependencies',doc->'source_dependencies',
     'logical_time',logical_time,'payload_sha256',payload_hash));
   ELSE
    seq:=(head->>'event_seq')::bigint+1;head:=head||jsonb_build_object('event_seq',seq);
    IF action IN ('revise','state','grant') THEN
     IF action='revise' THEN head:=head||jsonb_build_object('data',head->'data'||(op-'reason'));END IF;
     IF action='state' THEN head:=head||jsonb_build_object('state',op->>'state');END IF;
     IF action='grant' THEN
      head:=head||jsonb_build_object('acl_revision',(head->>'acl_revision')::bigint+1);
      key:=op->>'grantee';
      IF op->'capability'='null'::jsonb THEN maps:=jsonb_set(maps,'{grants}',(maps->'grants')-key);
      ELSE maps:=jsonb_set(maps,ARRAY['grants',key],jsonb_build_object('case_id',cid,'grantee_id',key,
       'capability',op->>'capability','revision',(head->>'acl_revision')::bigint));END IF;
     END IF;
     head:=head||jsonb_build_object('revision',(head->>'revision')::bigint+1);
    ELSIF action='add_evidence' THEN
     SELECT page.value,region.value INTO STRICT page_proof,region_proof FROM rkb_v4.source_manifests m
      CROSS JOIN LATERAL jsonb_array_elements(m.manifest->'pages') page(value)
      CROSS JOIN LATERAL jsonb_array_elements(m.manifest->'regions') region(value)
      WHERE m.source_id=(op->>'document_id')::uuid AND m.source_revision=(op->>'source_revision')::bigint
       AND page.value->>'page_id'=op->>'page_id' AND region.value->>'region_id'=op->>'region_id'
       AND region.value->>'page_id'=page.value->>'page_id';
     data:=op||jsonb_build_object('text_end',(op->>'text_start')::integer+length(op->>'quote'),
      'verification','accepted_text_match','visual_confirmation',NULL,
      'physical_page_index',(page_proof->>'physical_page_index')::integer,'page_provenance',page_proof,'region_provenance',region_proof);
     maps:=jsonb_set(maps,ARRAY['evidence',object_id::text||':1'],jsonb_build_object('evidence_id',object_id,'version',1,
      'case_id',cid,'source_id',op->>'document_id','source_revision',(op->>'source_revision')::bigint,
      'page_id',op->>'page_id','region_id',op->>'region_id','data',data,'actor_id',actor,'event_seq',seq,
      'logical_time',logical_time,'payload_sha256',payload_hash));
    ELSIF action IN ('contribute','correct_contribution','draft_publish') THEN
     contribution:=object_id;
     IF action='correct_contribution' THEN
      version:=(expected->>'object_version')::bigint+1;
      prior:=maps->'contributions'->(object_id::text||':'||(version-1));
      IF prior IS NULL OR prior->>'actor_id'<>actor::text THEN RETURN false;END IF;
      data:=prior->'data'||op;
     ELSIF action='draft_publish' THEN
      contribution:=(ids->>'result_id')::uuid;
      prior:=maps->'draft_versions'->(object_id::text||':'||(expected->>'object_version'));
      IF prior IS NULL OR maps->'drafts'->object_id::text->>'actor_id'<>actor::text THEN RETURN false;END IF;
      data:=jsonb_build_object('kind',op->>'kind','original_text',substring(prior->'data'->>'text'
       FROM (op->>'text_start')::integer+1 FOR (op->>'text_end')::integer-(op->>'text_start')::integer),
       'interpretation','','evidence_ids',op->'evidence_ids','position_ids',op->'position_ids','reply_to',NULL,'withdrawn',false);
      key:=object_id::text||':'||(expected->>'object_version')||':'||contribution::text;
      maps:=jsonb_set(maps,ARRAY['publications',key],jsonb_build_object('draft_id',object_id,
       'version',(expected->>'object_version')::bigint,'contribution_id',contribution,'case_id',cid,'actor_id',actor,'contribution_version',1));
     ELSE data:=op||jsonb_build_object('withdrawn',false);END IF;
     data:=data||jsonb_build_object('attribution',doc->'attribution','human_reading_confirmed',false,
      'origin',CASE WHEN doc->'attribution'->>'route'='expert' THEN 'verified_external_account' ELSE 'authenticated_mcp_account' END);
     maps:=jsonb_set(maps,ARRAY['contributions',contribution::text||':'||version],jsonb_build_object(
      'contribution_id',contribution,'version',version,'case_id',cid,'actor_id',actor,
      'recorded_by',doc->'attribution'->>'recorded_by','data',data,'event_seq',seq,'logical_time',logical_time,'payload_sha256',payload_hash));
    ELSIF action='conclude' THEN
     ordinal:=ordinal+1;
     maps:=jsonb_set(maps,ARRAY['conclusions',object_id::text],jsonb_build_object('conclusion_id',object_id,
      'case_id',cid,'ordinal',ordinal,'actor_id',actor,'data',op||jsonb_build_object('human_support','[]'::jsonb),
      'based_on_seq',(op->>'based_on_seq')::bigint,'event_seq',seq,'logical_time',logical_time,'payload_sha256',payload_hash));
     IF op->'make_current'='true'::jsonb THEN head:=head||jsonb_build_object('revision',(head->>'revision')::bigint+1,
      'current_conclusion_id',object_id);END IF;
    END IF;
    IF action IN ('create','revise','state','grant') OR action='conclude' AND op->'make_current'='true'::jsonb THEN
     maps:=jsonb_set(maps,ARRAY['versions',head->>'revision'],jsonb_build_object('case_id',cid,
      'revision',(head->>'revision')::bigint,'state',head->>'state','current_conclusion_id',head->'current_conclusion_id',
      'data',head->'data','actor_id',actor,'logical_time',logical_time,'payload_sha256',payload_hash));
    END IF;
    maps:=jsonb_set(maps,ARRAY['events',seq::text],jsonb_build_object('case_id',cid,'seq',seq,'actor_id',actor,
     'kind',action,'object_id',coalesce(contribution,object_id),'version',version,'data',
     CASE WHEN action='grant' THEN op ELSE jsonb_build_object('object_id',coalesce(contribution,object_id),'version',version) END,
     'logical_time',logical_time,'payload_sha256',payload_hash));
   END IF;
   maps:=jsonb_set(maps,ARRAY['receipts',receipt.operation_key],jsonb_build_object('operation_key',receipt.operation_key,
    'case_id',cid,'object_id',object_id,'action',action,'sequence',receipt.sequence,'person_id',actor::text,
    'request_id',c->>'request_id','command_sha256',receipt.command_sha256,'payload_sha256',payload_hash,
    'result',convert_from(decode(convert_from(receipt.outcome_bytes,'UTF8')::jsonb->>'result_b64','base64'),'UTF8')::jsonb));
  END LOOP;
  SELECT to_jsonb(h) INTO actual FROM rkb_v4.disagreement_cases h WHERE h.case_id=cid;
  IF actual IS DISTINCT FROM head THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.revision::text,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_case_versions v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'versions' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(g.grantee_id::text,to_jsonb(g)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_grants g WHERE g.case_id=cid;
  IF actual IS DISTINCT FROM maps->'grants' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(e.evidence_id::text||':'||e.version,to_jsonb(e)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_evidence e WHERE e.case_id=cid;
  IF actual IS DISTINCT FROM maps->'evidence' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.contribution_id::text||':'||v.version,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_contributions v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'contributions' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.conclusion_id::text,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_conclusions v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'conclusions' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.seq::text,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_events v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'events' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.draft_id::text,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_drafts v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'drafts' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.draft_id::text||':'||v.version,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_draft_versions v JOIN rkb_v4.disagreement_drafts d USING(draft_id) WHERE d.case_id=cid;
  IF actual IS DISTINCT FROM maps->'draft_versions' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.draft_id::text||':'||v.version||':'||v.contribution_id,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_draft_publications v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'publications' THEN RETURN false;END IF;
  SELECT coalesce(jsonb_object_agg(v.operation_key,to_jsonb(v)),'{}'::jsonb) INTO actual FROM rkb_v4.disagreement_receipts v WHERE v.case_id=cid;
  IF actual IS DISTINCT FROM maps->'receipts' THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;
REVOKE ALL ON FUNCTION rkb_v4.verify_disagreement_origins_v1(uuid[]) FROM PUBLIC;
