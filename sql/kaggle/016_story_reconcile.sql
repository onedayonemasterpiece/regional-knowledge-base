-- UNACTIVATED SOURCE CANDIDATE: 016_story_reconcile.sql.
-- Ordered predecessor: book 013/014 and media 015. The runtime owner must
-- approve exact preceding artifacts, their source histories and transitions.
-- This file has NOT been applied to Kaggle or any local PostgreSQL server.
-- It adds only a reconciliation-owned PG cursor and accepted-command effect;
-- it does NOT edit immutable source assertions, original files, variants,
-- human contributions, Case CRUD, recovery_state, or the global registry.

CREATE TABLE rkb_v4.story_reconciliation_cursor (
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources(source_id),
 source_revision bigint NOT NULL CHECK(source_revision>=1),
 source_frontier_id uuid NOT NULL UNIQUE REFERENCES rkb_v4.source_research_frontier(frontier_id),
 reconciliation_state text NOT NULL DEFAULT 'open' CHECK(reconciliation_state IN ('open','complete')),
 comparison_cursor text,
 progress_version bigint NOT NULL DEFAULT 1 CHECK(progress_version>0),
 last_applied_sequence bigint CHECK(last_applied_sequence>0),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);

-- Source-pass complete is never interpreted as cross-book complete.
INSERT INTO rkb_v4.story_reconciliation_cursor(source_id,source_revision,source_frontier_id)
 SELECT f.source_id,f.revision,f.frontier_id FROM rkb_v4.source_research_frontier f
 JOIN rkb_v4.active_revision a ON a.source_id=f.source_id AND a.revision=f.revision
 ON CONFLICT(source_id) DO NOTHING;

CREATE FUNCTION rkb_v4.seed_story_reconciliation_cursor_v1() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 INSERT INTO rkb_v4.story_reconciliation_cursor(source_id,source_revision,source_frontier_id)
 VALUES(NEW.source_id,NEW.revision,NEW.frontier_id)
 ON CONFLICT(source_id) DO NOTHING;
 RETURN NULL;
END $$;
CREATE TRIGGER seed_story_reconciliation AFTER INSERT ON rkb_v4.source_research_frontier
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.seed_story_reconciliation_cursor_v1();

CREATE TABLE rkb_v4.story_reconciliation_intents (
 operation_key text PRIMARY KEY CHECK(operation_key ~ '^story-reconcile:[0-9a-f]{64}$'),
 actor_id uuid NOT NULL,
 source_id uuid NOT NULL REFERENCES rkb_v4.sources(source_id),
 source_revision bigint NOT NULL CHECK(source_revision>=1),
 source_eligibility_revision bigint NOT NULL CHECK(source_eligibility_revision>=1),
 source_frontier_id uuid NOT NULL REFERENCES rkb_v4.source_research_frontier(frontier_id),
 peer_source_id uuid NOT NULL REFERENCES rkb_v4.sources(source_id),
 peer_source_revision bigint NOT NULL CHECK(peer_source_revision>=1),
 peer_eligibility_revision bigint NOT NULL CHECK(peer_eligibility_revision>=1),
 assertion_id uuid NOT NULL,
 peer_assertion_id uuid NOT NULL,
 comparison_key text NOT NULL CHECK(length(comparison_key) BETWEEN 1 AND 128),
 request_sha256 text NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
 intent_sha256 text NOT NULL CHECK(intent_sha256 ~ '^[0-9a-f]{64}$'),
 intent_bytes bytea NOT NULL CHECK(octet_length(intent_bytes) BETWEEN 1 AND 196608),
 immutable_request jsonb NOT NULL CHECK(jsonb_typeof(immutable_request)='object'),
 current_step integer NOT NULL DEFAULT 0 CHECK(current_step>=0),
 attempted boolean NOT NULL DEFAULT false,
 case_id uuid REFERENCES rkb_v4.disagreement_cases(case_id),
 done boolean NOT NULL DEFAULT false,
 rejected boolean NOT NULL DEFAULT false,
 rejected_step integer CHECK(rejected_step>=0),
 progress_version bigint NOT NULL DEFAULT 1 CHECK(progress_version>0),
 CHECK(source_id<>peer_source_id),
 CHECK(NOT(done AND rejected)),
 CHECK((rejected AND rejected_step=current_step) OR (NOT rejected AND rejected_step IS NULL)),
 CHECK(intent_sha256=encode(sha256(intent_bytes),'hex'))
);
CREATE INDEX story_reconciliation_actor_cursor
 ON rkb_v4.story_reconciliation_intents(actor_id,source_id,operation_key);
-- No human variant, original/source assertion, or Case table is writable here.

-- Stage/transition checks consult current active source/edition/evidence;
-- never trust actor-supplied source IDs, old eligibility, or summary text.
CREATE FUNCTION rkb_v4.story_reconcile_source_current_v1(
 wanted uuid,revision bigint,eligibility bigint,frontier uuid,
 assertion uuid,original_sha text,page uuid,region uuid,begin_offset integer,excerpt text)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT begin_offset>=0 AND char_length(excerpt) BETWEEN 1 AND 8000 AND EXISTS(
  SELECT 1 FROM rkb_v4.sources s
  JOIN rkb_v4.active_revision ar ON ar.source_id=s.source_id
  JOIN rkb_v4.source_visibility v ON v.source_id=s.source_id
  JOIN rkb_v4.source_research_frontier f ON f.source_id=s.source_id AND f.revision=ar.revision
  JOIN rkb_v4.story_reconciliation_cursor c ON c.source_id=s.source_id
   AND c.source_revision=ar.revision AND c.source_frontier_id=f.frontier_id
  JOIN rkb_v4.source_import_origins o ON o.source_id=s.source_id AND o.revision=ar.revision
  JOIN rkb_v4.story_assertion_versions a ON a.source_id=s.source_id AND a.source_revision=ar.revision
   AND a.revision=1 AND a.assertion_id=assertion
  JOIN rkb_v4.source_regions r ON r.source_id=s.source_id AND r.source_revision=ar.revision
   AND r.region_id=region AND r.page_id=page
  WHERE s.source_id=wanted AND s.accepted AND NOT s.withdrawn
   AND ar.revision=revision AND v.eligibility_revision=eligibility AND f.frontier_id=frontier
   AND o.original_sha256=original_sha
   AND EXISTS(SELECT 1 FROM jsonb_array_elements(a.data->'evidence') e
    WHERE e->>'region_id'=region::text AND e->>'original_excerpt'=excerpt
     AND (e->>'start')::integer=begin_offset AND (e->>'end')::integer=begin_offset+char_length(excerpt))
   AND substring(r.source_text FROM begin_offset+1 FOR char_length(excerpt))=excerpt)
$$;

CREATE FUNCTION rkb_v4.story_reconcile_intent_current_v1(doc jsonb,actor uuid)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE a jsonb; b jsonb; cmp jsonb; al jsonb; bl jsonb; ap jsonb; bp jsonb;
BEGIN
 IF jsonb_typeof(doc) IS DISTINCT FROM 'object'
  OR NOT doc ?& ARRAY['actor_id','operation_key','request_sha256','anchor_source','peer_source','comparison']
  OR doc->>'actor_id' IS DISTINCT FROM actor::text THEN RETURN false; END IF;
 a:=doc->'anchor_source';b:=doc->'peer_source';cmp:=doc->'comparison';
 al:=cmp->'anchor';bl:=cmp->'peer';
 IF a->>'source_id' IS NULL OR b->>'source_id' IS NULL OR a->>'source_id'=b->>'source_id'
  OR jsonb_array_length(a->'proofs')<>1 OR jsonb_array_length(b->'proofs')<>1
  OR al->>'source_id' IS DISTINCT FROM a->>'source_id'
  OR bl->>'source_id' IS DISTINCT FROM b->>'source_id'
  OR (al->>'source_revision')::bigint<>(a->>'source_revision')::bigint
  OR (bl->>'source_revision')::bigint<>(b->>'source_revision')::bigint
  OR (al->>'eligibility_revision')::bigint<>(a->>'eligibility_revision')::bigint
  OR (bl->>'eligibility_revision')::bigint<>(b->>'eligibility_revision')::bigint
  OR (al->>'assertion_id') IS DISTINCT FROM a#>>'{proofs,0,assertion_id}'
  OR (bl->>'assertion_id') IS DISTINCT FROM b#>>'{proofs,0,assertion_id}'
  OR a->'accepted' IS DISTINCT FROM 'true'::jsonb OR b->'accepted' IS DISTINCT FROM 'true'::jsonb
  OR a->'withdrawn' IS DISTINCT FROM 'false'::jsonb OR b->'withdrawn' IS DISTINCT FROM 'false'::jsonb
  THEN RETURN false;END IF;
 ap:=a#>'{proofs,0}';bp:=b#>'{proofs,0}';
 IF NOT rkb_v4.story_reconcile_source_current_v1(
    (a->>'source_id')::uuid,(a->>'source_revision')::bigint,(a->>'eligibility_revision')::bigint,
    (a->>'frontier_id')::uuid,(ap->>'assertion_id')::uuid,ap->>'source_sha256',
    (ap->>'page_id')::uuid,(ap->>'region_id')::uuid,(ap->>'text_start')::integer,ap->>'quote')
  OR NOT rkb_v4.story_reconcile_source_current_v1(
    (b->>'source_id')::uuid,(b->>'source_revision')::bigint,(b->>'eligibility_revision')::bigint,
    (b->>'frontier_id')::uuid,(bp->>'assertion_id')::uuid,bp->>'source_sha256',
    (bp->>'page_id')::uuid,(bp->>'region_id')::uuid,(bp->>'text_start')::integer,bp->>'quote')
  THEN RETURN false; END IF;
 IF (cmp->'case_plan'->>'action')='add_evidence' AND
    (coalesce(rkb_v4.case_role_v1((cmp->'case_plan'->>'case_id')::uuid,actor),0)<1
      OR rkb_v4.case_sources_v1((cmp->'case_plan'->>'case_id')::uuid) IS NULL) THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

-- Stable native Case operation is the CURRENT actor's existing Case backend
-- key around frozen reconciliation child _effect_key(parent,index).
-- The same immutable Case command body must match the model-authored plan.
CREATE FUNCTION rkb_v4.story_reconcile_case_receipt_v1(
 parent text,actor uuid,step integer,expected_status text,expected_case uuid,plan jsonb)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE child text; native_key text; recorded rkb_v4.applied_ops; command jsonb; body jsonb;
 expected_action text; expected_command jsonb; actual_case uuid; receipt rkb_v4.disagreement_receipts;
BEGIN
 IF parent !~ '^story-reconcile:[0-9a-f]{64}$' OR step<0
    OR expected_status NOT IN ('applied','rejected_precondition') THEN RETURN false;END IF;
 child:='reconcile-case:'||encode(sha256(convert_to('["'||parent||'",'||step::text||']','UTF8')),'hex');
 native_key:='case-op:'||actor::text||':'||encode(sha256(convert_to(child,'UTF8')),'hex');
 SELECT * INTO recorded FROM rkb_v4.applied_ops WHERE operation_key=native_key;
 IF recorded.operation_key IS NULL OR recorded.status<>expected_status THEN RETURN false;END IF;
 command:=convert_from(recorded.command_bytes,'UTF8')::jsonb;
 body:=convert_from(decode(command->>'payload_b64','base64'),'UTF8')::jsonb;
 expected_action:=CASE WHEN plan->>'action'='create' AND step=0 THEN 'create' ELSE 'add_evidence' END;
 expected_command:=CASE WHEN expected_action='create' THEN plan->'create'
    ELSE plan->'evidence'->(step-CASE WHEN plan->>'action'='create' THEN 1 ELSE 0 END) END;
 IF expected_command IS NULL OR body->>'action' IS DISTINCT FROM expected_action
  OR body->'command' IS DISTINCT FROM expected_command
  OR command->>'person_id' IS DISTINCT FROM actor::text
  OR command->>'route' IS DISTINCT FROM 'disagreement.edit.v1'
  OR body->>'codec' IS DISTINCT FROM 'rkb.disagreement-command.v1' THEN RETURN false;END IF;
 actual_case:=(command->'ids'->>'case_id')::uuid;
 IF expected_case IS NOT NULL AND expected_case<>actual_case THEN RETURN false;END IF;
 IF expected_status='applied' THEN
  SELECT * INTO receipt FROM rkb_v4.disagreement_receipts r WHERE r.operation_key=native_key;
  IF receipt.operation_key IS NULL OR receipt.action<>expected_action
   OR receipt.person_id IS DISTINCT FROM actor::text OR receipt.case_id<>actual_case
   OR receipt.result->>'case_id' IS DISTINCT FROM actual_case::text THEN RETURN false;END IF;
 END IF;
 RETURN true;
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_story_reconcile_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb; encoded bytea; doc jsonb; row rkb_v4.story_reconciliation_intents;
 actor uuid; action text; ver bigint; step integer; total integer; plan jsonb; wanted_case uuid;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 262144
    OR jsonb_typeof(ids) IS DISTINCT FROM 'object'
    OR ids-ARRAY['actor_id','source_id','peer_source_id']<>'{}'::jsonb
    OR NOT ids ?& ARRAY['actor_id','source_id','peer_source_id']
    OR jsonb_typeof(expected) IS DISTINCT FROM 'object'
    OR expected-ARRAY['progress_version','step_index']<>'{}'::jsonb
    OR NOT expected ?& ARRAY['progress_version','step_index'] THEN RETURN false;END IF;
 d:=convert_from(p,'UTF8')::jsonb;action:=d->>'action';actor:=(ids->>'actor_id')::uuid;
 IF d->>'codec' IS DISTINCT FROM 'rkb.story-reconcile-command.v1'
    OR action NOT IN ('stage','begin','advance','reject','finish')
    OR d-ARRAY['codec','action','parent_operation_key','request_sha256','intent_sha256','intent_b64','case_id','case_receipt_key']<>'{}'::jsonb
    OR NOT d ?& ARRAY['codec','action','parent_operation_key','request_sha256','intent_sha256','intent_b64','case_id','case_receipt_key']
    OR d->>'parent_operation_key' !~ '^story-reconcile:[0-9a-f]{64}$'
    OR d->>'request_sha256' !~ '^[0-9a-f]{64}$'
    OR d->>'intent_sha256' !~ '^[0-9a-f]{64}$'
    OR (expected->>'progress_version') !~ '^(0|[1-9][0-9]{0,18})$'
    OR (expected->>'step_index') !~ '^(0|[1-9][0-9]{0,9})$' THEN RETURN false;END IF;
 ver:=(expected->>'progress_version')::bigint;step:=(expected->>'step_index')::integer;
 IF action='stage' THEN
  IF ver<>0 OR step<>0 OR d->'case_id'<>'null'::jsonb OR d->'case_receipt_key'<>'null'::jsonb
   OR jsonb_typeof(d->'intent_b64') IS DISTINCT FROM 'string' THEN RETURN false;END IF;
  encoded:=decode(d->>'intent_b64','base64');
  IF octet_length(encoded) NOT BETWEEN 1 AND 196608
   OR encode(sha256(encoded),'hex')<>d->>'intent_sha256'
   OR replace(encode(encoded,'base64'), E'\n','')<>d->>'intent_b64' THEN RETURN false;END IF;
  doc:=convert_from(encoded,'UTF8')::jsonb;
  IF doc->>'actor_id' IS DISTINCT FROM actor::text
   OR doc->>'operation_key' IS DISTINCT FROM d->>'parent_operation_key'
   OR doc->>'request_sha256' IS DISTINCT FROM d->>'request_sha256'
   OR doc#>>'{anchor_source,source_id}' IS DISTINCT FROM ids->>'source_id'
   OR doc#>>'{peer_source,source_id}' IS DISTINCT FROM ids->>'peer_source_id'
   OR EXISTS(SELECT 1 FROM rkb_v4.story_reconciliation_intents
       WHERE operation_key=d->>'parent_operation_key')
   OR NOT rkb_v4.story_reconcile_intent_current_v1(doc,actor) THEN RETURN false;END IF;
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.story_reconciliation_cursor c
   WHERE c.source_id=(ids->>'source_id')::uuid AND c.reconciliation_state='open') THEN RETURN false;END IF;
  RETURN true;
 END IF;
 IF d->'intent_b64'<>'null'::jsonb OR ver<1 THEN RETURN false;END IF;
 SELECT * INTO row FROM rkb_v4.story_reconciliation_intents
  WHERE operation_key=d->>'parent_operation_key' AND actor_id=actor;
 IF row.operation_key IS NULL OR row.source_id<>(ids->>'source_id')::uuid
    OR row.peer_source_id<>(ids->>'peer_source_id')::uuid
    OR row.progress_version<>ver OR row.current_step<>step
    OR row.request_sha256 IS DISTINCT FROM d->>'request_sha256'
    OR row.intent_sha256 IS DISTINCT FROM d->>'intent_sha256'
    OR row.done OR row.rejected
    OR NOT rkb_v4.story_reconcile_intent_current_v1(row.immutable_request,actor) THEN RETURN false;END IF;
 plan:=row.immutable_request#>'{comparison,case_plan}';
 total:=jsonb_array_length(plan->'evidence')+CASE WHEN plan->>'action'='create' THEN 1 ELSE 0 END;
 wanted_case:=coalesce(row.case_id, NULLIF(plan->>'case_id','')::uuid);
 IF wanted_case IS NOT NULL AND (coalesce(rkb_v4.case_role_v1(wanted_case,actor),0)<1
   OR rkb_v4.case_sources_v1(wanted_case) IS NULL) THEN RETURN false;END IF;
 IF action='begin' THEN RETURN NOT row.attempted AND step<total
    AND d->'case_id'='null'::jsonb AND d->'case_receipt_key'='null'::jsonb;END IF;
 IF action='finish' THEN RETURN NOT row.attempted AND step=total
    AND d->'case_id'='null'::jsonb AND d->'case_receipt_key'='null'::jsonb;END IF;
 IF NOT row.attempted OR step>=total THEN RETURN false;END IF;
 IF d->>'case_receipt_key' IS DISTINCT FROM ('case-op:'||actor::text||':'||
     encode(sha256(convert_to('reconcile-case:'||encode(sha256(convert_to('["'||row.operation_key||'",'||step::text||']','UTF8')),'hex'),'UTF8')),'hex'))
    THEN RETURN false;END IF;
 IF action='advance' THEN
  IF d->>'case_id' IS NULL THEN RETURN false;END IF;
  RETURN rkb_v4.story_reconcile_case_receipt_v1(row.operation_key,actor,step,'applied',
    (d->>'case_id')::uuid,plan);
 END IF;
 IF action='reject' THEN
  RETURN d->'case_id'='null'::jsonb
   AND rkb_v4.story_reconcile_case_receipt_v1(row.operation_key,actor,step,'rejected_precondition',
      wanted_case,plan);
 END IF;
 RETURN false;
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_story_reconcile_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb:=convert_from(p,'UTF8')::jsonb; doc jsonb; act text:=d->>'action';
 actor uuid:=(ids->>'actor_id')::uuid; row rkb_v4.story_reconciliation_intents;
 ver bigint:=(expected->>'progress_version')::bigint; step integer:=(expected->>'step_index')::integer;
BEGIN
 -- Same singleton writer row held by existing apply_accepted. Never run as
 -- free SQL: the controller checks exact journal acceptance and function SHA.
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 SELECT * INTO row FROM rkb_v4.story_reconciliation_intents
    WHERE operation_key=d->>'parent_operation_key' FOR UPDATE;
 IF NOT rkb_v4.check_story_reconcile_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'reconciliation accepted-command CAS precondition differs';
 END IF;
 IF act='stage' THEN
  doc:=convert_from(decode(d->>'intent_b64','base64'),'UTF8')::jsonb;
  INSERT INTO rkb_v4.story_reconciliation_intents
   (operation_key,actor_id,source_id,source_revision,source_eligibility_revision,source_frontier_id,
    peer_source_id,peer_source_revision,peer_eligibility_revision,assertion_id,peer_assertion_id,
    comparison_key,request_sha256,intent_sha256,intent_bytes,immutable_request)
   VALUES(d->>'parent_operation_key',actor,(ids->>'source_id')::uuid,
    (doc#>>'{anchor_source,source_revision}')::bigint,(doc#>>'{anchor_source,eligibility_revision}')::bigint,
    (doc#>>'{anchor_source,frontier_id}')::uuid,(ids->>'peer_source_id')::uuid,
    (doc#>>'{peer_source,source_revision}')::bigint,(doc#>>'{peer_source,eligibility_revision}')::bigint,
    (doc#>>'{comparison,anchor,assertion_id}')::uuid,(doc#>>'{comparison,peer,assertion_id}')::uuid,
    doc#>>'{comparison,comparison_key}',d->>'request_sha256',d->>'intent_sha256',
    decode(d->>'intent_b64','base64'),doc);
 ELSE
  IF act='begin' THEN
   UPDATE rkb_v4.story_reconciliation_intents SET attempted=true,progress_version=progress_version+1
    WHERE operation_key=row.operation_key;
  ELSIF act='advance' THEN
   UPDATE rkb_v4.story_reconciliation_intents
    SET current_step=current_step+1,attempted=false,case_id=(d->>'case_id')::uuid,
        progress_version=progress_version+1 WHERE operation_key=row.operation_key;
  ELSIF act='reject' THEN
   UPDATE rkb_v4.story_reconciliation_intents SET rejected=true,rejected_step=step,
     progress_version=progress_version+1 WHERE operation_key=row.operation_key;
  ELSE
   UPDATE rkb_v4.story_reconciliation_intents SET done=true,progress_version=progress_version+1
    WHERE operation_key=row.operation_key;
  END IF;
 END IF;
 SELECT * INTO STRICT row FROM rkb_v4.story_reconciliation_intents
   WHERE operation_key=d->>'parent_operation_key';
 RETURN convert_to(jsonb_build_object('parent_operation_key',row.operation_key,
  'actor_id',row.actor_id,'request_sha256',row.request_sha256,'progress_version',row.progress_version,
  'step_index',row.current_step,'attempted',row.attempted,'case_id',row.case_id,
  'done',row.done,'rejected',row.rejected,'rejected_step',row.rejected_step)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.read_story_reconciliation_v1(wanted text,actor uuid)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE row rkb_v4.story_reconciliation_intents; plan jsonb; effective_case uuid;
BEGIN
 SELECT * INTO row FROM rkb_v4.story_reconciliation_intents
  WHERE operation_key=wanted AND actor_id=actor;
 IF row.operation_key IS NULL OR NOT rkb_v4.story_reconcile_intent_current_v1(row.immutable_request,actor)
  THEN RETURN NULL;END IF;
 plan:=row.immutable_request#>'{comparison,case_plan}';
 effective_case:=coalesce(row.case_id,NULLIF(plan->>'case_id','')::uuid);
 IF effective_case IS NOT NULL AND (coalesce(rkb_v4.case_role_v1(effective_case,actor),0)<1
  OR rkb_v4.case_sources_v1(effective_case) IS NULL) THEN RETURN NULL;END IF;
 RETURN jsonb_build_object('operation_key',row.operation_key,'actor_id',row.actor_id,
  'intent_b64',replace(encode(row.intent_bytes,'base64'), E'\n',''),'request_sha256',row.request_sha256,
  'intent_sha256',row.intent_sha256,'version',row.progress_version,'step_index',row.current_step,
  'attempted',row.attempted,'case_id',row.case_id,'done',row.done,
  'rejected',row.rejected,'rejected_step',row.rejected_step);
END $$;

-- Existing writer role receives only this domain effect. Reader receives
-- selected source-frontier identity and the two new reconciliation tables;
-- no Case draft, private original bytes, unrestricted source archive grants.
REVOKE ALL ON rkb_v4.story_reconciliation_cursor,rkb_v4.story_reconciliation_intents FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.seed_story_reconciliation_cursor_v1(),
 rkb_v4.story_reconcile_source_current_v1(uuid,bigint,bigint,uuid,uuid,text,uuid,uuid,integer,text),
 rkb_v4.story_reconcile_intent_current_v1(jsonb,uuid),
 rkb_v4.story_reconcile_case_receipt_v1(text,uuid,integer,text,uuid,jsonb),
 rkb_v4.check_story_reconcile_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_story_reconcile_v1(bytea,jsonb,jsonb,text),
 rkb_v4.read_story_reconciliation_v1(text,uuid) FROM PUBLIC;
GRANT SELECT,INSERT,UPDATE ON rkb_v4.story_reconciliation_cursor,
 rkb_v4.story_reconciliation_intents TO rkb_writer;
GRANT EXECUTE ON FUNCTION rkb_v4.seed_story_reconciliation_cursor_v1(),
 rkb_v4.story_reconcile_source_current_v1(uuid,bigint,bigint,uuid,uuid,text,uuid,uuid,integer,text),
 rkb_v4.story_reconcile_intent_current_v1(jsonb,uuid),
 rkb_v4.story_reconcile_case_receipt_v1(text,uuid,integer,text,uuid,jsonb),
 rkb_v4.check_story_reconcile_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_story_reconcile_v1(bytea,jsonb,jsonb,text),
 rkb_v4.read_story_reconciliation_v1(text,uuid) TO rkb_writer;
GRANT SELECT(source_id,revision,frontier_id,state)
 ON rkb_v4.source_research_frontier TO rkb_reader;
GRANT SELECT ON rkb_v4.story_reconciliation_cursor,rkb_v4.story_reconciliation_intents TO rkb_reader;
GRANT EXECUTE ON FUNCTION
 rkb_v4.story_reconcile_source_current_v1(uuid,bigint,bigint,uuid,uuid,text,uuid,uuid,integer,text),
 rkb_v4.story_reconcile_intent_current_v1(jsonb,uuid),
 rkb_v4.read_story_reconciliation_v1(text,uuid) TO rkb_reader;
