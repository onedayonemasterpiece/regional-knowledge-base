-- SQL031 candidate: Case evidence after immutable source packet publication.
-- Legacy source_manifests preserves the first accepted batch, while the
-- existing read_source_manifest_v1 accessor folds later accepted packets.
-- Reuse that verified accessor for the exact same (source, revision, page,
-- region) proof at apply and historical Case replay, without new authority,
-- source writes, ACL grants, changed input contracts, or old DDL edits.
-- Ordinal 031 is tentative: PR372 reserves SQL031; Opus resolves before merge.

CREATE OR REPLACE FUNCTION rkb_v4.apply_disagreement_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
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
    CROSS JOIN LATERAL rkb_v4.read_source_manifest_v1(m.source_id) resolved(manifest)
   CROSS JOIN LATERAL jsonb_array_elements(resolved.manifest->'pages') page(value)
   CROSS JOIN LATERAL jsonb_array_elements(resolved.manifest->'regions') region(value)
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

CREATE OR REPLACE FUNCTION rkb_v4.verify_disagreement_origins_v1(wanted uuid[]) RETURNS boolean
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
  FOR receipt IN SELECT a.operation_key,a.sequence,a.status,a.command_bytes,a.command_sha256,a.outcome_bytes
   FROM rkb_v4.applied_ops a WHERE a.status='applied'
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
    CROSS JOIN LATERAL rkb_v4.read_source_manifest_v1(m.source_id) resolved(manifest)
      CROSS JOIN LATERAL jsonb_array_elements(resolved.manifest->'pages') page(value)
      CROSS JOIN LATERAL jsonb_array_elements(resolved.manifest->'regions') region(value)
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
