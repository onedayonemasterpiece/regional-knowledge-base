-- Ordered native title-only SetMetadata; imported metadata and variant/vector bytes stay immutable.
ALTER TABLE rkb_v4.editorial_story_versions DROP CONSTRAINT editorial_story_versions_action_check;
ALTER TABLE rkb_v4.editorial_story_versions ADD CONSTRAINT editorial_story_versions_action_check
 CHECK(action IN ('source_import','upsert_variant','access_change','set_metadata'));

CREATE FUNCTION rkb_v4.check_story_title_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;op jsonb;wanted uuid;actor uuid;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 4096 OR length(logical_time) NOT BETWEEN 1 AND 256
 OR jsonb_typeof(ids) IS DISTINCT FROM 'object' OR ids-ARRAY['source_id','story_id','actor_id']<>'{}'::jsonb
 OR NOT ids ?& ARRAY['source_id','story_id','actor_id']
 OR jsonb_typeof(expected) IS DISTINCT FROM 'object'
 OR expected-ARRAY['story_revision','source_revision','eligibility_revision']<>'{}'::jsonb
 OR NOT expected ?& ARRAY['story_revision','source_revision','eligibility_revision'] THEN RETURN false;END IF;
 d:=convert_from(p,'UTF8')::jsonb;op:=d->'operation';
 IF jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['codec','operation','reason']<>'{}'::jsonb
 OR NOT d ?& ARRAY['codec','operation','reason'] OR d->>'codec' IS DISTINCT FROM 'rkb.story-title.v1'
 OR jsonb_typeof(op) IS DISTINCT FROM 'object' OR op-ARRAY['op','title']<>'{}'::jsonb
 OR NOT op ?& ARRAY['op','title'] OR op->>'op' IS DISTINCT FROM 'set_metadata'
 OR jsonb_typeof(op->'title') IS DISTINCT FROM 'string' OR length(op->>'title') NOT BETWEEN 1 AND 250
 OR btrim(op->>'title')='' OR (d->'reason'<>'null'::jsonb AND
   (jsonb_typeof(d->'reason') IS DISTINCT FROM 'string' OR length(d->>'reason')>3000))
 THEN RETURN false;END IF;
 wanted:=(ids->>'story_id')::uuid;actor:=(ids->>'actor_id')::uuid;
 RETURN rkb_v4.story_editor_role_v1(wanted,actor) IN ('editor','publisher','manager')
 AND rkb_v4.editorial_source_current_v1(wanted,(ids->>'source_id')::uuid,
     (expected->>'source_revision')::bigint,(expected->>'eligibility_revision')::bigint)
 AND EXISTS(SELECT 1 FROM rkb_v4.editorial_story_heads
     WHERE story_id=wanted AND revision=(expected->>'story_revision')::bigint);
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_story_title_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb:=convert_from(p,'UTF8')::jsonb;wanted uuid:=(ids->>'story_id')::uuid;
 head rkb_v4.editorial_story_heads;previous jsonb;state rkb_v4.recovery_state;prior rkb_v4.read_frontier;
BEGIN
 IF NOT rkb_v4.check_story_title_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'Story title precondition differs';END IF;
 SELECT * INTO STRICT head FROM rkb_v4.editorial_story_heads WHERE story_id=wanted FOR UPDATE;
 SELECT data INTO STRICT previous FROM rkb_v4.editorial_story_versions
  WHERE story_id=wanted AND revision=head.revision;
 UPDATE rkb_v4.editorial_story_heads SET revision=head.revision+1 WHERE story_id=wanted;
 INSERT INTO rkb_v4.editorial_story_versions VALUES(wanted,head.revision+1,
  previous||jsonb_build_object('title',d->'operation'->>'title'),
  (ids->>'actor_id')::uuid,logical_time,'set_metadata');
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton;
 SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
  prior.visibility_epoch,state.applied_seq+1,
  prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
  prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
 RETURN convert_to(jsonb_build_object('story_id',wanted,'revision',head.revision+1,
  'title',d->'operation'->>'title')::text,'UTF8');
END $$;

-- Existing immutable receipt table gains a title-specific trigger; old trigger remains untouched.
CREATE FUNCTION rkb_v4.capture_story_title_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route'<>'story.title.v1' THEN RETURN NULL;END IF;
 INSERT INTO rkb_v4.editorial_story_receipts VALUES(
  (c->'ids'->>'story_id')::uuid,(c->'expected_revisions'->>'story_revision')::bigint+1,
  NEW.operation_key,NEW.sequence,(c->>'person_id')::uuid,c->>'request_id',NEW.command_sha256);
 RETURN NULL;
END $$;
CREATE TRIGGER capture_story_title_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_story_title_receipt_v1();

-- Keep the existing reader identity for all source/search/Story callers, including historical readback.
CREATE OR REPLACE FUNCTION rkb_v4.read_editorial_story_v1(wanted uuid,wanted_revision bigint,view_name text,after_revision bigint,page_limit integer,after_variant uuid DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE head rkb_v4.editorial_story_heads;version rkb_v4.editorial_story_versions;
 variant rkb_v4.editorial_variant_versions;metadata jsonb;source rkb_v4.sources;eligibility bigint;entries jsonb;
 next_revision bigint;next_variant uuid;result jsonb;
BEGIN
 IF view_name NOT IN ('get','history','variants') OR after_revision<0 OR page_limit NOT BETWEEN 1 AND 10
 OR wanted_revision IS NOT NULL AND wanted_revision<1 THEN RAISE EXCEPTION 'bounded editorial read required';END IF;
 SELECT h.* INTO head FROM rkb_v4.editorial_story_heads h JOIN rkb_v4.sources s ON s.source_id=h.source_id
 JOIN rkb_v4.active_revision a ON a.source_id=h.source_id AND a.revision=h.source_revision
 WHERE h.story_id=wanted AND s.accepted AND NOT s.withdrawn;
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO version FROM rkb_v4.editorial_story_versions WHERE story_id=wanted AND revision=coalesce(wanted_revision,head.revision);
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT data INTO STRICT metadata FROM rkb_v4.story_versions WHERE story_id=wanted AND revision=1;
 SELECT * INTO STRICT variant FROM rkb_v4.editorial_variant_versions WHERE story_id=wanted
 AND variant_id=(version.data->'default_variant_ref'->>'variant_id')::uuid
 AND revision=(version.data->'default_variant_ref'->>'revision')::bigint;
 SELECT * INTO STRICT source FROM rkb_v4.sources WHERE source_id=head.source_id;
 SELECT eligibility_revision INTO STRICT eligibility FROM rkb_v4.source_visibility WHERE source_id=head.source_id;
 IF view_name='history' THEN
  SELECT coalesce(jsonb_agg(row ORDER BY revision),'[]'::jsonb) INTO entries FROM(
   SELECT v.revision,jsonb_build_object('revision',v.revision,'action',v.action,'logical_time',v.logical_time,
    'actor_id',r.person_id,'request_id',r.request_id,'operation_key',r.operation_key,'sequence',r.sequence,
    'command_sha256',r.command_sha256) row FROM rkb_v4.editorial_story_versions v
   JOIN rkb_v4.editorial_story_receipts r USING(story_id,revision)
   WHERE v.story_id=wanted AND v.revision>after_revision ORDER BY v.revision LIMIT page_limit) bounded;
  IF EXISTS(SELECT 1 FROM rkb_v4.editorial_story_versions WHERE story_id=wanted
   AND revision>coalesce((entries->-1->>'revision')::bigint,after_revision)) THEN
   next_revision:=(entries->-1->>'revision')::bigint;END IF;
 ELSIF view_name='variants' THEN
  SELECT coalesce(jsonb_agg(row ORDER BY variant_id),'[]'::jsonb) INTO entries FROM(
   SELECT b.variant_id,b.data||jsonb_build_object('body_sha256',b.body_sha256) row
   FROM jsonb_array_elements(version.data->'variants') ref
   JOIN rkb_v4.editorial_variant_versions b ON b.story_id=wanted AND b.variant_id=(ref->>'variant_id')::uuid
    AND b.revision=(ref->>'revision')::bigint
   WHERE b.variant_id>coalesce(after_variant,'00000000-0000-0000-0000-000000000000'::uuid)
   ORDER BY b.variant_id LIMIT page_limit) bounded;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(version.data->'variants') ref
   WHERE (ref->>'variant_id')::uuid>coalesce((entries->-1->>'variant_id')::uuid,after_variant,
    '00000000-0000-0000-0000-000000000000'::uuid)) THEN
   next_variant:=(entries->-1->>'variant_id')::uuid;END IF;
 ELSE entries:='[]'::jsonb;END IF;
 result:=jsonb_build_object('story_id',wanted,'revision',version.revision,'current_revision',head.revision,
  'title',coalesce((SELECT edited.data->>'title' FROM rkb_v4.editorial_story_versions edited
   WHERE edited.story_id=wanted AND edited.revision<=version.revision AND edited.action='set_metadata'
   ORDER BY edited.revision DESC LIMIT 1),metadata->>'title'),'summary',metadata->>'summary','material_type',metadata->>'material_type',
  'body',variant.body,'body_sha256',variant.body_sha256,'variant',CASE WHEN view_name='variants' THEN NULL ELSE variant.data END,
  'default_variant_ref',version.data->'default_variant_ref','state',version.data->>'state',
  'narrative_availability','available','visibility','shared','publication_approval',NULL,
  'bibliography',source.bibliography,'source',jsonb_build_object('source_id',head.source_id,
   'content_revision',head.source_revision,'eligibility_revision',eligibility),
  'finding_ids',metadata->'finding_ids','view',view_name,'entries',entries,'next_revision',next_revision,'next_variant_id',next_variant);
 -- Return a byte-bounded page without duplicating the default metadata in a
 -- variants page. All remaining immutable rows stay reachable by keyset cursor.
 WHILE octet_length(result::text)>196608 AND view_name='variants' AND jsonb_array_length(entries)>1 LOOP
  entries:=entries-(jsonb_array_length(entries)-1);next_variant:=(entries->-1->>'variant_id')::uuid;
  result:=result||jsonb_build_object('entries',entries,'next_variant_id',next_variant);
 END LOOP;
 IF octet_length(result::text)>196608 THEN RAISE EXCEPTION 'bounded editorial response required';END IF;
 RETURN result;
END $$;

-- The existing source/graph/Saved verifier invokes this same v2 function identity.
CREATE OR REPLACE FUNCTION rkb_v4.verify_editorial_story_origins_v2(scope uuid[])
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
    ELSIF c->>'route'='story.title.v1' THEN
     op:=doc->'operation';
     IF version.action<>'set_metadata' OR role IS NULL OR role NOT IN ('editor','publisher','manager')
       OR doc->>'codec' IS DISTINCT FROM 'rkb.story-title.v1'
       OR jsonb_typeof(doc) IS DISTINCT FROM 'object'
       OR doc-ARRAY['codec','operation','reason']<>'{}'::jsonb OR NOT doc ?& ARRAY['codec','operation','reason']
       OR jsonb_typeof(op) IS DISTINCT FROM 'object'
       OR op-ARRAY['op','title']<>'{}'::jsonb OR NOT op ?& ARRAY['op','title']
       OR op->>'op' IS DISTINCT FROM 'set_metadata'
       OR jsonb_typeof(op->'title') IS DISTINCT FROM 'string'
       OR length(op->>'title') NOT BETWEEN 1 AND 250 OR btrim(op->>'title')=''
       OR c->'expected_revisions'-ARRAY['story_revision','source_revision','eligibility_revision']<>'{}'::jsonb
       OR NOT c->'expected_revisions' ?& ARRAY['story_revision','source_revision','eligibility_revision']
     THEN RETURN false;END IF;
     snapshot:=snapshot||jsonb_build_object('title',op->>'title');
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

REVOKE ALL ON FUNCTION rkb_v4.check_story_title_v1(bytea,jsonb,jsonb,text),
 rkb_v4.apply_story_title_v1(bytea,jsonb,jsonb,text),
 rkb_v4.capture_story_title_receipt_v1() FROM PUBLIC;
