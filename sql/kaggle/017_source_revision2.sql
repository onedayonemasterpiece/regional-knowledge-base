-- SQL017: source-owned revision2/rechunk successor, only after pinned SQL015/016.
-- Additive immutable records; SQL001-016/functions/pins are never rewritten.
-- This file is SOURCE ONLY. No live migration, roles, credentials or provider calls.
-- A successor MUST keep the canonical source_id and the exact archived original.
CREATE TABLE rkb_v4.source_revision2_sessions(
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources(source_id),
 target_revision bigint NOT NULL DEFAULT 2 CHECK(target_revision=2),
 prior_revision bigint NOT NULL CHECK(prior_revision=1),
 ingestion_id uuid NOT NULL UNIQUE, origin_ingestion_id uuid NOT NULL,
 owner_person_id uuid NOT NULL,
 original_receipt bytea NOT NULL CHECK(octet_length(original_receipt) BETWEEN 1 AND 16384),
 original_sha256 text NOT NULL CHECK(original_sha256~'^[0-9a-f]{64}$'),
 predecessor_command_sha256 text NOT NULL CHECK(predecessor_command_sha256~'^[0-9a-f]{64}$'),
 source_metadata jsonb NOT NULL,
 start_logical_time text NOT NULL,
 FOREIGN KEY(source_id,prior_revision) REFERENCES rkb_v4.revisions(source_id,revision)
);
CREATE TABLE rkb_v4.source_revision2_batches(
 source_id uuid NOT NULL REFERENCES rkb_v4.source_revision2_sessions,
 target_revision bigint NOT NULL DEFAULT 2 CHECK(target_revision=2),
 batch_index integer NOT NULL CHECK(batch_index BETWEEN 0 AND 2499),
 payload_bytes bytea NOT NULL CHECK(octet_length(payload_bytes) BETWEEN 1 AND 716800),
 payload_sha256 text NOT NULL CHECK(payload_sha256=encode(sha256(payload_bytes),'hex')),
 previous_batch_sha256 text NOT NULL CHECK(previous_batch_sha256~'^[0-9a-f]{64}$'),
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
 PRIMARY KEY(source_id,target_revision,batch_index)
);
CREATE TABLE rkb_v4.source_revision2_seals(
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_revision2_sessions,
 ingestion_id uuid NOT NULL, batch_count integer NOT NULL CHECK(batch_count BETWEEN 1 AND 2500),
 last_batch_sha256 text NOT NULL, seal_bytes bytea NOT NULL, seal_sha256 text NOT NULL,
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE rkb_v4.source_revision2_candidates(
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_revision2_sessions,
 ingestion_id uuid NOT NULL,
 batch_count integer NOT NULL, last_batch_sha256 text NOT NULL,
 materializer_sha256 text NOT NULL CHECK(materializer_sha256~'^[0-9a-f]{64}$'),
 knowledge_bytes bytea NOT NULL CHECK(octet_length(knowledge_bytes) BETWEEN 1 AND 33554432),
 knowledge_sha256 text NOT NULL CHECK(knowledge_sha256=encode(sha256(knowledge_bytes),'hex')),
 knowledge jsonb NOT NULL CHECK(knowledge=convert_from(knowledge_bytes,'UTF8')::jsonb),
 total_inputs integer GENERATED ALWAYS AS
  (jsonb_array_length(knowledge->'passages')+
   jsonb_array_length(knowledge->'semantic'->'stories')) STORED,
 prepared_database text NOT NULL, prepared_writer text NOT NULL
);
CREATE TABLE rkb_v4.source_revision2_vectors(
 source_id uuid NOT NULL REFERENCES rkb_v4.source_revision2_candidates,
 ordinal integer NOT NULL CHECK(ordinal>=0),
 input_id uuid NOT NULL,input_sha256 text NOT NULL,
 embedding public.vector(1024) NOT NULL
  CHECK(abs(public.vector_norm(embedding)-1.0)<=0.002),
 PRIMARY KEY(source_id,ordinal),UNIQUE(source_id,input_id)
);
CREATE TABLE rkb_v4.source_revision2_materialized(
 source_id uuid NOT NULL,ordinal integer NOT NULL,
 knowledge_sha256 text NOT NULL,input_sha256 text NOT NULL,
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
 PRIMARY KEY(source_id,ordinal),
 FOREIGN KEY(source_id,ordinal) REFERENCES rkb_v4.source_revision2_vectors
);
CREATE TABLE rkb_v4.source_revision2_origins(
 source_id uuid NOT NULL REFERENCES rkb_v4.sources,
 revision bigint NOT NULL CHECK(revision=2),
 ingestion_id uuid NOT NULL,origin_ingestion_id uuid NOT NULL,
 predecessor_command_sha256 text NOT NULL,
 original_receipt_sha256 text NOT NULL, original_sha256 text NOT NULL,
 knowledge_sha256 text NOT NULL,
 operation_key text NOT NULL REFERENCES rkb_v4.applied_ops DEFERRABLE INITIALLY DEFERRED,
 accepted_sequence bigint NOT NULL,command_sha256 text NOT NULL,
 owner_person_id uuid NOT NULL, payload_sha256 text NOT NULL,
 PRIMARY KEY(source_id,revision)
);
CREATE TABLE rkb_v4.source_revision2_research_frontier(
 source_id uuid NOT NULL,revision bigint NOT NULL CHECK(revision=2),
 frontier_id uuid NOT NULL UNIQUE,state text NOT NULL CHECK(state IN('pending','complete')),
 continuation jsonb NOT NULL CHECK(jsonb_typeof(continuation)='object'),
 PRIMARY KEY(source_id,revision),
 FOREIGN KEY(source_id,revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.source_revision2_semantic(
 source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
 revision bigint NOT NULL CHECK(revision=2),
 semantic jsonb NOT NULL CHECK(jsonb_typeof(semantic)='object'),
 source_graph jsonb, image_manifest jsonb NOT NULL,
 payload_sha256 text NOT NULL CHECK(payload_sha256~'^[0-9a-f]{64}$')
);
-- The old row, original story bodies and original vectors are NEVER updated.
-- A rejected successor leaves active_revision=1 and every v1 row readable.
DO $$
DECLARE n text;
BEGIN
 FOREACH n IN ARRAY ARRAY[
  'source_revision2_sessions','source_revision2_batches','source_revision2_seals',
  'source_revision2_candidates','source_revision2_vectors','source_revision2_materialized',
  'source_revision2_origins','source_revision2_research_frontier','source_revision2_semantic'] LOOP
  EXECUTE format('CREATE TRIGGER immutable_%I BEFORE UPDATE OR DELETE ON rkb_v4.%I
                 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update()',n,n);
 END LOOP;
END $$;

CREATE FUNCTION rkb_v4.revision2_stage_chain_v1(wanted uuid,ingestion uuid,batches integer,tail text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE b record;previous text:=repeat('0',64);n integer:=0;s rkb_v4.source_revision2_sessions;
BEGIN
 SELECT * INTO s FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 IF NOT FOUND OR s.ingestion_id<>ingestion OR batches NOT BETWEEN 1 AND 2500 THEN RETURN false;END IF;
 FOR b IN SELECT x.*,a.status FROM rkb_v4.source_revision2_batches x
   JOIN rkb_v4.applied_ops a ON a.sequence=x.accepted_sequence
   WHERE x.source_id=wanted ORDER BY x.batch_index LOOP
  IF b.batch_index<>n OR b.status<>'applied'
    OR b.previous_batch_sha256<>previous OR b.payload_sha256<>encode(sha256(b.payload_bytes),'hex') THEN
   RETURN false;END IF;
  previous:=b.payload_sha256;n:=n+1;
 END LOOP;
 RETURN n=batches AND previous=tail;
END $$;

CREATE FUNCTION rkb_v4.check_source_stage_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;src jsonb;old jsonb;wanted uuid;idx integer;prior record;session record;
 h text;previous text;tail record;old_image record;page jsonb;image jsonb;i integer;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 716800 OR octet_length(logical_time) NOT BETWEEN 1 AND 256
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id') THEN RETURN false;END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF wanted IS NULL OR wanted::text<>ids->>'source_id'
  OR d->>'codec'<>'rkb.source-stage-batch.v2'
  OR d->'source_revision'<>'2'::jsonb OR d->'prior_revision'<>'1'::jsonb
  OR NOT d ?& ARRAY['ingestion_id','batch_index','previous_batch_sha256','source','model_payload','issued_images_b64','research','original_ingestion_id','predecessor_command_sha256']
  OR jsonb_typeof(d->'model_payload'->'pages')<>'array'
  OR jsonb_array_length(d->'model_payload'->'pages') NOT BETWEEN 1 AND 8
  OR jsonb_typeof(d->'issued_images_b64')<>'array'
  OR jsonb_array_length(d->'issued_images_b64')<>jsonb_array_length(d->'model_payload'->'pages')
  OR d->>'predecessor_command_sha256' !~ '^[0-9a-f]{64}$'
  OR d->>'previous_batch_sha256' !~ '^[0-9a-f]{64}$'
  OR d->>'ingestion_id' IS NULL OR d->>'original_ingestion_id' IS NULL
  OR (d->>'ingestion_id')::uuid=(d->>'original_ingestion_id')::uuid
  OR d->>'batch_index' !~ '^(0|[1-9][0-9]{0,3})$'
  OR (d->>'batch_index')::integer>2499 THEN RETURN false;END IF;
 idx:=(d->>'batch_index')::integer;previous:=d->>'previous_batch_sha256';
 IF expected IS DISTINCT FROM jsonb_build_object('active_revision','1','stage_head',previous,
       'predecessor_command_sha256',d->>'predecessor_command_sha256') THEN RETURN false;END IF;
 SELECT a.revision,o.original_sha256,o.edition_id,i.command_sha256,i.person_id,i.ingestion_id,
  v.accepted,v.withdrawn INTO prior FROM rkb_v4.active_revision a
  JOIN rkb_v4.sources v USING(source_id)
  JOIN rkb_v4.source_import_origins o ON o.source_id=a.source_id
  JOIN rkb_v4.source_image_origins i ON i.source_id=a.source_id
  WHERE a.source_id=wanted;
 IF NOT FOUND OR prior.revision<>1 OR NOT prior.accepted OR prior.withdrawn
  OR prior.person_id::text IS DISTINCT FROM current_setting('rkb_actor_person_id',true)
  OR prior.command_sha256 IS DISTINCT FROM d->>'predecessor_command_sha256'
  OR prior.ingestion_id::text IS DISTINCT FROM d->>'original_ingestion_id' THEN RETURN false; END IF;
 src:=d->'source';old:=convert_from(decode(src->>'original_receipt_b64','base64'),'UTF8')::jsonb;
 IF old->>'source_id' IS DISTINCT FROM wanted::text OR old->>'ingestion_id'<>d->>'original_ingestion_id'
   OR old->>'sha256' IS DISTINCT FROM prior.original_sha256
   OR src->>'edition_id' IS DISTINCT FROM prior.edition_id::text
   OR old->>'revision'<>'1' OR jsonb_typeof(d->'research')<>'object' THEN RETURN false;END IF;
 IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_stage_sessions o WHERE o.source_id=wanted
  AND o.original_receipt=decode(src->>'original_receipt_b64','base64')) THEN RETURN false;END IF;
 FOR i IN 0..jsonb_array_length(d->'model_payload'->'pages')-1 LOOP
  page:=d->'model_payload'->'pages'->i;
  image:=convert_from(decode(d->'issued_images_b64'->>i,'base64'),'UTF8')::jsonb;
  SELECT * INTO old_image FROM rkb_v4.source_stage_pages z
   WHERE z.source_id=wanted AND z.page_id=(image->>'page_id')::uuid;
  IF NOT FOUND OR old_image.image_receipt IS DISTINCT FROM decode(d->'issued_images_b64'->>i,'base64')
    OR page->>'page_id' IS DISTINCT FROM image->>'page_id'
    OR page->>'source_image_receipt_sha256' IS DISTINCT FROM old_image.image_receipt_sha256
    OR page->>'source_material'<>'visual_reviewed' OR length(page->>'source_review_note') NOT BETWEEN 1 AND 500 THEN
   RETURN false;END IF;
 END LOOP;
 SELECT * INTO session FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 IF FOUND AND (session.ingestion_id::text<>d->>'ingestion_id'
   OR session.owner_person_id::text<>prior.person_id::text
   OR session.original_receipt<>decode(src->>'original_receipt_b64','base64')
   OR session.predecessor_command_sha256<>d->>'predecessor_command_sha256') THEN RETURN false;END IF;
 IF EXISTS(SELECT 1 FROM rkb_v4.source_revision2_seals WHERE source_id=wanted) THEN RETURN false;END IF;
 SELECT * INTO tail FROM rkb_v4.source_revision2_batches WHERE source_id=wanted
  ORDER BY batch_index DESC LIMIT 1;
 RETURN (NOT FOUND AND idx=0 AND previous=repeat('0',64))
  OR (FOUND AND idx=tail.batch_index+1 AND previous=tail.payload_sha256);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_stage_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;src jsonb;wanted uuid;seq bigint;
BEGIN
 SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT FOUND OR NOT rkb_v4.check_source_stage_revision2_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'revision2 source stage precondition rejected';END IF;
 d:=convert_from(p,'UTF8')::jsonb;src:=d->'source';wanted:=(ids->>'source_id')::uuid;
 INSERT INTO rkb_v4.source_revision2_sessions VALUES(wanted,2,1,(d->>'ingestion_id')::uuid,
  (d->>'original_ingestion_id')::uuid,current_setting('rkb_actor_person_id')::uuid,
  decode(src->>'original_receipt_b64','base64'),
  convert_from(decode(src->>'original_receipt_b64','base64'),'UTF8')::jsonb->>'sha256',
  d->>'predecessor_command_sha256',src,logical_time)
 ON CONFLICT(source_id) DO NOTHING;
 INSERT INTO rkb_v4.source_revision2_batches VALUES(wanted,2,(d->>'batch_index')::integer,
  p,encode(sha256(p),'hex'),d->>'previous_batch_sha256',seq);
 RETURN convert_to(jsonb_build_object('source_id',wanted,'source_revision',2,
   'ingestion_id',d->>'ingestion_id','batch_index',d->'batch_index',
   'batch_sha256',encode(sha256(p),'hex'),'candidate_state','staged_inactive')::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.check_source_stage_seal_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;wanted uuid;session rkb_v4.source_revision2_sessions;head record;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 THEN RETURN false;END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF d->>'codec'<>'rkb.source-stage-seal.v2' OR d->'source_revision'<>'2'::jsonb
  OR d->>'batch_count' !~ '^[1-9][0-9]{0,3}$'
  OR d->>'last_batch_sha256' !~ '^[0-9a-f]{64}$' THEN RETURN false;END IF;
 SELECT * INTO session FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 IF NOT FOUND OR session.ingestion_id::text<>d->>'ingestion_id'
  OR session.owner_person_id::text<>current_setting('rkb_actor_person_id',true)
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.active_revision WHERE source_id=wanted AND revision=1)
  OR expected IS DISTINCT FROM jsonb_build_object('active_revision','1','stage_head',d->>'last_batch_sha256',
   'predecessor_command_sha256',session.predecessor_command_sha256)
  OR NOT rkb_v4.revision2_stage_chain_v1(wanted,session.ingestion_id,
   (d->>'batch_count')::integer,d->>'last_batch_sha256') THEN RETURN false;END IF;
 SELECT * INTO head FROM rkb_v4.source_revision2_seals WHERE source_id=wanted;
 RETURN NOT FOUND OR head.seal_bytes=p;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_stage_seal_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;wanted uuid;seq bigint;
BEGIN
 SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_stage_seal_revision2_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'revision2 seal predecessor invalid';END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 INSERT INTO rkb_v4.source_revision2_seals VALUES(wanted,(d->>'ingestion_id')::uuid,
  (d->>'batch_count')::integer,d->>'last_batch_sha256',p,encode(sha256(p),'hex'),seq)
 ON CONFLICT(source_id) DO NOTHING;
 RETURN convert_to(jsonb_build_object('source_id',wanted,'source_revision',2,'ingestion_id',
  d->>'ingestion_id','seal_sha256',encode(sha256(p),'hex'),
  'batch_count',d->'batch_count','last_batch_sha256',d->>'last_batch_sha256',
  'candidate_state','sealed_inactive')::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.revision2_activation_input_v1(wanted uuid,n integer) RETURNS jsonb
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT CASE WHEN n<jsonb_array_length(k.knowledge->'passages') THEN
  jsonb_build_object('input_id',k.knowledge->'passages'->n->'passage_id',
    'body',k.knowledge->'passages'->n->'body') ELSE
  jsonb_build_object('input_id',k.knowledge->'semantic'->'stories'->
    (n-jsonb_array_length(k.knowledge->'passages'))->'default_variant'->'variant_id',
    'body',k.knowledge->'semantic'->'stories'->
    (n-jsonb_array_length(k.knowledge->'passages'))->'default_variant'->'body') END
 FROM rkb_v4.source_revision2_candidates k WHERE k.source_id=wanted
  AND n>=0 AND n<k.total_inputs
$$;

CREATE FUNCTION rkb_v4.prepare_source_revision2_activation_v1(
 wanted uuid,ingestion uuid,batches integer,tail text,materializer text,
 resolved bytea,expected_database text,expected_writer text) RETURNS text
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE db rkb_v4.recovery_state;session rkb_v4.source_revision2_sessions;
 prior rkb_v4.source_revision2_candidates;doc jsonb;hash text;research jsonb;
BEGIN
 IF octet_length(resolved) NOT BETWEEN 1 AND 33554432 OR materializer !~ '^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION 'bounded exact revision2 knowledge required';END IF;
 SELECT * INTO STRICT db FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF db.database_generation<>expected_database OR db.writer_generation<>expected_writer
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.writer_generations WHERE generation=expected_writer AND NOT retired) THEN
  RAISE EXCEPTION 'revision2 candidate writer changed';END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(wanted::text,0));
 SELECT * INTO STRICT session FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 IF session.ingestion_id<>ingestion OR NOT rkb_v4.revision2_stage_chain_v1(wanted,ingestion,batches,tail)
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_revision2_seals
   WHERE source_id=wanted AND batch_count=batches AND last_batch_sha256=tail)
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.active_revision WHERE source_id=wanted AND revision=1) THEN
  RAISE EXCEPTION 'revision2 accepted chain changed';END IF;
 doc:=convert_from(resolved,'UTF8')::jsonb;hash:=encode(sha256(resolved),'hex');
 IF doc->>'codec'<>'rkb.source-activation-projection.v1' OR doc->'revision'<>'2'::jsonb
  OR doc->'original'->>'sha256'<>session.original_sha256
  OR doc->'image_manifest'->>'source_id'<>wanted::text
  OR jsonb_typeof(doc->'passages')<>'array' OR jsonb_array_length(doc->'passages')>100*batches
  OR jsonb_typeof(doc->'semantic'->'stories')<>'array'
  OR jsonb_array_length(doc->'semantic'->'stories')>12*batches THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='revision2 knowledge/source identity invalid';END IF;
 SELECT payload_bytes::text::jsonb->'research' INTO research FROM rkb_v4.source_revision2_batches
  WHERE source_id=wanted ORDER BY batch_index DESC LIMIT 1;
 IF doc->'research' IS DISTINCT FROM research THEN
  RAISE EXCEPTION 'revision2 model frontier differs';END IF;
 SELECT * INTO prior FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted;
 IF FOUND THEN
  IF prior.ingestion_id<>ingestion OR prior.last_batch_sha256<>tail OR prior.knowledge_bytes<>resolved
   OR prior.materializer_sha256<>materializer THEN RAISE EXCEPTION 'immutable revision2 knowledge changed';END IF;
  RETURN prior.knowledge_sha256;
 END IF;
 INSERT INTO rkb_v4.source_revision2_candidates(source_id,ingestion_id,batch_count,last_batch_sha256,
  materializer_sha256,knowledge_bytes,knowledge_sha256,knowledge,prepared_database,prepared_writer)
 VALUES(wanted,ingestion,batches,tail,materializer,resolved,hash,doc,expected_database,expected_writer);
 RETURN hash;
END $$;

CREATE FUNCTION rkb_v4.read_source_revision2_activation_input_v1(wanted uuid,knowledge_hash text,n integer)
RETURNS TABLE(input_id uuid,input_sha256 text,body text,installed boolean)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT (i->>'input_id')::uuid,encode(sha256(convert_to(i->>'body','UTF8')),'hex'),i->>'body',
  EXISTS(SELECT 1 FROM rkb_v4.source_revision2_vectors v WHERE v.source_id=wanted
   AND v.ordinal=n AND v.input_id=(i->>'input_id')::uuid
   AND v.input_sha256=encode(sha256(convert_to(i->>'body','UTF8')),'hex'))
 FROM rkb_v4.source_revision2_candidates k
 CROSS JOIN LATERAL(SELECT rkb_v4.revision2_activation_input_v1(wanted,n) i) x
 WHERE k.source_id=wanted AND k.knowledge_sha256=knowledge_hash AND i IS NOT NULL
$$;

CREATE FUNCTION rkb_v4.install_source_revision2_vector_v1(
 wanted uuid,knowledge_hash text,n integer,v public.vector,
 expected_database text,expected_writer text) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state;i jsonb;known record;h text;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR SHARE;
 IF state.database_generation<>expected_database OR state.writer_generation<>expected_writer
  OR v IS NULL OR public.vector_dims(v)<>1024 OR abs(public.vector_norm(v)-1.0)>0.002
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted
    AND knowledge_sha256=knowledge_hash) THEN RAISE EXCEPTION 'revision2 vector writer/knowledge changed';END IF;
 i:=rkb_v4.revision2_activation_input_v1(wanted,n);
 IF i IS NULL THEN RAISE EXCEPTION 'revision2 input missing';END IF;
 h:=encode(sha256(convert_to(i->>'body','UTF8')),'hex');
 INSERT INTO rkb_v4.source_revision2_vectors VALUES(wanted,n,(i->>'input_id')::uuid,h,v)
 ON CONFLICT(source_id,ordinal) DO NOTHING;
 SELECT * INTO STRICT known FROM rkb_v4.source_revision2_vectors WHERE source_id=wanted AND ordinal=n;
 IF known.input_id<>(i->>'input_id')::uuid OR known.input_sha256<>h THEN
  RAISE EXCEPTION 'revision2 immutable vector input changed';END IF;
 RETURN h;
END $$;

CREATE FUNCTION rkb_v4.check_source_materialize_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;wanted uuid;candidate rkb_v4.source_revision2_candidates;
 v rkb_v4.source_revision2_vectors;i jsonb;ordinal integer;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 THEN RETURN false;END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF d->>'codec'<>'rkb.source-materialize.v2' OR d->'source_revision'<>'2'::jsonb
  OR d->>'ordinal' !~ '^(0|[1-9][0-9]{0,5})$'
  OR expected->>'active_revision'<>'1' THEN RETURN false;END IF;
 ordinal:=(d->>'ordinal')::integer;
 SELECT * INTO candidate FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted;
 IF NOT FOUND OR ordinal>=candidate.total_inputs OR candidate.ingestion_id::text<>d->>'ingestion_id'
  OR candidate.materializer_sha256<>d->>'materializer_sha256'
  OR candidate.last_batch_sha256<>d->>'last_batch_sha256'
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.active_revision WHERE source_id=wanted AND revision=1)
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted
   AND owner_person_id::text=current_setting('rkb_actor_person_id',true)) THEN RETURN false;END IF;
 SELECT * INTO v FROM rkb_v4.source_revision2_vectors WHERE source_id=wanted AND ordinal=ordinal;
 i:=rkb_v4.revision2_activation_input_v1(wanted,ordinal);
 RETURN v.input_id=(i->>'input_id')::uuid
  AND v.input_sha256=encode(sha256(convert_to(i->>'body','UTF8')),'hex');
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_materialize_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;wanted uuid;seq bigint;i jsonb;k text;
BEGIN
 SELECT applied_seq+1 INTO seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_materialize_revision2_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'revision2 materialization precondition rejected';END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 i:=rkb_v4.revision2_activation_input_v1(wanted,(d->>'ordinal')::integer);
 SELECT knowledge_sha256 INTO STRICT k FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted;
 INSERT INTO rkb_v4.source_revision2_materialized VALUES(wanted,(d->>'ordinal')::integer,k,
  encode(sha256(convert_to(i->>'body','UTF8')),'hex'),seq) ON CONFLICT(source_id,ordinal) DO NOTHING;
 RETURN convert_to(jsonb_build_object('source_id',wanted,'source_revision',2,'ingestion_id',
  d->>'ingestion_id','ordinal',d->'ordinal','total_inputs',
  (SELECT total_inputs FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted),
  'knowledge_sha256',k)::text,'UTF8');
END $$;

CREATE FUNCTION rkb_v4.check_source_activate_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;wanted uuid;candidate rkb_v4.source_revision2_candidates;s rkb_v4.source_revision2_sessions;
 n integer;i jsonb;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 2048 THEN RETURN false;END IF;
 wanted:=(ids->>'source_id')::uuid;d:=convert_from(p,'UTF8')::jsonb;
 IF d->>'codec'<>'rkb.source-activate.v2' OR d->'source_revision'<>'2'::jsonb
  OR ids IS DISTINCT FROM jsonb_build_object('source_id',ids->>'source_id')
  OR expected->>'active_revision'<>'1' THEN RETURN false;END IF;
 SELECT * INTO candidate FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted;
 SELECT * INTO s FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 IF candidate.source_id IS NULL OR s.source_id IS NULL
  OR candidate.ingestion_id<>s.ingestion_id OR candidate.ingestion_id::text<>d->>'ingestion_id'
  OR candidate.batch_count::text<>d->>'batch_count'
  OR candidate.last_batch_sha256<>d->>'last_batch_sha256'
  OR candidate.materializer_sha256<>d->>'materializer_sha256'
  OR s.owner_person_id::text<>current_setting('rkb_actor_person_id',true)
  OR s.predecessor_command_sha256<>expected->>'predecessor_command_sha256'
  OR NOT EXISTS(SELECT 1 FROM rkb_v4.active_revision WHERE source_id=wanted AND revision=1)
  OR NOT rkb_v4.revision2_stage_chain_v1(wanted,s.ingestion_id,candidate.batch_count,candidate.last_batch_sha256)
  OR (SELECT count(*) FROM rkb_v4.source_revision2_materialized WHERE source_id=wanted)<>candidate.total_inputs
  OR (SELECT count(*) FROM rkb_v4.source_revision2_vectors WHERE source_id=wanted)<>candidate.total_inputs
  THEN RETURN false;END IF;
 FOR n IN 0..candidate.total_inputs-1 LOOP
  i:=rkb_v4.revision2_activation_input_v1(wanted,n);
  IF NOT EXISTS(SELECT 1 FROM rkb_v4.source_revision2_vectors v
    JOIN rkb_v4.source_revision2_materialized m USING(source_id,ordinal)
    JOIN rkb_v4.applied_ops op ON op.sequence=m.accepted_sequence
    WHERE v.source_id=wanted AND v.ordinal=n AND op.status='applied'
      AND m.knowledge_sha256=candidate.knowledge_sha256 AND m.input_sha256=v.input_sha256
      AND v.input_id=(i->>'input_id')::uuid
      AND v.input_sha256=encode(sha256(convert_to(i->>'body','UTF8')),'hex')) THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_source_activate_revision2_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state;before rkb_v4.read_frontier;doc jsonb;
 wanted uuid;item jsonb;ordinal integer:=0;v public.vector;candidate rkb_v4.source_revision2_candidates;
 s rkb_v4.source_revision2_sessions;frontier rkb_v4.read_frontier;
BEGIN
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT before FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_source_activate_revision2_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'revision2 source activation predecessor/coverage invalid';END IF;
 wanted:=(ids->>'source_id')::uuid;SELECT * INTO STRICT candidate FROM rkb_v4.source_revision2_candidates WHERE source_id=wanted;
 SELECT * INTO STRICT s FROM rkb_v4.source_revision2_sessions WHERE source_id=wanted;
 doc:=candidate.knowledge;
 INSERT INTO rkb_v4.revisions VALUES(wanted,2);
 FOR item IN SELECT value FROM jsonb_array_elements(doc->'passages') LOOP
  SELECT embedding INTO STRICT v FROM rkb_v4.source_revision2_vectors
   WHERE source_id=wanted AND ordinal=ordinal AND input_id=(item->>'passage_id')::uuid;
  INSERT INTO rkb_v4.passage_search(passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
   VALUES((item->>'passage_id')::uuid,wanted,2,ordinal,item->>'locator',item->>'body',item->>'language',
    CASE item->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
      WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,v);
  ordinal:=ordinal+1;
 END LOOP;
 -- Model-authored facts and Story.body remain source-bound durable knowledge;
 -- projection into the editorial registries requires their independently
 -- versioned mutators, never a hidden rewrite of human-approved revision1.
 INSERT INTO rkb_v4.source_revision2_semantic VALUES(wanted,2,doc->'semantic',
   doc->'source_graph',doc->'image_manifest',encode(sha256(p),'hex'));
 INSERT INTO rkb_v4.source_revision2_research_frontier VALUES(wanted,2,
   (doc->'research'->>'frontier_id')::uuid,doc->'research'->>'state',
   doc->'research'->'continuation');
 INSERT INTO rkb_v4.source_revision2_origins VALUES(wanted,2,s.ingestion_id,s.origin_ingestion_id,
   s.predecessor_command_sha256,encode(sha256(s.original_receipt),'hex'),s.original_sha256,
   candidate.knowledge_sha256,'revision2-activation:'||s.ingestion_id,state.applied_seq+1,
   encode(sha256(p),'hex'),s.owner_person_id,encode(sha256(p),'hex'));
 UPDATE rkb_v4.active_revision SET revision=2 WHERE source_id=wanted AND revision=1;
 IF NOT FOUND THEN RAISE EXCEPTION 'source successor lost active predecessor';END IF;
 SELECT * INTO STRICT frontier FROM rkb_v4.read_frontier WHERE singleton;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,
  frontier.visibility_epoch,state.applied_seq+1,
  before.text_ready AND before.projection_seq IS NOT DISTINCT FROM state.applied_seq,
  before.dense_ready AND before.projection_seq IS NOT DISTINCT FROM state.applied_seq);
 RETURN convert_to(jsonb_build_object('source_id',wanted,'content_revision',2,
  'accepted',true,'prior_revision',1,'ingestion_id',s.ingestion_id,'knowledge_sha256',
  candidate.knowledge_sha256,'passage_count',jsonb_array_length(doc->'passages'),
  'research_frontier_id',doc->'research'->>'frontier_id','research_state',doc->'research'->>'state',
  'payload_sha256',encode(sha256(p),'hex'),'logical_time',logical_time)::text,'UTF8');
END $$;

-- New objects private by default; privilege decisions/grants belong to sole
-- runtime owner and are NOT made by this source-only migration PR.
REVOKE ALL ON rkb_v4.source_revision2_sessions,rkb_v4.source_revision2_batches,
 rkb_v4.source_revision2_seals,rkb_v4.source_revision2_candidates,
 rkb_v4.source_revision2_vectors,rkb_v4.source_revision2_materialized,
 rkb_v4.source_revision2_origins,rkb_v4.source_revision2_research_frontier,
 rkb_v4.source_revision2_semantic FROM PUBLIC;
