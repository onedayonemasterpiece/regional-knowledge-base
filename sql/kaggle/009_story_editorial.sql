-- Explicit schema009 semantic barrier. Never install implicitly into008.
-- All001..008 definitions remain byte-identical. New shared editorial versions
-- preserve the imported basic body and never create/access a personal case draft.
CREATE TABLE rkb_v4.editorial_story_heads (
 story_id uuid PRIMARY KEY REFERENCES rkb_v4.stories,revision bigint NOT NULL CHECK(revision>=1),
 owner_id uuid NOT NULL,source_id uuid NOT NULL,source_revision bigint NOT NULL,
 default_variant_id uuid NOT NULL,default_variant_revision bigint NOT NULL,
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE TABLE rkb_v4.editorial_variant_versions (
 variant_id uuid NOT NULL,revision bigint NOT NULL CHECK(revision>=1),
 story_id uuid NOT NULL REFERENCES rkb_v4.editorial_story_heads,
 body text NOT NULL CHECK(length(body) BETWEEN 1 AND 4000),
 body_sha256 text NOT NULL CHECK(body_sha256=encode(sha256(convert_to(body,'UTF8')),'hex')),
 data jsonb NOT NULL CHECK(jsonb_typeof(data)='object' AND data->>'body'=body
   AND data->>'state'='drafting' AND data->'approved_revision'='null'::jsonb
   AND data->'approval_fingerprint'='null'::jsonb),
 embedding public.vector(1024) NOT NULL,input_tokens integer NOT NULL CHECK(input_tokens BETWEEN 1 AND 512),
 actor_id uuid NOT NULL,logical_time text NOT NULL,
 PRIMARY KEY(variant_id,revision),UNIQUE(story_id,variant_id,revision)
);
CREATE TABLE rkb_v4.editorial_variant_heads (
 variant_id uuid PRIMARY KEY,story_id uuid NOT NULL REFERENCES rkb_v4.editorial_story_heads,
 revision bigint NOT NULL,
 FOREIGN KEY(story_id,variant_id,revision) REFERENCES rkb_v4.editorial_variant_versions(story_id,variant_id,revision)
);
ALTER TABLE rkb_v4.editorial_story_heads ADD CONSTRAINT editorial_default_exact_variant
 FOREIGN KEY(story_id,default_variant_id,default_variant_revision)
 REFERENCES rkb_v4.editorial_variant_versions(story_id,variant_id,revision) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE rkb_v4.editorial_story_versions (
 story_id uuid NOT NULL REFERENCES rkb_v4.editorial_story_heads,revision bigint NOT NULL CHECK(revision>=1),
 data jsonb NOT NULL,actor_id uuid NOT NULL,logical_time text NOT NULL,
 action text NOT NULL CHECK(action IN ('source_import','upsert_variant','access_change')),
 PRIMARY KEY(story_id,revision)
);
CREATE TABLE rkb_v4.story_editor_grants (
 story_id uuid NOT NULL REFERENCES rkb_v4.editorial_story_heads,grantee_id uuid NOT NULL,
 capability text NOT NULL CHECK(capability IN ('viewer','contributor','researcher','editor','publisher','manager')),
 granted_by uuid NOT NULL,story_revision bigint NOT NULL,
 PRIMARY KEY(story_id,grantee_id),
 FOREIGN KEY(story_id,story_revision) REFERENCES rkb_v4.editorial_story_versions DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE rkb_v4.editorial_story_receipts (
 story_id uuid NOT NULL,revision bigint NOT NULL,operation_key text NOT NULL REFERENCES rkb_v4.applied_ops,
 sequence bigint NOT NULL,person_id uuid NOT NULL,request_id text NOT NULL,command_sha256 text NOT NULL,
 PRIMARY KEY(story_id,revision),FOREIGN KEY(story_id,revision) REFERENCES rkb_v4.editorial_story_versions
);
CREATE TABLE rkb_v4.editorial_story_search (
 story_id uuid NOT NULL,variant_id uuid NOT NULL,variant_revision bigint NOT NULL,
 source_id uuid NOT NULL,source_revision bigint NOT NULL,body text NOT NULL,
 config regconfig NOT NULL,embedding public.vector(1024) NOT NULL,
 fts tsvector GENERATED ALWAYS AS(to_tsvector(config,body)) STORED,
 PRIMARY KEY(story_id,variant_id,variant_revision),
 FOREIGN KEY(story_id,variant_id,variant_revision) REFERENCES rkb_v4.editorial_variant_versions(story_id,variant_id,revision),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions
);
CREATE INDEX editorial_story_search_fts ON rkb_v4.editorial_story_search USING gin(fts);
CREATE INDEX editorial_story_search_dense ON rkb_v4.editorial_story_search USING hnsw(embedding public.vector_cosine_ops);
CREATE INDEX editorial_story_source ON rkb_v4.editorial_story_heads(source_id,story_id);
CREATE INDEX editorial_story_variant_page ON rkb_v4.editorial_variant_heads(story_id,variant_id);
CREATE TRIGGER immutable_editorial_variant BEFORE UPDATE OR DELETE ON rkb_v4.editorial_variant_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_editorial_story_version BEFORE UPDATE OR DELETE ON rkb_v4.editorial_story_versions
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_editorial_story_receipt BEFORE UPDATE OR DELETE ON rkb_v4.editorial_story_receipts
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_editorial_story_projection BEFORE UPDATE OR DELETE ON rkb_v4.editorial_story_search
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER editorial_story_writer_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.editorial_story_heads
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER editorial_variant_writer_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.editorial_variant_heads
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER editorial_grant_writer_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.story_editor_grants
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();
CREATE TRIGGER editorial_projection_writer_fence BEFORE INSERT OR UPDATE OR DELETE ON rkb_v4.editorial_story_search
 FOR EACH STATEMENT EXECUTE FUNCTION rkb_v4.lock_read_fence_writer();

CREATE FUNCTION rkb_v4.story_editor_role_v1(wanted uuid,actor uuid)
RETURNS text LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT CASE WHEN h.owner_id=actor THEN 'manager' ELSE g.capability END
 FROM rkb_v4.editorial_story_heads h LEFT JOIN rkb_v4.story_editor_grants g
 ON g.story_id=h.story_id AND g.grantee_id=actor WHERE h.story_id=wanted
$$;
CREATE FUNCTION rkb_v4.editorial_source_current_v1(wanted uuid,source uuid,content bigint,eligibility bigint)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM rkb_v4.editorial_story_heads h
 JOIN rkb_v4.sources s ON s.source_id=h.source_id
 JOIN rkb_v4.active_revision a ON a.source_id=h.source_id AND a.revision=h.source_revision
 JOIN rkb_v4.source_visibility v ON v.source_id=h.source_id
 WHERE h.story_id=wanted AND h.source_id=source AND h.source_revision=content
 AND v.eligibility_revision=eligibility AND s.accepted AND NOT s.withdrawn)
$$;
CREATE FUNCTION rkb_v4.check_story_edit_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;op jsonb;b jsonb;variant uuid;wanted uuid;actor uuid;current_variant bigint;v public.vector;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 98304 OR length(logical_time) NOT BETWEEN 1 AND 256
 OR jsonb_typeof(ids) IS DISTINCT FROM 'object' OR NOT ids ?& ARRAY['source_id','story_id','actor_id']
 OR ids-ARRAY['source_id','story_id','actor_id']<>'{}'
 OR jsonb_typeof(expected) IS DISTINCT FROM 'object'
 OR NOT expected ?& ARRAY['story_revision','variant_revision','source_revision','eligibility_revision']
 OR expected-ARRAY['story_revision','variant_revision','source_revision','eligibility_revision']<>'{}'
 THEN RETURN false;END IF;
 wanted:=(ids->>'story_id')::uuid;actor:=(ids->>'actor_id')::uuid;
 IF rkb_v4.story_editor_role_v1(wanted,actor) NOT IN ('editor','publisher','manager')
 OR rkb_v4.story_editor_role_v1(wanted,actor) IS NULL
 OR NOT rkb_v4.editorial_source_current_v1(wanted,(ids->>'source_id')::uuid,
   (expected->>'source_revision')::bigint,(expected->>'eligibility_revision')::bigint)
 OR NOT EXISTS(SELECT 1 FROM rkb_v4.editorial_story_heads WHERE story_id=wanted AND revision=(expected->>'story_revision')::bigint)
 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;op:=doc->'operation';
 IF jsonb_typeof(doc) IS DISTINCT FROM 'object'
 OR NOT doc ?& ARRAY['codec','operation','resolved_variant_id','embedding','input_tokens','reason']
 OR doc-ARRAY['codec','operation','resolved_variant_id','embedding','input_tokens','reason']<>'{}'
 OR doc->>'codec' IS DISTINCT FROM 'rkb.story-edit.v1'
 OR jsonb_typeof(op) IS DISTINCT FROM 'object'
 OR NOT op ?& ARRAY['op','variant_id','expected_variant_revision','audience','format','channel','language','body','blocks','attributions','media_refs']
 OR op-ARRAY['op','variant_id','expected_variant_revision','audience','format','channel','language','body','blocks','attributions','media_refs']<>'{}'
 OR op->>'op' IS DISTINCT FROM 'upsert_variant'
 OR jsonb_typeof(op->'body') IS DISTINCT FROM 'string' OR length(op->>'body') NOT BETWEEN 1 AND 4000
 OR jsonb_typeof(op->'audience') IS DISTINCT FROM 'string' OR length(op->>'audience') NOT BETWEEN 1 AND 500
 OR jsonb_typeof(op->'language') IS DISTINCT FROM 'string' OR length(op->>'language') NOT BETWEEN 2 AND 24
 OR op->>'format' NOT IN ('story','video','short_video','article','post','audio','lecture','other')
 OR jsonb_typeof(op->'format') IS DISTINCT FROM 'string'
 OR (op->'channel'<>'null'::jsonb AND (jsonb_typeof(op->'channel') IS DISTINCT FROM 'string' OR length(op->>'channel')>120))
 OR (doc->'reason'<>'null'::jsonb AND (jsonb_typeof(doc->'reason') IS DISTINCT FROM 'string' OR length(doc->>'reason')>3000))
 OR jsonb_typeof(op->'blocks') IS DISTINCT FROM 'array' OR jsonb_array_length(op->'blocks')>25
 OR jsonb_typeof(op->'attributions') IS DISTINCT FROM 'array' OR jsonb_array_length(op->'attributions')>15
 OR jsonb_typeof(op->'media_refs') IS DISTINCT FROM 'array' OR jsonb_array_length(op->'media_refs')>12
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(op->'attributions') t WHERE jsonb_typeof(t) IS DISTINCT FROM 'string' OR length(t#>>'{}')>500)
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(op->'media_refs') t WHERE jsonb_typeof(t) IS DISTINCT FROM 'string' OR length(t#>>'{}')>250)
 THEN RETURN false;END IF;
 variant:=(doc->>'resolved_variant_id')::uuid;
 SELECT revision INTO current_variant FROM rkb_v4.editorial_variant_heads WHERE variant_id=variant AND story_id=wanted;
 IF coalesce(current_variant,0)<>(expected->>'variant_revision')::bigint THEN RETURN false;END IF;
 IF current_variant IS NULL THEN
  IF op->'variant_id'<>'null'::jsonb OR (SELECT count(*) FROM rkb_v4.editorial_variant_heads WHERE story_id=wanted)>=25
   OR EXISTS(SELECT 1 FROM rkb_v4.editorial_variant_heads WHERE variant_id=variant) THEN RETURN false;END IF;
 ELSE
  IF (op->>'variant_id')::uuid IS DISTINCT FROM variant
   OR (op->>'expected_variant_revision')::bigint IS DISTINCT FROM current_variant THEN RETURN false;END IF;
 END IF;
 FOR b IN SELECT value FROM jsonb_array_elements(op->'blocks') LOOP
  IF jsonb_typeof(b) IS DISTINCT FROM 'object' OR NOT b ?& ARRAY['text','assertion_id','assertion_revision','attribution']
   OR b-ARRAY['text','assertion_id','assertion_revision','attribution']<>'{}'
   OR jsonb_typeof(b->'text') IS DISTINCT FROM 'string' OR length(b->>'text') NOT BETWEEN 1 AND 4000
   OR (b->'attribution'<>'null'::jsonb AND (jsonb_typeof(b->'attribution') IS DISTINCT FROM 'string' OR length(b->>'attribution')>500))
   OR (b->'assertion_id'='null'::jsonb AND b->'assertion_revision'<>'null'::jsonb)
   THEN RETURN false;END IF;
  IF b->'assertion_id'<>'null'::jsonb AND NOT EXISTS(SELECT 1 FROM rkb_v4.story_assertion_versions a
   JOIN rkb_v4.story_versions s ON s.story_id=wanted AND s.revision=1
   WHERE a.assertion_id=(b->>'assertion_id')::uuid AND a.revision=(b->>'assertion_revision')::bigint
    AND a.source_id=(ids->>'source_id')::uuid AND s.data->'finding_ids' ? a.assertion_id::text) THEN RETURN false;END IF;
 END LOOP;
 IF jsonb_typeof(doc->'embedding') IS DISTINCT FROM 'array' OR jsonb_array_length(doc->'embedding')<>1024
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(doc->'embedding') e WHERE jsonb_typeof(e) IS DISTINCT FROM 'number')
 OR jsonb_typeof(doc->'input_tokens') IS DISTINCT FROM 'number' OR doc->>'input_tokens' !~ '^[1-9][0-9]{0,2}$'
 OR (doc->>'input_tokens')::integer NOT BETWEEN 1 AND 512 THEN RETURN false;END IF;
 v:=(doc->'embedding')::text::public.vector;
 RETURN public.vector_norm(v) BETWEEN .999 AND 1.001;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_story_edit_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb:=convert_from(p,'UTF8')::jsonb;op jsonb:=doc->'operation';wanted uuid:=(ids->>'story_id')::uuid;
 variant uuid:=(doc->>'resolved_variant_id')::uuid;head rkb_v4.editorial_story_heads;
 variant_revision bigint:=(expected->>'variant_revision')::bigint+1;new_revision bigint;refs jsonb;data jsonb;
 state rkb_v4.recovery_state;visibility bigint;
BEGIN
 IF NOT rkb_v4.check_story_edit_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'editorial precondition differs';END IF;
 SELECT * INTO STRICT head FROM rkb_v4.editorial_story_heads WHERE story_id=wanted FOR UPDATE;
 new_revision:=head.revision+1;
 data:=(op-ARRAY['op','expected_variant_revision'])||jsonb_build_object('variant_id',variant,'revision',variant_revision,
   'state','drafting','approved_revision',NULL,'approval_fingerprint',NULL);
 INSERT INTO rkb_v4.editorial_variant_versions VALUES(variant,variant_revision,wanted,op->>'body',
  encode(sha256(convert_to(op->>'body','UTF8')),'hex'),data,(doc->'embedding')::text::public.vector,
  (doc->>'input_tokens')::integer,(ids->>'actor_id')::uuid,logical_time);
 INSERT INTO rkb_v4.editorial_variant_heads VALUES(variant,wanted,variant_revision)
 ON CONFLICT(variant_id) DO UPDATE SET revision=excluded.revision;
 SELECT jsonb_agg(jsonb_build_object('variant_id',variant_id,'revision',revision) ORDER BY variant_id)
 INTO refs FROM rkb_v4.editorial_variant_heads WHERE story_id=wanted;
 UPDATE rkb_v4.editorial_story_heads SET revision=new_revision,
  default_variant_revision=CASE WHEN default_variant_id=variant THEN variant_revision ELSE default_variant_revision END
 WHERE story_id=wanted;
 INSERT INTO rkb_v4.editorial_story_versions
 SELECT wanted,new_revision,jsonb_build_object('default_variant_ref',jsonb_build_object(
   'variant_id',h.default_variant_id,'revision',h.default_variant_revision),'variants',refs,'state','drafting'),
   (ids->>'actor_id')::uuid,logical_time,'upsert_variant' FROM rkb_v4.editorial_story_heads h WHERE h.story_id=wanted;
 IF head.default_variant_id=variant THEN
  INSERT INTO rkb_v4.editorial_story_search VALUES(wanted,variant,variant_revision,head.source_id,head.source_revision,
   op->>'body',CASE op->>'language' WHEN 'ru' THEN 'pg_catalog.russian'::regconfig
    WHEN 'de' THEN 'pg_catalog.german'::regconfig ELSE 'pg_catalog.simple'::regconfig END,
   (doc->'embedding')::text::public.vector,DEFAULT);
 END IF;
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton;
 SELECT visibility_epoch INTO STRICT visibility FROM rkb_v4.read_frontier WHERE singleton;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,visibility,state.applied_seq+1,true,true);
 RETURN convert_to(jsonb_build_object('story_id',wanted,'revision',new_revision,'variant_id',variant,
  'variant_revision',variant_revision,'body_sha256',encode(sha256(convert_to(op->>'body','UTF8')),'hex'),
  'state','drafting','publication_approval',NULL)::text,'UTF8');
END $$;
CREATE FUNCTION rkb_v4.check_story_access_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;wanted uuid;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 98304 OR length(logical_time) NOT BETWEEN 1 AND 256
 OR jsonb_typeof(ids) IS DISTINCT FROM 'object' OR NOT ids ?& ARRAY['source_id','story_id','actor_id']
 OR ids-ARRAY['source_id','story_id','actor_id']<>'{}'
 OR jsonb_typeof(expected) IS DISTINCT FROM 'object'
 OR NOT expected ?& ARRAY['story_revision','source_revision','eligibility_revision']
 OR expected-ARRAY['story_revision','source_revision','eligibility_revision']<>'{}' THEN RETURN false;END IF;
 wanted:=(ids->>'story_id')::uuid;
 IF rkb_v4.story_editor_role_v1(wanted,(ids->>'actor_id')::uuid) IS DISTINCT FROM 'manager'
 OR NOT rkb_v4.editorial_source_current_v1(wanted,(ids->>'source_id')::uuid,
   (expected->>'source_revision')::bigint,(expected->>'eligibility_revision')::bigint)
 OR NOT EXISTS(SELECT 1 FROM rkb_v4.editorial_story_heads WHERE story_id=wanted AND revision=(expected->>'story_revision')::bigint)
 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF jsonb_typeof(doc) IS DISTINCT FROM 'object' OR NOT doc ?& ARRAY['codec','grantee_user_id','capability']
 OR doc-ARRAY['codec','grantee_user_id','capability']<>'{}' OR doc->>'codec' IS DISTINCT FROM 'rkb.story-access.v1'
 OR jsonb_typeof(doc->'grantee_user_id') IS DISTINCT FROM 'string'
 OR (doc->>'grantee_user_id')::uuid::text IS DISTINCT FROM doc->>'grantee_user_id'
 OR jsonb_typeof(doc->'capability') IS DISTINCT FROM 'string'
 OR doc->>'capability' NOT IN ('viewer','contributor','researcher','editor','publisher','manager','revoke')
 THEN RETURN false;END IF;
 RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_story_access_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb:=convert_from(p,'UTF8')::jsonb;wanted uuid:=(ids->>'story_id')::uuid;
 head rkb_v4.editorial_story_heads;prior jsonb;state rkb_v4.recovery_state;visibility bigint;
BEGIN
 IF NOT rkb_v4.check_story_access_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'manager precondition differs';END IF;
 SELECT * INTO STRICT head FROM rkb_v4.editorial_story_heads WHERE story_id=wanted FOR UPDATE;
 SELECT data INTO STRICT prior FROM rkb_v4.editorial_story_versions WHERE story_id=wanted AND revision=head.revision;
 IF doc->>'capability'='revoke' THEN
  DELETE FROM rkb_v4.story_editor_grants WHERE story_id=wanted AND grantee_id=(doc->>'grantee_user_id')::uuid;
 ELSE
  INSERT INTO rkb_v4.story_editor_grants VALUES(wanted,(doc->>'grantee_user_id')::uuid,
   doc->>'capability',(ids->>'actor_id')::uuid,head.revision+1)
  ON CONFLICT(story_id,grantee_id) DO UPDATE SET capability=excluded.capability,
   granted_by=excluded.granted_by,story_revision=excluded.story_revision;
 END IF;
 UPDATE rkb_v4.editorial_story_heads SET revision=revision+1 WHERE story_id=wanted;
 INSERT INTO rkb_v4.editorial_story_versions VALUES(wanted,head.revision+1,prior,
  (ids->>'actor_id')::uuid,logical_time,'access_change');
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton;
 SELECT visibility_epoch INTO STRICT visibility FROM rkb_v4.read_frontier WHERE singleton;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,visibility,state.applied_seq+1,true,true);
 RETURN convert_to(jsonb_build_object('story_id',wanted,'revision',head.revision+1,
  'grantee_user_id',doc->>'grantee_user_id','capability',doc->>'capability')::text,'UTF8');
END $$;

-- A new exact import route binds the fresh9 semantics; v3 and its callbacks are
-- preserved unchanged for original ordered replay, never relabelled as v4.
CREATE FUNCTION rkb_v4.check_source_import_v4(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT rkb_v4.check_source_import_v3(p,ids,expected,logical_time)
$$;
CREATE FUNCTION rkb_v4.apply_source_import_v4(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE sql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT rkb_v4.apply_source_import_v3(p,ids,expected,logical_time)
$$;
CREATE FUNCTION rkb_v4.capture_editorial_receipt_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c jsonb;wanted uuid;story record;v jsonb;refs jsonb;new_revision bigint;
BEGIN
 IF NEW.status<>'applied' THEN RETURN NULL;END IF;
 c:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 IF c->>'route'='source.import.v4' THEN
  FOR story IN SELECT s.*,sv.data version_data,b.data variant_data,b.body,b.body_sha256,
     p.embedding,p.config FROM rkb_v4.stories s JOIN rkb_v4.story_versions sv USING(story_id)
     JOIN rkb_v4.story_variant_versions b ON b.variant_id=s.default_variant_id AND b.revision=s.default_variant_revision
     JOIN rkb_v4.story_search p ON p.story_id=s.story_id
     WHERE s.source_id=(c->'ids'->>'source_id')::uuid LOOP
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
 ELSIF c->>'route' IN ('story.edit.v1','story.access.v1') THEN
  wanted:=(c->'ids'->>'story_id')::uuid;new_revision:=(c->'expected_revisions'->>'story_revision')::bigint+1;
  INSERT INTO rkb_v4.editorial_story_receipts VALUES(wanted,new_revision,NEW.operation_key,NEW.sequence,
   (c->>'person_id')::uuid,c->>'request_id',NEW.command_sha256);
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER capture_editorial_receipt AFTER INSERT ON rkb_v4.applied_ops
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.capture_editorial_receipt_v1();

CREATE FUNCTION rkb_v4.read_editorial_story_v1(wanted uuid,wanted_revision bigint,view_name text,after_revision bigint,page_limit integer,after_variant uuid DEFAULT NULL)
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
  'title',metadata->>'title','summary',metadata->>'summary','material_type',metadata->>'material_type',
  'body',variant.body,'body_sha256',variant.body_sha256,'variant',variant.data,
  'default_variant_ref',version.data->'default_variant_ref','state',version.data->>'state',
  'narrative_availability','available','visibility','shared','publication_approval',NULL,
  'bibliography',source.bibliography,'source',jsonb_build_object('source_id',head.source_id,
   'content_revision',head.source_revision,'eligibility_revision',eligibility),
  'finding_ids',metadata->'finding_ids','view',view_name,'entries',entries,'next_revision',next_revision,'next_variant_id',next_variant);
 IF octet_length(result::text)>131072 THEN RAISE EXCEPTION 'bounded editorial response required';END IF;
 RETURN result;
END $$;
REVOKE ALL ON rkb_v4.editorial_story_heads,rkb_v4.editorial_story_versions,rkb_v4.editorial_variant_heads,
 rkb_v4.editorial_variant_versions,rkb_v4.story_editor_grants,rkb_v4.editorial_story_receipts,
 rkb_v4.editorial_story_search FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.story_editor_role_v1(uuid,uuid),rkb_v4.editorial_source_current_v1(uuid,uuid,bigint,bigint),
 rkb_v4.check_story_edit_v1(bytea,jsonb,jsonb,text),rkb_v4.apply_story_edit_v1(bytea,jsonb,jsonb,text),
 rkb_v4.check_story_access_v1(bytea,jsonb,jsonb,text),rkb_v4.apply_story_access_v1(bytea,jsonb,jsonb,text),
 rkb_v4.check_source_import_v4(bytea,jsonb,jsonb,text),rkb_v4.apply_source_import_v4(bytea,jsonb,jsonb,text),
 rkb_v4.capture_editorial_receipt_v1(),rkb_v4.read_editorial_story_v1(uuid,bigint,text,bigint,integer,uuid) FROM PUBLIC;

CREATE FUNCTION rkb_v4.verify_source_story_origins_v3(scope uuid[])
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE op record;c jsonb;doc jsonb;sem jsonb;source uuid;item jsonb;v jsonb;
 verified uuid[]:='{}';seen integer:=0;
BEGIN
 IF scope IS NULL OR cardinality(scope)>10000 THEN RETURN false;END IF;
 IF cardinality(scope)=0 THEN RETURN true;END IF;
 FOR op IN SELECT command_bytes FROM rkb_v4.applied_ops WHERE status='applied' ORDER BY sequence LIMIT 10001 LOOP
  seen:=seen+1;IF seen>10000 THEN RETURN false;END IF;
  c:=convert_from(op.command_bytes,'UTF8')::jsonb;
  IF c->>'route' NOT IN ('source.import.v2','source.import.v3','source.import.v4') OR NOT((c->'ids'->>'source_id')::uuid=ANY(scope)) THEN CONTINUE;END IF;
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

REVOKE ALL ON FUNCTION rkb_v4.verify_source_story_origins_v3(uuid[]) FROM PUBLIC;

CREATE FUNCTION rkb_v4.editorial_lexical_candidates_v1(q text,n integer,scope uuid[])
RETURNS TABLE(story_id uuid,score real) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH configs AS (
  SELECT c,websearch_to_tsquery(c,q) query FROM unnest(ARRAY[
   'pg_catalog.russian'::regconfig,'pg_catalog.german'::regconfig,'pg_catalog.simple'::regconfig]) c
 ),candidates AS (
  SELECT hit.* FROM configs CROSS JOIN LATERAL (
   SELECT p.story_id,ts_rank_cd(p.fts,configs.query) score
   FROM rkb_v4.editorial_story_search p JOIN rkb_v4.editorial_story_heads h ON h.story_id=p.story_id AND h.default_variant_id=p.variant_id AND h.default_variant_revision=p.variant_revision JOIN rkb_v4.sources s ON s.source_id=p.source_id
   JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
   WHERE s.accepted AND NOT s.withdrawn AND p.config=configs.c AND p.fts@@configs.query
    AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
   ORDER BY score DESC,p.story_id LIMIT least(greatest(n,1),200)
  ) hit
 ) SELECT * FROM candidates ORDER BY score DESC,story_id LIMIT least(greatest(n,1),200)
$$;
CREATE FUNCTION rkb_v4.editorial_dense_candidates_v1(v public.vector,n integer,scope uuid[])
RETURNS TABLE(story_id uuid,score double precision) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT p.story_id,1-(p.embedding OPERATOR(public.<=>) v) score
 FROM rkb_v4.editorial_story_search p JOIN rkb_v4.editorial_story_heads h ON h.story_id=p.story_id AND h.default_variant_id=p.variant_id AND h.default_variant_revision=p.variant_revision JOIN rkb_v4.sources s ON s.source_id=p.source_id
 JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
 WHERE s.accepted AND NOT s.withdrawn AND (scope IS NULL OR p.source_id=ANY(rkb_v4.checked_source_scope(scope)))
 ORDER BY p.embedding OPERATOR(public.<=>) v LIMIT least(greatest(n,1),200)
$$;
CREATE FUNCTION rkb_v4.editorial_hydrate_v1(ids uuid[],scope uuid[])
RETURNS TABLE(story_id uuid,record jsonb) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT s.story_id,rkb_v4.read_editorial_story_v1(s.story_id,NULL,'get',0,1)
 FROM unnest(ids[1:5]) WITH ORDINALITY requested(id,position)
 JOIN rkb_v4.editorial_story_heads s ON s.story_id=requested.id
 WHERE scope IS NULL OR s.source_id=ANY(rkb_v4.checked_source_scope(scope))
 ORDER BY requested.position
$$;

REVOKE ALL ON FUNCTION rkb_v4.editorial_lexical_candidates_v1(text,integer,uuid[]),
 rkb_v4.editorial_dense_candidates_v1(public.vector,integer,uuid[]),rkb_v4.editorial_hydrate_v1(uuid[],uuid[]) FROM PUBLIC;
