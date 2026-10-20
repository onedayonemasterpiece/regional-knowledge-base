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
 state rkb_v4.recovery_state;visibility bigint;prior rkb_v4.read_frontier;
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
 SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton;visibility:=prior.visibility_epoch;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,visibility,state.applied_seq+1,
  prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
  prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
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
 head rkb_v4.editorial_story_heads;previous_data jsonb;state rkb_v4.recovery_state;visibility bigint;prior rkb_v4.read_frontier;
BEGIN
 IF NOT rkb_v4.check_story_access_v1(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'manager precondition differs';END IF;
 SELECT * INTO STRICT head FROM rkb_v4.editorial_story_heads WHERE story_id=wanted FOR UPDATE;
 SELECT data INTO STRICT previous_data FROM rkb_v4.editorial_story_versions WHERE story_id=wanted AND revision=head.revision;
 IF doc->>'capability'='revoke' THEN
  DELETE FROM rkb_v4.story_editor_grants WHERE story_id=wanted AND grantee_id=(doc->>'grantee_user_id')::uuid;
 ELSE
  INSERT INTO rkb_v4.story_editor_grants VALUES(wanted,(doc->>'grantee_user_id')::uuid,
   doc->>'capability',(ids->>'actor_id')::uuid,head.revision+1)
  ON CONFLICT(story_id,grantee_id) DO UPDATE SET capability=excluded.capability,
   granted_by=excluded.granted_by,story_revision=excluded.story_revision;
 END IF;
 UPDATE rkb_v4.editorial_story_heads SET revision=revision+1 WHERE story_id=wanted;
 INSERT INTO rkb_v4.editorial_story_versions VALUES(wanted,head.revision+1,previous_data,
  (ids->>'actor_id')::uuid,logical_time,'access_change');
 SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton;
 SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton;visibility:=prior.visibility_epoch;
 PERFORM rkb_v4.prepare_read_frontier(state.database_generation,state.writer_generation,visibility,state.applied_seq+1,
  prior.text_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq,
  prior.dense_ready AND prior.projection_seq IS NOT DISTINCT FROM state.applied_seq);
 RETURN convert_to(jsonb_build_object('story_id',wanted,'revision',head.revision+1,
  'grantee_user_id',doc->>'grantee_user_id','capability',doc->>'capability')::text,'UTF8');
END $$;

-- A new exact import route binds the fresh9 semantics; v3 and its callbacks are
-- preserved unchanged for original ordered replay, never relabelled as v4.
CREATE TABLE rkb_v4.source_manifests (
 source_id uuid NOT NULL,source_revision bigint NOT NULL,
 manifest jsonb NOT NULL,payload_sha256 text NOT NULL CHECK(payload_sha256 ~ '^[0-9a-f]{64}$'),
 original_receipt bytea NOT NULL CHECK(octet_length(original_receipt) BETWEEN 1 AND 12288),
 PRIMARY KEY(source_id,source_revision),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions,
 CHECK(manifest->>'source_id'=source_id::text AND manifest->>'source_revision'=source_revision::text)
);
CREATE TRIGGER immutable_source_manifest BEFORE UPDATE OR DELETE ON rkb_v4.source_manifests
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();

CREATE TABLE rkb_v4.source_catalog (
 source_id uuid NOT NULL,source_revision bigint NOT NULL,metadata jsonb NOT NULL,search_text text NOT NULL,
 fts tsvector GENERATED ALWAYS AS(to_tsvector('pg_catalog.simple'::regconfig,search_text)) STORED,
 PRIMARY KEY(source_id,source_revision),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.source_manifests
);
CREATE INDEX source_catalog_fts ON rkb_v4.source_catalog USING gin(fts);
CREATE TABLE rkb_v4.source_passage_provenance (
 passage_id uuid PRIMARY KEY REFERENCES rkb_v4.passage_search,
 source_id uuid NOT NULL,source_revision bigint NOT NULL,data jsonb NOT NULL,
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.source_manifests
);
CREATE INDEX source_passage_provenance_source ON rkb_v4.source_passage_provenance(source_id,source_revision);
CREATE TRIGGER immutable_source_catalog BEFORE UPDATE OR DELETE ON rkb_v4.source_catalog
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE TRIGGER immutable_source_passage_provenance BEFORE UPDATE OR DELETE ON rkb_v4.source_passage_provenance
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.reject_revision_update();
CREATE FUNCTION rkb_v4.project_source_manifest_v1() RETURNS trigger
 LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE metadata jsonb:=NEW.manifest->'metadata';item jsonb;author_text text;
BEGIN
 SELECT string_agg(x,E'\n' ORDER BY ordinal) INTO author_text
 FROM jsonb_array_elements_text(metadata->'authors') WITH ORDINALITY a(x,ordinal);
 INSERT INTO rkb_v4.source_catalog(source_id,source_revision,metadata,search_text)
 VALUES(NEW.source_id,NEW.source_revision,metadata,coalesce(metadata->>'title','')||E'\n'||coalesce(author_text,''));
 FOR item IN SELECT value FROM jsonb_array_elements(NEW.manifest->'passages') LOOP
  INSERT INTO rkb_v4.source_passage_provenance VALUES((item->>'passage_id')::uuid,NEW.source_id,NEW.source_revision,item);
 END LOOP;
 RETURN NEW;
END $$;
CREATE TRIGGER source_manifest_projected AFTER INSERT ON rkb_v4.source_manifests
 FOR EACH ROW EXECUTE FUNCTION rkb_v4.project_source_manifest_v1();

-- Python str.strip whitespace, explicitly fixed rather than database locale.
CREATE FUNCTION rkb_v4.source_text_trim_v1(value text) RETURNS text
 LANGUAGE sql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT btrim(value,U&'\0009\000a\000b\000c\000d\001c\001d\001e\001f\0020\0085\00a0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200a\2028\2029\202f\205f\3000')
$$;
CREATE FUNCTION rkb_v4.check_source_passage_manifest_v1(doc jsonb) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'source_manifest';item jsonb;span jsonb;r jsonb;p jsonb;refs jsonb;
 seen text[]:='{}';selected text[];page_ids text[];parts text[];key text;begin_offset integer;end_offset integer;last_offsets jsonb;
BEGIN
 IF jsonb_typeof(m->'passages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'passages') NOT BETWEEN 1 AND 100
 OR jsonb_array_length(m->'passages')<>jsonb_array_length(doc->'passages') THEN RETURN false;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(m->'passages') LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object'
   OR NOT item ?& ARRAY['passage_id','region_ids','footnote_region_ids','page_ids','source_spans']
   OR item-ARRAY['passage_id','region_ids','footnote_region_ids','page_ids','source_spans']<>'{}'::jsonb
   OR item->>'passage_id'=ANY(seen) THEN RETURN false;END IF;
  SELECT x INTO STRICT p FROM jsonb_array_elements(doc->'passages') x WHERE x->>'passage_id'=item->>'passage_id';
  seen:=array_append(seen,item->>'passage_id');selected:='{}';page_ids:='{}';parts:='{}';last_offsets:='{}';
  FOREACH key IN ARRAY ARRAY['region_ids','footnote_region_ids','page_ids'] LOOP
   refs:=item->key;
   IF jsonb_typeof(refs) IS DISTINCT FROM 'array' OR jsonb_array_length(refs)>CASE key WHEN 'region_ids' THEN 100
    WHEN 'footnote_region_ids' THEN 50 ELSE 20 END
    OR (SELECT count(DISTINCT v) FROM jsonb_array_elements(refs) v)<>jsonb_array_length(refs) THEN RETURN false;END IF;
  END LOOP;
  IF jsonb_array_length(item->'region_ids')=0 OR jsonb_array_length(item->'page_ids')=0
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(item->'region_ids') x WHERE item->'footnote_region_ids' @> jsonb_build_array(x))
   THEN RETURN false;END IF;
  FOR refs IN SELECT value FROM jsonb_array_elements((item->'region_ids')||(item->'footnote_region_ids')) LOOP
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
   IF NOT item->'page_ids' @> jsonb_build_array(r->'page_id') THEN RETURN false;END IF;
   page_ids:=array_append(page_ids,r->>'page_id');
  END LOOP;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(item->'page_ids') x WHERE NOT x=ANY(page_ids)) THEN RETURN false;END IF;
  IF jsonb_typeof(item->'source_spans') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'source_spans')>100 THEN RETURN false;END IF;
  FOR span IN SELECT value FROM jsonb_array_elements(item->'source_spans') LOOP
   IF jsonb_typeof(span) IS DISTINCT FROM 'object' OR NOT span ?& ARRAY['region_id','start','end','source_text_sha256']
    OR span-ARRAY['region_id','start','end','source_text_sha256']<>'{}'::jsonb
    OR NOT item->'region_ids' @> jsonb_build_array(span->'region_id')
    OR jsonb_typeof(span->'start') IS DISTINCT FROM 'number' OR span->>'start' !~ '^(0|[1-9][0-9]*)$'
    OR jsonb_typeof(span->'end') IS DISTINCT FROM 'number' OR span->>'end' !~ '^[1-9][0-9]*$' THEN RETURN false;END IF;
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=span->'region_id';
   begin_offset:=(span->>'start')::integer;end_offset:=(span->>'end')::integer;
   IF span->>'source_text_sha256' IS DISTINCT FROM r->>'source_text_sha256' OR begin_offset>=end_offset
    OR end_offset>length(r->>'source_text') OR begin_offset<coalesce((last_offsets->>(span->>'region_id'))::integer,0) THEN RETURN false;END IF;
   last_offsets:=last_offsets||jsonb_build_object(span->>'region_id',end_offset);
   selected:=array_append(selected,span->>'region_id');
   parts:=array_append(parts,rkb_v4.source_text_trim_v1(substring(r->>'source_text' FROM begin_offset+1 FOR end_offset-begin_offset)));
  END LOOP;
  IF jsonb_array_length(item->'source_spans')>0 THEN
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(item->'region_ids') x WHERE NOT x=ANY(selected)) THEN RETURN false;END IF;
  ELSE
   FOR refs IN SELECT value FROM jsonb_array_elements(item->'region_ids') LOOP
    SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
    IF length(rkb_v4.source_text_trim_v1(r->>'source_text'))>0 THEN parts:=array_append(parts,rkb_v4.source_text_trim_v1(r->>'source_text'));END IF;
   END LOOP;
  END IF;
  FOR refs IN SELECT value FROM jsonb_array_elements(item->'footnote_region_ids') LOOP
   SELECT x INTO STRICT r FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->'region_id'=refs;
   IF length(rkb_v4.source_text_trim_v1(r->>'source_text'))>0 THEN parts:=array_append(parts,'[Footnote] '||rkb_v4.source_text_trim_v1(r->>'source_text'));END IF;
  END LOOP;
  IF rkb_v4.source_text_trim_v1(array_to_string(parts,E'\n')) IS DISTINCT FROM p->>'body' THEN RETURN false;END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.check_source_manifest_v1(doc jsonb,source uuid)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE m jsonb:=doc->'source_manifest';original jsonb:=doc->'original';receipt jsonb;raw bytea;
 metadata jsonb;catalog jsonb;page jsonb;region jsonb;render jsonb;value jsonb;key text;
 pages uuid[]:='{}';regions uuid[]:='{}';page_index integer:=0;matching jsonb;
BEGIN
 IF jsonb_typeof(m) IS DISTINCT FROM 'object'
 OR NOT m ?& ARRAY['codec','source_id','source_revision','original_receipt_b64','native_pages_proof_sha256','metadata','pages','regions','passages']
 OR m-ARRAY['codec','source_id','source_revision','original_receipt_b64','native_pages_proof_sha256','metadata','pages','regions','passages']<>'{}'::jsonb
 OR m->>'codec' IS DISTINCT FROM 'rkb.source-manifest.v1' OR m->>'source_id' IS DISTINCT FROM source::text
 OR m->'source_revision' IS DISTINCT FROM '1'::jsonb OR m->>'native_pages_proof_sha256' !~ '^[0-9a-f]{64}$'
 OR jsonb_typeof(m->'original_receipt_b64') IS DISTINCT FROM 'string'
 OR length(m->>'original_receipt_b64') NOT BETWEEN 1 AND 16384
 OR m->>'original_receipt_b64' !~ '^[A-Za-z0-9+/]*={0,2}$' THEN RETURN false;END IF;
 raw:=decode(m->>'original_receipt_b64','base64');receipt:=convert_from(raw,'UTF8')::jsonb;
 IF replace(encode(raw,'base64'),E'\n','')<>m->>'original_receipt_b64'
 OR jsonb_typeof(receipt) IS DISTINCT FROM 'object'
 OR NOT receipt ?& ARRAY['codec','source_id','ingestion_id','request_id','revision','sha256','size','key','native_version','format','page_count','parser_sha256','acquisition_sha256','native_verification_sha256']
 OR receipt-ARRAY['codec','source_id','ingestion_id','request_id','revision','sha256','size','key','native_version','format','page_count','parser_sha256','acquisition_sha256','native_verification_sha256']<>'{}'::jsonb
 OR receipt->>'codec' IS DISTINCT FROM 'rkb.original-receipt.v1' OR receipt->>'source_id' IS DISTINCT FROM source::text
 OR receipt->'revision' IS DISTINCT FROM '1'::jsonb OR receipt->>'format' IS DISTINCT FROM 'pdf'
 OR (receipt->>'ingestion_id')::uuid::text IS DISTINCT FROM receipt->>'ingestion_id'
 OR (receipt->>'request_id')::uuid::text IS DISTINCT FROM receipt->>'request_id'
 OR jsonb_typeof(receipt->'page_count') IS DISTINCT FROM 'number' OR receipt->>'page_count' !~ '^[1-9][0-9]*$'
 OR (receipt->>'page_count')::integer NOT BETWEEN 1 AND 32
 OR jsonb_typeof(receipt->'size') IS DISTINCT FROM 'number' OR receipt->>'size' !~ '^[1-9][0-9]*$'
 OR (receipt->>'size')::bigint NOT BETWEEN 1 AND 536870912
 OR jsonb_typeof(receipt->'key') IS DISTINCT FROM 'string' OR length(receipt->>'key') NOT BETWEEN 1 AND 1024
 OR receipt->>'key' ~ '://|(^|/)(\.|\.\.)?(/|$)'
 OR jsonb_typeof(receipt->'native_version') IS DISTINCT FROM 'string' OR length(receipt->>'native_version') NOT BETWEEN 1 AND 512
 OR original IS DISTINCT FROM jsonb_build_object('sha256',receipt->>'sha256','size',receipt->'size',
    'ref','archive:'||(receipt->>'key'),'receipt_sha256',encode(sha256(raw),'hex')) THEN RETURN false;END IF;
 FOREACH key IN ARRAY ARRAY['sha256','parser_sha256','acquisition_sha256','native_verification_sha256'] LOOP
  IF receipt->>key !~ '^[0-9a-f]{64}$' THEN RETURN false;END IF;
 END LOOP;
 metadata:=m->'metadata';catalog:=metadata->'catalog';
 IF jsonb_typeof(metadata) IS DISTINCT FROM 'object'
 OR NOT metadata ?& ARRAY['title','authors','publication_year','language','catalog']
 OR metadata-ARRAY['title','authors','publication_year','language','catalog']<>'{}'::jsonb
 -- jsonb rendering has additional insignificant spaces; full input is bounded.
 OR octet_length(metadata::text)>32768
 OR jsonb_typeof(metadata->'authors') IS DISTINCT FROM 'array' OR jsonb_array_length(metadata->'authors')>50
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(metadata->'authors') a WHERE jsonb_typeof(a)<>'string' OR length(a#>>'{}')>4000)
 OR metadata->'title'<>'null'::jsonb AND (jsonb_typeof(metadata->'title')<>'string' OR length(metadata->>'title')>500)
 OR metadata->'language'<>'null'::jsonb AND (jsonb_typeof(metadata->'language')<>'string' OR length(metadata->>'language')>80)
 OR metadata->'publication_year'<>'null'::jsonb AND (jsonb_typeof(metadata->'publication_year')<>'number'
   OR metadata->>'publication_year' !~ '^[1-9][0-9]*$' OR (metadata->>'publication_year')::integer NOT BETWEEN 1 AND 3000)
 THEN RETURN false;END IF;
 IF catalog<>'null'::jsonb THEN
  IF jsonb_typeof(catalog) IS DISTINCT FROM 'object'
   OR NOT catalog ?& ARRAY['kind','parent_id','region_ids','subtitle','editors','translators','compilers','publisher','date','edition','volume','issue','identifiers','form','purpose','searchable','topics','annotation','annotation_origin','annotation_page','cover_page','cover_kind','physical_page_start','physical_page_end']
   OR catalog-ARRAY['kind','parent_id','region_ids','subtitle','editors','translators','compilers','publisher','date','edition','volume','issue','identifiers','form','purpose','searchable','topics','annotation','annotation_origin','annotation_page','cover_page','cover_kind','physical_page_start','physical_page_end']<>'{}'::jsonb
   OR catalog->>'kind' IS DISTINCT FROM 'book' OR catalog->'parent_id' IS DISTINCT FROM 'null'::jsonb
   OR catalog->'region_ids' IS DISTINCT FROM '[]'::jsonb OR jsonb_typeof(catalog->'searchable') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(catalog->'identifiers') IS DISTINCT FROM 'array' OR jsonb_array_length(catalog->'identifiers')>30
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(catalog->'identifiers') i WHERE jsonb_typeof(i)<>'object') THEN RETURN false;END IF;
  FOREACH key IN ARRAY ARRAY['editors','translators','compilers','topics'] LOOP
   IF jsonb_typeof(catalog->key) IS DISTINCT FROM 'array' OR jsonb_array_length(catalog->key)>50
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(catalog->key) a WHERE jsonb_typeof(a)<>'string' OR length(a#>>'{}')>4000) THEN RETURN false;END IF;
  END LOOP;
  FOREACH key IN ARRAY ARRAY['annotation_page','cover_page','physical_page_start','physical_page_end'] LOOP
   IF catalog->key<>'null'::jsonb AND (jsonb_typeof(catalog->key)<>'number' OR catalog->>key !~ '^(0|[1-9][0-9]*)$'
    OR (catalog->>key)::bigint>2147483647) THEN RETURN false;END IF;
  END LOOP;
  IF catalog->'physical_page_start'<>'null'::jsonb AND catalog->'physical_page_end'<>'null'::jsonb
    AND (catalog->>'physical_page_start')::bigint>(catalog->>'physical_page_end')::bigint THEN RETURN false;END IF;
 END IF;
 IF jsonb_typeof(m->'pages') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'pages')<>(receipt->>'page_count')::integer THEN RETURN false;END IF;
 FOR page IN SELECT x FROM jsonb_array_elements(m->'pages') x LOOP
  IF jsonb_typeof(page) IS DISTINCT FROM 'object'
   OR NOT page ?& ARRAY['page_id','physical_page_index','printed_page_number','width','height','page_receipt_sha256','image_sha256','native_text_sha256','native_blocks_sha256','preview_render']
   OR page-ARRAY['page_id','physical_page_index','printed_page_number','width','height','page_receipt_sha256','image_sha256','native_text_sha256','native_blocks_sha256','preview_render']<>'{}'::jsonb
   OR page->'physical_page_index' IS DISTINCT FROM to_jsonb(page_index)
   OR (page->>'page_id')::uuid=ANY(pages) OR (page->>'page_id')::uuid::text IS DISTINCT FROM page->>'page_id'
   OR page->'printed_page_number'<>'null'::jsonb AND (jsonb_typeof(page->'printed_page_number')<>'string' OR length(page->>'printed_page_number')>80)
   THEN RETURN false;END IF;
  pages:=array_append(pages,(page->>'page_id')::uuid);page_index:=page_index+1;
  FOREACH key IN ARRAY ARRAY['page_receipt_sha256','image_sha256','native_text_sha256','native_blocks_sha256'] LOOP
   IF page->>key !~ '^[0-9a-f]{64}$' THEN RETURN false;END IF;
  END LOOP;
  FOREACH key IN ARRAY ARRAY['width','height'] LOOP
   IF jsonb_typeof(page->key) IS DISTINCT FROM 'number' OR (page->>key)::numeric<=0 OR (page->>key)::numeric>100000 THEN RETURN false;END IF;
  END LOOP;
  render:=page->'preview_render';
  IF jsonb_typeof(render) IS DISTINCT FROM 'object' OR NOT render ?& ARRAY['policy','width','height','scale','max_edge','jpeg_quality','byte_limit','preview_only']
   OR render-ARRAY['policy','width','height','scale','max_edge','jpeg_quality','byte_limit','preview_only']<>'{}'::jsonb
   OR render->>'policy' IS DISTINCT FROM 'rkb.pdf-preview.v1' OR render->'preview_only' IS DISTINCT FROM 'true'::jsonb
   OR render->'byte_limit' IS DISTINCT FROM '262144'::jsonb OR jsonb_typeof(render->'scale') IS DISTINCT FROM 'number'
   OR (render->>'scale')::numeric<=0 OR (render->>'scale')::numeric>100 THEN RETURN false;END IF;
  FOREACH key IN ARRAY ARRAY['width','height','max_edge','jpeg_quality'] LOOP
   IF jsonb_typeof(render->key) IS DISTINCT FROM 'number' OR render->>key !~ '^[1-9][0-9]*$'
    OR (render->>key)::bigint>CASE WHEN key='jpeg_quality' THEN 100 ELSE 20000 END THEN RETURN false;END IF;
  END LOOP;
 END LOOP;
 IF jsonb_typeof(m->'regions') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'regions') NOT BETWEEN 1 AND 800
  OR jsonb_array_length(m->'regions')<>jsonb_array_length(doc->'semantic'->'regions') THEN RETURN false;END IF;
 FOR region IN SELECT x FROM jsonb_array_elements(m->'regions') x LOOP
  IF jsonb_typeof(region) IS DISTINCT FROM 'object'
   OR NOT region ?& ARRAY['region_id','page_id','kind','bbox','reading_order','column_id','text_sha256','native_block_index','native_block_sha256','start_offset','end_offset']
   OR region-ARRAY['region_id','page_id','kind','bbox','reading_order','column_id','text_sha256','native_block_index','native_block_sha256','start_offset','end_offset']<>'{}'::jsonb
   OR (region->>'region_id')::uuid=ANY(regions) OR NOT((region->>'page_id')::uuid=ANY(pages))
   OR region->>'text_sha256' !~ '^[0-9a-f]{64}$' OR region->>'native_block_sha256' !~ '^[0-9a-f]{64}$'
   OR region->>'kind' NOT IN ('heading','body','caption','footnote','figure','table','header','footer','page_number','marginalia')
   OR region->'column_id'<>'null'::jsonb AND (jsonb_typeof(region->'column_id')<>'string' OR length(region->>'column_id')>120)
   THEN RETURN false;END IF;
  regions:=array_append(regions,(region->>'region_id')::uuid);
  SELECT x INTO STRICT matching FROM jsonb_array_elements(doc->'semantic'->'regions') x WHERE x->>'region_id'=region->>'region_id';
  IF region->>'page_id' IS DISTINCT FROM matching->>'page_id' OR region->>'text_sha256' IS DISTINCT FROM matching->>'source_text_sha256'
   OR jsonb_typeof(region->'bbox') IS DISTINCT FROM 'object' OR NOT(region->'bbox') ?& ARRAY['left','top','right','bottom']
   OR (region->'bbox')-ARRAY['left','top','right','bottom']<>'{}'::jsonb THEN RETURN false;END IF;
  FOREACH key IN ARRAY ARRAY['left','top','right','bottom'] LOOP
   IF jsonb_typeof(region->'bbox'->key) IS DISTINCT FROM 'number' OR region->'bbox'->>key !~ '^(0|[1-9][0-9]*)$'
    OR (region->'bbox'->>key)::bigint>1000 THEN RETURN false;END IF;
  END LOOP;
  IF (region->'bbox'->>'left')::integer>=(region->'bbox'->>'right')::integer
   OR (region->'bbox'->>'top')::integer>=(region->'bbox'->>'bottom')::integer THEN RETURN false;END IF;
  FOREACH key IN ARRAY ARRAY['reading_order','native_block_index','start_offset','end_offset'] LOOP
   IF jsonb_typeof(region->key) IS DISTINCT FROM 'number' OR region->>key !~ '^(0|[1-9][0-9]*)$'
    OR (region->>key)::bigint>2147483647 THEN RETURN false;END IF;
  END LOOP;
  IF (region->>'end_offset')::bigint-(region->>'start_offset')::bigint<>length(matching->>'source_text')
   OR length(btrim(matching->>'source_text'))=0 THEN RETURN false;END IF;
 END LOOP;
 RETURN rkb_v4.check_source_passage_manifest_v1(doc);
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.find_source_catalog_v1(query_text text,budget integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,metadata jsonb)
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT c.source_id,c.source_revision,v.eligibility_revision,c.metadata
 FROM rkb_v4.source_catalog c JOIN rkb_v4.active_revision a ON a.source_id=c.source_id AND a.revision=c.source_revision
 JOIN rkb_v4.sources s ON s.source_id=c.source_id JOIN rkb_v4.source_visibility v ON v.source_id=c.source_id
 WHERE s.accepted AND NOT s.withdrawn AND c.metadata->>'title' IS NOT NULL
 AND c.fts @@ plainto_tsquery('pg_catalog.simple'::regconfig,query_text)
 ORDER BY c.source_id LIMIT greatest(0,least(budget,8))
$$;
CREATE FUNCTION rkb_v4.source_passage_metadata_v1(wanted uuid[])
RETURNS TABLE(passage_id uuid,metadata jsonb)
 LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT p.passage_id,jsonb_build_object('catalog',m.manifest->'metadata','original_sha256',o.original_sha256,
  'source_revision',p.source_revision,'passage',p.data,
  'pages',(SELECT jsonb_agg(x) FROM jsonb_array_elements(m.manifest->'pages') x WHERE p.data->'page_ids' @> jsonb_build_array(x->'page_id')),
  'regions',(SELECT jsonb_agg(x) FROM jsonb_array_elements(m.manifest->'regions') x WHERE
    ((p.data->'region_ids')||(p.data->'footnote_region_ids')) @> jsonb_build_array(x->'region_id')))
 FROM rkb_v4.source_passage_provenance p
 JOIN rkb_v4.source_manifests m USING(source_id,source_revision)
 JOIN rkb_v4.source_import_origins o ON o.source_id=p.source_id AND o.revision=p.source_revision
 JOIN rkb_v4.active_revision a ON a.source_id=p.source_id AND a.revision=p.source_revision
 JOIN rkb_v4.sources s ON s.source_id=p.source_id
 WHERE p.passage_id=ANY(wanted[1:5]) AND s.accepted AND NOT s.withdrawn
$$;

CREATE FUNCTION rkb_v4.check_source_import_v4(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE doc jsonb;legacy bytea;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 786432 THEN RETURN false;END IF;
 doc:=convert_from(p,'UTF8')::jsonb;
 IF doc->>'codec' IS DISTINCT FROM 'rkb.source-import.v4' OR NOT doc ? 'source_manifest' THEN RETURN false;END IF;
 legacy:=convert_to(((doc-'source_manifest')||jsonb_build_object('codec','rkb.source-import.v2'))::text,'UTF8');
 RETURN rkb_v4.check_source_import_v3(legacy,ids,expected,logical_time)
    AND rkb_v4.check_source_manifest_v1(doc,(ids->>'source_id')::uuid);
EXCEPTION WHEN data_exception THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.apply_source_import_v4(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE state rkb_v4.recovery_state; prior rkb_v4.read_frontier; frontier rkb_v4.read_frontier;
        source uuid; doc jsonb; item jsonb; research jsonb; original jsonb; eligibility bigint; story jsonb; variant jsonb;
BEGIN
    SELECT * INTO STRICT state FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
    SELECT * INTO STRICT prior FROM rkb_v4.read_frontier WHERE singleton FOR UPDATE;
    IF NOT rkb_v4.check_source_import_v4(p,ids,expected,logical_time) THEN
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
    INSERT INTO rkb_v4.source_manifests VALUES(source,1,doc->'source_manifest',encode(sha256(p),'hex'),
        decode(doc->'source_manifest'->>'original_receipt_b64','base64'));
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

CREATE FUNCTION rkb_v4.read_source_manifest_v1(wanted uuid)
RETURNS jsonb LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT m.manifest FROM rkb_v4.source_manifests m JOIN rkb_v4.sources s USING(source_id)
 JOIN rkb_v4.active_revision a ON a.source_id=m.source_id AND a.revision=m.source_revision
 WHERE m.source_id=wanted AND s.accepted AND NOT s.withdrawn
$$;
REVOKE ALL ON rkb_v4.source_manifests FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.check_source_manifest_v1(jsonb,uuid),rkb_v4.read_source_manifest_v1(uuid) FROM PUBLIC;
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
  IF c->>'route'='source.import.v4' AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_manifests m
   WHERE m.source_id=source AND m.source_revision=1 AND m.manifest=doc->'source_manifest'
    AND m.payload_sha256=encode(sha256(decode(c->>'payload_b64','base64')),'hex')
    AND m.original_receipt=decode(doc->'source_manifest'->>'original_receipt_b64','base64')) THEN RETURN false;END IF;
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
