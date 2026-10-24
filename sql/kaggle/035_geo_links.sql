-- Native GEO decisions are append-only accepted commands. Source data and SQL001-034 remain immutable.
-- Only the existing ordered schema migration owner installs this file.
CREATE TABLE rkb_v4.geo_links (
 source_id uuid NOT NULL,
 mention_id uuid NOT NULL,
 accepted_sequence bigint NOT NULL REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED,
 source_revision bigint NOT NULL CHECK(source_revision>=1),
 status text NOT NULL CHECK(status IN ('candidate','unresolved','ambiguous')),
 poi_ref text CHECK(poi_ref IS NULL OR octet_length(poi_ref) BETWEEN 1 AND 256),
 url text CHECK(url IS NULL OR octet_length(url) BETWEEN 1 AND 2048),
 checked_at text CHECK(checked_at IS NULL OR octet_length(checked_at) BETWEEN 1 AND 64),
 reason text NOT NULL CHECK(octet_length(reason) BETWEEN 1 AND 1024),
 PRIMARY KEY(source_id,mention_id,accepted_sequence),
 FOREIGN KEY(source_id,mention_id) REFERENCES rkb_v4.source_entity_mentions(source_id,mention_id),
 FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions(source_id,revision),
 CHECK ((status='candidate')=(poi_ref IS NOT NULL)),
 CHECK ((url IS NULL)=(checked_at IS NULL))
);
CREATE INDEX geo_links_latest_lookup ON rkb_v4.geo_links(source_id,mention_id,accepted_sequence DESC);

-- Every accepted sequence retains the original decision. The latest projection
-- never overwrites or deletes history and is scoped again by source read gates.
CREATE VIEW rkb_v4.geo_links_latest WITH (security_invoker=true) AS
 SELECT DISTINCT ON (source_id,mention_id)
  source_id,mention_id,accepted_sequence,source_revision,status,poi_ref,url,checked_at,reason
 FROM rkb_v4.geo_links
 ORDER BY source_id,mention_id,accepted_sequence DESC;

-- Exactly one bounded SELECT for unresolved and new source-place work.
-- Latest candidate is settled for its revision; a revised source may be queued again.
-- The owner can correct any previously accepted pair through a NEW operation_key.
CREATE FUNCTION rkb_v4.read_geo_queue_v1(take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,record_id text,
 data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
 SELECT m.source_id,a.revision,v.eligibility_revision,m.mention_id::text,
        m.data||jsonb_build_object('entity_id',m.entity_id,
          'latest_geo_resolution',CASE WHEN latest.accepted_sequence IS NULL THEN NULL
          ELSE jsonb_build_object('accepted_sequence',latest.accepted_sequence,
             'source_revision',latest.source_revision,'status',latest.status,
             'poi_ref',latest.poi_ref,'url',latest.url,
             'checked_at',latest.checked_at,'reason',latest.reason) END),
        m.data->'evidence'->>'exact_quote',s.bibliography
 FROM rkb_v4.source_entity_mentions m
 JOIN rkb_v4.source_entity_ids e ON e.entity_id=m.entity_id AND e.kind='poi_ref'
 JOIN rkb_v4.source_graph_documents g ON g.source_id=m.source_id
 JOIN rkb_v4.sources s ON s.source_id=m.source_id
 JOIN rkb_v4.active_revision a ON a.source_id=m.source_id AND a.revision=g.source_revision
 JOIN rkb_v4.source_visibility v ON v.source_id=m.source_id
 JOIN rkb_v4.source_graph_receipts r ON r.source_id=m.source_id
 LEFT JOIN LATERAL (
   SELECT l.accepted_sequence,l.source_revision,l.status,l.poi_ref,l.url,l.checked_at,l.reason
   FROM rkb_v4.geo_links l
   WHERE l.source_id=m.source_id AND l.mention_id=m.mention_id
   ORDER BY l.accepted_sequence DESC LIMIT 1
 ) latest ON true
 WHERE s.accepted AND NOT s.withdrawn
   AND (latest.accepted_sequence IS NULL OR latest.source_revision<>a.revision
        OR latest.status IN ('unresolved','ambiguous'))
 ORDER BY m.source_id,m.mention_id
 LIMIT least(greatest(coalesce(take,1),1),5)
$$;

-- Full codec, field types, URL/POI/UTC and canonical JSON shape are validated in
-- kaggle_source_geo_mvp.py before spool acceptance. SQL verifies only the IDs
-- against accepted source/mention proof and the current active source revision.
CREATE FUNCTION rkb_v4.check_geo_resolve_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;source uuid;mention uuid;want_revision bigint;
BEGIN
 IF octet_length(p) NOT BETWEEN 1 AND 8192 THEN RETURN false;END IF;
 source:=(ids->>'source_id')::uuid;
 mention:=(ids->>'mention_id')::uuid;
 want_revision:=(expected->>'source_revision')::bigint;
 d:=convert_from(p,'UTF8')::jsonb;
 IF d->>'source_id' IS DISTINCT FROM source::text
    OR d->>'mention_id' IS DISTINCT FROM mention::text THEN RETURN false;END IF;
 RETURN EXISTS (
  SELECT 1 FROM rkb_v4.source_entity_mentions m
  JOIN rkb_v4.source_entity_ids e ON e.entity_id=m.entity_id AND e.kind='poi_ref'
  JOIN rkb_v4.source_graph_documents g ON g.source_id=m.source_id
  JOIN rkb_v4.sources s ON s.source_id=m.source_id
  JOIN rkb_v4.active_revision a ON a.source_id=m.source_id
     AND a.revision=g.source_revision AND a.revision=want_revision
  JOIN rkb_v4.source_graph_receipts r ON r.source_id=m.source_id
  WHERE m.source_id=source AND m.mention_id=mention AND s.accepted AND NOT s.withdrawn
 );
EXCEPTION WHEN data_exception OR invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE FUNCTION rkb_v4.apply_geo_resolve_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE d jsonb;source uuid;mention uuid;revision bigint;seq bigint;
BEGIN
 -- The accepted PG dispatcher locks this single writer row and inserts the
 -- immutable applied_ops receipt in the SAME transaction. No second ledger.
 SELECT applied_seq+1 INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.check_geo_resolve_v1(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'accepted GEO source integrity precondition required';
 END IF;
 d:=convert_from(p,'UTF8')::jsonb;
 source:=(ids->>'source_id')::uuid;mention:=(ids->>'mention_id')::uuid;
 revision:=(expected->>'source_revision')::bigint;
 INSERT INTO rkb_v4.geo_links(source_id,mention_id,accepted_sequence,source_revision,
    status,poi_ref,url,checked_at,reason)
 VALUES(source,mention,seq,revision,d->>'status',d->>'poi_ref',
        d->>'url',d->>'checked_at',d->>'reason');
 RETURN convert_to(jsonb_build_object('source_id',source,'mention_id',mention,
    'accepted_sequence',seq,'source_revision',revision,
    'status',d->>'status','poi_ref',d->>'poi_ref','url',d->>'url',
    'checked_at',d->>'checked_at','reason',d->>'reason',
    'logical_time',logical_time)::text,'UTF8');
END $$;

-- Same roles/read channel. All accepted writes use the existing writer authority.
REVOKE ALL ON rkb_v4.geo_links, rkb_v4.geo_links_latest FROM PUBLIC;
REVOKE ALL ON FUNCTION rkb_v4.read_geo_queue_v1(integer),
    rkb_v4.check_geo_resolve_v1(bytea,jsonb,jsonb,text),
    rkb_v4.apply_geo_resolve_v1(bytea,jsonb,jsonb,text) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
  GRANT SELECT ON rkb_v4.geo_links,rkb_v4.geo_links_latest TO rkb_reader;
  GRANT EXECUTE ON FUNCTION rkb_v4.read_geo_queue_v1(integer) TO rkb_reader;
 END IF;
END $$;
