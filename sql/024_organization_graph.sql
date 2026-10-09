BEGIN;
-- Optional compatibility migration for installations still running the
-- legacy PostgreSQL graph authority. The production RKB authority is SQLite;
-- do not move identities or evidence to PostgreSQL.
ALTER TABLE public.rkb_entities
  DROP CONSTRAINT IF EXISTS rkb_entities_kind_check;
ALTER TABLE public.rkb_entities
  ADD CONSTRAINT rkb_entities_kind_check
  CHECK (kind IN ('person','organization','event','historical_thread','poi_ref'));

ALTER TABLE public.rkb_entity_relations
  DROP CONSTRAINT IF EXISTS rkb_entity_relations_kind_check;
ALTER TABLE public.rkb_entity_relations
  ADD CONSTRAINT rkb_entity_relations_kind_check
  CHECK (kind IN (
    'participated_in','occurred_at','member_of',
    'affiliated_with','operated_at','predecessor_of','founded_by'
  ));

-- Preserve old RLS and evidence checks. Expand only explicit directed shapes;
-- historical organization identity never follows identical display names.
CREATE OR REPLACE FUNCTION public.rkb_graph_evidence_guard()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=public AS $$
DECLARE
  item jsonb;
  sk text;
  tk text;
BEGIN
  IF TG_TABLE_NAME='rkb_entity_relations' THEN
    SELECT kind INTO sk FROM public.rkb_entities
      WHERE id=NEW.source_id AND owner_user_id=public.rkb_current_actor_id();
    SELECT kind INTO tk FROM public.rkb_entities
      WHERE id=NEW.target_id AND owner_user_id=public.rkb_current_actor_id();
    IF sk IS NULL OR tk IS NULL OR NEW.source_id=NEW.target_id
       OR NOT (
         (NEW.kind='participated_in' AND sk IN ('person','organization') AND tk='event') OR
         (NEW.kind='occurred_at' AND sk='event' AND tk='poi_ref') OR
         (NEW.kind='member_of' AND sk IN ('person','organization','event','poi_ref') AND tk='historical_thread') OR
         (NEW.kind='affiliated_with' AND sk='person' AND tk='organization') OR
         (NEW.kind='operated_at' AND sk='organization' AND tk='poi_ref') OR
         (NEW.kind='predecessor_of' AND sk='organization' AND tk='organization') OR
         (NEW.kind='founded_by' AND sk='organization' AND tk='person')
       ) THEN
      RAISE EXCEPTION 'invalid graph relation shape';
    END IF;
    FOR item IN SELECT value FROM pg_catalog.jsonb_array_elements(NEW.evidence) LOOP
      IF NOT public.rkb_graph_check_evidence(NEW.document_id,NEW.revision,item) THEN
        RAISE EXCEPTION 'invalid graph evidence';
      END IF;
    END LOOP;
  ELSE
    IF NOT EXISTS (
      SELECT 1 FROM public.rkb_entities
      WHERE id=NEW.entity_id AND owner_user_id=public.rkb_current_actor_id()
    ) THEN RAISE EXCEPTION 'entity ownership required'; END IF;
    IF NOT public.rkb_graph_check_evidence(NEW.document_id,NEW.revision,NEW.evidence) THEN
      RAISE EXCEPTION 'invalid graph evidence';
    END IF;
  END IF;
  RETURN NEW;
END $$;
COMMIT;
