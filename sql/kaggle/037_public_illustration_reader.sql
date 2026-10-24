-- Existing reader role needs accepted figure metadata.
-- No new role, participant capability, table, publication or source transition.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
  GRANT SELECT ON rkb_v4.source_illustrations TO rkb_reader;
  GRANT SELECT(source_id,batch_index,component,manifest,media,figure_ids)
   ON rkb_v4.source_batch_publications TO rkb_reader;
 END IF;
END $$;
