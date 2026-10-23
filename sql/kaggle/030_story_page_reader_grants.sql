-- Story→accepted passage needs page identity/index. Raw model/image columns stay private.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
  GRANT SELECT(source_id,page_id,physical_page_index) ON rkb_v4.source_stage_pages TO rkb_reader;
 END IF;
END $$;
