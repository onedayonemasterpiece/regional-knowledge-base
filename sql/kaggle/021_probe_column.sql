-- Live migration acceptance: additive nullable metadata; no data rewrite.
ALTER TABLE rkb_v4.sources ADD COLUMN migration_probe text;
