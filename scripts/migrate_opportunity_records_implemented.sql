-- Suivi « maillage réalisé » + contrainte d’unicité (project, source, cible directionnelle).
-- À exécuter sur la base Silo PostgreSQL (après sauvegarde si production).

ALTER TABLE opportunity_records
    ADD COLUMN IF NOT EXISTS implemented BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE opportunity_records
    ADD COLUMN IF NOT EXISTS implemented_at TIMESTAMP NULL;
ALTER TABLE opportunity_records
    ADD COLUMN IF NOT EXISTS implemented_by VARCHAR(255) NULL;
ALTER TABLE opportunity_records
    ADD COLUMN IF NOT EXISTS implementation_note TEXT NULL;

-- Déduplication avant contrainte unique (garde la ligne la plus récente par paire).
DELETE FROM opportunity_records a
    USING opportunity_records b
WHERE a.id < b.id
  AND a.project_id = b.project_id
  AND a.source_page_id = b.source_page_id
  AND a.target_page_id = b.target_page_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_record_project_src_tgt
    ON opportunity_records (project_id, source_page_id, target_page_id);
