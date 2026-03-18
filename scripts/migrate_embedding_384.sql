-- Migration: Vector 1024 → 384 (pour e5-small)
-- À exécuter AVANT de passer à SILO_EMBEDDING_MODEL=intfloat/multilingual-e5-small
-- Les embeddings existants seront supprimés (recalculés au prochain crawl)

BEGIN;

-- Pages: recréer la colonne embedding
ALTER TABLE pages DROP COLUMN IF EXISTS embedding;
ALTER TABLE pages ADD COLUMN embedding vector(384);

-- Edges: recréer la colonne context_embedding
ALTER TABLE edges DROP COLUMN IF EXISTS context_embedding;
ALTER TABLE edges ADD COLUMN context_embedding vector(384);

COMMIT;
