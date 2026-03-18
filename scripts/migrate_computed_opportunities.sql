-- Table pour stocker les opportunités pré-calculées (vue Toutes global)
-- Permet d'éviter le calcul O(n²) à chaque requête
-- Stocke les paires avec similarity >= 0.7 ; le filtrage par seuil se fait à la lecture
CREATE TABLE IF NOT EXISTS computed_opportunities (
    id SERIAL PRIMARY KEY,
    project_id VARCHAR(36) NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source_page_id VARCHAR(64) NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    target_page_id VARCHAR(64) NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    similarity REAL NOT NULL,
    source_url TEXT,
    target_url TEXT,
    zone_texte TEXT,
    phrase_ancre_proposee VARCHAR(512),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(project_id, source_page_id, target_page_id)
);

CREATE INDEX IF NOT EXISTS idx_computed_opp_project ON computed_opportunities(project_id);
CREATE INDEX IF NOT EXISTS idx_computed_opp_computed_at ON computed_opportunities(project_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_computed_opp_similarity ON computed_opportunities(project_id, similarity DESC);
