# Prompt de Refactorisation — Algorithme d'Opportunités de Maillage

> **Adapté à la structure Silo existante** (tags, schéma DB, conventions)

---

## Contexte technique actuel

| Élément | Valeur actuelle |
|---------|-----------------|
| **Champ NER** | `pages.entities` (JSONB, liste de chaînes) — *pas* `ner_tags` |
| **Silo théorique** | `get_theoretical_silo_from_url(url)` → premier segment path hors codes langue |
| **Silo Louvain** | `pages.silo_id` (communauté Louvain, chaîne) |
| **Embeddings** | `pages.embedding` (pgvector 384 ou 1024) |
| **Graphe** | `pages` + `edges` (source_id, target_id) |
| **Seuils** | `COMPUTED_OPP_MIN_SIMILARITY = 0.7`, `min_similarity` par défaut 0.9 |
| **Entités invalides** | `__FETCH_FAILED__`, `__NO_ENTITIES__` à exclure |

---

## Objectif

Réduire la latence de calcul et augmenter la pertinence sémantique/topologique en :

1. **Ne recalculant rien** : utiliser `embedding` et `entities` déjà en BDD.
2. **Limitant l'analyse au voisinage** : fenêtre glissante `k=4` au lieu de O(n²).
3. **Comparant siloing théorique vs Louvain** pour détecter les opportunités de re-maillage.

---

## Instructions pour Cursor

Refactorise l'algorithme de calcul d'opportunités en suivant cette logique :

### 1. Ne recalcule rien

- Utiliser les champs `embedding` et `entities` déjà présents en BDD.
- Pour le top-k sémantique : **pgvector** `ORDER BY embedding <=> query_vector LIMIT k` (cosine distance).
- Pas de recalcul d'embeddings ni de NER à la volée.

### 2. Approche « Voisinage Connexe » (k=4)

**Input :** URL cible `U_t` (page_id).

**Méthode :** Extraire les **4 URLs les plus proches** selon deux axes :

1. **Topologique :** Liens directs (inbound + outbound via `edges`).
2. **Sémantique :** `SELECT ... ORDER BY embedding <=> (SELECT embedding FROM pages WHERE id = :page_id) LIMIT 4`.

**Union :** Construire un sous-graphe local = `{U_t} ∪ top_4_topologiques ∪ top_4_sémantiques`.

**Action :** Appliquer Louvain sur ce sous-graphe local pour vérifier si `U_t` appartient naturellement à une communauté ou est isolée.

### 3. Comparaison de Siloing

| Modèle | Méthode | Rôle |
|--------|---------|------|
| **Siloing théorique** | `get_theoretical_silo_from_url(url)` | Conformité à la structure des répertoires. |
| **Siloing Louvain** | `pages.silo_id` (déjà calculé globalement) ou Louvain local sur le sous-graphe | Détecter les opportunités « naturelles » ignorées par la structure. |

**Arbitrage :** Si l'URL est dans le silo théorique A mais que Louvain (local ou global) la place en B → alerte « re-maillage » ou « pont sémantique ».

### 4. Validation NER (entities)

- Si deux URLs partagent des entités clés dans `entities` (ex. « iPhone 15 », « Apple »), **augmenter le score d'opportunité**.
- Exclure `__FETCH_FAILED__` et `__NO_ENTITIES__`.
- Utiliser `entities` pour enrichir `phrase_ancre_proposee` (déjà fait dans `_suggest_phrase_ancre`).

### 5. Sortie

- Liste d'opportunités de liens internes basée sur le **différentiel** entre siloing théorique et Louvain.
- Prioriser la vitesse : analyse limitée au voisinage immédiat (k=4).
- Conserver la compatibilité avec :
  - `get_opportunities_for_page()` (vue par URL)
  - `get_computed_opportunities()` / `run_compute_and_store_opportunities()` (vue globale)
  - `get_top_similar_pairs_for_page()` (top N sans seuil)

### 6. Fichiers modifiés (refacto effectuée)

- `silo/database/service.py` :
  - `_get_top_semantic_neighbors_pgvector()` : top-k via pgvector `ORDER BY embedding <=>`
  - `_get_topological_neighbors()` : voisins inbound/outbound
  - `_entities_overlap_score()` : boost NER
  - `_get_silo_mismatch_alert()` : alerte théorique vs Louvain
  - `get_opportunities()` : pgvector par page, k=50
  - `get_opportunities_for_page()` : voisinage k=20, boost NER, `silo_mismatch_alert`
  - `get_top_similar_pairs_for_page()` : pgvector direct
  - `get_similarity_stats_for_page()` : pgvector top-1
  - `run_compute_and_store_opportunities()` : pgvector k=50 par page

---

## Références code existant

- `get_theoretical_silo_from_url` : `worker/url_utils.py:57`
- `get_silo_analysis` : `database/service.py:111` (comparaison theo vs réel)
- `_cosine_sim`, `_suggest_phrase_ancre`, `_extract_zone_texte` : `database/service.py`
- Modèles : `database/models.py` (Page, Edge, ComputedOpportunity)
