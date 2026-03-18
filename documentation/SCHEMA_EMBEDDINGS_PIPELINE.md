# Schéma et récapitulatif — Pipeline Embeddings Silo

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                        PIPELINE EMBEDDINGS — Freevap FR Niv<3 v2                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     POST /api/projects/{id}/compute-embeddings     ┌──────────────────┐
  │   Frontend   │ ─────────────────────────────────────────────────► │   API Silo       │
  │  (Log8ot)    │                                                     │  (main.py:592)   │
  └──────────────┘                                                     └────────┬─────────┘
                                                                                 │
                                                                                 │ r.rpush(COMPUTE_EMBEDDINGS_QUEUE_KEY, {...})
                                                                                 ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                              REDIS (silo-redis)                                        │
  │  Queue: silo:compute_embeddings_queue                                                  │
  │  Payload: {"project_id": "d6cdf288", "page_id": null}                                  │
  └──────────────────────────────────────────────────────────────────────────────────────┘
                                                                                 │
                                                                                 │ blpop(queues, timeout=30)
                                                                                 ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                         SILO-WORKER (log8ot-silo-worker)                               │
  │  run.py → run_compute_embeddings(project_id, page_id)                                 │
  │  crawler.py → run_compute_embeddings()                                                │
  └──────────────────────────────────────────────────────────────────────────────────────┘
         │
         │  Pour chaque lot de 200 pages (DB_CHUNK_SIZE) :
         │    Pour chaque batch de 64 pages (EMBEDDING_BATCH_SIZE) :
         │      1. content_text (tronqué 3000 chars)
         │      2. extract_entities_batch() → spaCy fr_core_news_lg
         │      3. model.embed() → FastEmbed multilingual-e5-large
         │      4. session.commit() → PostgreSQL
         ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                         POSTGRESQL + pgvector (silo-db)                               │
  │  Table: pages                                                                         │
  │  Colonnes mises à jour: embedding (vector 1024), entities (jsonb)                    │
  └──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Éléments impliqués

| Élément | Rôle | Fichier / Conteneur |
|---------|------|---------------------|
| **API Silo** | Reçoit la requête, push le job dans Redis | `main.py` → `compute_embeddings_endpoint()` |
| **Redis** | File d'attente des jobs embeddings | `silo:compute_embeddings_queue` |
| **Worker** | Consomme les jobs, exécute le calcul | `worker/run.py` → `worker/crawler.py` |
| **FastEmbed** | Modèle d'embeddings (1024 dim) | `intfloat/multilingual-e5-large` |
| **spaCy** | NER pour enrichir les entités | `fr_core_news_lg` |
| **PostgreSQL** | Stockage pages + embeddings | Table `pages`, colonne `embedding` |

---

## Flux détaillé

```
1. DÉCLENCHEMENT (Frontend)
   └─► Clic "Calculer embeddings" ou "Rechercher opportunités" (si pas d'embeddings)
   └─► POST /api/projects/d6cdf288/compute-embeddings

2. API (main.py)
   └─► r.rpush("silo:compute_embeddings_queue", {"project_id": "d6cdf288"})
   └─► Réponse: {"message": "Calcul des embeddings lancé (traitement en cours)"}

3. WORKER (run.py)
   └─► blpop(queues) reçoit le job
   └─► run_compute_embeddings(project_id, page_id=None)

4. CRAWLER (crawler.py → run_compute_embeddings)
   └─► Boucle: SELECT pages WHERE embedding IS NULL LIMIT 200
   └─► _process_chunk():
       ├─► Pour chaque page: content_text[:3000]
       ├─► Batch de 64 → _embed_texts() + extract_entities_batch()
       ├─► pg.embedding = emb, pg.entities = entities
       └─► session.commit()
   └─► Répète jusqu'à plus de pages sans embedding

5. RÉSULTAT
   └─► Colonne pages.embedding remplie (vector 1024)
   └─► Colonne pages.entities remplie (NER)
   └─► Permet: recherche d'opportunités, similarité sémantique
```

---

## Logs et visibilité

| Destination | Contenu | Commande / Accès |
|-------------|---------|-------------------|
| **stdout (docker logs)** | Démarrage, batchs (si logger.info activé) | `docker logs log8ot-silo-worker -f` |
| **Redis** | Messages détaillés (_push_log) | Clé `silo:crawl_logs:d6cdf288` |
| **API** | Statut embeddings | `GET /api/projects/{id}/embeddings-status` |
| **Base** | Comptage direct | `SELECT COUNT(*) ... WHERE embedding IS NOT NULL` |

---

## Vérification que tout fonctionne

```bash
# 1. Progression en base (à exécuter plusieurs fois)
docker exec log8ot-silo-db psql -U admin -d semantic_cocoon -t -c "
SELECT COUNT(*) FILTER (WHERE embedding IS NOT NULL) as avec_embedding,
       COUNT(*) FILTER (WHERE embedding IS NULL) as sans_embedding
FROM pages WHERE project_id = 'd6cdf288';
"

# 2. Worker actif (CPU élevé = en cours)
docker top log8ot-silo-worker

# 3. Logs récents
docker logs log8ot-silo-worker --tail 30
```

---

## Paramètres configurables

| Variable | Défaut | Effet |
|----------|--------|-------|
| `SILO_EMBEDDING_BATCH_SIZE` | 64 | Pages par batch (embed + NER) |
| `SILO_EMBEDDING_MODEL` | multilingual-e5-large | Modèle FastEmbed |
| `SILO_EMBEDDING_TEXT_MAX_CHARS` | 3000 | Troncature du texte |
| `DB_CHUNK_SIZE` | 200 | Pages par requête SQL |

---

## Ordre de grandeur (projet Freevap)

| Métrique | Valeur |
|----------|--------|
| Pages totales | ~4 362 |
| Pages avec embedding (déjà) | ~1 016 (avant job) |
| Pages à traiter | ~3 346 |
| Temps par batch 64 | ~2–3 min |
| Durée totale estimée | ~35–45 min |

---

## Dépendances en aval

Une fois les embeddings terminés :

1. **Recalcul silos** (optionnel) — améliore le clustering
2. **Recherche d'opportunités** — compare similarité sémantique entre pages
3. **API opportunités** — `GET /api/projects/{id}/opportunities`
