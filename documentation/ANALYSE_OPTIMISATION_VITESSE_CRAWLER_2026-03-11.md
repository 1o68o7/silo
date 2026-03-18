# Analyse — Optimisation de la vitesse du crawler Silo

**Date :** 11 mars 2026  
**Objectif :** Identifier les goulots d'étranglement et proposer des optimisations pour réduire le temps d'exécution du crawl.

---

## 1. Vue d'ensemble des phases

| Phase | Rôle | Temps estimé (50 pages) | Goulots principaux |
|-------|------|-------------------------|--------------------|
| **Phase 1** | Fetch HTML, extraction, liens, Louvain | **~25–60 min** | Fetch séquentiel, StealthyFetcher, commits DB |
| **Phase 2** | NER, embeddings, Reasonable Surfer | **~5–15 min** | spaCy, FastEmbed, embeddings par edge |

---

## 2. Goulots d'étranglement identifiés

### 2.1 Phase 1 — Fetch HTML (principal goulot)

**Problème :** Chaque URL est fetchée **séquentiellement**, une par une.

```python
# crawler.py L177-186
while to_visit and len(visited) < max_pages:
    url, depth = to_visit.pop(0)
    ...
    html = fetch_html(url)  # Bloquant, 1 URL à la fois
```

**Impact :**
- **StealthyFetcher** (Scrapling) : lance un navigateur Chromium headless par page. Temps typique : **5–30 s/page** (network_idle=True attend le chargement complet).
- **Timeout** : 60 s par défaut (`SILO_FETCH_TIMEOUT`). Une page lente bloque tout.
- Pour 50 pages : **4–25 min** uniquement en fetch.

### 2.2 Phase 1 — Commits DB excessifs

**Problème :** `session.commit()` est appelé **3–4 fois par page** :
- Après création/mise à jour de la page
- Après création des edges
- Après mise à jour `project.urls_count` et `project.status`

```python
# crawler.py L231, L260, L263-264
session.commit()  # page
session.commit()  # edges
session.commit()  # project
```

**Impact :** Latence réseau/disk à chaque commit. Pour 50 pages : ~150–200 commits.

### 2.3 Phase 1 — Requêtes N+1 pour les stubs et edges

**Problème :** Pour chaque lien découvert, une requête `session.query(Page).filter(...).first()` et `session.query(Edge).filter(...).first()`.

```python
# crawler.py L240-248
if not session.query(Page).filter(Page.id == tid, ...).first():
    stub = Page(...)
# L244-248
if not session.query(Edge).filter(...).first():
    edge = Edge(...)
```

**Impact :** Si une page a 20 liens → 40 requêtes SQL supplémentaires par page.

### 2.4 Phase 1 — Louvain : requêtes Page une par une

**Problème :** Après Louvain, mise à jour du `silo_id` par nœud avec une requête par page.

```python
# crawler.py L288-292
for nid in community:
    p = session.query(Page).filter(Page.id == nid).first()
    if p:
        p.silo_id = str(i)
session.commit()
```

**Impact :** N requêtes pour N nœuds. Pour 50 pages : 50 requêtes.

### 2.5 Phase 2 — NER page par page

**Problème :** `extract_entities()` est appelé pour chaque page individuellement. spaCy n'utilise pas le batching natif `nlp.pipe()`.

```python
# crawler.py L377
entities = extract_entities((p.content_text or "")[:5000])
```

**Impact :** spaCy `fr_core_news_lg` a un overhead par appel. Pas de vectorisation batch.

### 2.6 Phase 2 — Modèle embedding lourd

**Problème :** `multilingual-e5-large` (~1,5 Go, 1024 dimensions) est plus lent que `multilingual-e5-small` (384 dim).

**Impact :** Latence par batch d'embeddings ~2–3× plus élevée.

### 2.7 Phase 2 — Reasonable Surfer : embeddings par edge

**Problème :** Pour chaque edge, on calcule un embedding du contexte (anchor ou extrait). Si 500 edges → 500 appels (ou batchés par 32).

**Impact :** ~16 batches d'embeddings supplémentaires pour 500 edges.

---

## 3. Pistes d'optimisation (par impact)

### 3.1 Fort impact — Fetch parallèle (Phase 1)

**Idée :** Fetcher plusieurs URLs en parallèle avec un pool de workers (2–4).

**Implémentation :**
```python
# fetcher.py ou crawler.py
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_urls_parallel(urls: list, max_workers: int = 3) -> dict[str, str]:
    """Fetch N URLs en parallèle. max_workers=3 pour éviter de surcharger les sites."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_html, url): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                html = future.result(timeout=FETCH_TIMEOUT)
                if html:
                    results[url] = html
            except Exception as e:
                logger.warning(f"Fetch {url}: {e}")
    return results
```

**Adaptation BFS :** Au lieu de `pop(0)` une URL, prendre un batch de K URLs (ex. 3) à la même profondeur, les fetcher en parallèle, puis traiter les résultats.

**Gain estimé :** **2–3×** sur la Phase 1 (ex. 25 min → 10 min pour 50 pages).

**Risque :** Politique de politeness (ne pas surcharger un même domaine). Limiter à 2–3 requêtes simultanées par domaine.

---

### 3.2 Fort impact — Réduire le timeout et prioriser Trafilatura

**Idée :**
- Réduire `SILO_FETCH_TIMEOUT` de 60 à **15–20 s** pour les sites réactifs.
- Pour les sites sans protection anti-bot, utiliser **Trafilatura en priorité** (`SILO_USE_STEALTHY_FETCHER=false`) : fetch HTTP simple, ~1–3 s/page.

**Config :**
```bash
SILO_FETCH_TIMEOUT=20
SILO_USE_STEALTHY_FETCHER=false  # si les sites cibles n'ont pas d'anti-bot
```

**Gain estimé :** **2–5×** si Trafilatura suffit (sites classiques).

---

### 3.3 Moyen impact — Batch commits (Phase 1)

**Idée :** Accumuler les pages et edges, committer toutes les 5–10 pages.

```python
BATCH_COMMIT_SIZE = 5
pages_batch, edges_batch = [], []

# Dans la boucle, au lieu de session.commit() immédiat :
pages_batch.append(page)
edges_batch.extend(new_edges)
if len(pages_batch) >= BATCH_COMMIT_SIZE:
    session.add_all(pages_batch)
    session.add_all(edges_batch)
    session.commit()
    pages_batch, edges_batch = [], []
```

**Gain estimé :** **10–20 %** (réduction des round-trips DB).

---

### 3.4 Moyen impact — Cache des pages/edges existants (Phase 1)

**Idée :** Charger en mémoire les IDs des pages et edges déjà présents pour le projet, au lieu de requêter à chaque lien.

```python
# Au début de la boucle ou par batch
existing_page_ids = {p.id for p in session.query(Page.id).filter(
    Page.project_id == project_id
).all()}
existing_edges = {(e.source_id, e.target_id) for e in session.query(Edge.source_id, Edge.target_id).filter(
    Edge.project_id == project_id
).all()}

# Dans la boucle :
if tid not in existing_page_ids:
    session.add(stub)
    existing_page_ids.add(tid)
if (pid, tid) not in existing_edges:
    session.add(edge)
    existing_edges.add((pid, tid))
```

**Gain estimé :** **15–25 %** sur la Phase 1 (évite des centaines de requêtes).

---

### 3.5 Moyen impact — Bulk update Louvain (Phase 1)

**Idée :** Utiliser `session.bulk_update_mappings()` ou une requête SQL directe pour mettre à jour `silo_id` en une fois.

```python
from sqlalchemy import update

updates = []
for i, community in enumerate(coms.communities):
    for nid in community:
        updates.append({"id": nid, "silo_id": str(i)})
if updates:
    session.execute(update(Page), updates)
    session.commit()
```

**Gain estimé :** **5–10 %** sur la fin de Phase 1.

---

### 3.6 Moyen impact — NER en batch avec `nlp.pipe()` (Phase 2)

**Idée :** Utiliser `nlp.pipe(texts)` pour traiter plusieurs textes en une fois (meilleure utilisation du CPU).

```python
# ner.py
def extract_entities_batch(texts: list[str], max_entities: int = 20) -> list[list[str]]:
    nlp = get_nlp()
    if not nlp or not texts:
        return [[] for _ in texts]
    results = []
    for doc in nlp.pipe(texts):
        entities = []
        for ent in doc.ents:
            if ent.label_ != "MONEY" and not _is_price_entity(ent.text):
                entities.append(ent.text.strip())
        results.append(list(dict.fromkeys(entities))[:max_entities])
    return results
```

**Gain estimé :** **20–40 %** sur la partie NER de la Phase 2.

---

### 3.7 Moyen impact — Modèle embedding plus léger (Phase 2)

**Idée :** Utiliser `intfloat/multilingual-e5-small` (384 dim) au lieu de `large` (1024 dim).

```bash
SILO_EMBEDDING_MODEL=intfloat/multilingual-e5-small
```

**Attention :** Adapter la colonne `embedding` en base (Vector(384)) et les requêtes de similarité.

**Gain estimé :** **2–3×** sur le temps d'embedding, **~60 %** de RAM en moins.

---

### 3.8 Faible impact — Augmenter EMBEDDING_BATCH_SIZE (Phase 2)

**Idée :** Passer de 32 à 64 ou 128 si la RAM le permet. FastEmbed scale bien.

```python
EMBEDDING_BATCH_SIZE = int(os.environ.get("SILO_EMBEDDING_BATCH_SIZE", "64"))
```

**Gain estimé :** **5–15 %** sur la Phase 2.

---

### 3.9 Faible impact — Réduire context_window (Phase 1)

**Idée :** `get_links_with_context(html, url, context_window=200)` → 100 ou 150. Moins de traitement lxml.

**Gain estimé :** Négligeable (< 5 %).

---

## 4. Synthèse des gains estimés

| Optimisation | Effort | Gain Phase 1 | Gain Phase 2 | Priorité |
|--------------|--------|--------------|--------------|----------|
| Fetch parallèle (3 workers) | Moyen | **2–3×** | — | **P0** |
| Trafilatura prioritaire / timeout 20s | Faible | **2–5×** | — | **P0** |
| Batch commits | Faible | 10–20 % | — | P1 |
| Cache pages/edges existants | Moyen | 15–25 % | — | P1 |
| Bulk update Louvain | Faible | 5–10 % | — | P2 |
| NER batch (nlp.pipe) | Moyen | — | 20–40 % | P1 |
| Modèle e5-small | Moyen* | — | **2–3×** | P1 |
| EMBEDDING_BATCH_SIZE=64 | Faible | — | 5–15 % | P2 |

\* Nécessite migration schéma DB (Vector 384).

---

## 5. Plan d'action recommandé

### Court terme (1–2 jours)

1. **Réduire le timeout** : `SILO_FETCH_TIMEOUT=20` (ou 25).
2. **Tester Trafilatura seul** : `SILO_USE_STEALTHY_FETCHER=false` sur un crawl de test. Si les sites cibles sont accessibles, garder.
3. **Batch commits** : Implémenter `BATCH_COMMIT_SIZE=5` dans Phase 1.
4. **Cache pages/edges** : Éviter les requêtes N+1 avec des sets en mémoire.

### Moyen terme (1 semaine)

5. **Fetch parallèle** : Adapter la boucle BFS pour fetcher 2–3 URLs en parallèle (même profondeur).
6. **NER batch** : Refactoriser `extract_entities` pour accepter une liste et utiliser `nlp.pipe()`.
7. **Bulk update Louvain** : Remplacer la boucle par un bulk update.

### Long terme (si besoin)

8. **Modèle e5-small** : Migration schéma + variable d'env. À évaluer selon la qualité des opportunités.
9. **Workers séparés** : Un worker dédié au fetch (Phase 1) et un au NLP (Phase 2) pour paralléliser les jobs.

---

## 6. Métriques à suivre

Pour valider les gains :

```bash
# Temps Phase 1 (avant/après)
grep "Phase 1 terminée" docker logs log8ot-silo-worker

# Temps par page (logs)
grep "Crawl Phase1" docker logs log8ot-silo-worker

# Mémoire
docker stats log8ot-silo-worker --no-stream
```

---

## 7. Implémentation (11 mars 2026)

Les optimisations suivantes ont été implémentées :

| Optimisation | Fichier | Variable d'env |
|--------------|---------|----------------|
| Timeout réduit (25s) | `fetcher.py` | `SILO_FETCH_TIMEOUT` |
| Fetch parallèle (3 workers) | `fetcher.py`, `crawler.py` | `SILO_FETCH_PARALLEL_WORKERS` (0 = désactivé) |
| Batch commits (5 pages) | `crawler.py` | `SILO_PHASE1_BATCH_COMMIT` |
| Cache pages/edges | `crawler.py` | — |
| Bulk update Louvain | `crawler.py` | — |
| NER batch (nlp.pipe) | `ner.py`, `crawler.py` | — |
| EMBEDDING_BATCH_SIZE (64) | `crawler.py` | `SILO_EMBEDDING_BATCH_SIZE` |
| Bulk update PageRank | `crawler.py` | — |
| spaCy model configurable | `ner.py` | `SILO_SPACY_MODEL` (fr_core_news_sm = rapide, fr_core_news_lg = précis) |
| Pipeline fetch (pre-fetch) | `crawler.py` | `SILO_PIPELINE_FETCH` (true par défaut) |
| EMBEDDING_TEXT_MAX_CHARS (3000) | `crawler.py` | `SILO_EMBEDDING_TEXT_MAX_CHARS` |
| Cache embeddings contexte | `crawler.py` | — |
| Batch H1 entities (Reasonable Surfer) | `crawler.py` | — |
| LINK_CONTEXT_WINDOW (150) | `crawler.py` | `SILO_LINK_CONTEXT_WINDOW` |
| **e5-small** (2–3× Phase 2) | `models.py`, `crawler.py` | `SILO_EMBEDDING_MODEL=...e5-small` + migration |
| **Fetch async (aiohttp)** | `fetcher.py` | `SILO_USE_STEALTHY_FETCHER=false` + `SILO_USE_ASYNC_FETCH=true` |
| **Workers séparés** | `run.py` | `SILO_WORKER_MODE=crawl` ou `nlp` |
| **Skip Reasonable Surfer** | `crawler.py` | `SILO_RUN_REASONABLE_SURFER=false` (20–40 % Phase 2) |
| **Louvain différé** | `crawler.py` | `SILO_LOUVAIN_DEFERRED=true` (Phase 1 plus rapide) |
| **Bulk insert edges** | `crawler.py` | — |
| **Connection pool DB** | `database/db.py` | `SILO_DB_POOL_SIZE`, `SILO_DB_MAX_OVERFLOW` |
| **Preload modèles** | `run.py` | — (mode nlp/full) |
| **Throttle entre jobs** | `run.py` | `SILO_JOB_DELAY_SECONDS` (0.5 s par défaut) |
| **Limite CPU worker** | `docker-compose.yml` | `deploy.resources.limits.cpus: "2.0"` |

**Réduire la charge serveur** (mode url_list = 1 job/URL, 1000+ URLs) :
```bash
# Délai entre jobs (évite surcharge CPU)
SILO_JOB_DELAY_SECONDS=1

# Modèles légers (optionnel)
SILO_EMBEDDING_MODEL=intfloat/multilingual-e5-small
SILO_SPACY_MODEL=fr_core_news_sm
SILO_RUN_REASONABLE_SURFER=false
```

**Désactiver le fetch parallèle** (sites sensibles) :
```bash
SILO_FETCH_PARALLEL_WORKERS=0
```

**Utiliser spaCy small** (30–50 % plus rapide sur NER, moins précis) :
```bash
SILO_SPACY_MODEL=fr_core_news_sm
```

**Désactiver le pipeline fetch** (si problèmes de concurrence) :
```bash
SILO_PIPELINE_FETCH=false
```

**Modèle e5-small** (2–3× plus rapide, migration DB requise) :
```bash
# 1. Migration (base existante)
psql $DATABASE_URL -f scripts/migrate_embedding_384.sql
# 2. Config
SILO_EMBEDDING_MODEL=intfloat/multilingual-e5-small
```

**Workers séparés** (crawl léger sans spaCy/FastEmbed, NLP dédié) :
```bash
# Terminal 1 - Worker crawl (Phase 1 uniquement)
SILO_WORKER_MODE=crawl python -m worker.run

# Terminal 2 - Worker NLP (Phase 2, NER, silos, embeddings)
SILO_WORKER_MODE=nlp python -m worker.run
```

**Skip Reasonable Surfer** (20–40 % Phase 2 plus rapide, moins de précision sur les poids) :
```bash
SILO_RUN_REASONABLE_SURFER=false
```

**Louvain différé** (Phase 1 plus rapide, silos à recalculer à la demande) :
```bash
SILO_LOUVAIN_DEFERRED=true
```

**Connection pool** (prod, hors localhost) :
```bash
SILO_DB_POOL_SIZE=10
SILO_DB_MAX_OVERFLOW=20
```

---

## 8. Références

- `BRIEF_CRAWLER_SILO_2026-03-11.md` — Architecture du crawler
- `RAPPORT_MEMOIRE_SILO_LAB.md` — Consommation mémoire, batch embeddings
- `worker/crawler.py` — Code source
- `worker/fetcher.py` — Fetch HTML
