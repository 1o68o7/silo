# Compte rendu : Consommation mémoire Silo Lab

**Date :** 10 mars 2026  
**Objectif :** Identifier les causes de forte consommation RAM et proposer des solutions pour éviter les ralentissements.

---

## 1. Architecture actuelle

Le worker Silo (`silo-worker`) est un **processus unique** qui traite en séquence :
- **Crawl** (Phase 1 + Phase 2)
- **NER on-demand**
- **Recalcul des silos**
- **Calcul des embeddings**

Tous ces traitements partagent la même mémoire. Les modèles chargés restent en RAM jusqu’à la fin du processus.

---

## 2. Sources de forte consommation mémoire

### 2.1 Modèles ML chargés en mémoire

| Composant | Modèle | Estimation RAM | Remarques |
|-----------|--------|----------------|-----------|
| **spaCy** | `fr_core_news_lg` | **~1,5–2 Go** | Chargé une fois (singleton dans `ner.py`), reste en mémoire |
| **FastEmbed** | `intfloat/multilingual-e5-large` | **~1,5 Go au démarrage** | Problème connu : croissance jusqu’à **6 Go+** en usage prolongé ([issue #222](https://github.com/qdrant/fastembed/issues/222)) |
| **Scrapling** | StealthyFetcher (Chromium headless) | **~300–600 Mo** | Navigateur headless par requête |

**Total estimé en pic :** 4–8 Go (spaCy + FastEmbed + navigateur + données).

### 2.2 Chargement répété du modèle FastEmbed

Le modèle FastEmbed est instancié **à chaque exécution** de :
- `run_crawl_phase2()`
- `run_compute_embeddings()`

```python
# crawler.py lignes 299 et 390
model = TextEmbedding("intfloat/multilingual-e5-large", cache_dir="/tmp/fastembed_cache")
```

Aucun singleton : chaque job recrée le modèle, ce qui augmente la charge mémoire et le temps de démarrage.

### 2.3 Chargement massif en mémoire (ORM)

Plusieurs requêtes chargent **toutes** les pages ou edges en une fois :

| Fichier | Requête | Impact |
|---------|---------|--------|
| `crawler.py:300` | `session.query(Page).filter(...).all()` | Toutes les pages du projet |
| `crawler.py:324-328` | `session.query(Edge).filter(...).all()` | Toutes les edges (2×) |
| `crawler.py:391` | `session.query(Page).filter(...).all()` | Toutes les pages sans embedding |
| `crawler.py:468` | `session.query(Edge).filter(...).all()` | Toutes les edges |
| `crawler.py:526` | `session.query(Page).filter(...).all()` | Toutes les pages (NER) |

Pour un projet de **1000+ pages** avec `content_text` (~5 Ko/page) et embeddings (1024 floats × 8 octets ≈ 8 Ko/page), l’ordre de grandeur est **~15–20 Mo** pour les pages seules, plus les structures Python (dicts, objets ORM).

### 2.4 Traitement des embeddings un par un

```python
# crawler.py:316 et 410
emb = list(model.embed([text_content]))[0]
```

Chaque page est embedée individuellement. FastEmbed gère mieux le batch, et les appels répétés favorisent la croissance mémoire signalée dans l’issue #222.

### 2.5 Graphe NetworkX

```python
# crawler.py:241, 324, 371
G = nx.DiGraph()
for e in session.query(Edge).filter(...).all():
    G.add_edge(e.source_id, e.target_id, weight=...)
```

Pour des graphes de 1000+ nœuds et 5000+ arêtes, NetworkX ajoute une surcharge mémoire non négligeable (quelques dizaines de Mo).

### 2.6 Absence de limites Docker

Le service `silo-worker` n’a pas de `deploy.resources.limits` dans le `docker-compose`. En cas de fuite ou de pic, le conteneur peut consommer toute la RAM disponible.

---

## 3. Solutions recommandées

### 3.1 Court terme (rapides à mettre en place)

#### A. Limites mémoire Docker

```yaml
# docker-compose.yml - silo-worker
silo-worker:
  deploy:
    resources:
      limits:
        memory: 6G
      reservations:
        memory: 2G
```

Objectif : éviter qu’un seul worker sature la machine.

#### B. Modèle FastEmbed plus léger

Remplacer `intfloat/multilingual-e5-large` par un modèle plus petit :

```python
# Option 1 : multilingual-e5-small (~400 Mo)
model = TextEmbedding("intfloat/multilingual-e5-small", cache_dir="/tmp/fastembed_cache")

# Option 2 : BAAI/bge-small-en-v1.5 (anglais uniquement, ~100 Mo)
# model = TextEmbedding("BAAI/bge-small-en-v1.5", ...)
```

Attention : `multilingual-e5-small` a une dimension différente (384 vs 1024). Il faudra adapter :

- `Vector(384)` dans `models.py`
- Les requêtes de similarité côté API

#### C. Singleton pour FastEmbed

Éviter de charger le modèle à chaque job :

```python
# crawler.py - ajouter en tête du module
_embedding_model = None

def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = TextEmbedding("intfloat/multilingual-e5-large", cache_dir="/tmp/fastembed_cache")
    return _embedding_model
```

Puis remplacer `model = TextEmbedding(...)` par `model = _get_embedding_model()`.

#### D. Traitement par lots (batch) pour les embeddings

```python
# Au lieu de : for p in pages: emb = model.embed([text])[0]
BATCH_SIZE = 32
for i in range(0, len(pages), BATCH_SIZE):
    batch = pages[i:i+BATCH_SIZE]
    texts = [(p.content_text or "")[:5000] for p in batch]
    embs = list(model.embed(texts))
    for p, emb in zip(batch, embs):
        p.embedding = emb.tolist() if hasattr(emb, "tolist") else list(emb)
```

Réduit le nombre d’appels au modèle et peut limiter la croissance mémoire.

### 3.2 Moyen terme (refactoring)

#### E. Traitement par lots (streaming) pour les pages

Remplacer `session.query(Page).all()` par un `yield_per()` :

```python
# Au lieu de pages = session.query(Page).filter(...).all()
for p in session.query(Page).filter(Page.project_id == project_id).yield_per(100):
    # traiter p
    session.expire(p)  # libérer l'objet du cache
```

Réduit la quantité de données chargées en mémoire en même temps.

#### F. Séparation des workers (processus dédiés)

Créer des workers spécialisés pour éviter de cumuler tous les modèles dans un seul processus :

| Worker | Rôle | Modèles chargés |
|--------|------|-----------------|
| `silo-worker-crawl` | Phase 1 + fetch | Scrapling (navigateur) |
| `silo-worker-nlp` | NER + embeddings + silos | spaCy + FastEmbed |

Implémentation possible :
- Plusieurs queues Redis (`crawl`, `ner`, `embeddings`, `silos`)
- Un conteneur par type de worker
- Chaque worker ne charge que les modèles dont il a besoin

#### G. Option spaCy plus légère

Pour le NER, tester `fr_core_news_sm` (~50 Mo) au lieu de `fr_core_news_lg` :

```python
# ner.py
_nlp = spacy.load("fr_core_news_sm")  # au lieu de fr_core_news_lg
```

Qualité NER un peu inférieure, mais gain mémoire important.

### 3.3 Long terme (architecture)

#### H. API d’embeddings externe

Déléguer les embeddings à un service dédié (ex. API Sentence-Transformers, Qdrant, ou service interne) pour :
- Ne pas charger le modèle dans le worker
- Réutiliser un modèle déjà chargé ailleurs
- Mieux isoler la charge CPU/RAM

#### I. Redémarrage périodique du worker

Avec un gestionnaire de processus (supervisor, systemd) ou un cron :

```bash
# Redémarrer le worker toutes les 6 h pour libérer la mémoire
0 */6 * * * docker restart log8ot-silo-worker
```

Solution simple pour limiter les fuites mémoire à long terme.

#### J. Monitoring et alertes

- Exporter des métriques Prometheus (RAM du conteneur)
- Alertes si dépassement d’un seuil (ex. 5 Go)
- Logs structurés pour tracer les pics mémoire par type de job

---

## 4. Plan d’action priorisé

| Priorité | Action | Effort | Impact estimé | Statut |
|----------|--------|--------|---------------|--------|
| 1 | Limites mémoire Docker (3.1.A) | Faible | Limite les dégâts en cas de pic | ✅ Implémenté |
| 2 | Singleton FastEmbed (3.1.C) | Faible | Évite rechargements inutiles | ✅ Implémenté |
| 3 | Batch embeddings (3.1.D) | Moyen | Réduction de la croissance mémoire | ✅ Implémenté |
| 4 | Modèle e5-small (3.1.B) | Moyen | -1 à 2 Go RAM (avec adaptation schéma) | Non implémenté |
| 5 | Streaming des pages (3.2.E) | Moyen | Moins de pics mémoire sur gros projets | ✅ Implémenté |
| 6 | Workers séparés (3.2.F) | Élevé | Meilleure isolation et stabilité | Non implémenté |
| 7 | API embeddings externe (3.3.H) | Élevé | Découplage et scalabilité | Non implémenté |

---

## 5. Vérification des logs

Pour valider les hypothèses en production :

```bash
# Consommation mémoire du worker
docker stats log8ot-silo-worker --no-stream

# Logs du worker (erreurs, OOM)
docker logs log8ot-silo-worker --tail 500 2>&1 | grep -E "memory|OOM|Killed|Error"
```

Si le conteneur est tué par l’OOM Killer, les logs système (`dmesg`, `journalctl`) contiendront des traces du type `Out of memory: Killed process`.

---

## 6. Implémentations réalisées (10 mars 2026)

- **Singleton FastEmbed** : `_get_embedding_model()` dans `crawler.py`, évite ~1.5 Go par job
- **Batch embeddings** : `EMBEDDING_BATCH_SIZE=32` pour Phase 2, run_compute_embeddings et boucle context_embedding
- **Streaming** : `yield_per(100)` dans run_compute_embeddings et run_ner_on_demand (silo/projet entier)
- **Limites Docker** : `mem_limit: 6g`, `mem_reservation: 2g` sur silo-worker

## 7. Références

- [FastEmbed Issue #222 - High memory usage](https://github.com/qdrant/fastembed/issues/222)
- [spaCy Memory Management](https://spacy.io/usage/memory-management)
- [Docker Compose deploy resources](https://docs.docker.com/compose/compose-file/deploy/#resources)
