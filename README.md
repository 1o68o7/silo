# Silo - Semantic Cocoon / OSINT SEO

Outil OSINT/SEO de nouvelle génération pour l'analyse du graphe sémantique et des liens internes.

## Stack

- **API**: FastAPI (Python)
- **Crawl**: Trafilatura, FastEmbed (BGE-M3)
- **Stockage**: PostgreSQL + pgvector (optionnel), Redis (file d'attente)
- **Graphe**: NetworkX, CDlib

## Démarrage rapide

### Local (sans Docker)

```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate sur Windows
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Docker

```bash
docker build -t silo-lab:local .
docker run -p 8000:8000 silo-lab:local
```

### Intégration Log8ot

Le service Silo est intégré au `docker-compose` principal de log8ot-frontend. Le frontend proxy `/api/silo` vers le conteneur `silo:8000`.

### Vérifier les dépendances (dans le conteneur)

```bash
# FastEmbed est installé dans le worker (pas sur l'hôte)
docker exec log8ot-silo-worker pip show fastembed

# Logs du crawler / embeddings
docker logs log8ot-silo-worker --tail 100 -f
```

**Note** : `pip show fastembed` sur l'hôte renverra "not found" car FastEmbed est uniquement dans l'image Docker du worker.

## API

- `GET /health` - Healthcheck
- `GET /api/projects` - Liste des projets
- `POST /api/projects?name=...&seed_url=...` - Créer un projet
- `GET /api/projects/{id}/graph` - Graphe sémantique
- `GET /api/projects/{id}/crawl-status` - Statut du crawler
- `POST /api/projects/{id}/crawl` - Démarrer le crawl
- `GET /api/opportunities/{id}?min_similarity=0.9` - Gap Analysis

## Structure

```
silo/
├── main.py           # API FastAPI
├── requirements.txt
├── Dockerfile
├── docker-compose.yml  # Standalone (db, redis, api)
└── README.md
```
