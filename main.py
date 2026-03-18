"""
Silo - Semantic Cocoon / OSINT SEO Tool
API FastAPI pour le graphe sémantique, projets et statut du crawler.
"""
import os
import json
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
NER_QUEUE_KEY = "silo:ner_queue"

# Modèles Pydantic pour l'API
class Project(BaseModel):
    id: str
    name: str
    seed_url: str
    created_at: str
    urls_count: int = 0
    status: str = "idle"


class GraphNode(BaseModel):
    id: str
    url: str
    title: Optional[str] = None
    h1: Optional[str] = None
    page_rank: float = 0.0
    depth: int = 0
    silo_id: Optional[str] = None
    entities: list[str] = []
    excluded: bool = False  # True si URL avec paramètres (filtrée de l'analyse)


class GraphEdge(BaseModel):
    source: str
    target: str
    weight: float
    anchor: Optional[str] = None


class GraphData(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    excluded_count: int = 0  # Nombre de pages exclues (URLs avec paramètres)


class CrawlStatus(BaseModel):
    project_id: str
    status: str
    urls_discovered: int = 0
    urls_processed: int = 0
    progress_percent: float = 0.0
    message: Optional[str] = None


class CrawlConfig(BaseModel):
    max_depth: int = 3
    max_pages: int = 50
    run_ner: bool = True
    seed_url: Optional[str] = None  # Override: crawler depuis cette URL (ex: nœud sélectionné)
    url_list: Optional[List[str]] = None  # Liste d'URLs à crawler (uniquement ces URLs, max_depth=0)
    path_prefix: Optional[str] = None  # Borne le crawl au répertoire (ex. /fr). "" ou null = pas de restriction
    exclude_urls_with_params: bool = True  # Exclure les URLs avec query string (pagination, utm_*, etc.)


# Store en mémoire (fallback si pas de DB)
_projects: dict[str, dict] = {}
_graph_cache: dict[str, GraphData] = {}
_crawl_status: dict[str, CrawlStatus] = {}

USE_DB = bool(os.environ.get("DATABASE_URL"))
REDIS_URL = os.environ.get("REDIS_URL")
QUEUE_KEY = "silo:crawl_queue"


def _get_redis():
    if not REDIS_URL:
        return None
    try:
        import redis
        return redis.from_url(REDIS_URL)
    except Exception:
        return None


def _init_memory_demo():
    """Données démo en mémoire."""
    _projects["demo"] = {
        "id": "demo",
        "name": "Site Démo",
        "seed_url": "https://example.com",
        "created_at": "2025-03-05T00:00:00Z",
        "urls_count": 3,
        "status": "done",
    }
    _graph_cache["demo"] = GraphData(
        nodes=[
            GraphNode(id="n1", url="https://example.com", title="Example", h1="Welcome", page_rank=0.5, depth=0, silo_id="A"),
            GraphNode(id="n2", url="https://example.com/page1", title="Page 1", h1="Page 1", page_rank=0.3, depth=1, silo_id="A"),
            GraphNode(id="n3", url="https://example.com/page2", title="Page 2", h1="Page 2", page_rank=0.2, depth=1, silo_id="B"),
        ],
        edges=[
            GraphEdge(source="n1", target="n2", weight=0.8, anchor="Lien 1"),
            GraphEdge(source="n1", target="n3", weight=0.5, anchor="Lien 2"),
        ],
    )
    _crawl_status["demo"] = CrawlStatus(
        project_id="demo",
        status="done",
        urls_discovered=3,
        urls_processed=3,
        progress_percent=100.0,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    if USE_DB:
        try:
            from database.db import init_db
            init_db()
        except Exception:
            pass  # Les requêtes échoueront, on garde USE_DB pour cohérence
    if not USE_DB:
        _init_memory_demo()
    yield


app = FastAPI(
    title="Silo API",
    description="Semantic Cocoon / OSINT SEO - Graphe sémantique et analyse de liens",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.log8ot.com",
        "https://log8ot.com",
        "http://localhost:3000",
        "http://localhost:3001",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin", "X-Requested-With"],
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "silo", "db": USE_DB}


@app.get("/api/admin/long-queries")
async def get_long_queries(min_duration_sec: int = 30):
    """
    Surveille pg_stat_activity pour requêtes actives trop longtemps.
    Utile pour détecter blocages (DELETE, etc.).
    """
    if not USE_DB:
        return {"queries": [], "count": 0}
    try:
        from database.db import get_session
        from sqlalchemy import text

        session = get_session()
        try:
            r = session.execute(
                text("""
                    SELECT pid, state, EXTRACT(EPOCH FROM (now() - query_start))::int as duration_sec,
                           wait_event_type, wait_event, LEFT(query, 120) as query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND state = 'active'
                      AND query NOT LIKE '%pg_stat_activity%'
                      AND query_start < now() - interval '1 second' * :min_sec
                    ORDER BY query_start
                """),
                {"min_sec": min_duration_sec},
            )
            rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]
            return {"queries": rows, "count": len(rows)}
        finally:
            session.close()
    except Exception as e:
        return {"queries": [], "count": 0, "error": str(e)}


@app.get("/api/projects", response_model=list[Project])
async def list_projects():
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import list_projects as db_list
            session = get_session()
            try:
                return [Project(**p) for p in db_list(session)]
            finally:
                session.close()
        except Exception:
            pass
    return [Project(**p) for p in _projects.values()]


@app.post("/api/projects", response_model=Project)
async def create_project(name: str, seed_url: str):
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import create_project as db_create
            session = get_session()
            try:
                p = db_create(session, name, seed_url)
                return Project(**p)
            finally:
                session.close()
        except Exception:
            pass
    pid = str(uuid.uuid4())[:8]
    _projects[pid] = {
        "id": pid,
        "name": name,
        "seed_url": seed_url,
        "created_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "urls_count": 0,
        "status": "idle",
    }
    _crawl_status[pid] = CrawlStatus(project_id=pid, status="idle")
    return Project(**_projects[pid])


@app.get("/api/projects/{project_id}/silo-analysis")
async def get_silo_analysis_endpoint(project_id: str):
    """
    Analyse silos théorique vs réel (Phase 6).
    Métriques pré-calculées : Dispersion Louvain, Diversité URL, cohérence, matrice, pages incohérentes.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_silo_analysis
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                analysis = get_silo_analysis(session, project_id)
                if analysis is None:
                    return {"by_theoretical": {}, "by_real": {}, "global_coherence": 0, "inconsistent_pages": [], "matrix_theo_to_real": [], "all_louvain_ids": []}
                return analysis
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur analyse silos")
    return {"by_theoretical": {}, "by_real": {}, "global_coherence": 0, "inconsistent_pages": [], "matrix_theo_to_real": [], "all_louvain_ids": []}


@app.get("/api/projects/{project_id}/graph", response_model=GraphData)
async def get_graph(project_id: str, include_excluded: bool = False):
    """
    Graphe du projet. Par défaut exclut les URLs avec paramètres (utm_*, fbclid, etc.).
    ?include_excluded=true pour les inclure (avec badge excluded).
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_graph as db_graph, get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                g = db_graph(session, project_id, include_excluded=include_excluded)
                return GraphData(**g)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            pass
    if project_id not in _graph_cache:
        if project_id in _projects:
            return GraphData(nodes=[], edges=[])
        raise HTTPException(status_code=404, detail="Projet non trouvé")
    return _graph_cache[project_id]


CRAWL_LOGS_KEY = "silo:crawl_logs"
CRAWL_PAUSE_KEY = "silo:crawl_pause"
CRAWL_STOP_KEY = "silo:crawl_stop"


def _get_crawl_logs(project_id: str) -> list[dict]:
    """Récupère les logs de crawl depuis Redis."""
    r = _get_redis()
    if not r:
        return []
    try:
        key = f"{CRAWL_LOGS_KEY}:{project_id}"
        raw = r.lrange(key, 0, -1)
        return [json.loads(x) for x in raw if isinstance(x, (str, bytes))]
    except Exception:
        return []


@app.get("/api/projects/{project_id}/crawl-logs")
async def get_crawl_logs(project_id: str):
    """Logs temps réel du crawler (pour affichage terminal UI)."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            pass
    elif project_id not in _projects:
        raise HTTPException(status_code=404, detail="Projet non trouvé")
    return {"logs": _get_crawl_logs(project_id)}


@app.get("/api/projects/{project_id}/crawl-status", response_model=CrawlStatus)
async def get_crawl_status(project_id: str):
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_crawl_status as db_status, get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                s = db_status(session, project_id)
                return CrawlStatus(**s)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            pass
    if project_id not in _crawl_status:
        if project_id in _projects:
            return CrawlStatus(project_id=project_id, status="idle")
        raise HTTPException(status_code=404, detail="Projet non trouvé")
    return _crawl_status[project_id]


@app.post("/api/projects/{project_id}/crawl/pause")
async def pause_crawl(project_id: str):
    """Met le crawl en pause."""
    r = _get_redis()
    if r:
        try:
            r.set(f"{CRAWL_PAUSE_KEY}:{project_id}", "1", ex=86400)
        except Exception:
            pass
    return {"ok": True, "message": "Crawl mis en pause"}


@app.post("/api/projects/{project_id}/crawl/resume")
async def resume_crawl(project_id: str):
    """Reprend le crawl après une pause."""
    r = _get_redis()
    if r:
        try:
            r.delete(f"{CRAWL_PAUSE_KEY}:{project_id}")
        except Exception:
            pass
    return {"ok": True, "message": "Crawl repris"}


@app.post("/api/projects/{project_id}/crawl/stop")
async def stop_crawl(project_id: str):
    """Stoppe définitivement le crawl en cours."""
    r = _get_redis()
    if r:
        try:
            r.set(f"{CRAWL_STOP_KEY}:{project_id}", "1", ex=86400)
            r.delete(f"{EMBEDDING_IN_PROGRESS_KEY}:{project_id}")  # Libère le verrou pour permettre un nouveau lancement
        except Exception:
            pass

    # Mise à jour immédiate du statut en BDD pour éviter blocage NER/autres actions
    # (le worker mettra aussi "done" à sa sortie, mais on ne veut pas attendre)
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import update_project_status, get_project
            session = get_session()
            try:
                if get_project(session, project_id):
                    update_project_status(session, project_id, "done")
            finally:
                session.close()
        except Exception:
            pass
    else:
        if project_id in _crawl_status:
            _crawl_status[project_id].status = "done"

    return {"ok": True, "message": "Signal d'arrêt envoyé"}


@app.post("/api/projects/{project_id}/crawl")
async def start_crawl(project_id: str, config: Optional[CrawlConfig] = Body(default=None)):
    """Démarre le crawl. Body optionnel: { max_depth, max_pages, run_ner }"""
    # Vérifier que le projet existe
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project
            session = get_session()
            try:
                p = get_project(session, project_id)
                if not p:
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                seed_url = p.seed_url
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=404, detail="Projet non trouvé")
    else:
        if project_id not in _projects:
            raise HTTPException(status_code=404, detail="Projet non trouvé")
        seed_url = _projects[project_id]["seed_url"]

    cfg = config or CrawlConfig()
    if cfg.seed_url:
        seed_url = cfg.seed_url

    # path_prefix: null/absent → auto-détection depuis seed_url; "" → pas de restriction
    path_prefix = cfg.path_prefix
    if path_prefix is None:
        from worker.url_utils import extract_lang_path_prefix
        path_prefix = extract_lang_path_prefix(seed_url)
    elif path_prefix == "":
        path_prefix = None

    r = _get_redis()
    if r:
        try:
            r.delete(f"{CRAWL_PAUSE_KEY}:{project_id}", f"{CRAWL_STOP_KEY}:{project_id}")
            if cfg.url_list and len(cfg.url_list) > 0:
                # Mode batch: une job par URL (crawl uniquement ces URLs, pas de suivi des liens)
                from urllib.parse import urlparse
                for u in cfg.url_list:
                    u = (u or "").strip()
                    if not u or not u.startswith(("http://", "https://")):
                        continue
                    if cfg.exclude_urls_with_params and urlparse(u).query:
                        continue
                    payload = {
                        "project_id": project_id,
                        "seed_url": u,
                        "max_depth": 0,
                        "max_pages": 1,
                        "run_ner": cfg.run_ner,
                        "path_prefix": path_prefix,
                        "exclude_urls_with_params": cfg.exclude_urls_with_params,
                    }
                    r.rpush(QUEUE_KEY, json.dumps(payload))
            else:
                payload = {
                    "project_id": project_id,
                    "seed_url": seed_url,
                    "max_depth": cfg.max_depth,
                    "max_pages": cfg.max_pages,
                    "run_ner": cfg.run_ner,
                    "path_prefix": path_prefix,
                    "exclude_urls_with_params": cfg.exclude_urls_with_params,
                }
                r.rpush(QUEUE_KEY, json.dumps(payload))
        except Exception:
            pass

    # Mise à jour statut
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import update_project_status
            session = get_session()
            try:
                update_project_status(session, project_id, "crawling")
            finally:
                session.close()
        except Exception:
            pass
    else:
        _crawl_status[project_id] = CrawlStatus(
            project_id=project_id,
            status="crawling",
            urls_discovered=0,
            urls_processed=0,
            progress_percent=0.0,
            message="Crawl en attente (worker à connecter)" if not r else "Crawl en cours",
        )

    return {"ok": True, "message": "Crawl démarré"}


# Seuil edges au-delà duquel la suppression est asynchrone. Défaut 0 = toujours async (évite timeout front).
DELETE_ASYNC_EDGES_THRESHOLD = int(os.environ.get("SILO_DELETE_ASYNC_THRESHOLD", "0"))


@app.delete("/api/projects/{project_id}")
async def delete_project_endpoint(project_id: str, async_only: bool = False):
    """
    Supprime un projet et ses données (pages, edges).
    Toujours async par défaut : 202 Accepted, soft delete immédiat, worker en arrière-plan.
    Évite les timeouts front (30s) sur les projets avec beaucoup de pages.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import delete_project as db_delete, get_project, count_project_edges, mark_project_deleted
            session = get_session()
            try:
                p = get_project(session, project_id)
                if not p:
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                edges_count = count_project_edges(session, project_id)
                session.close()

                # Projet volumineux ou async_only: enqueue pour traitement worker
                if async_only or edges_count >= DELETE_ASYNC_EDGES_THRESHOLD:
                    r = _get_redis()
                    if r:
                        # Soft delete immédiat pour que GET /api/projects exclue le projet tout de suite
                        session = get_session()
                        try:
                            mark_project_deleted(session, project_id)
                        finally:
                            session.close()
                        r.rpush("silo:delete_project_queue", json.dumps({"project_id": project_id}))
                        return JSONResponse(
                            status_code=202,
                            content={"ok": True, "message": "Suppression en cours (arrière-plan)", "project_id": project_id},
                        )
                    # Fallback sync si Redis indisponible
                session = get_session()
                try:
                    if not db_delete(session, project_id):
                        raise HTTPException(status_code=404, detail="Projet non trouvé")
                    return {"ok": True, "message": "Projet supprimé"}
                finally:
                    session.close()
            except HTTPException:
                raise
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur lors de la suppression")
    if project_id in _projects:
        del _projects[project_id]
        if project_id in _graph_cache:
            del _graph_cache[project_id]
        if project_id in _crawl_status:
            del _crawl_status[project_id]
        return {"ok": True, "message": "Projet supprimé"}
    raise HTTPException(status_code=404, detail="Projet non trouvé")


class NerRequest(BaseModel):
    node_id: Optional[str] = None
    silo_id: Optional[str] = None


@app.post("/api/projects/{project_id}/ner")
async def run_ner_on_demand_endpoint(project_id: str, body: Optional[NerRequest] = Body(default=None)):
    """
    Lance la détection NER sur un nœud ou un silo.
    Disponible uniquement quand le crawl n'est pas en cours.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_crawl_status
            session = get_session()
            try:
                p = get_project(session, project_id)
                if not p:
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                status = get_crawl_status(session, project_id)
                if status and status.get("status") == "crawling":
                    raise HTTPException(status_code=409, detail="Crawl en cours. Arrêtez le crawl avant de lancer NER.")
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur vérification statut")
    else:
        if _crawl_status.get(project_id, {}).get("status") == "crawling":
            raise HTTPException(status_code=409, detail="Crawl en cours. Arrêtez le crawl avant de lancer NER.")

    # Délègue au worker (spaCy + fr_core_news_lg) via Redis
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        if r:
            r.delete(f"{CRAWL_STOP_KEY}:{project_id}")  # Réinitialiser le stop pour permettre le nouveau run
        payload = {"project_id": project_id}
        if body and body.node_id:
            payload["node_id"] = body.node_id
        if body and body.silo_id:
            payload["silo_id"] = body.silo_id
        r.rpush(NER_QUEUE_KEY, json.dumps(payload))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lancer NER: {e}")
    return {"ok": True, "message": "Détection NER lancée (traitement en cours par le worker)"}


RECOMPUTE_SILOS_QUEUE_KEY = "silo:recompute_silos_queue"


@app.post("/api/projects/{project_id}/recompute-silos")
async def recompute_silos_endpoint(project_id: str):
    """
    Recalcule les silos (Louvain) sur le graphe existant.
    Utile quand Title/H1/Silo sont vides après un crawl partiel.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur vérification projet")
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        if r:
            r.delete(f"{CRAWL_STOP_KEY}:{project_id}")  # Réinitialiser le stop pour le nouveau run
        r.rpush(RECOMPUTE_SILOS_QUEUE_KEY, json.dumps({"project_id": project_id}))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lancer: {e}")
    return {"ok": True, "message": "Recalcul des silos lancé (traitement en cours)"}


@app.get("/api/projects/{project_id}/embeddings-status")
async def get_embeddings_status_endpoint(project_id: str, page_id: str = None):
    """Statut des embeddings (nécessaires pour la recherche d'opportunités). Optionnel: page_id pour savoir si cette page a un embedding."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_embeddings_status
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                result = get_embeddings_status(session, project_id, page_id)
                # Indicateur backend : calcul en cours (persiste après navigation)
                try:
                    import redis
                    r = redis.from_url(REDIS_URL)
                    if r:
                        result["embedding_in_progress"] = bool(r.exists(f"{EMBEDDING_IN_PROGRESS_KEY}:{project_id}"))
                    else:
                        result["embedding_in_progress"] = False
                except Exception:
                    result["embedding_in_progress"] = False
                return result
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur statut embeddings")
    return {"total_pages": 0, "pages_with_embedding": 0, "has_embeddings": False, "embedding_in_progress": False}


@app.get("/api/projects/{project_id}/ner-status")
async def get_ner_status_endpoint(project_id: str):
    """Statut NER (pages avec entités détectées) pour suivi de progression."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_ner_status
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                return get_ner_status(session, project_id)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur statut NER")
    return {"total_pages": 0, "pages_with_entities": 0}


COMPUTE_EMBEDDINGS_QUEUE_KEY = "silo:compute_embeddings_queue"
EMBEDDING_IN_PROGRESS_KEY = "silo:embedding_in_progress"
COMPUTE_OPPORTUNITIES_QUEUE_KEY = "silo:compute_opportunities_queue"
OPPORTUNITIES_IN_PROGRESS_KEY = "silo:opportunities_in_progress"


@app.post("/api/projects/{project_id}/compute-embeddings")
async def compute_embeddings_endpoint(project_id: str, page_id: str = None):
    """
    Calcule les embeddings pour les pages (Phase 2 partielle).
    Si page_id est fourni, ne traite que cette page.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur vérification projet")
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        if r:
            r.delete(f"{CRAWL_STOP_KEY}:{project_id}")  # Réinitialiser le stop pour le nouveau run
            # Verrou pour éviter double lancement (set NX = seulement si absent)
            if not r.set(f"{EMBEDDING_IN_PROGRESS_KEY}:{project_id}", "1", ex=7200, nx=True):
                raise HTTPException(status_code=409, detail="Un calcul d'embeddings est déjà en cours pour ce projet")
        payload = {"project_id": project_id}
        if page_id:
            payload["page_id"] = page_id
        r.rpush(COMPUTE_EMBEDDINGS_QUEUE_KEY, json.dumps(payload))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lancer: {e}")
    msg = "Calcul embedding pour cette URL lancé" if page_id else "Calcul des embeddings lancé (traitement en cours)"
    return {"ok": True, "message": msg}


@app.get("/api/opportunities/{project_id}")
async def get_opportunities(project_id: str, min_similarity: float = 0.9, with_script: bool = False):
    """
    Gap Analysis: paires de pages avec similarité sémantique forte (>= min_similarity)
    mais sans lien physique. Opportunités de maillage interne SEO.
    Lit depuis computed_opportunities si disponible (vue Toutes), sinon retourne vide + hint compute_required.
    Si with_script=true, enrichit avec zone_texte et phrase_ancre_proposee.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import (
                get_project,
                get_embeddings_status,
                get_computed_opportunities_status,
                get_computed_opportunities,
                enrich_computed_opportunities_with_script,
            )
            session = get_session()
            try:
                if not get_project(session, project_id):
                    return {"pairs": [], "hint": "project_not_found"}
                emb_status = get_embeddings_status(session, project_id)
                if not emb_status["has_embeddings"]:
                    return {"pairs": [], "hint": "no_embeddings", "embeddings_status": emb_status}
                # Priorité aux opportunités pré-calculées (stockées en base)
                comp_status = get_computed_opportunities_status(session, project_id)
                if comp_status and comp_status.get("count", 0) > 0:
                    pairs = get_computed_opportunities(session, project_id, min_similarity)
                    if with_script:
                        pairs = enrich_computed_opportunities_with_script(session, project_id, pairs)
                    return {
                        "pairs": pairs,
                        "embeddings_status": emb_status,
                        "computed_status": comp_status,
                        "hint": "try_lower_similarity" if len(pairs) == 0 and comp_status.get("count", 0) > 0 else None,
                    }
                # Pas de données pré-calculées : retourner vide et inviter à lancer le calcul
                return {
                    "pairs": [],
                    "embeddings_status": emb_status,
                    "computed_status": None,
                    "hint": "compute_required",
                }
            finally:
                session.close()
        except Exception as e:
            import logging
            logging.exception("get_opportunities error")
            return {"pairs": [], "hint": "error"}
    if project_id not in _graph_cache:
        return {"pairs": []}
    return {"pairs": []}


@app.post("/api/opportunities/{project_id}/compute")
async def compute_opportunities_endpoint(project_id: str):
    """
    Lance le calcul et le stockage des opportunités en arrière-plan (worker).
    Les résultats seront disponibles via GET /api/opportunities/{project_id} une fois terminé.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_embeddings_status
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                emb = get_embeddings_status(session, project_id)
                if not emb.get("has_embeddings"):
                    raise HTTPException(status_code=400, detail="Calculez d'abord les embeddings")
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur vérification projet")
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        if r:
            if not r.set(f"{OPPORTUNITIES_IN_PROGRESS_KEY}:{project_id}", "1", ex=3600, nx=True):
                raise HTTPException(status_code=409, detail="Un calcul d'opportunités est déjà en cours pour ce projet")
        r.rpush(COMPUTE_OPPORTUNITIES_QUEUE_KEY, json.dumps({"project_id": project_id}))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lancer: {e}")
    return {"ok": True, "message": "Calcul des opportunités lancé (traitement en arrière-plan)"}


def _slug_project_name(name: str) -> str:
    """Sanitise le nom du projet pour un nom de fichier."""
    if not name:
        return "projet"
    import re
    s = re.sub(r"[^\w\s-]", "", name)
    s = re.sub(r"[\s_]+", "-", s.strip())
    return (s or "projet")[:50]


@app.get("/api/opportunities/{project_id}/export")
async def export_opportunities(project_id: str, min_similarity: float = 0.9, format: str = "json"):
    """
    Export des opportunités en fichier (json, csv ou md).
    Retourne le contenu avec headers pour téléchargement.
    Le nom du fichier inclut le nom du projet.
    """
    project_name = "projet"
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import (
                get_project,
                get_computed_opportunities_status,
                get_computed_opportunities,
                enrich_computed_opportunities_with_script,
            )
            session = get_session()
            try:
                proj = get_project(session, project_id)
                if not proj:
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                project_name = _slug_project_name(proj.name)
                comp_status = get_computed_opportunities_status(session, project_id)
                if comp_status and comp_status.get("count", 0) > 0:
                    pairs = get_computed_opportunities(session, project_id, min_similarity)
                    pairs = enrich_computed_opportunities_with_script(session, project_id, pairs)
                else:
                    pairs = []
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            pairs = []
    else:
        pairs = []

    base_filename = f"opportunites_{project_name}"
    fmt = (format or "json").lower()

    def _sanitize_zone_csv(zone: str, max_len: int = 150) -> str:
        s = (zone or "").replace("\r", " ").replace("\n", " ").strip()
        s = " ".join(s.split())
        return (s[:max_len] + "…") if len(s) > max_len else s

    if fmt == "csv":
        import csv
        import io
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow([
            "similarity", "source_url", "target_url", "phrase_ancre_proposee",
            "zone_texte_extrait", "entities_shared", "silo_source", "silo_target"
        ])
        for p in pairs:
            ent_shared = "; ".join(p.get("entities_shared") or [])
            silo_src = f"{p.get('silo_theoretical_source', '')}/{p.get('silo_louvain_source', '')}"
            silo_tgt = f"{p.get('silo_theoretical_target', '')}/{p.get('silo_louvain_target', '')}"
            writer.writerow([
                f"{(p.get('similarity', 0) * 100):.0f}%",
                p.get("source_url", ""),
                p.get("target_url", ""),
                p.get("phrase_ancre_proposee", ""),
                _sanitize_zone_csv(p.get("zone_texte", "")),
                ent_shared,
                silo_src,
                silo_tgt,
            ])
        content = "\uFEFF" + out.getvalue()
        return {"content": content, "filename": f"{base_filename}.csv", "mime": "text/csv;charset=utf-8"}

    if fmt == "md":
        from datetime import datetime
        now = datetime.utcnow().strftime("%Y-%m-%d")
        lines = [f"# Opportunités de maillage — {project_name}\n"]
        lines.append(f"> Export du {now}. **{len(pairs)}** opportunité(s).\n")
        lines.append("---\n")
        lines.append("## Tableau récapitulatif\n")
        lines.append("| # | Similarité | Source | Cible | Ancre proposée |")
        lines.append("|---|------------|--------|-------|----------------|")
        for i, p in enumerate(pairs, 1):
            src = ((p.get("source_url") or "")[:50] + ("…" if len(p.get("source_url") or "") > 50 else "")).replace("|", "\\|")
            tgt = ((p.get("target_url") or "")[:50] + ("…" if len(p.get("target_url") or "") > 50 else "")).replace("|", "\\|")
            ancre = ((p.get("phrase_ancre_proposee") or "—")[:35] + ("…" if len(p.get("phrase_ancre_proposee") or "") > 35 else "")).replace("|", "\\|")
            lines.append(f"| {i} | {(p.get('similarity', 0) * 100):.0f}% | {src} | {tgt} | {ancre} |")
        lines.append("\n---\n")
        lines.append("## Détail des opportunités\n")
        for i, p in enumerate(pairs, 1):
            lines.append(f"### {i}. {(p.get('similarity', 0) * 100):.0f}% — {p.get('source_url', '—')}\n")
            lines.append("| Champ | Valeur |")
            lines.append("|-------|--------|")
            lines.append(f"| **Source** | {p.get('source_url', '—')} |")
            lines.append(f"| **Cible (à lier)** | {p.get('target_url', '—')} |")
            lines.append(f"| **Phrase d'ancrage** | \"{p.get('phrase_ancre_proposee', '—')}\" |")
            if p.get("entities_shared"):
                lines.append(f"| **NER partagés** | {', '.join(p['entities_shared'])} |")
            if p.get("silo_theoretical_source") or p.get("silo_theoretical_target"):
                lines.append(f"| **Silos** | {p.get('silo_theoretical_source', '—')}/{p.get('silo_louvain_source', '—')} → {p.get('silo_theoretical_target', '—')}/{p.get('silo_louvain_target', '—')} |")
            if p.get("silo_mismatch_alert"):
                lines.append(f"| **Alerte** | {p['silo_mismatch_alert']} |")
            zone = p.get("zone_texte") or ""
            if zone:
                lines.append("\n**Zone de texte (où placer le lien)**\n")
                lines.append("```")
                lines.append(zone.replace("```", "` ` `"))
                lines.append("```\n")
            lines.append("---\n")
        return {"content": "\n".join(lines), "filename": f"{base_filename}.md", "mime": "text/markdown"}
    # json par défaut
    return {"content": json.dumps({"pairs": pairs, "project_id": project_id}, ensure_ascii=False, indent=2),
            "filename": f"{base_filename}.json", "mime": "application/json"}


@app.get("/api/projects/{project_id}/pages/{page_id}")
async def get_page_endpoint(project_id: str, page_id: str):
    """Détails d'une page (inspecteur)."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_page
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                page = get_page(session, project_id, page_id)
                if not page:
                    raise HTTPException(status_code=404, detail="Page non trouvée")
                return page
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur")
    raise HTTPException(status_code=404, detail="Projet non trouvé")


@app.get("/api/projects/{project_id}/pages/{page_id}/links")
async def get_page_links_endpoint(project_id: str, page_id: str):
    """Liens entrants et sortants d'une page (vue macro)."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_page_links
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                return get_page_links(session, project_id, page_id)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur")
    return {"outgoing": [], "incoming": []}


@app.get("/api/projects/{project_id}/pages/{page_id}/opportunities")
async def get_page_opportunities_endpoint(project_id: str, page_id: str, min_similarity: float = 0.9):
    """Opportunités où la page est source ou cible."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_opportunities_for_page
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                pairs = get_opportunities_for_page(session, project_id, page_id, min_similarity)
                return {"pairs": pairs}
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            return {"pairs": []}
    return {"pairs": []}


@app.get("/api/projects/{project_id}/pages/{page_id}/similarity-stats")
async def get_similarity_stats_endpoint(project_id: str, page_id: str):
    """Similarité max pour cette page avec les pages non liées (diagnostic)."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_similarity_stats_for_page
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                return get_similarity_stats_for_page(session, project_id, page_id)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            return {"max_similarity": None, "pages_compared": 0}
    return {"max_similarity": None, "pages_compared": 0}


@app.get("/api/projects/{project_id}/pages/{page_id}/top-similar-pairs")
async def get_top_similar_pairs_endpoint(project_id: str, page_id: str, limit: int = 10):
    """Top N paires les plus proches (sans lien) pour cette page, quel que soit le seuil."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, get_top_similar_pairs_for_page, get_embeddings_status
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                emb = get_embeddings_status(session, project_id, page_id)
                if not emb.get("page_has_embedding"):
                    return {"pairs": [], "hint": "no_embedding"}
                pairs = get_top_similar_pairs_for_page(session, project_id, page_id, min(limit, 500))
                return {"pairs": pairs, "hint": "all_linked" if len(pairs) == 0 else None}
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            return {"pairs": [], "hint": "error"}
    return {"pairs": []}


class SaveOpportunitiesRequest(BaseModel):
    pairs: list[dict]


@app.post("/api/projects/{project_id}/opportunities/save")
async def save_opportunities_endpoint(project_id: str, body: SaveOpportunitiesRequest):
    """Enregistre des opportunités en BDD (stockage indéfini)."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, save_opportunity_records
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                saved = save_opportunity_records(session, project_id, body.pairs or [])
                return {"saved": saved, "count": len(saved)}
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur lors de l'enregistrement")
    raise HTTPException(status_code=404, detail="Projet non trouvé")


@app.get("/api/projects/{project_id}/opportunities/records")
async def list_opportunity_records_endpoint(project_id: str, page_id: str = None):
    """Liste les opportunités enregistrées, optionnellement filtrées par page."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, list_opportunity_records
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                records = list_opportunity_records(session, project_id, page_id)
                return {"records": records}
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            return {"records": []}
    return {"records": []}


@app.delete("/api/projects/{project_id}/opportunities/records/{record_id:int}")
async def delete_opportunity_record_endpoint(project_id: str, record_id: int):
    """Supprime une opportunité enregistrée."""
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_project, delete_opportunity_record
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                if not delete_opportunity_record(session, project_id, record_id):
                    raise HTTPException(status_code=404, detail="Enregistrement non trouvé")
                return {"ok": True, "message": "Opportunité supprimée"}
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur lors de la suppression")
    raise HTTPException(status_code=404, detail="Projet non trouvé")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
