"""
Silo - Semantic Cocoon / OSINT SEO Tool
API FastAPI pour le graphe sémantique, projets et statut du crawler.
"""
import os
import json
import uuid
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from typing import Optional, List
import jwt as pyjwt

# Une seule source de vérité (évite l'écrasement ultérieur par `os.environ.get("REDIS_URL")` sans défaut).
REDIS_URL = os.environ.get("REDIS_URL") or "redis://localhost:6380/0"
NER_QUEUE_KEY = "silo:ner_queue"

# ============================================================================
# AUTHENTIFICATION JWT
# ============================================================================

_SILO_JWT_SECRET = os.environ.get("JWT_SECRET") or os.environ.get("JWT_SECRET_KEY", "")
_SILO_JWT_ALGORITHM = "HS256"
_SILO_REQUIRE_AUTH = os.environ.get("SILO_REQUIRE_AUTH", "true").lower() == "true"

_bearer_scheme = HTTPBearer(auto_error=False)


def require_auth(credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme)) -> str:
    """Valide le JWT et retourne le user_id. Lève 401 si invalide."""
    if not _SILO_REQUIRE_AUTH:
        return "anonymous"
    if not credentials:
        raise HTTPException(status_code=401, detail="Token d'authentification requis")
    if not _SILO_JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT_SECRET_KEY non configuré sur le serveur")
    try:
        payload = pyjwt.decode(credentials.credentials, _SILO_JWT_SECRET, algorithms=[_SILO_JWT_ALGORITHM])
        user_id: str = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token invalide : user_id manquant")
        return user_id
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expiré")
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

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


class DirectoryTreeNode(BaseModel):
    """Nœud du graphe arborescence de répertoires (BRIEF_SILO_VUE_ARBORESCENCE_REPERTOIRES_2026-03-19)."""
    id: str
    path: str
    path_depth: int
    is_terminal: bool
    url: Optional[str] = None
    page_id: Optional[str] = None
    children_count: int = 0
    indexable_count: int = 0
    non_indexable_count: int = 0
    excluded: bool = False
    title: Optional[str] = None
    depth: Optional[int] = None


class DirectoryTreeEdge(BaseModel):
    source: str
    target: str


class DirectoryTreeData(BaseModel):
    nodes: list[DirectoryTreeNode]
    edges: list[DirectoryTreeEdge]
    excluded_count: int = 0


class CrawlStatus(BaseModel):
    """Réponse GET /api/projects/{id}/crawl-status — progression fiable (BRIEF crawl-status enrichi mars 2026)."""
    project_id: str
    status: str  # crawling | phase1_done | done | paused | stopped | idle | error
    urls_discovered: int = 0
    urls_processed: int = 0
    urls_total: Optional[int] = None  # Mode url_list: len(url_list). Mode seed_url: null.
    progress_percent: Optional[float] = None  # Fiable si urls_total défini ; sinon estimation ou null
    message: Optional[str] = None
    # Pipeline async (Phase 1 worker → Phase 2 worker) : le front peut rafraîchir / proposer une action
    pipeline_pending: bool = False
    pending_actions: list[str] = Field(
        default_factory=list,
        description="Codes stables pour i18n : phase2_required, embeddings_incomplete, pipeline_error.",
    )
    embeddings_remaining: Optional[int] = Field(
        default=None,
        description="Pages sans embedding (même critère que Phase 2). Null si pipeline_pending est faux.",
    )


class FetchModeOption(BaseModel):
    value: bool
    label_key: str
    description_key: str


def _default_fetch_mode_options() -> list[FetchModeOption]:
    return [
        FetchModeOption(
            value=True,
            label_key="crawl.fetch.stealthy",
            description_key="crawl.fetch.stealthy.help",
        ),
        FetchModeOption(
            value=False,
            label_key="crawl.fetch.http",
            description_key="crawl.fetch.http.help",
        ),
    ]


class CrawlConfig(BaseModel):
    max_depth: int = 3
    max_pages: int = 50
    run_ner: bool = True
    seed_url: Optional[str] = None  # Override: crawler depuis cette URL (ex: nœud sélectionné)
    url_list: Optional[List[str]] = None  # Liste d'URLs à crawler (uniquement ces URLs, max_depth=0)
    path_prefix: Optional[str] = None  # Borne le crawl au répertoire (ex. /fr). "" ou null = pas de restriction
    exclude_urls_with_params: bool = True  # Exclure les URLs avec query string (pagination, utm_*, etc.)
    # null/absent : pas d'alignement worker ; true/false : garantir ce mode avant enqueue (brief 2026-03-30)
    use_stealthy_fetcher: Optional[bool] = None


class CrawlConfigResponse(BaseModel):
    """Réponse GET /api/config — options dynamiques pour le formulaire de crawl (BRIEF_BACKEND_SILO_GET_API_CONFIG_2026-03-19)."""
    path_prefix_options: list[str]
    default_exclude_urls_with_params: bool = True
    fetch_mode_options: list[FetchModeOption] = Field(default_factory=_default_fetch_mode_options)
    default_use_stealthy_fetcher: bool = True
    worker_runtime_use_stealthy_fetcher: Optional[bool] = None
    worker_healthy: bool = False
    worker_restart_supported: bool = False
    # Deux workers crawl dédiés (SILO_DUAL_CRAWL_WORKERS=true) — choix JS / HTTP sans redémarrage
    dual_crawl_workers: bool = False
    worker_crawl_stealthy_healthy: Optional[bool] = None
    worker_crawl_http_healthy: Optional[bool] = None


# Store en mémoire (fallback si pas de DB)
_projects: dict[str, dict] = {}
_graph_cache: dict[str, GraphData] = {}
_crawl_status: dict[str, CrawlStatus] = {}

USE_DB = bool(os.environ.get("DATABASE_URL"))
QUEUE_KEY = "silo:crawl_queue"

_logger = logging.getLogger("silo-api")


def _api_default_use_stealthy_fetcher() -> bool:
    return os.getenv("SILO_USE_STEALTHY_FETCHER", "true").lower() == "true"


def _worker_restart_supported() -> bool:
    try:
        from worker_align import worker_restart_command_configured

        return worker_restart_command_configured()
    except Exception:
        return bool(os.environ.get("SILO_WORKER_RESTART_COMMAND", "").strip())


def _dual_crawl_workers() -> bool:
    """Deux files + deux processus crawl (stealthy vs HTTP), sans alignement par redémarrage."""
    return os.getenv("SILO_DUAL_CRAWL_WORKERS", "").lower() in ("1", "true", "yes")


def _crawl_queue_key_stealthy() -> str:
    return os.getenv("SILO_CRAWL_QUEUE_STEALTHY", "silo:crawl_queue:stealthy")


def _crawl_queue_key_http() -> str:
    return os.getenv("SILO_CRAWL_QUEUE_HTTP", "silo:crawl_queue:http")


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
        urls_total=3,
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
        "https://app.log8ot.com",
        "https://dev.log8ot.com",
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


class WorkerFetchConfigResponse(BaseModel):
    """État fetch worker (admin / support) — brief stealthy 2026-03-30."""

    desired_use_stealthy: bool
    runtime_use_stealthy: Optional[bool] = None
    healthy: bool = False
    last_restart_at: Optional[str] = None
    restart_in_progress: bool = False
    worker_restart_supported: bool = False


class WorkerRestartBody(BaseModel):
    use_stealthy_fetcher: bool


@app.get("/api/admin/worker/fetch-config", response_model=WorkerFetchConfigResponse)
async def admin_worker_fetch_config(_user: str = Depends(require_auth)):
    """
    Configuration fetch worker : valeurs Redis + intention de déploiement (SILO_USE_STEALTHY_FETCHER côté API).
    """
    wr: Optional[bool] = None
    healthy = False
    last_at: Optional[str] = None
    restarting = False
    r = _get_redis()
    if r:
        try:
            from worker.redis_runtime import (
                read_runtime_stealthy,
                is_worker_healthy,
                WORKER_LAST_RESTART_AT_KEY,
                WORKER_RESTART_IN_PROGRESS_KEY,
                dual_crawl_worker_health,
            )

            if _dual_crawl_workers():
                ws, wh = dual_crawl_worker_health(r)
                healthy = bool(ws and wh)
                wr = None
            else:
                wr = read_runtime_stealthy(r)
                healthy = is_worker_healthy(r)
            raw_lr = r.get(WORKER_LAST_RESTART_AT_KEY)
            if raw_lr:
                last_at = raw_lr.decode() if isinstance(raw_lr, bytes) else str(raw_lr)
            restarting = bool(r.get(WORKER_RESTART_IN_PROGRESS_KEY))
        except Exception:
            pass
    wrs = True if _dual_crawl_workers() else _worker_restart_supported()
    return WorkerFetchConfigResponse(
        desired_use_stealthy=_api_default_use_stealthy_fetcher(),
        runtime_use_stealthy=wr,
        healthy=healthy,
        last_restart_at=last_at,
        restart_in_progress=restarting,
        worker_restart_supported=wrs,
    )


@app.post("/api/admin/worker/restart")
async def admin_worker_restart(body: WorkerRestartBody, _user: str = Depends(require_auth)):
    """Redémarrage aligné sur un mode fetch explicite (sans lancer de crawl)."""
    if _dual_crawl_workers():
        return JSONResponse(
            status_code=400,
            content={
                "detail": "Mode SILO_DUAL_CRAWL_WORKERS actif : redémarrer les services silo-worker-crawl-stealthy / silo-worker-crawl-http via Docker.",
                "code": "DUAL_WORKERS_NO_SINGLE_RESTART",
            },
        )
    r = _get_redis()
    if not r:
        return JSONResponse(
            status_code=503,
            content={"detail": "Redis indisponible.", "code": "WORKER_UNAVAILABLE"},
        )
    try:
        from worker_align import ensure_worker_fetch_mode, AlignWorkerError

        action = ensure_worker_fetch_mode(r, body.use_stealthy_fetcher)
    except AlignWorkerError as e:
        _logger.warning(
            "worker_restart_failed user=%s code=%s detail=%s",
            _user,
            e.code,
            e.detail,
        )
        status = 503
        if e.code == "WORKER_ALIGN_IN_PROGRESS":
            status = 409
        elif e.code == "WORKER_ALIGN_TIMEOUT":
            status = 504
        return JSONResponse(status_code=status, content={"detail": e.detail, "code": e.code})
    _logger.info(
        "worker_restart_ok user=%s use_stealthy=%s worker_action=%s",
        _user,
        body.use_stealthy_fetcher,
        action,
    )
    return {"ok": True, "message": "Worker aligné sur le mode demandé.", "worker_action": action}


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


# Configuration crawl dynamique (path_prefix_options, etc.) — BRIEF_BACKEND_SILO_GET_API_CONFIG_2026-03-19
DEFAULT_PATH_PREFIX_OPTIONS = ["fr", "en", "de", "it", "es", "nl", "pt"]


def _get_path_prefix_options() -> list[str]:
    """Codes de répertoire de langue supportés (sans slash). Configurable via SILO_PATH_PREFIX_OPTIONS."""
    raw = os.getenv("SILO_PATH_PREFIX_OPTIONS", "fr,en,de,it,es,nl,pt")
    options = [s.strip().lstrip("/") for s in raw.split(",") if s.strip()]
    # Validation : 2–5 caractères (codes ISO 639-1)
    valid = [o for o in options if 2 <= len(o) <= 5]
    return valid if valid else DEFAULT_PATH_PREFIX_OPTIONS


def _get_default_exclude_urls_with_params() -> bool:
    """Valeur par défaut du toggle « Exclure les URLs avec paramètres ». Configurable via SILO_DEFAULT_EXCLUDE_URLS_WITH_PARAMS."""
    return os.getenv("SILO_DEFAULT_EXCLUDE_URLS_WITH_PARAMS", "true").lower() == "true"


@app.get("/api/config", response_model=CrawlConfigResponse)
async def get_crawl_config():
    """
    Configuration pour le formulaire de crawl (options path_prefix dynamiques).
    Le frontend utilise ces valeurs pour le select « Répertoire de langue ».
    Sans authentification (données non sensibles).
    Configurable via SILO_PATH_PREFIX_OPTIONS et SILO_DEFAULT_EXCLUDE_URLS_WITH_PARAMS.
    """
    wr: Optional[bool] = None
    healthy = False
    dual = _dual_crawl_workers()
    ws: Optional[bool] = None
    wh: Optional[bool] = None
    r = _get_redis()
    if r:
        try:
            from worker.redis_runtime import read_runtime_stealthy, is_worker_healthy, dual_crawl_worker_health

            if dual:
                ws, wh = dual_crawl_worker_health(r)
                wr = None
                healthy = bool(ws and wh)
            else:
                wr = read_runtime_stealthy(r)
                healthy = is_worker_healthy(r)
        except Exception:
            pass
    wrs = True if dual else _worker_restart_supported()
    return CrawlConfigResponse(
        path_prefix_options=_get_path_prefix_options(),
        default_exclude_urls_with_params=_get_default_exclude_urls_with_params(),
        fetch_mode_options=_default_fetch_mode_options(),
        default_use_stealthy_fetcher=_api_default_use_stealthy_fetcher(),
        worker_runtime_use_stealthy_fetcher=wr,
        worker_healthy=healthy,
        worker_restart_supported=wrs,
        dual_crawl_workers=dual,
        worker_crawl_stealthy_healthy=ws,
        worker_crawl_http_healthy=wh,
    )


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
async def create_project(name: str, seed_url: str, _user: str = Depends(require_auth)):
    from urllib.parse import urlparse
    parsed = urlparse(seed_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="seed_url doit être une URL http ou https valide")
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


@app.get("/api/projects/{project_id}/graph-directory-tree", response_model=DirectoryTreeData)
async def get_graph_directory_tree(project_id: str, include_excluded: bool = False):
    """
    Graphe arborescence de répertoires (structure URL). Par défaut exclut les URLs avec paramètres.
    ?include_excluded=true pour les inclure.
    Brief: BRIEF_SILO_VUE_ARBORESCENCE_REPERTOIRES_2026-03-19.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_graph_directory_tree as db_dir_tree, get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                g = db_dir_tree(session, project_id, include_excluded=include_excluded)
                return DirectoryTreeData(**g)
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            pass
    if project_id in _projects:
        return DirectoryTreeData(nodes=[], edges=[], excluded_count=0)
    raise HTTPException(status_code=404, detail="Projet non trouvé")


CRAWL_LOGS_KEY = "silo:crawl_logs"
CRAWL_PAUSE_KEY = "silo:crawl_pause"
CRAWL_STOP_KEY = "silo:crawl_stop"
CRAWL_URLS_TOTAL_KEY = "silo:crawl_urls_total"  # Mode url_list: total connu dès le début


def _get_crawl_urls_total(project_id: str) -> Optional[int]:
    """Récupère urls_total depuis Redis (mode url_list). Retourne None si absent."""
    r = _get_redis()
    if not r:
        return None
    try:
        val = r.get(f"{CRAWL_URLS_TOTAL_KEY}:{project_id}")
        if val is not None:
            return int(val)
    except (ValueError, TypeError):
        pass
    return None


def _set_crawl_urls_total(project_id: str, total: int):
    """Stocke urls_total dans Redis (mode url_list). TTL 7 jours."""
    r = _get_redis()
    if r and total > 0:
        try:
            r.set(f"{CRAWL_URLS_TOTAL_KEY}:{project_id}", str(total), ex=604800)
        except Exception:
            pass


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


def _build_crawl_status_enriched(project_id: str, s: dict) -> CrawlStatus:
    """
    Enrichit la réponse crawl-status avec urls_total (Redis) et progress_percent fiable.
    Brief: progression fiable pour mode url_list (recrawl) et seed_url (découverte).
    """
    urls_total = _get_crawl_urls_total(project_id)
    urls_processed = s.get("urls_processed", 0) or 0
    status = s.get("status") or "idle"
    pipeline_pending = bool(s.get("pipeline_pending"))
    pending_actions = list(s.get("pending_actions") or [])
    embeddings_remaining = s.get("embeddings_remaining")

    if urls_total is not None:
        urls_discovered = urls_total
        if status == "done" and not pipeline_pending:
            progress_percent = 100.0
        elif status == "done" and pipeline_pending:
            progress_percent = None
        elif urls_total > 0:
            pct = urls_processed / urls_total * 100
            progress_percent = min(100.0, round(pct))
        else:
            progress_percent = 0
    else:
        # Mode seed_url : total inconnu. urls_discovered = max(DB, processed) pour affichage.
        urls_discovered = max(
            s.get("urls_discovered", 0) or 0,
            urls_processed,
        )
        if status == "done" and not pipeline_pending:
            progress_percent = 100.0
        elif status == "done" and pipeline_pending:
            progress_percent = None
        elif status == "phase1_done":
            # Phase 1 terminée, Phase 2 à venir : éviter d'afficher 100 % comme « tout fini »
            progress_percent = None
        else:
            # Total inconnu en mode seed_url → progression non fiable (évite 526300% si urls_discovered=1)
            progress_percent = None

    return CrawlStatus(
        project_id=project_id,
        status=status,
        urls_discovered=urls_discovered,
        urls_processed=urls_processed,
        urls_total=urls_total,
        progress_percent=progress_percent,
        message=s.get("message"),
        pipeline_pending=pipeline_pending,
        pending_actions=pending_actions,
        embeddings_remaining=embeddings_remaining,
    )


@app.get("/api/projects/{project_id}/crawl-status", response_model=CrawlStatus)
async def get_crawl_status(project_id: str):
    """
    Statut du crawl avec progression fiable.
    urls_total: défini en mode url_list (recrawl), null en mode seed_url.
    progress_percent: fiable quand urls_total défini ; sinon estimation ou null.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import get_crawl_status as db_status, get_project
            session = get_session()
            try:
                if not get_project(session, project_id):
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                s = db_status(session, project_id)
                if s:
                    return _build_crawl_status_enriched(project_id, s)
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
async def pause_crawl(project_id: str, _user: str = Depends(require_auth)):
    """Met le crawl en pause."""
    r = _get_redis()
    if r:
        try:
            r.set(f"{CRAWL_PAUSE_KEY}:{project_id}", "1", ex=86400)
        except Exception:
            pass
    return {"ok": True, "message": "Crawl mis en pause"}


@app.post("/api/projects/{project_id}/crawl/resume")
async def resume_crawl(project_id: str, _user: str = Depends(require_auth)):
    """Reprend le crawl après une pause."""
    r = _get_redis()
    if r:
        try:
            r.delete(f"{CRAWL_PAUSE_KEY}:{project_id}")
        except Exception:
            pass
    return {"ok": True, "message": "Crawl repris"}


@app.post("/api/projects/{project_id}/crawl/stop")
async def stop_crawl(project_id: str, _user: str = Depends(require_auth)):
    """Stoppe définitivement le crawl en cours."""
    _force_crawl_stop_signals(project_id)

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
async def start_crawl(project_id: str, config: Optional[CrawlConfig] = Body(default=None), _user: str = Depends(require_auth)):
    """
    Démarre le crawl. Body optionnel: CrawlConfig.
    Si use_stealthy_fetcher est true/false, aligne le worker (redémarrage possible) avant enqueue.
    """
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

    t_align0 = time.monotonic()
    worker_action: str = "none"
    r = _get_redis()
    dual = _dual_crawl_workers()
    target_queue = QUEUE_KEY
    effective_fetch: Optional[bool] = cfg.use_stealthy_fetcher

    if dual:
        if not r:
            return JSONResponse(
                status_code=503,
                content={"detail": "Redis indisponible : impossible de vérifier les workers crawl.", "code": "WORKER_UNAVAILABLE"},
            )
        if effective_fetch is None:
            effective_fetch = _api_default_use_stealthy_fetcher()
        target_queue = _crawl_queue_key_stealthy() if effective_fetch else _crawl_queue_key_http()
        try:
            from worker.redis_runtime import CRAWL_ROLE_STEALTHY, CRAWL_ROLE_HTTP, is_worker_healthy

            role_ok = is_worker_healthy(
                r,
                CRAWL_ROLE_STEALTHY if effective_fetch else CRAWL_ROLE_HTTP,
            )
            if not role_ok:
                return JSONResponse(
                    status_code=503,
                    content={
                        "detail": "Le worker crawl demandé (navigateur/JS ou HTTP) ne répond pas (heartbeat).",
                        "code": "WORKER_CRAWL_UNHEALTHY",
                        "use_stealthy_fetcher": effective_fetch,
                    },
                )
        except Exception as e:
            _logger.warning("dual_crawl health check: %s", e)
            return JSONResponse(
                status_code=503,
                content={"detail": "Impossible de vérifier l'état des workers crawl.", "code": "WORKER_UNAVAILABLE"},
            )
    elif cfg.use_stealthy_fetcher is not None:
        if not r:
            return JSONResponse(
                status_code=503,
                content={"detail": "Redis indisponible : impossible de vérifier le mode du worker.", "code": "WORKER_UNAVAILABLE"},
            )
        try:
            from worker_align import ensure_worker_fetch_mode, AlignWorkerError

            worker_action = ensure_worker_fetch_mode(r, cfg.use_stealthy_fetcher)
        except AlignWorkerError as e:
            _logger.warning(
                "crawl_align_failed user=%s project=%s code=%s duration_ms=%s",
                _user,
                project_id,
                e.code,
                int((time.monotonic() - t_align0) * 1000),
            )
            status = 503
            if e.code == "WORKER_ALIGN_IN_PROGRESS":
                status = 409
            elif e.code == "WORKER_ALIGN_TIMEOUT":
                status = 504
            return JSONResponse(status_code=status, content={"detail": e.detail, "code": e.code})

    log_extra = effective_fetch if dual else cfg.use_stealthy_fetcher
    if log_extra is None and r and not dual:
        try:
            from worker_align import get_worker_state

            s, _ = get_worker_state(r)
            log_extra = s
        except Exception:
            pass

    _logger.info(
        "crawl_start user=%s project=%s use_stealthy_requested=%s worker_action=%s restarted=%s duration_align_ms=%s outcome=%s dual=%s queue=%s",
        _user,
        project_id,
        cfg.use_stealthy_fetcher,
        worker_action,
        worker_action == "restarted",
        int((time.monotonic() - t_align0) * 1000),
        "enqueue",
        dual,
        target_queue,
    )

    if not r:
        return JSONResponse(
            status_code=503,
            content={"detail": "Redis indisponible : impossible d'enfiler le crawl.", "code": "WORKER_UNAVAILABLE"},
        )

    urls_total_for_status = None
    try:
        r.delete(f"{CRAWL_PAUSE_KEY}:{project_id}", f"{CRAWL_STOP_KEY}:{project_id}")
        base_log: dict = {}
        if cfg.use_stealthy_fetcher is not None:
            base_log["requested_fetch_mode"] = cfg.use_stealthy_fetcher
        elif dual and effective_fetch is not None:
            base_log["requested_fetch_mode"] = effective_fetch

        if cfg.url_list and len(cfg.url_list) > 0:
            from urllib.parse import urlparse

            valid_count = 0
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
                    **base_log,
                }
                r.rpush(target_queue, json.dumps(payload))
                valid_count += 1
            urls_total_for_status = valid_count
            _set_crawl_urls_total(project_id, valid_count)
        else:
            payload = {
                "project_id": project_id,
                "seed_url": seed_url,
                "max_depth": cfg.max_depth,
                "max_pages": cfg.max_pages,
                "run_ner": cfg.run_ner,
                "path_prefix": path_prefix,
                "exclude_urls_with_params": cfg.exclude_urls_with_params,
                **base_log,
            }
            r.rpush(target_queue, json.dumps(payload))
    except Exception as e:
        _logger.exception("Enqueue crawl: %s", e)
        return JSONResponse(
            status_code=503,
            content={"detail": "Erreur lors de l'enfilement du crawl.", "code": "WORKER_UNAVAILABLE"},
        )

    use_eff: Optional[bool] = effective_fetch if dual else cfg.use_stealthy_fetcher
    if use_eff is None and not dual:
        try:
            from worker.redis_runtime import read_runtime_stealthy

            use_eff = read_runtime_stealthy(r)
        except Exception:
            use_eff = None

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
            urls_discovered=urls_total_for_status or 0,
            urls_processed=0,
            urls_total=urls_total_for_status,
            progress_percent=0.0 if urls_total_for_status else None,
            message="Crawl en cours",
        )

    return {
        "ok": True,
        "message": "Crawl démarré",
        "worker_action": worker_action,
        "use_stealthy_fetcher": use_eff,
    }


# Seuil edges au-delà duquel la suppression est asynchrone. Défaut 0 = toujours async (évite timeout front).
DELETE_ASYNC_EDGES_THRESHOLD = int(os.environ.get("SILO_DELETE_ASYNC_THRESHOLD", "0"))


@app.delete("/api/projects/{project_id}")
async def delete_project_endpoint(project_id: str, async_only: bool = False, _user: str = Depends(require_auth)):
    """
    Supprime un projet et ses données (pages, edges).
    Toujours async par défaut : 202 Accepted, soft delete immédiat, worker en arrière-plan.
    Évite les timeouts front (30s) sur les projets avec beaucoup de pages.
    Force l'arrêt du crawl en cours (même signaux Redis que POST .../crawl/stop) avant la suppression.
    """
    if USE_DB:
        try:
            from database.db import get_session
            from database.service import (
                delete_project as db_delete,
                get_project,
                count_project_edges,
                mark_project_deleted,
                update_project_status,
            )
            session = get_session()
            try:
                p = get_project(session, project_id)
                if not p:
                    raise HTTPException(status_code=404, detail="Projet non trouvé")
                edges_count = count_project_edges(session, project_id)
                session.close()

                _force_crawl_stop_signals(project_id)
                # Cohérence statut BDD (comme stop_crawl) tant que le projet est encore visible
                try:
                    session = get_session()
                    try:
                        if get_project(session, project_id):
                            update_project_status(session, project_id, "done")
                    finally:
                        session.close()
                except Exception:
                    pass

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
async def run_ner_on_demand_endpoint(project_id: str, body: Optional[NerRequest] = Body(default=None), _user: str = Depends(require_auth)):
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
NER_IN_PROGRESS_KEY = "silo:ner_in_progress"
RECOMPUTE_IN_PROGRESS_KEY = "silo:recompute_in_progress"


@app.post("/api/projects/{project_id}/recompute-silos")
async def recompute_silos_endpoint(project_id: str, _user: str = Depends(require_auth)):
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
            if r.exists(f"{RECOMPUTE_IN_PROGRESS_KEY}:{project_id}"):
                raise HTTPException(status_code=409, detail="Un recalcul des silos est déjà en cours pour ce projet")
        r.rpush(RECOMPUTE_SILOS_QUEUE_KEY, json.dumps({"project_id": project_id}))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lancer: {e}")
    return {"ok": True, "message": "Recalcul des silos lancé (traitement en cours)"}


@app.get("/api/projects/{project_id}/recompute-silos-status")
async def get_recompute_silos_status_endpoint(project_id: str):
    """Statut du recalcul des silos (Louvain) — progression ou indicateur en cours."""
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
            in_progress = bool(r.exists(f"{RECOMPUTE_IN_PROGRESS_KEY}:{project_id}"))
            result = {"recompute_in_progress": in_progress}
            if in_progress:
                progress_data = r.get(f"silo:recompute_progress:{project_id}")
                if progress_data:
                    try:
                        data = json.loads(progress_data)
                        result["total_edges"] = data.get("total_edges")
                        result["edges_processed"] = data.get("edges_processed")
                        total = data.get("total_nodes") or data.get("total_edges") or 0
                        processed = data.get("edges_processed") or 0
                        result["progress_percent"] = round(100 * processed / total) if total > 0 else None
                    except (json.JSONDecodeError, TypeError):
                        result["progress_percent"] = None
                else:
                    result["total_edges"] = None
                    result["edges_processed"] = None
                    result["progress_percent"] = None
            else:
                result["total_edges"] = None
                result["edges_processed"] = None
                result["progress_percent"] = None
            return result
    except Exception:
        pass
    return {"recompute_in_progress": False, "total_edges": None, "edges_processed": None, "progress_percent": None}


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
                        # Progression temps réel (Redis mis à jour dès le début du job + avant chaque lot)
                        if result["embedding_in_progress"]:
                            progress_data = r.get(f"silo:embedding_progress:{project_id}")
                            if progress_data:
                                try:
                                    data = json.loads(progress_data)
                                    result["pages_with_embedding"] = data.get(
                                        "pages_with_embedding", result["pages_with_embedding"]
                                    )
                                    result["total_pages"] = data.get("total_pages", result["total_pages"])
                                    result["has_embeddings"] = result["pages_with_embedding"] > 0
                                    if "pages_pending_embedding" in data:
                                        result["pages_pending_embedding"] = data["pages_pending_embedding"]
                                    if data.get("phase"):
                                        result["embedding_phase"] = data["phase"]
                                    if data.get("chunk") is not None:
                                        result["embedding_chunk"] = data["chunk"]
                                        result["embedding_chunk_size"] = data.get("chunk_size")
                                    elif data.get("batch") is not None:
                                        result["embedding_chunk"] = data["batch"]
                                        result["embedding_chunk_size"] = data.get("batch_size")
                                    pw = result.get("pages_with_embedding") or 0
                                    pend = result.get("pages_pending_embedding")
                                    if pend is not None and (pw + pend) > 0:
                                        result["progress_percent"] = round(100.0 * pw / (pw + pend), 1)
                                    elif result.get("pages_embedding_eligible"):
                                        elg = result["pages_embedding_eligible"]
                                        result["progress_percent"] = round(100.0 * pw / elg, 1)
                                except (json.JSONDecodeError, TypeError):
                                    pass
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
                result = get_ner_status(session, project_id)
                # Indicateur backend : job NER en cours (verrou Redis)
                try:
                    import redis
                    r = redis.from_url(REDIS_URL)
                    if r:
                        result["ner_in_progress"] = bool(r.exists(f"{NER_IN_PROGRESS_KEY}:{project_id}"))
                        if result["ner_in_progress"]:
                            raw_np = r.get(f"silo:ner_progress:{project_id}")
                            if raw_np:
                                try:
                                    nd = json.loads(raw_np)
                                    if "pages_pending_ner" in nd:
                                        result["pages_pending_ner"] = nd["pages_pending_ner"]
                                    if "pages_ner_eligible" in nd:
                                        result["pages_ner_eligible"] = nd["pages_ner_eligible"]
                                    if nd.get("batch_size") is not None:
                                        result["ner_batch_size"] = nd["batch_size"]
                                    pe = result.get("pages_ner_eligible") or 0
                                    pp = result.get("pages_pending_ner")
                                    if pe > 0 and pp is not None:
                                        result["progress_percent"] = round(100.0 * (pe - pp) / pe, 1)
                                except (json.JSONDecodeError, TypeError):
                                    pass
                    else:
                        result["ner_in_progress"] = False
                except Exception:
                    result["ner_in_progress"] = False
                return result
            finally:
                session.close()
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=500, detail="Erreur statut NER")
    return {"total_pages": 0, "pages_with_entities": 0, "ner_in_progress": False}


COMPUTE_EMBEDDINGS_QUEUE_KEY = "silo:compute_embeddings_queue"
EMBEDDING_IN_PROGRESS_KEY = "silo:embedding_in_progress"
COMPUTE_OPPORTUNITIES_QUEUE_KEY = "silo:compute_opportunities_queue"
OPPORTUNITIES_IN_PROGRESS_KEY = "silo:opportunities_in_progress"


def _force_crawl_stop_signals(project_id: str) -> None:
    """Signaux Redis pour arrêter le crawl (stop explicite ou suppression projet)."""
    r = _get_redis()
    if not r:
        return
    try:
        r.set(f"{CRAWL_STOP_KEY}:{project_id}", "1", ex=86400)
        r.delete(f"{CRAWL_PAUSE_KEY}:{project_id}")
        r.delete(f"{EMBEDDING_IN_PROGRESS_KEY}:{project_id}")  # idem stop crawl
    except Exception:
        pass


@app.post("/api/projects/{project_id}/compute-embeddings")
async def compute_embeddings_endpoint(project_id: str, page_id: str = None, _user: str = Depends(require_auth)):
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
async def compute_opportunities_endpoint(project_id: str, _user: str = Depends(require_auth)):
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


def _export_attachment_response(body: bytes, filename: str, content_type: str) -> Response:
    """Fichier brut + Content-Disposition (évite un JSON {content:...} qui double la taille et fait planter le navigateur)."""
    safe = filename.replace('"', "").replace("\r", "").replace("\n", "")
    return Response(
        content=body,
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{safe}"'},
    )


@app.get("/api/opportunities/{project_id}/export")
async def export_opportunities(project_id: str, min_similarity: float = 0.9, format: str = "json"):
    """
    Export des opportunités en fichier (json, csv ou md).
    Corps HTTP = fichier brut ; en-tête Content-Disposition: attachment.
    Le front doit utiliser response.blob() puis lien de téléchargement (pas response.json()).
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
        content_b = ("\uFEFF" + out.getvalue()).encode("utf-8")
        return _export_attachment_response(content_b, f"{base_filename}.csv", "text/csv; charset=utf-8")

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
        md_b = "\n".join(lines).encode("utf-8")
        return _export_attachment_response(md_b, f"{base_filename}.md", "text/markdown; charset=utf-8")
    # json par défaut : compact (indent=false) pour limiter la taille sur gros volumes
    json_b = json.dumps(
        {"pairs": pairs, "project_id": project_id},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return _export_attachment_response(json_b, f"{base_filename}.json", "application/json; charset=utf-8")


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
async def save_opportunities_endpoint(project_id: str, body: SaveOpportunitiesRequest, _user: str = Depends(require_auth)):
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
async def delete_opportunity_record_endpoint(project_id: str, record_id: int, _user: str = Depends(require_auth)):
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
