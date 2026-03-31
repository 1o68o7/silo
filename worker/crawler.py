"""
Worker de crawl Silo - Brief complet.
Architecture en 2 phases:
  Phase 1: Crawl rapide (URLs, structure, silos) - sans NER ni embeddings lourds
  Phase 2: Détection NER (à la demande ou en batch après Phase 1)
Stack: Scrapling (StealthyFetcher) + Trafilatura + FastEmbed + spaCy + CDlib.
Reasonable Surfer: poids positionnel, similarité sémantique, bonus ancre NER.
"""
import gc
import os
import re
import hashlib
import json
import logging
from urllib.parse import urljoin, urlparse

import redis
from trafilatura import extract
from fastembed import TextEmbedding
import networkx as nx

from worker.fetcher import fetch_html, fetch_urls_parallel
from worker.link_extractor import get_links_with_context
from worker.ner import extract_entities, extract_entities_batch, anchor_contains_entity
from worker.redis_runtime import refresh_worker_heartbeat
from worker.url_utils import url_has_query_params, url_matches_path_prefix

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silo-worker")

# Singleton FastEmbed pour éviter rechargements et fuites mémoire
_embedding_model = None
# e5-large (~2.2 Go) : batch 16 pour éviter OOM. e5-small : 64 OK.
EMBEDDING_BATCH_SIZE = int(os.environ.get("SILO_EMBEDDING_BATCH_SIZE", "16"))
# Taille des lots DB : évite "named cursor isn't valid anymore" (connexion tenue trop longtemps)
DB_CHUNK_SIZE = 200
# Chunk DB pour autres chemins ; run_compute_embeddings utilise curseur + EMBEDDING_BATCH_SIZE uniquement
EMBEDDING_DB_CHUNK = int(os.environ.get("SILO_EMBEDDING_DB_CHUNK", "50"))
# Phase 1: batch commits (réduit round-trips DB)
PHASE1_BATCH_COMMIT = int(os.environ.get("SILO_PHASE1_BATCH_COMMIT", "5"))
# Phase 1: fetch parallèle (0 = désactivé)
PHASE1_FETCH_PARALLEL = int(os.environ.get("SILO_FETCH_PARALLEL_WORKERS", "3"))
# Phase 1: pipeline fetch (pre-fetch next batch pendant le traitement)
PHASE1_PIPELINE_FETCH = os.environ.get("SILO_PIPELINE_FETCH", "true").lower() == "true"
# Modèle configurable : intfloat/multilingual-e5-large (défaut) ou intfloat/multilingual-e5-small (plus léger)
EMBEDDING_MODEL = os.environ.get("SILO_EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
# Troncature texte pour embeddings (réduit calcul, 3000 suffit pour la sémantique)
EMBEDDING_TEXT_MAX_CHARS = int(os.environ.get("SILO_EMBEDDING_TEXT_MAX_CHARS", "3000"))
# Fenêtre contexte liens (Phase 1) — 150 réduit légèrement le traitement lxml
LINK_CONTEXT_WINDOW = int(os.environ.get("SILO_LINK_CONTEXT_WINDOW", "150"))
# Phase 2: skip Reasonable Surfer (embeddings contexte) — 20–40 % plus rapide
RUN_REASONABLE_SURFER = os.environ.get("SILO_RUN_REASONABLE_SURFER", "true").lower() == "true"
# Phase 1: Louvain différé (exécuter à la demande via Recalcul silos)
LOUVAIN_DEFERRED = os.environ.get("SILO_LOUVAIN_DEFERRED", "false").lower() == "true"


def _get_embedding_model():
    """Retourne le modèle FastEmbed (singleton, évite ~1.5 Go par chargement)."""
    global _embedding_model
    if _embedding_model is None:
        import warnings
        with warnings.catch_warnings(action="ignore", category=UserWarning):
            _embedding_model = TextEmbedding(
                EMBEDDING_MODEL,
                cache_dir="/tmp/fastembed_cache",
                threads=int(os.environ.get("SILO_FASTEMBED_THREADS", "1") or "1"),
            )
        logger.info(f"Modèle FastEmbed chargé: {EMBEDDING_MODEL}")
    return _embedding_model


def _embed_texts(model, texts: list):
    """Encapsule model.embed : itération + heartbeat pendant l'inférence (ONNX peut monopoliser le CPU longtemps)."""
    if not texts:
        return []
    try:
        logger.debug(f"Embedding batch de {len(texts)} texte(s)")
        out = []
        r = redis.from_url(REDIS_URL)
        role = _worker_redis_heartbeat_role()
        bs = max(1, min(len(texts), EMBEDDING_BATCH_SIZE))
        n = 0
        for emb in model.embed(texts, batch_size=bs, parallel=None):
            out.append(emb)
            n += 1
            if n % 2 == 0 or n == len(texts):
                try:
                    refresh_worker_heartbeat(r, role)
                except Exception:
                    pass
        return out
    except Exception as e:
        logger.exception(f"Erreur model.embed: {e}")
        raise

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://admin:password_secret@localhost:5433/semantic_cocoon")


def _worker_redis_heartbeat_role():
    """Même clé que run.py / HEALTHCHECK worker (None → silo:worker_runtime:heartbeat)."""
    return os.environ.get("SILO_WORKER_CRAWL_ROLE", "").strip() or None


def _refresh_worker_hb():
    try:
        refresh_worker_heartbeat(redis.from_url(REDIS_URL), _worker_redis_heartbeat_role())
    except Exception:
        pass
CRAWL_LOGS_KEY = "silo:crawl_logs"
CRAWL_PAUSE_KEY = "silo:crawl_pause"
CRAWL_STOP_KEY = "silo:crawl_stop"
MAX_LOGS = 300


def _push_log(project_id: str, level: str, msg: str, url: str = None, extra: dict = None):
    try:
        r = redis.from_url(REDIS_URL)
        entry = {
            "ts": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "level": level,
            "msg": msg,
            "url": url,
            **(extra or {}),
        }
        key = f"{CRAWL_LOGS_KEY}:{project_id}"
        r.rpush(key, json.dumps(entry))
        r.ltrim(key, -MAX_LOGS, -1)
    except Exception:
        pass


def _check_pause(project_id: str) -> bool:
    """Vérifie si le crawl doit être mis en pause."""
    try:
        r = redis.from_url(REDIS_URL)
        return bool(r.get(f"{CRAWL_PAUSE_KEY}:{project_id}"))
    except Exception:
        return False


def _check_stop(project_id: str) -> bool:
    """Vérifie si le crawl doit être stoppé."""
    try:
        r = redis.from_url(REDIS_URL)
        return bool(r.get(f"{CRAWL_STOP_KEY}:{project_id}"))
    except Exception:
        return False


def run_delete_project(project_id: str) -> bool:
    """
    Supprime un projet en arrière-plan (batch avec COMMIT).
    Appelé par le worker depuis la queue silo:delete_project_queue.
    """
    lock_key = f"silo:delete_project_lock:{project_id}"
    try:
        rlock = redis.from_url(REDIS_URL)
        # Évite deux workers qui purgent le même projet en parallèle
        if not rlock.set(lock_key, "1", nx=True, ex=7200):
            logger.info(f"Suppression déjà en cours pour {project_id}, on ignore ce job")
            return True
    except Exception as e:
        logger.warning("Lock suppression %s: %s — nouvel essai ultérieur", project_id, e)
        return False

    try:
        from database.db import get_session
        from database.service import delete_project, get_project

        session = get_session()
        try:
            if not get_project(session, project_id, include_deleted=True):
                logger.info(f"Projet {project_id} déjà absent (purge idempotente)")
                return True
            delete_project(session, project_id)
            logger.info(f"Projet {project_id} supprimé (async)")
            return True
        finally:
            session.close()
    except Exception as e:
        logger.exception(f"Erreur suppression projet {project_id}: {e}")
        return False
    finally:
        try:
            redis.from_url(REDIS_URL).delete(lock_key)
        except Exception:
            pass


def url_to_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def url_to_page_id(project_id: str, url: str) -> str:
    """ID unique par projet (évite collision entre projets partageant les mêmes URLs)."""
    return f"{project_id}_{url_to_id(url)}"


def _cosine_sim(a: list, b: list) -> float:
    """Similarité cosine entre deux vecteurs. Évite ValueError sur numpy arrays."""
    if a is None or b is None or len(a) == 0 or len(b) == 0 or len(a) != len(b):
        return 0.0
    try:
        import numpy as np
        va, vb = np.array(a, dtype=float), np.array(b, dtype=float)
        n = np.linalg.norm(va) * np.linalg.norm(vb)
        if n < 1e-9:
            return 0.0
        return float(np.dot(va, vb) / n)
    except ImportError:
        dot = sum(x * y for x, y in zip(a, b))
        na = sum(x * x for x in a) ** 0.5
        nb = sum(y * y for y in b) ** 0.5
        if na * nb < 1e-9:
            return 0.0
        return dot / (na * nb)


def run_crawl_phase1(
    project_id: str,
    seed_url: str,
    max_depth: int = 3,
    max_pages: int = 50,
    path_prefix: str | None = None,
    exclude_urls_with_params: bool = True,
):
    """
    Phase 1: Crawl rapide - URLs, structure, silos.
    Optimisations: fetch parallèle, cache pages/edges, batch commits, bulk update Louvain.
    """
    import time
    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker
    from database.db import get_engine
    from database.models import Project, Page, Edge, Base

    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        session.commit()
    except Exception:
        session.rollback()

    project = session.query(Project).filter(Project.id == project_id).first()
    if not project:
        logger.error(f"Projet {project_id} non trouvé")
        session.close()
        return
    project.status = "crawling"
    session.commit()
    _push_log(project_id, "info", f"[Phase 1] Crawl rapide depuis {seed_url} (URLs + structure)", url=seed_url)

    discovered = {seed_url}
    to_visit = [(seed_url, 0)]
    visited = set()
    G = nx.DiGraph()

    # Cache pour éviter N+1 (pages et edges existants)
    existing_page_ids = {r[0] for r in session.query(Page.id).filter(Page.project_id == project_id).all()}
    existing_edges = {(r[0], r[1]) for r in session.query(Edge.source_id, Edge.target_id).filter(
        Edge.project_id == project_id
    ).all()}

    batch_size = PHASE1_FETCH_PARALLEL if PHASE1_FETCH_PARALLEL > 1 else 1
    pages_since_commit = 0

    def _flush_batch(commit_project=True):
        nonlocal pages_since_commit
        if pages_since_commit > 0:
            session.commit()
            if commit_project:
                project.urls_count = len(visited)
                project.status = "crawling"
                session.commit()
            pages_since_commit = 0

    fetch_future = None
    from concurrent.futures import ThreadPoolExecutor
    _fetch_executor = ThreadPoolExecutor(max_workers=1) if PHASE1_PIPELINE_FETCH and batch_size > 1 else None

    def _pop_batch():
        out = []
        while len(out) < batch_size and to_visit and len(visited) + len(out) < max_pages:
            url, depth = to_visit.pop(0)
            if url not in visited:
                out.append((url, depth))
        return out

    try:
        urls_batch = _pop_batch()
        fetched = {}
        if urls_batch:
            urls_only = [u for u, _ in urls_batch]
            if batch_size > 1:
                fetched = fetch_urls_parallel(urls_only)
            else:
                html = fetch_html(urls_only[0]) if urls_only else None
                fetched = {urls_only[0]: html} if html else {}

        while urls_batch and len(visited) < max_pages:
            if _check_stop(project_id):
                _push_log(project_id, "info", "Crawl stoppé par l'utilisateur")
                project.status = "done"
                _flush_batch()
                return

            while _check_pause(project_id):
                _push_log(project_id, "info", "Crawl en pause...")
                time.sleep(2)

            # Pipeline: récupérer le fetch lancé à l'itération précédente
            if fetch_future is not None:
                try:
                    fetched = fetch_future.result(timeout=120)
                except Exception as e:
                    logger.warning(f"Fetch pipeline: {e}")
                    fetched = {}
                fetch_future = None

            # Lancer le fetch du prochain batch en arrière-plan (pendant le traitement)
            next_urls_batch = _pop_batch()
            if next_urls_batch and _fetch_executor:
                next_urls_only = [u for u, _ in next_urls_batch]
                fetch_future = _fetch_executor.submit(fetch_urls_parallel, next_urls_only)

            for (url, depth), html in (((u, d), fetched.get(u)) for u, d in urls_batch):
                if not html:
                    _push_log(project_id, "warn", f"Pas de HTML récupéré: {url}", url=url)
                    continue
                if url in visited:
                    continue
                visited.add(url)

                logger.info(f"Crawl Phase1 {url} (depth={depth})")
                _push_log(project_id, "info", f"[depth={depth}] Fetch {url}", url=url)
                try:
                    if isinstance(html, bytes):
                        html = html.decode("utf-8", errors="ignore")
                    text_content = extract(html, include_links=False)
                    if not text_content or len(text_content) < 50:
                        _push_log(project_id, "warn", f"Contenu trop court ou vide: {url}", url=url)
                        continue

                    title = None
                    h1 = None
                    t_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
                    if t_match:
                        title = t_match.group(1).strip()[:512]
                    h1_match = re.search(r"<h1[^>]*>([^<]+)</h1>", html, re.I)
                    if h1_match:
                        h1 = h1_match.group(1).strip()[:512]

                    pid = url_to_page_id(project_id, url)
                    is_excluded = exclude_urls_with_params and url_has_query_params(url)

                    page = session.query(Page).filter(Page.id == pid, Page.project_id == project_id).first()
                    if page:
                        page.title = title
                        page.h1 = h1
                        page.content_text = text_content[:5000]
                        page.depth = depth
                        page.entities = []
                        page.excluded = is_excluded
                    else:
                        page = Page(
                            id=pid,
                            project_id=project_id,
                            url=url,
                            title=title,
                            h1=h1,
                            content_text=text_content[:5000],
                            depth=depth,
                            entities=[],
                            excluded=is_excluded,
                        )
                        session.add(page)
                    existing_page_ids.add(pid)

                    links_data = get_links_with_context(
                        html, url, context_window=LINK_CONTEXT_WINDOW, exclude_urls_with_params=exclude_urls_with_params
                    )
                    new_edges_data = []
                    for target_url, anchor, context_text, position_ratio in links_data:
                        if path_prefix and not url_matches_path_prefix(target_url, path_prefix):
                            continue
                        tid = url_to_page_id(project_id, target_url)
                        if target_url not in discovered:
                            discovered.add(target_url)
                            if depth + 1 <= max_depth:
                                to_visit.append((target_url, depth + 1))
                            if tid not in existing_page_ids:
                                stub = Page(id=tid, project_id=project_id, url=target_url, depth=depth + 1)
                                session.add(stub)
                                existing_page_ids.add(tid)

                        if (pid, tid) not in existing_edges:
                            pos_mult = 1.5 if position_ratio < 0.2 else 1.0
                            base_weight = 0.5 * pos_mult
                            new_edges_data.append({
                                "project_id": project_id,
                                "source_id": pid,
                                "target_id": tid,
                                "weight": base_weight,
                                "anchor": anchor[:512] if anchor else None,
                            })
                            existing_edges.add((pid, tid))
                    if new_edges_data:
                        session.flush()  # Flush pages/stubs avant bulk_insert (FK edges_source_id)
                        session.bulk_insert_mappings(Edge, new_edges_data)

                    pages_since_commit += 1
                    if pages_since_commit >= PHASE1_BATCH_COMMIT:
                        _flush_batch(commit_project=True)

                    _push_log(
                        project_id, "info",
                        f"✓ Phase 1: {len(visited)} pages · {len(links_data)} liens",
                        url=url,
                        extra={"links_count": len(links_data), "visited": len(visited)},
                    )

                except Exception as e:
                    logger.warning(f"Erreur {url}: {e}")
                    _push_log(project_id, "error", str(e), url=url)
                    session.rollback()
                    pages_since_commit = 0
                    continue

            # Rotation pipeline: prochain batch
            if next_urls_batch:
                if fetch_future is not None:
                    try:
                        fetched = fetch_future.result(timeout=120)
                    except Exception as e:
                        logger.warning(f"Fetch pipeline: {e}")
                        fetched = {}
                    fetch_future = None
                else:
                    urls_only = [u for u, _ in next_urls_batch]
                    fetched = fetch_urls_parallel(urls_only) if batch_size > 1 else (
                        {urls_only[0]: fetch_html(urls_only[0])} if urls_only else {}
                    )
                urls_batch = next_urls_batch
            elif to_visit and len(visited) < max_pages:
                # next_urls_batch était vide car _pop_batch() en début d’itération précède
                # l’enrichissement de to_visit depuis les liens de la page courante.
                urls_batch = _pop_batch()
                if not urls_batch:
                    break
                urls_only = [u for u, _ in urls_batch]
                fetched = fetch_urls_parallel(urls_only) if batch_size > 1 else (
                    {urls_only[0]: fetch_html(urls_only[0])} if urls_only else {}
                )
                fetch_future = None
                continue
            else:
                break

        if _fetch_executor:
            _fetch_executor.shutdown(wait=True)
        _flush_batch(commit_project=False)

        # Louvain pour silos (Phase 1) — différé si SILO_LOUVAIN_DEFERRED=true
        if not LOUVAIN_DEFERRED:
            _push_log(project_id, "info", "Détection des silos (Louvain)...")
            for e in session.query(Edge).filter(Edge.project_id == project_id).all():
                G.add_edge(e.source_id, e.target_id, weight=e.weight or 0.5)
            if G.number_of_nodes() > 0:
                try:
                    import cdlib
                    from cdlib import algorithms
                    G_undir = G.to_undirected()
                    coms = algorithms.louvain(G_undir)
                    updates = [
                        {"id": nid, "silo_id": str(i)}
                        for i, community in enumerate(coms.communities)
                        for nid in community
                    ]
                    if updates:
                        session.bulk_update_mappings(Page, updates)
                    session.commit()
                except Exception as e:
                    logger.warning(f"Louvain non disponible: {e}")
        else:
            _push_log(project_id, "info", "Louvain différé (Recalcul silos à la demande)")

        project.status = "phase1_done"
        project.urls_count = len(visited)
        session.commit()
        _push_log(project_id, "info", f"Phase 1 terminée: {len(visited)} pages, silos identifiés")

    except Exception as e:
        logger.exception(e)
        session.rollback()
        project.status = "error"
        session.commit()
        _push_log(project_id, "error", f"Erreur fatale: {e}")
    finally:
        session.close()


def run_crawl_phase2(project_id: str):
    """
    Phase 2: NER + embeddings + Reasonable Surfer.
    S'exécute après Phase 1. Peut être lancée manuellement ou automatiquement.
    """
    from sqlalchemy.orm import sessionmaker
    from database.models import Project, Page, Edge, Base
    from database.db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    session = Session()
    try:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            logger.error(f"Projet {project_id} non trouvé")
            return
    finally:
        session.close()

    if _check_stop(project_id):
        _push_log(project_id, "info", "Phase 2 annulée (stop demandé)")
        return

    session = Session()
    try:
        project.status = "crawling"
        session.commit()
    finally:
        session.close()
    _push_log(project_id, "info", "[Phase 2] NER + embeddings" + (" + Reasonable Surfer" if RUN_REASONABLE_SURFER else "") + "...")

    model = _get_embedding_model()
    G = nx.DiGraph()
    chunk_num = 0

    # 1. NER + embeddings par lots (évite "named cursor isn't valid anymore")
    # Ne traiter que les pages avec contenu suffisant (exclut les stubs sans content_text)
    from sqlalchemy import func
    while True:
        if _check_stop(project_id):
            break
        while _check_pause(project_id):
            import time
            time.sleep(2)
        session = Session()
        try:
            pages = session.query(Page).filter(
                Page.project_id == project_id,
                Page.embedding.is_(None),
                Page.content_text.isnot(None),
                func.length(Page.content_text) >= 20,
            ).limit(DB_CHUNK_SIZE).all()
            if not pages:
                break
            chunk_num += 1
            logger.info(f"[Embeddings] Lot {chunk_num}: {len(pages)} pages sans embedding")
            pages_to_embed = pages
            for i in range(0, len(pages_to_embed), EMBEDDING_BATCH_SIZE):
                if _check_stop(project_id):
                    break
                batch = pages_to_embed[i : i + EMBEDDING_BATCH_SIZE]
                texts = [(p.content_text or "")[:EMBEDDING_TEXT_MAX_CHARS] for p in batch]
                _refresh_worker_hb()
                embs = _embed_texts(model, texts)
                entities_list = extract_entities_batch(texts)
                for p, emb, entities in zip(batch, embs, entities_list):
                    emb_list = emb.tolist() if hasattr(emb, "tolist") else list(emb)
                    p.entities = entities
                    p.embedding = emb_list
                    _push_log(project_id, "info", f"NER: {p.url[:60]}... → {len(entities)} entités", url=p.url)
                _refresh_worker_hb()
            session.commit()
            _refresh_worker_hb()
        except Exception as e:
            logger.exception(e)
            session.rollback()
            session2 = Session()
            try:
                pj = session2.query(Project).filter(Project.id == project_id).first()
                if pj:
                    pj.status = "error"
                    session2.commit()
            finally:
                session2.close()
            _push_log(project_id, "error", f"Erreur Phase 2: {e}")
            return
        finally:
            session.close()

    # 2. Reasonable Surfer + PageRank (session fraîche)
    session = Session()
    try:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            return
        for e in session.query(Edge).filter(Edge.project_id == project_id).all():
            G.add_edge(e.source_id, e.target_id, weight=e.weight or 0.5)

        if RUN_REASONABLE_SURFER:
            pages_by_id = {p.id: p for p in session.query(Page).filter(
                Page.project_id == project_id,
                Page.embedding.isnot(None),
            ).all()}
            edges_list = session.query(Edge).filter(Edge.project_id == project_id).all()
            edge_batches = []
            for edge in edges_list:
                target = pages_by_id.get(edge.target_id)
                if not target or target.embedding is None:
                    continue
                context_text = edge.anchor or ""
                if not context_text and target.content_text:
                    context_text = (target.content_text or "")[:200]
                edge_batches.append((edge, target, context_text or " "))

            _ctx_emb_cache = {}

            for j in range(0, len(edge_batches), EMBEDDING_BATCH_SIZE):
                if _check_stop(project_id):
                    break
                batch = edge_batches[j : j + EMBEDDING_BATCH_SIZE]
                context_texts = [ctx for _, _, ctx in batch]
                h1_texts = [(target.h1 or "") for _, target, _ in batch]
                h1_entities_list = extract_entities_batch(h1_texts, max_entities=10)

                to_embed = list(dict.fromkeys(ctx for ctx in context_texts if ctx not in _ctx_emb_cache))
                if to_embed:
                    new_embs = _embed_texts(model, to_embed)
                    for ctx, emb in zip(to_embed, new_embs):
                        el = emb.tolist() if hasattr(emb, "tolist") else list(emb)
                        _ctx_emb_cache[ctx] = el
                    if len(_ctx_emb_cache) > 2000:
                        _ctx_emb_cache = dict(list(_ctx_emb_cache.items())[-1000:])

                for i, ((edge, target, _), h1_entities) in enumerate(zip(batch, h1_entities_list)):
                    ctx = context_texts[i]
                    context_emb_list = _ctx_emb_cache[ctx]
                    edge.context_embedding = context_emb_list
                    target_emb = target.embedding
                    target_emb_list = target_emb.tolist() if hasattr(target_emb, "tolist") else (list(target_emb) if target_emb is not None else [])
                    sim = _cosine_sim(context_emb_list, target_emb_list)
                    sim_factor = max(0, min(1, 0.5 + 0.5 * sim))
                    anchor_bonus = 1.2 if anchor_contains_entity(edge.anchor or "", h1_entities or target.entities or []) else 1.0
                    edge.weight = (edge.weight or 0.5) * sim_factor * anchor_bonus

            session.commit()

        if G.number_of_nodes() > 0:
            pr = nx.pagerank(G)
            # Bulk update page_rank (une opération au lieu de N requêtes)
            updates = [{"id": pid, "page_rank": score} for pid, score in pr.items()]
            if updates:
                session.bulk_update_mappings(Page, updates)
            session.commit()

        project.status = "done"
        session.commit()
        _push_log(project_id, "info", "Phase 2 terminée: NER" + (" + Reasonable Surfer" if RUN_REASONABLE_SURFER else "") + " appliqués")
    except Exception as e:
        logger.exception(e)
        session.rollback()
        try:
            pj = session.query(Project).filter(Project.id == project_id).first()
            if pj:
                pj.status = "error"
                session.commit()
        except Exception:
            pass
        _push_log(project_id, "error", f"Erreur Phase 2: {e}")
    finally:
        session.close()


def run_compute_embeddings(project_id: str, page_id: str = None):
    """
    Calcule les embeddings pour les pages (Phase 2 partielle).
    Pagination par curseur (Page.id) + micro-lots : au plus EMBEDDING_BATCH_SIZE lignes
    chargées en même temps, session fermée entre séries → évite accumuler 12+ pages en RAM
    (cause typique de SIGKILL après le 1er batch avec e5-large + spaCy).
    """
    from sqlalchemy.orm import sessionmaker
    from database.models import Project, Page, Base
    from database.db import get_engine

    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    spacy_pipe_cap = int(os.environ.get("SILO_SPACY_PIPE_BATCH_SIZE", "8"))

    def _process_batch(session, batch_pages, model):
        """Traite un batch de pages. Retourne nb traité. Lève en cas d'erreur."""
        if not batch_pages:
            return 0
        texts = [(x.content_text or "")[:EMBEDDING_TEXT_MAX_CHARS] for x in batch_pages]
        _refresh_worker_hb()
        embs = _embed_texts(model, texts)
        _refresh_worker_hb()
        pipe_bs = max(1, min(spacy_pipe_cap, len(texts)))
        entities_list = extract_entities_batch(texts, pipe_batch_size=pipe_bs)
        _refresh_worker_hb()
        for pg, emb, entities in zip(batch_pages, embs, entities_list):
            pg.entities = entities
            pg.embedding = emb.tolist() if hasattr(emb, "tolist") else list(emb)
            _push_log(project_id, "info", f"Embed: {pg.url[:50]}...", url=pg.url)
        session.commit()
        for pg in batch_pages:
            try:
                session.expunge(pg)
            except Exception:
                pass
        _refresh_worker_hb()
        gc.collect()
        return len(batch_pages)

    session = Session()
    try:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            logger.error(f"Projet {project_id} non trouvé")
            return
    finally:
        session.close()

    model = _get_embedding_model()
    _refresh_worker_hb()
    logger.info(f"[Embeddings] Démarrage pour projet {project_id}" + (f" page={page_id}" if page_id else ""))
    if page_id:
        _push_log(project_id, "info", f"[Embeddings] Calcul embedding pour la page {page_id[:20]}...")
    else:
        _push_log(project_id, "info", "[Embeddings] Calcul des embeddings pour les opportunités...")

    total_processed = 0
    batch_idx = 0
    last_id = None
    from sqlalchemy import func

    try:
        _s0 = Session()
        try:
            total0 = _s0.query(Page).filter(Page.project_id == project_id).count()
            with0 = _s0.query(Page).filter(
                Page.project_id == project_id,
                Page.embedding.isnot(None),
            ).count()
            pend0 = _s0.query(Page).filter(
                Page.project_id == project_id,
                Page.embedding.is_(None),
                Page.content_text.isnot(None),
                func.length(Page.content_text) >= 20,
            ).count()
            redis.from_url(REDIS_URL).set(
                f"silo:embedding_progress:{project_id}",
                json.dumps({
                    "pages_with_embedding": with0,
                    "total_pages": total0,
                    "pages_pending_embedding": pend0,
                    "phase": "started",
                }),
                ex=7200,
            )
        finally:
            _s0.close()
    except Exception:
        pass

    fetch_limit = max(1, EMBEDDING_BATCH_SIZE)

    while True:
        if _check_stop(project_id):
            _push_log(project_id, "info", "Calcul embeddings stoppé par l'utilisateur")
            break
        session = Session()
        try:
            q = session.query(Page).filter(
                Page.project_id == project_id,
                Page.embedding.is_(None),
                Page.content_text.isnot(None),
                func.length(Page.content_text) >= 20,
            )
            if page_id:
                q = q.filter(Page.id == page_id)
                pages = q.order_by(Page.id).limit(1).all()
            else:
                if last_id is not None:
                    q = q.filter(Page.id > last_id)
                pages = q.order_by(Page.id).limit(fetch_limit).all()
            if not pages:
                break
            if not page_id:
                last_id = pages[-1].id
            batch_idx += 1
            logger.info("[Embeddings] Série %s: %s page(s) chargée(s)", batch_idx, len(pages))
            try:
                total_pages_pre = session.query(Page).filter(Page.project_id == project_id).count()
                with_pre = session.query(Page).filter(
                    Page.project_id == project_id,
                    Page.embedding.isnot(None),
                ).count()
                pend_pre = session.query(Page).filter(
                    Page.project_id == project_id,
                    Page.embedding.is_(None),
                    Page.content_text.isnot(None),
                    func.length(Page.content_text) >= 20,
                ).count()
                redis.from_url(REDIS_URL).set(
                    f"silo:embedding_progress:{project_id}",
                    json.dumps({
                        "pages_with_embedding": with_pre,
                        "total_pages": total_pages_pre,
                        "pages_pending_embedding": pend_pre,
                        "phase": "batch_processing",
                        "batch": batch_idx,
                        "batch_size": len(pages),
                    }),
                    ex=7200,
                )
            except Exception:
                pass
            _refresh_worker_hb()
            logger.info(
                "[Embeddings] ONNX / NER (fetch=%s, spaCy pipe≤%s)…",
                fetch_limit,
                spacy_pipe_cap,
            )
            gc.collect()
            try:
                total_processed += _process_batch(session, pages, model)
                logger.info("[Embeddings] Série %s OK → total %s page(s)", batch_idx, total_processed)
            except Exception as e:
                logger.warning("[Embeddings] Série %s échouée: %s — fallback page par page", batch_idx, e)
                try:
                    session.rollback()
                except Exception:
                    pass
                for pg in pages:
                    s2 = Session()
                    try:
                        p2 = s2.query(Page).filter(
                            Page.id == pg.id,
                            Page.project_id == project_id,
                        ).first()
                        if p2 and p2.embedding is None:
                            total_processed += _process_batch(s2, [p2], model)
                            logger.info("[Embeddings] Rattrapage 1 page: %s…", (p2.url or "")[:48])
                    except Exception as e2:
                        logger.warning("[Embeddings] Page %s ignorée: %s", pg.id, e2)
                    finally:
                        s2.close()
                        gc.collect()
            try:
                s3 = Session()
                try:
                    total_pages = s3.query(Page).filter(Page.project_id == project_id).count()
                    with_emb = s3.query(Page).filter(
                        Page.project_id == project_id,
                        Page.embedding.isnot(None),
                    ).count()
                    redis.from_url(REDIS_URL).set(
                        f"silo:embedding_progress:{project_id}",
                        json.dumps({"pages_with_embedding": with_emb, "total_pages": total_pages}),
                        ex=7200,
                    )
                finally:
                    s3.close()
            except Exception:
                pass
            if page_id:
                break
        except Exception as e:
            logger.exception(e)
            try:
                session.rollback()
            except Exception:
                pass
            _push_log(project_id, "error", f"Erreur embeddings: {e}")
            break
        finally:
            session.close()
            gc.collect()

    _push_log(project_id, "info", f"Embeddings terminés: {total_processed} page(s)")


def run_compute_opportunities(project_id: str):
    """
    Calcule et stocke les opportunités en BDD (vue Toutes global).
    Exécuté en arrière-plan par le worker.
    """
    from database.db import get_session
    from database.service import run_compute_and_store_opportunities

    logger.info(f"[Opportunités] Démarrage pour projet {project_id}")
    _push_log(project_id, "info", "[Opportunités] Calcul des opportunités en cours...")
    session = get_session()
    try:
        result = run_compute_and_store_opportunities(session, project_id)
        if result.get("ok"):
            n = result.get("pairs_stored", 0)
            _push_log(project_id, "info", f"[Opportunités] {n} opportunité(s) stockée(s) en base")
        else:
            _push_log(project_id, "warning", f"[Opportunités] {result.get('error', 'Erreur')}")
    except Exception as e:
        logger.exception(f"Erreur compute opportunités {project_id}: {e}")
        _push_log(project_id, "error", f"Erreur opportunités: {e}")
    finally:
        session.close()


def run_crawl(
    project_id: str,
    seed_url: str,
    max_depth: int = 3,
    max_pages: int = 50,
    run_ner: bool = True,
    phase1_only: bool = False,
    path_prefix: str | None = None,
    exclude_urls_with_params: bool = True,
    requested_fetch_mode: bool | None = None,
):
    """
    Exécute le crawl: Phase 1 (rapide) puis Phase 2 (NER) si run_ner=True.
    phase1_only=True: arrête après Phase 1 (pour workers séparés, le worker-nlp fera Phase 2).
    requested_fetch_mode: valeur demandée par l'API (logs audit EF4) ; le mode effectif vient du worker.
    """
    from worker.fetcher import USE_STEALTHY

    mode_label = "navigateur (stealthy)" if USE_STEALTHY else "HTTP simple"
    _push_log(
        project_id,
        "info",
        f"Démarrage crawl — mode fetch effectif: {mode_label}",
        extra={
            "use_stealthy_fetcher": USE_STEALTHY,
            **({"requested_fetch_mode": requested_fetch_mode} if requested_fetch_mode is not None else {}),
        },
    )
    run_crawl_phase1(
        project_id,
        seed_url,
        max_depth=max_depth,
        max_pages=max_pages,
        path_prefix=path_prefix,
        exclude_urls_with_params=exclude_urls_with_params,
    )
    if _check_stop(project_id):
        return
    if phase1_only:
        return
    if run_ner:
        run_crawl_phase2(project_id)
    else:
        from sqlalchemy.orm import sessionmaker
        from database.models import Project, Base
        from database.db import get_engine
        engine = get_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            project = session.query(Project).filter(Project.id == project_id).first()
            if project:
                project.status = "done"
                session.commit()
                _push_log(project_id, "info", "Phase 1 terminée (NER désactivé)")
        finally:
            session.close()


def recompute_silos(project_id: str):
    """
    Recalcule les silos (Louvain) sur le graphe existant.
    Utile quand des pages ont été ajoutées sans crawl complet.
    """
    from sqlalchemy.orm import sessionmaker
    from database.models import Page, Edge
    from database.db import get_engine

    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        if _check_stop(project_id):
            _push_log(project_id, "info", "Recalcul silos stoppé par l'utilisateur")
            return
        edges = session.query(Edge).filter(Edge.project_id == project_id).all()
        if not edges:
            _push_log(project_id, "info", "Recalcul silos: aucun lien, rien à faire")
            return

        G = nx.DiGraph()
        for e in edges:
            G.add_edge(e.source_id, e.target_id, weight=e.weight or 0.5)

        if G.number_of_nodes() == 0:
            return

        if _check_stop(project_id):
            _push_log(project_id, "info", "Recalcul silos stoppé par l'utilisateur")
            return
        import cdlib
        from cdlib import algorithms
        total_edges = len(edges)
        total_nodes = G.number_of_nodes()

        def _update_progress(nodes_processed: int):
            try:
                r = redis.from_url(REDIS_URL)
                r.set(
                    f"silo:recompute_progress:{project_id}",
                    json.dumps({
                        "total_edges": total_edges,
                        "edges_processed": nodes_processed,
                        "total_nodes": total_nodes,
                    }),
                    ex=3600,
                )
            except Exception:
                pass

        _update_progress(0)
        G_undir = G.to_undirected()
        coms = algorithms.louvain(G_undir)
        nodes_processed = 0
        for i, community in enumerate(coms.communities):
            if _check_stop(project_id):
                _push_log(project_id, "info", "Recalcul silos stoppé par l'utilisateur")
                session.commit()
                return
            for nid in community:
                p = session.query(Page).filter(Page.id == nid, Page.project_id == project_id).first()
                if p:
                    p.silo_id = str(i)
                nodes_processed += 1
            if nodes_processed % 500 == 0 or nodes_processed == total_nodes:
                _update_progress(nodes_processed)
        session.commit()
        _push_log(project_id, "info", f"Recalcul silos: {len(coms.communities)} communauté(s) identifiée(s)")
    except Exception as e:
        logger.exception(e)
        session.rollback()
        _push_log(project_id, "error", f"Erreur recalcul silos: {e}")
    finally:
        session.close()


def run_ner_on_demand(project_id: str, node_id: str = None, silo_id: str = None):
    """
    Lance la détection NER sur un nœud ou un silo entier.
    À utiliser quand le crawl est arrêté pour optimiser les ressources.
    Si content_text est vide, récupère le contenu via fetch_html + trafilatura.
    Utilise des lots par session pour éviter "named cursor isn't valid anymore".
    """
    from sqlalchemy.orm import sessionmaker
    from database.models import Page, Base
    from database.db import get_engine
    from sqlalchemy import or_, func

    engine = get_engine()
    Session = sessionmaker(bind=engine)

    def _process_page(session, p):
        text_content = (p.content_text or "")[:5000]
        need_fetch = not text_content or len(text_content) < 20
        need_title_h1 = (not p.title or not p.h1)

        if need_fetch or need_title_h1:
            try:
                html = fetch_html(p.url)
                if html:
                    if isinstance(html, bytes):
                        html = html.decode("utf-8", errors="ignore")
                    if need_fetch:
                        text_content = extract(html, include_links=False)
                        if text_content and len(text_content) >= 20:
                            p.content_text = text_content[:5000]
                        else:
                            _push_log(project_id, "warn", f"NER: contenu trop court pour {p.url[:50]}...", url=p.url)
                            p.entities = ["__FETCH_FAILED__"]  # Marquer pour éviter boucle infinie
                            session.commit()
                            return False
                    if need_title_h1:
                        t_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
                        if t_match:
                            p.title = t_match.group(1).strip()[:512]
                        h1_match = re.search(r"<h1[^>]*>([^<]+)</h1>", html, re.I)
                        if h1_match:
                            p.h1 = h1_match.group(1).strip()[:512]
                    session.commit()
                elif need_fetch:
                    _push_log(project_id, "warn", f"NER: impossible de récupérer {p.url[:50]}...", url=p.url)
                    p.entities = ["__FETCH_FAILED__"]  # Marquer pour éviter boucle infinie
                    session.commit()
                    return False
            except Exception as fe:
                _push_log(project_id, "warn", f"NER fetch {p.url[:40]}...: {fe}", url=p.url)
                if need_fetch:
                    p.entities = ["__FETCH_FAILED__"]  # Marquer pour éviter boucle infinie
                    session.commit()
                    return False

        if not text_content or len(text_content) < 20:
            p.entities = ["__FETCH_FAILED__"]  # Pas de contenu exploitable
            session.commit()
            return False

        entities = extract_entities(text_content)
        # Si aucune entité trouvée, marquer pour éviter boucle infinie (needs_ner exclut length=0)
        p.entities = entities if entities else ["__NO_ENTITIES__"]
        _push_log(project_id, "info", f"NER on-demand: {p.url[:50]}... → {len(entities)} entités", url=p.url)
        return True

    total_processed = 0
    while True:
        if _check_stop(project_id):
            _push_log(project_id, "info", "NER on-demand stoppé par l'utilisateur")
            break
        session = Session()
        try:
            needs_ner = or_(Page.entities.is_(None), func.coalesce(func.jsonb_array_length(Page.entities), 0) == 0)
            if node_id:
                pages = session.query(Page).filter(Page.project_id == project_id, Page.id == node_id).limit(1).all()
            elif silo_id is not None:
                pages = session.query(Page).filter(Page.project_id == project_id, Page.silo_id == str(silo_id), needs_ner).limit(DB_CHUNK_SIZE).all()
            else:
                pages = session.query(Page).filter(Page.project_id == project_id, needs_ner).limit(DB_CHUNK_SIZE).all()

            if not pages:
                break

            try:
                needs_ner_progress = or_(
                    Page.entities.is_(None),
                    func.coalesce(func.jsonb_array_length(Page.entities), 0) == 0,
                )
                eligible_n = (
                    session.query(Page)
                    .filter(
                        Page.project_id == project_id,
                        Page.content_text.isnot(None),
                        func.length(Page.content_text) >= 20,
                    )
                    .count()
                )
                pending_n = (
                    session.query(Page)
                    .filter(
                        Page.project_id == project_id,
                        Page.content_text.isnot(None),
                        func.length(Page.content_text) >= 20,
                        needs_ner_progress,
                    )
                    .count()
                )
                redis.from_url(REDIS_URL).set(
                    f"silo:ner_progress:{project_id}",
                    json.dumps({
                        "pages_pending_ner": pending_n,
                        "pages_ner_eligible": eligible_n,
                        "batch_size": len(pages),
                    }),
                    ex=7200,
                )
            except Exception:
                pass

            for p in pages:
                if _check_stop(project_id):
                    _push_log(project_id, "info", "NER on-demand stoppé par l'utilisateur")
                    session.commit()
                    session.close()
                    _push_log(project_id, "info", f"NER on-demand terminé: {total_processed} page(s) traitée(s)")
                    return
                if _process_page(session, p):
                    total_processed += 1
            session.commit()

            if node_id:
                break
        except Exception as e:
            logger.exception(e)
            session.rollback()
            _push_log(project_id, "error", f"Erreur NER on-demand: {e}")
            break
        finally:
            session.close()

    _push_log(project_id, "info", f"NER on-demand terminé: {total_processed} page(s) traitée(s)")
