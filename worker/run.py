#!/usr/bin/env python3
"""
Worker Silo - écoute Redis et exécute crawls + NER on-demand.
Usage: python -m worker.run
SILO_WORKER_MODE: full (défaut) | crawl | nlp
  - full: un worker fait tout (Phase 1 + Phase 2)
  - crawl: Phase 1 uniquement, push vers phase2_queue (léger, pas de spaCy/FastEmbed)
  - nlp: Phase 2, NER, silos, embeddings (charge spaCy + FastEmbed)
"""
import os
import json
import logging
import sys
import time
import faulthandler

# Ajouter le parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis
from worker.crawler import run_crawl, run_crawl_phase2, run_ner_on_demand, recompute_silos, run_compute_embeddings, run_compute_opportunities, run_delete_project, _check_stop
from worker.redis_runtime import (
    write_worker_runtime,
    refresh_worker_heartbeat,
    worker_heartbeat_keepalive,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silo-worker")

faulthandler.enable(all_threads=True)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
DEFAULT_CRAWL_QUEUE = "silo:crawl_queue"
CRAWL_QUEUE_KEY = os.environ.get("SILO_CRAWL_QUEUE_KEY", DEFAULT_CRAWL_QUEUE).strip() or DEFAULT_CRAWL_QUEUE
WORKER_CRAWL_ROLE = os.environ.get("SILO_WORKER_CRAWL_ROLE", "").strip() or None
PHASE2_QUEUE_KEY = "silo:phase2_queue"
NER_QUEUE_KEY = "silo:ner_queue"
RECOMPUTE_SILOS_QUEUE_KEY = "silo:recompute_silos_queue"
COMPUTE_EMBEDDINGS_QUEUE_KEY = "silo:compute_embeddings_queue"
COMPUTE_OPPORTUNITIES_QUEUE_KEY = "silo:compute_opportunities_queue"
DELETE_PROJECT_QUEUE_KEY = "silo:delete_project_queue"
WORKER_MODE = os.environ.get("SILO_WORKER_MODE", "full").lower()
# Délai (s) entre jobs crawl pour éviter surcharge CPU (mode url_list = 1 job/URL)
JOB_DELAY_SECONDS = float(os.environ.get("SILO_JOB_DELAY_SECONDS", "0.5"))
# Ré-enfile les projets soft-deleted orphelins + réessais après échec purge
SOFT_DELETE_SWEEP_SEC = float(os.environ.get("SILO_SOFT_DELETE_SWEEP_SEC", "120"))
DELETE_QUEUE_MAX_ATTEMPTS = int(os.environ.get("SILO_DELETE_QUEUE_MAX_ATTEMPTS", "12"))


def _preload_nlp_models():
    """Preload spaCy et FastEmbed au démarrage (mode nlp/full) pour éviter latence au 1er job."""
    try:
        from worker.ner import get_nlp
        get_nlp()
        from worker.crawler import _get_embedding_model
        _get_embedding_model()
        logger.info("Modèles NER + embeddings préchargés")
    except Exception as e:
        logger.warning(f"Preload modèles: {e}")


def main():
    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
    except redis.ConnectionError as e:
        logger.error(f"Impossible de se connecter à Redis ({REDIS_URL}): {e}")
        raise

    use_stealthy = os.environ.get("SILO_USE_STEALTHY_FETCHER", "true").lower() == "true"
    try:
        write_worker_runtime(r, use_stealthy, WORKER_CRAWL_ROLE)
        refresh_worker_heartbeat(r, WORKER_CRAWL_ROLE)
        logger.info(
            "Runtime worker Redis: use_stealthy_fetcher=%s role=%s queue=%s",
            use_stealthy,
            WORKER_CRAWL_ROLE or "legacy",
            CRAWL_QUEUE_KEY,
        )
    except Exception as e:
        logger.warning("Écriture état runtime Redis: %s", e)

    if WORKER_MODE == "crawl":
        queues = [CRAWL_QUEUE_KEY, DELETE_PROJECT_QUEUE_KEY]
        logger.info(
            "Worker Silo (mode crawl) démarré — queue=%s, Phase 1 + delete, push vers phase2_queue",
            CRAWL_QUEUE_KEY,
        )
    elif WORKER_MODE == "nlp":
        queues = [PHASE2_QUEUE_KEY, NER_QUEUE_KEY, RECOMPUTE_SILOS_QUEUE_KEY, COMPUTE_EMBEDDINGS_QUEUE_KEY, COMPUTE_OPPORTUNITIES_QUEUE_KEY, DELETE_PROJECT_QUEUE_KEY]
        logger.info("Worker Silo (mode nlp) démarré — Phase 2, NER, silos, embeddings, opportunités, delete")
        _preload_nlp_models()
    else:
        queues = [CRAWL_QUEUE_KEY, NER_QUEUE_KEY, RECOMPUTE_SILOS_QUEUE_KEY, COMPUTE_EMBEDDINGS_QUEUE_KEY, COMPUTE_OPPORTUNITIES_QUEUE_KEY, DELETE_PROJECT_QUEUE_KEY]
        logger.info("Worker Silo (mode full) démarré, écoute crawl + NER + silos + embeddings + delete...")
        _preload_nlp_models()

    last_soft_sweep = 0.0
    if os.environ.get("DATABASE_URL") and SOFT_DELETE_SWEEP_SEC > 0:
        try:
            from worker.soft_delete_reconcile import sweep_soft_deleted_projects

            sweep_soft_deleted_projects(r)
        except Exception as e:
            logger.warning("soft_delete sweep (démarrage): %s", e)

    while True:
        try:
            try:
                refresh_worker_heartbeat(r, WORKER_CRAWL_ROLE)
            except Exception:
                pass
            if SOFT_DELETE_SWEEP_SEC > 0 and time.monotonic() - last_soft_sweep >= SOFT_DELETE_SWEEP_SEC:
                last_soft_sweep = time.monotonic()
                if os.environ.get("DATABASE_URL"):
                    try:
                        from worker.soft_delete_reconcile import sweep_soft_deleted_projects

                        sweep_soft_deleted_projects(r)
                    except Exception as e:
                        logger.warning("soft_delete sweep: %s", e)
            result = r.blpop(queues, timeout=30)
            if result:
                queue_name, payload = result
                queue_name = queue_name.decode() if isinstance(queue_name, bytes) else queue_name
                try:
                    data = json.loads(payload)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Payload JSON invalide ({queue_name}): {e}")
                    continue

                if queue_name == PHASE2_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id and not _check_stop(project_id):
                        logger.info(f"Job Phase 2 reçu: {project_id}")
                        try:
                            with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                run_crawl_phase2(project_id)
                        except Exception as e:
                            logger.exception(f"Erreur Phase 2 {project_id}: {e}")
                    continue

                if queue_name == NER_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id:
                        if _check_stop(project_id):
                            logger.info(f"Job NER ignoré (stop demandé): {project_id}")
                        else:
                            node_id = data.get("node_id")
                            silo_id = data.get("silo_id")
                            logger.info(f"Job NER reçu: {project_id} node={node_id} silo={silo_id}")
                            try:
                                r.set(f"silo:ner_in_progress:{project_id}", "1", ex=7200)
                                with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                    run_ner_on_demand(project_id, node_id=node_id, silo_id=silo_id)
                            finally:
                                try:
                                    r.delete(f"silo:ner_in_progress:{project_id}")
                                    r.delete(f"silo:ner_progress:{project_id}")
                                except Exception:
                                    pass
                    continue

                if queue_name == RECOMPUTE_SILOS_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id:
                        if _check_stop(project_id):
                            logger.info(f"Job recalcul silos ignoré (stop demandé): {project_id}")
                        else:
                            logger.info(f"Job recalcul silos reçu: {project_id}")
                            try:
                                r.set(f"silo:recompute_in_progress:{project_id}", "1", ex=3600)
                                with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                    recompute_silos(project_id)
                            finally:
                                try:
                                    r.delete(f"silo:recompute_in_progress:{project_id}")
                                    r.delete(f"silo:recompute_progress:{project_id}")
                                except Exception:
                                    pass
                    continue

                if queue_name == COMPUTE_EMBEDDINGS_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id:
                        if _check_stop(project_id):
                            logger.info(f"Job compute embeddings ignoré (stop demandé): {project_id}")
                        else:
                            page_id = data.get("page_id")
                            logger.info(f"Job compute embeddings reçu: {project_id}" + (f" page={page_id}" if page_id else ""))
                            try:
                                r.set(f"silo:embedding_in_progress:{project_id}", "1", ex=7200)
                                with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                    run_compute_embeddings(project_id, page_id=page_id)
                            except Exception as e:
                                logger.exception(f"Erreur compute embeddings {project_id}: {e}")
                            finally:
                                try:
                                    r.delete(f"silo:embedding_in_progress:{project_id}")
                                    r.delete(f"silo:embedding_progress:{project_id}")
                                except Exception:
                                    pass
                    continue

                if queue_name == COMPUTE_OPPORTUNITIES_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id:
                        if _check_stop(project_id):
                            logger.info(f"Job compute opportunités ignoré (stop demandé): {project_id}")
                        else:
                            logger.info(f"Job compute opportunités reçu: {project_id}")
                            try:
                                with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                    run_compute_opportunities(project_id)
                            except Exception as e:
                                logger.exception(f"Erreur compute opportunités {project_id}: {e}")
                            finally:
                                try:
                                    r.delete(f"silo:opportunities_in_progress:{project_id}")
                                except Exception:
                                    pass
                    continue

                if queue_name == DELETE_PROJECT_QUEUE_KEY:
                    project_id = data.get("project_id")
                    attempt = int(data.get("attempt") or 0)
                    if project_id:
                        logger.info(
                            "Job suppression projet reçu: %s (attempt=%s source=%s)",
                            project_id,
                            attempt,
                            data.get("source") or "queue",
                        )
                        ok = False
                        try:
                            ok = run_delete_project(project_id)
                        except Exception as e:
                            logger.exception(f"Erreur suppression projet {project_id}: {e}")
                        if not ok and attempt < DELETE_QUEUE_MAX_ATTEMPTS:
                            r.rpush(
                                DELETE_PROJECT_QUEUE_KEY,
                                json.dumps({"project_id": project_id, "attempt": attempt + 1}),
                            )
                            logger.warning(
                                "Ré-enfilage suppression %s (tentative %s/%s)",
                                project_id,
                                attempt + 1,
                                DELETE_QUEUE_MAX_ATTEMPTS,
                            )
                        elif not ok:
                            logger.error(
                                "Abandon suppression après %s tentatives: %s",
                                DELETE_QUEUE_MAX_ATTEMPTS,
                                project_id,
                            )
                    continue

                project_id = data.get("project_id")
                seed_url = data.get("seed_url")
                if project_id and seed_url:
                    if _check_stop(project_id):
                        logger.info(f"Job crawl ignoré (stop demandé): {project_id}")
                        try:
                            r.rpush(queue_name, payload)
                            time.sleep(1)
                        except Exception:
                            pass
                    else:
                        max_depth = data.get("max_depth", 3)
                        max_pages = data.get("max_pages", 50)
                        run_ner = data.get("run_ner", True)
                        path_prefix = data.get("path_prefix")
                        exclude_urls_with_params = data.get("exclude_urls_with_params", True)
                        phase1_only = WORKER_MODE == "crawl" and run_ner
                        req_mode = data.get("requested_fetch_mode")
                        logger.info(
                            f"Job crawl reçu: {project_id} -> {seed_url} (depth={max_depth}, max={max_pages}, ner={run_ner}, path_prefix={path_prefix}, exclude_params={exclude_urls_with_params}, phase1_only={phase1_only}, use_stealthy_fetcher={use_stealthy}, requested_fetch_mode={req_mode})"
                        )
                        try:
                            with worker_heartbeat_keepalive(r, WORKER_CRAWL_ROLE):
                                run_crawl(
                                    project_id,
                                    seed_url,
                                    max_depth=max_depth,
                                    max_pages=max_pages,
                                    run_ner=run_ner,
                                    phase1_only=phase1_only,
                                    path_prefix=path_prefix,
                                    exclude_urls_with_params=exclude_urls_with_params,
                                    requested_fetch_mode=req_mode,
                                )
                            if phase1_only and run_ner and not _check_stop(project_id):
                                r.rpush(PHASE2_QUEUE_KEY, json.dumps({"project_id": project_id}))
                            # Throttle entre jobs pour éviter surcharge CPU (mode url_list = 1 job/URL)
                            if JOB_DELAY_SECONDS > 0:
                                time.sleep(JOB_DELAY_SECONDS)
                        except Exception as crawl_err:
                            logger.exception(f"Erreur crawl {seed_url[:60]}...: {crawl_err}")
        except redis.ConnectionError:
            logger.warning("Redis déconnecté, reconnexion dans 5s...")
            time.sleep(5)
        except Exception as e:
            logger.exception(e)


if __name__ == "__main__":
    main()
