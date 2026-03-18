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

# Ajouter le parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis
from worker.crawler import run_crawl, run_crawl_phase2, run_ner_on_demand, recompute_silos, run_compute_embeddings, run_compute_opportunities, _check_stop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silo-worker")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")
QUEUE_KEY = "silo:crawl_queue"
PHASE2_QUEUE_KEY = "silo:phase2_queue"
NER_QUEUE_KEY = "silo:ner_queue"
RECOMPUTE_SILOS_QUEUE_KEY = "silo:recompute_silos_queue"
COMPUTE_EMBEDDINGS_QUEUE_KEY = "silo:compute_embeddings_queue"
COMPUTE_OPPORTUNITIES_QUEUE_KEY = "silo:compute_opportunities_queue"
WORKER_MODE = os.environ.get("SILO_WORKER_MODE", "full").lower()
# Délai (s) entre jobs crawl pour éviter surcharge CPU (mode url_list = 1 job/URL)
JOB_DELAY_SECONDS = float(os.environ.get("SILO_JOB_DELAY_SECONDS", "0.5"))


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

    if WORKER_MODE == "crawl":
        queues = [QUEUE_KEY]
        logger.info("Worker Silo (mode crawl) démarré — Phase 1 uniquement, push vers phase2_queue")
    elif WORKER_MODE == "nlp":
        queues = [PHASE2_QUEUE_KEY, NER_QUEUE_KEY, RECOMPUTE_SILOS_QUEUE_KEY, COMPUTE_EMBEDDINGS_QUEUE_KEY, COMPUTE_OPPORTUNITIES_QUEUE_KEY]
        logger.info("Worker Silo (mode nlp) démarré — Phase 2, NER, silos, embeddings, opportunités")
        _preload_nlp_models()
    else:
        queues = [QUEUE_KEY, NER_QUEUE_KEY, RECOMPUTE_SILOS_QUEUE_KEY, COMPUTE_EMBEDDINGS_QUEUE_KEY, COMPUTE_OPPORTUNITIES_QUEUE_KEY]
        logger.info("Worker Silo (mode full) démarré, écoute crawl + NER + silos + embeddings...")
        _preload_nlp_models()

    while True:
        try:
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
                            run_ner_on_demand(project_id, node_id=node_id, silo_id=silo_id)
                    continue

                if queue_name == RECOMPUTE_SILOS_QUEUE_KEY:
                    project_id = data.get("project_id")
                    if project_id:
                        if _check_stop(project_id):
                            logger.info(f"Job recalcul silos ignoré (stop demandé): {project_id}")
                        else:
                            logger.info(f"Job recalcul silos reçu: {project_id}")
                            recompute_silos(project_id)
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
                                run_compute_embeddings(project_id, page_id=page_id)
                            except Exception as e:
                                logger.exception(f"Erreur compute embeddings {project_id}: {e}")
                            finally:
                                try:
                                    r.delete(f"silo:embedding_in_progress:{project_id}")
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
                                run_compute_opportunities(project_id)
                            except Exception as e:
                                logger.exception(f"Erreur compute opportunités {project_id}: {e}")
                            finally:
                                try:
                                    r.delete(f"silo:opportunities_in_progress:{project_id}")
                                except Exception:
                                    pass
                    continue

                project_id = data.get("project_id")
                seed_url = data.get("seed_url")
                if project_id and seed_url:
                    if _check_stop(project_id):
                        logger.info(f"Job crawl ignoré (stop demandé): {project_id}")
                        try:
                            r.rpush(QUEUE_KEY, payload)
                            time.sleep(1)
                        except Exception:
                            pass
                    else:
                        max_depth = data.get("max_depth", 3)
                        max_pages = data.get("max_pages", 50)
                        run_ner = data.get("run_ner", True)
                        phase1_only = WORKER_MODE == "crawl" and run_ner
                        logger.info(f"Job crawl reçu: {project_id} -> {seed_url} (depth={max_depth}, max={max_pages}, ner={run_ner}, phase1_only={phase1_only})")
                        try:
                            run_crawl(project_id, seed_url, max_depth=max_depth, max_pages=max_pages, run_ner=run_ner, phase1_only=phase1_only)
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
