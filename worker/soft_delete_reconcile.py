"""
Ré-enfile les projets encore en base avec deleted_at (soft-delete) pour purge worker.

Évite les lignes orphelines si le job silo:delete_project_queue était perdu ou a échoué.
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger("silo-worker")

DELETE_PROJECT_QUEUE_KEY = "silo:delete_project_queue"
SWEEP_LOCK_KEY = "silo:soft_delete_sweep_lock"
SWEEP_LOCK_TTL_SEC = int(os.environ.get("SILO_SOFT_DELETE_SWEEP_LOCK_SEC", "55"))
PER_PROJECT_ENQUEUE_COOLDOWN_SEC = int(os.environ.get("SILO_SOFT_DELETE_ENQUEUE_COOLDOWN_SEC", "300"))


def sweep_soft_deleted_projects(r) -> int:
    """
    Un seul worker à la fois (Redis NX). Pour chaque projet soft-deleted, RPUSH si pas ré-enfilé récemment.
    Retourne le nombre de messages enfilés.
    """
    if not os.environ.get("DATABASE_URL"):
        return 0
    try:
        got = r.set(SWEEP_LOCK_KEY, "1", nx=True, ex=SWEEP_LOCK_TTL_SEC)
    except Exception as e:
        logger.warning("soft_delete_sweep lock: %s", e)
        return 0
    if not got:
        return 0

    try:
        from database.db import get_session
        from database.service import list_soft_deleted_project_ids

        session = get_session()
        try:
            ids = list_soft_deleted_project_ids(session)
        finally:
            session.close()
    except Exception as e:
        logger.warning("soft_delete_sweep lecture DB: %s", e)
        return 0

    n = 0
    for pid in ids:
        cool_key = f"silo:soft_delete_sweep_enq:{pid}"
        try:
            if r.set(cool_key, "1", nx=True, ex=PER_PROJECT_ENQUEUE_COOLDOWN_SEC):
                r.rpush(
                    DELETE_PROJECT_QUEUE_KEY,
                    json.dumps({"project_id": pid, "attempt": 0, "source": "sweep"}),
                )
                n += 1
        except Exception as e:
            logger.warning("soft_delete_sweep enqueue %s: %s", pid, e)
    if n:
        logger.info("Soft-delete sweep: %s projet(s) renvoyé(s) vers la file de suppression", n)
    return n
