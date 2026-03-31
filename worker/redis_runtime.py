"""
État runtime du worker Silo dans Redis (BRIEF choix stealthy fetcher 2026-03-30).
Partagé entre l'API et le worker.
"""
import os
from typing import Optional

WORKER_RUNTIME_STEALTHY_KEY = "silo:worker_runtime:use_stealthy_fetcher"
WORKER_RUNTIME_HEARTBEAT_KEY = "silo:worker_runtime:heartbeat"

# Workers crawl dédiés (SILO_DUAL_CRAWL_WORKERS) — un heartbeat / mode par rôle
CRAWL_ROLE_STEALTHY = "crawl_stealthy"
CRAWL_ROLE_HTTP = "crawl_http"
WORKER_RESTART_LOCK_KEY = os.environ.get("SILO_WORKER_RESTART_LOCK_KEY", "silo:worker_runtime:restart_lock")
WORKER_RESTART_IN_PROGRESS_KEY = "silo:worker_runtime:restart_in_progress"
WORKER_LAST_RESTART_AT_KEY = "silo:worker_runtime:last_restart_at"


def _bounded_int(env_name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(os.environ.get(env_name, str(default)))
        return max(lo, min(v, hi))
    except (TypeError, ValueError):
        return default


# TTL heartbeat : doit rester > timeout BLPOP worker (30s) pour éviter faux « unhealthy ».
HEARTBEAT_TTL_SEC = _bounded_int("SILO_WORKER_HEARTBEAT_TTL_SEC", 45, 35, 86400)
RESTART_LOCK_TTL_SEC = _bounded_int("SILO_WORKER_RESTART_LOCK_TTL_SEC", 600, 30, 86400)


def _stealthy_key(role: Optional[str]) -> str:
    if role:
        return f"silo:worker_runtime:{role}:use_stealthy_fetcher"
    return WORKER_RUNTIME_STEALTHY_KEY


def _heartbeat_key(role: Optional[str]) -> str:
    if role:
        return f"silo:worker_runtime:{role}:heartbeat"
    return WORKER_RUNTIME_HEARTBEAT_KEY


def write_worker_runtime(r, use_stealthy: bool, role: Optional[str] = None) -> None:
    r.set(_stealthy_key(role), "1" if use_stealthy else "0")


def refresh_worker_heartbeat(r, role: Optional[str] = None) -> None:
    r.set(_heartbeat_key(role), "1", ex=HEARTBEAT_TTL_SEC)


def read_runtime_stealthy(r, role: Optional[str] = None) -> Optional[bool]:
    v = r.get(_stealthy_key(role))
    if v is None:
        return None
    if isinstance(v, bytes):
        v = v.decode()
    return str(v) == "1"


def is_worker_healthy(r, role: Optional[str] = None) -> bool:
    return bool(r.get(_heartbeat_key(role)))


def dual_crawl_worker_health(r) -> tuple[bool, bool]:
    """(stealthy_crawl_healthy, http_crawl_healthy) pour SILO_DUAL_CRAWL_WORKERS."""
    return is_worker_healthy(r, CRAWL_ROLE_STEALTHY), is_worker_healthy(r, CRAWL_ROLE_HTTP)
