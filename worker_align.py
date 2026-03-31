"""
Alignement du worker sur SILO_USE_STEALTHY_FETCHER avant enqueue crawl (brief 2026-03-30).
"""
import logging
import os
import subprocess
import time
import uuid
from typing import Literal, Optional, Tuple

from worker.redis_runtime import (
    WORKER_LAST_RESTART_AT_KEY,
    WORKER_RESTART_IN_PROGRESS_KEY,
    WORKER_RESTART_LOCK_KEY,
    RESTART_LOCK_TTL_SEC,
    read_runtime_stealthy,
    is_worker_healthy,
)

log = logging.getLogger("silo-api")

AlignAction = Literal["none", "restarted"]


class AlignWorkerError(Exception):
    """Erreur métier d'alignement ; `.code` sert au mapping HTTP."""

    def __init__(self, code: str, detail: str):
        self.code = code
        self.detail = detail
        super().__init__(detail)


def worker_restart_command_configured() -> bool:
    return bool(os.environ.get("SILO_WORKER_RESTART_COMMAND", "").strip())


def user_triggered_restart_allowed() -> bool:
    return os.environ.get("SILO_ALLOW_USER_TRIGGERED_WORKER_RESTART", "true").lower() == "true"


def align_timeout_sec() -> int:
    try:
        v = int(os.environ.get("SILO_WORKER_ALIGN_TIMEOUT_SEC", "120"))
        return max(1, min(v, 7200))
    except (TypeError, ValueError):
        return 120


def _restart_command_timeout_sec() -> int:
    try:
        v = int(os.environ.get("SILO_WORKER_RESTART_COMMAND_TIMEOUT_SEC", "300"))
        return max(1, min(v, 3600))
    except (TypeError, ValueError):
        return 300


def get_worker_state(r) -> Tuple[Optional[bool], bool]:
    """(runtime_use_stealthy, heartbeat_ok)."""
    try:
        stealthy = read_runtime_stealthy(r)
        healthy = is_worker_healthy(r)
        return stealthy, healthy
    except Exception as e:
        log.warning("Lecture état worker Redis: %s", e)
        return None, False


def _run_restart_command(desired_stealthy: bool) -> None:
    tmpl = os.environ.get("SILO_WORKER_RESTART_COMMAND", "").strip()
    if not tmpl:
        raise RuntimeError("SILO_WORKER_RESTART_COMMAND vide")
    use = "true" if desired_stealthy else "false"
    cmd = tmpl.replace("{use_stealthy}", use)
    timeout = _restart_command_timeout_sec()
    log.info("Exécution redémarrage worker (timeout=%ss)", timeout)
    try:
        result = subprocess.run(cmd, shell=True, timeout=timeout, capture_output=True, text=True)
    except subprocess.TimeoutExpired:
        log.error("Commande restart : timeout après %ss", timeout)
        raise AlignWorkerError(
            "WORKER_RESTART_FAILED",
            f"La commande de redémarrage du worker a dépassé le délai ({timeout}s).",
        ) from None
    if result.returncode != 0:
        err_tail = (result.stderr or result.stdout or "")[:800]
        log.warning(
            "Commande restart code=%s sortie=%s",
            result.returncode,
            err_tail,
        )
        raise AlignWorkerError(
            "WORKER_RESTART_FAILED",
            "La commande de redémarrage du worker a échoué (code non nul). Voir les logs serveur.",
        )


def _wait_healthy_or_timeout(r, desired: bool) -> None:
    """Attend que le heartbeat et le mode Redis soient cohérents (sans restart)."""
    deadline = time.time() + align_timeout_sec()
    while time.time() < deadline:
        stealthy, healthy = get_worker_state(r)
        if healthy and stealthy is not None and stealthy == desired:
            return
        time.sleep(1)
    raise AlignWorkerError(
        "WORKER_UNAVAILABLE",
        "Le worker ne répond pas (heartbeat) ou le mode runtime n'est pas encore cohérent.",
    )


def ensure_worker_fetch_mode(
    r,
    desired: bool,
) -> AlignAction:
    """
    Garantit que le worker annonce `desired` dans Redis après éventuel redémarrage.
    Lève AlignWorkerError avec .code pour mapping HTTP.
    """
    stealthy, healthy = get_worker_state(r)
    if stealthy is not None and stealthy == desired:
        if healthy:
            return "none"
        _wait_healthy_or_timeout(r, desired)
        return "none"

    if not worker_restart_command_configured():
        raise AlignWorkerError(
            "WORKER_RESTART_NOT_CONFIGURED",
            "Le mode de fetch demandé ne correspond pas au worker et aucune commande de redémarrage n'est configurée (SILO_WORKER_RESTART_COMMAND).",
        )
    if not user_triggered_restart_allowed():
        raise AlignWorkerError(
            "WORKER_RESTART_DISABLED",
            "Redémarrage du moteur désactivé par la politique (SILO_ALLOW_USER_TRIGGERED_WORKER_RESTART).",
        )

    token = str(uuid.uuid4())
    got_lock = r.set(WORKER_RESTART_LOCK_KEY, token, nx=True, ex=RESTART_LOCK_TTL_SEC)
    if not got_lock:
        raise AlignWorkerError(
            "WORKER_ALIGN_IN_PROGRESS",
            "Un autre alignement ou redémarrage du worker est déjà en cours.",
        )

    try:
        r.set(WORKER_RESTART_IN_PROGRESS_KEY, "1", ex=RESTART_LOCK_TTL_SEC)
        try:
            _run_restart_command(desired)
        finally:
            try:
                r.delete(WORKER_RESTART_IN_PROGRESS_KEY)
            except Exception:
                pass

        deadline = time.time() + align_timeout_sec()
        last_log = 0.0
        while time.time() < deadline:
            stealthy, healthy = get_worker_state(r)
            if stealthy is not None and healthy and stealthy == desired:
                from datetime import datetime, timezone

                try:
                    r.set(
                        WORKER_LAST_RESTART_AT_KEY,
                        datetime.now(timezone.utc).isoformat(),
                    )
                except Exception:
                    pass
                return "restarted"
            if time.time() - last_log > 10:
                log.info(
                    "Attente worker prêt: stealthy=%s healthy=%s (cible=%s)",
                    stealthy,
                    healthy,
                    desired,
                )
                last_log = time.time()
            time.sleep(1)

        raise AlignWorkerError(
            "WORKER_ALIGN_TIMEOUT",
            "Timeout en attendant que le worker redémarre avec le mode de fetch demandé.",
        )
    finally:
        try:
            cur = r.get(WORKER_RESTART_LOCK_KEY)
            if cur and (cur.decode() if isinstance(cur, bytes) else str(cur)) == token:
                r.delete(WORKER_RESTART_LOCK_KEY)
        except Exception:
            pass
