import re
import uuid
import time
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

CHECK_WEIGHTS = {
    "discoverability.link_headers": 8,
    "discoverability.robots_txt": 4,
    "discoverability.sitemap": 4,
    "discoverability.llms_txt": 4,
    "content_accessibility.markdown_negotiation": 20,
    "bot_access_control.web_bot_auth": 20,
    "protocol_discovery.api_catalog": 6,
    "protocol_discovery.oauth_discovery": 6,
    "protocol_discovery.oauth_protected_resource": 4,
    "protocol_discovery.mcp_server_card": 4,
    "protocol_discovery.webmcp": 4,
    "protocol_discovery.a2a_agent_card": 3,
    "protocol_discovery.agent_skills": 3,
    "commerce.x402_ucp_acp": 10,
}

_SCAN_CACHE: dict[str, dict[str, Any]] = {}
_SCAN_CACHE_TTL_SEC = int(os.environ.get("AGENT_READINESS_CACHE_TTL_SEC", "300"))
_SCAN_CACHE_METRICS = {"hits": 0, "misses": 0, "evictions": 0}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        raise ValueError("URL requise")
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = f"https://{url}"
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("URL invalide")
    cleaned = parsed._replace(fragment="")
    return cleaned.geturl()


def _status_from_presence(present: bool) -> str:
    return "pass" if present else "warn"


def _pick_headers(headers: httpx.Headers, keys: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key in keys:
        value = headers.get(key)
        if value:
            out[key] = value
    return out


def _looks_like_transient_error(status_code: int | None, error: str | None) -> bool:
    if status_code in (408, 425, 429, 500, 502, 503, 504):
        return True
    if not error:
        return False
    lowered = error.lower()
    return any(token in lowered for token in ["timeout", "temporarily", "connection reset", "tlsv1 alert internal error"])


def _probe(
    client: httpx.Client,
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    *,
    retries: int = 2,
    timeout_seconds: float = 8.0,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    for idx in range(retries + 1):
        try:
            response = client.request(
                method,
                url,
                headers=headers or {},
                follow_redirects=True,
                timeout=timeout_seconds + (idx * 2.0),
            )
            one = {
                "ok": True,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "text": response.text[:20000],
                "url": str(response.url),
            }
            attempts.append({"ok": True, "status_code": response.status_code, "url": str(response.url), "attempt": idx + 1})
            if response.status_code < 500 and response.status_code not in (408, 429):
                one["attempts"] = attempts
                return one
            if idx == retries:
                one["attempts"] = attempts
                return one
        except Exception as exc:
            err = str(exc)
            attempts.append({"ok": False, "error": err, "url": url, "attempt": idx + 1})
            if idx == retries:
                return {"ok": False, "error": err, "url": url, "attempts": attempts}
            if not _looks_like_transient_error(None, err):
                return {"ok": False, "error": err, "url": url, "attempts": attempts}
    return {"ok": False, "error": "Probe failed", "url": url, "attempts": attempts}


def _safe_presence_status(
    present: bool,
    probe_result: dict[str, Any] | None = None,
    *,
    strict: bool = False,
) -> str:
    if present:
        return "pass"
    if not probe_result:
        return "warn"
    if probe_result.get("ok") is False:
        return "fail" if strict else "warn"
    status_code = probe_result.get("status_code")
    if status_code in (401, 403):
        return "warn"
    if status_code in (404, 410):
        return "warn"
    if status_code and status_code >= 500:
        return "fail" if strict else "warn"
    return "warn"


def _mk_check(
    check_id: str,
    category: str,
    label: str,
    status: str,
    evidence: dict[str, Any],
    recommendation: str,
    spec_url: str,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "category": category,
        "label": label,
        "status": status,
        "evidence": evidence,
        "recommendation": recommendation,
        "specUrl": spec_url,
    }


def _cache_key(url: str, checks: dict[str, bool] | None) -> str:
    checks = checks or {}
    normalized_checks = ",".join(f"{k}:{str(v).lower()}" for k, v in sorted(checks.items()))
    return f"{normalize_url(url)}|{normalized_checks}"


def get_cached_scan_result(url: str, checks: dict[str, bool] | None = None) -> dict[str, Any] | None:
    if _SCAN_CACHE_TTL_SEC <= 0:
        return None
    key = _cache_key(url, checks)
    row = _SCAN_CACHE.get(key)
    if not row:
        _SCAN_CACHE_METRICS["misses"] += 1
        return None
    if row.get("expires_at", 0) < time.time():
        _SCAN_CACHE.pop(key, None)
        _SCAN_CACHE_METRICS["evictions"] += 1
        _SCAN_CACHE_METRICS["misses"] += 1
        return None
    _SCAN_CACHE_METRICS["hits"] += 1
    cached = dict(row.get("result", {}))
    if cached:
        cached["cache"] = {"hit": True, "ttlSec": _SCAN_CACHE_TTL_SEC}
    return cached


def set_cached_scan_result(url: str, checks: dict[str, bool] | None, result: dict[str, Any]) -> None:
    if _SCAN_CACHE_TTL_SEC <= 0:
        return
    key = _cache_key(url, checks)
    _SCAN_CACHE[key] = {
        "expires_at": time.time() + _SCAN_CACHE_TTL_SEC,
        "result": dict(result),
    }


def invalidate_cache_for_domain(domain: str) -> int:
    target = (domain or "").strip().lower()
    if not target:
        return 0
    to_delete: list[str] = []
    for key in _SCAN_CACHE.keys():
        url_part = key.split("|", 1)[0]
        try:
            netloc = urlparse(url_part).netloc.lower()
        except Exception:
            netloc = ""
        if netloc == target:
            to_delete.append(key)
    for k in to_delete:
        _SCAN_CACHE.pop(k, None)
    return len(to_delete)


def get_cache_metrics() -> dict[str, Any]:
    return {
        "ttlSec": _SCAN_CACHE_TTL_SEC,
        "entries": len(_SCAN_CACHE),
        "hits": int(_SCAN_CACHE_METRICS["hits"]),
        "misses": int(_SCAN_CACHE_METRICS["misses"]),
        "evictions": int(_SCAN_CACHE_METRICS["evictions"]),
    }


def flush_cache() -> int:
    count = len(_SCAN_CACHE)
    _SCAN_CACHE.clear()
    return count


def run_agent_readiness_scan(target_url: str, checks: dict[str, bool] | None = None) -> dict[str, Any]:
    normalized = normalize_url(target_url)
    domain = urlparse(normalized).netloc.lower()
    requested = checks or {
        "discoverability": True,
        "contentAccessibility": True,
        "botAccessControl": True,
        "protocolDiscovery": True,
        "commerce": True,
    }

    all_checks: list[dict[str, Any]] = []
    with httpx.Client(timeout=12.0, headers={"User-Agent": "log8ot-agent-readiness/1.1"}) as client:
        homepage = _probe(client, normalized, timeout_seconds=10.0, retries=2)
        home_headers = homepage.get("headers", {}) if homepage.get("ok") else {}
        link_header = (home_headers.get("link") or home_headers.get("Link") or "")
        body = homepage.get("text", "") if homepage.get("ok") else ""

        if requested.get("discoverability", True):
            has_sitemap_link = 'rel="sitemap"' in link_header.lower() or "sitemap.xml" in link_header.lower()
            all_checks.append(
                _mk_check(
                    "discoverability.link_headers",
                    "discoverability",
                    "Link headers discoverability",
                    _safe_presence_status(has_sitemap_link, homepage),
                    {
                        "linkHeader": link_header[:1500],
                        "homepageStatus": homepage.get("status_code"),
                        "attempts": homepage.get("attempts", []),
                    },
                    "Ajouter un header Link explicite pour exposer sitemap/alternate.",
                    "https://www.rfc-editor.org/rfc/rfc8288",
                )
            )

            robots = _probe(client, urljoin(normalized, "/robots.txt"), method="GET", timeout_seconds=6.0, retries=2)
            sitemap = _probe(client, urljoin(normalized, "/sitemap.xml"), method="GET", timeout_seconds=6.0, retries=2)
            llms = _probe(client, urljoin(normalized, "/llms.txt"), method="GET", timeout_seconds=6.0, retries=2)
            all_checks.append(
                _mk_check(
                    "discoverability.robots_txt",
                    "discoverability",
                    "robots.txt disponible",
                    _safe_presence_status(bool(robots.get("ok") and robots.get("status_code") == 200), robots),
                    {"statusCode": robots.get("status_code"), "url": robots.get("url"), "attempts": robots.get("attempts", [])},
                    "Publier un robots.txt lisible et cohérent avec les règles d'accès.",
                    "https://www.rfc-editor.org/rfc/rfc9309",
                )
            )
            all_checks.append(
                _mk_check(
                    "discoverability.sitemap",
                    "discoverability",
                    "sitemap.xml disponible",
                    _safe_presence_status(bool(sitemap.get("ok") and sitemap.get("status_code") == 200), sitemap),
                    {"statusCode": sitemap.get("status_code"), "url": sitemap.get("url"), "attempts": sitemap.get("attempts", [])},
                    "Exposer un sitemap XML à la racine ou via Link header.",
                    "https://www.sitemaps.org/protocol.html",
                )
            )
            all_checks.append(
                _mk_check(
                    "discoverability.llms_txt",
                    "discoverability",
                    "llms.txt disponible",
                    _safe_presence_status(bool(llms.get("ok") and llms.get("status_code") == 200), llms),
                    {"statusCode": llms.get("status_code"), "url": llms.get("url"), "attempts": llms.get("attempts", [])},
                    "Ajouter un llms.txt pour guider les agents IA sur le contenu autorisé.",
                    "https://llmstxt.org/",
                )
            )

        if requested.get("contentAccessibility", True):
            md_probe = _probe(
                client,
                normalized,
                headers={"Accept": "text/markdown,text/plain;q=0.9,*/*;q=0.1"},
                timeout_seconds=10.0,
                retries=2,
            )
            content_type = ""
            if md_probe.get("ok"):
                md_headers = md_probe.get("headers", {})
                content_type = md_headers.get("content-type", "") or md_headers.get("Content-Type", "")
            markdown_hints = "markdown" in content_type.lower() or "text/plain" in content_type.lower()
            weak_false_positive = bool(md_probe.get("ok")) and md_probe.get("status_code") == 200 and len((md_probe.get("text") or "").strip()) < 80
            md_ok = markdown_hints and not weak_false_positive
            all_checks.append(
                _mk_check(
                    "content_accessibility.markdown_negotiation",
                    "contentAccessibility",
                    "Markdown negotiation",
                    "pass" if md_ok else "warn",
                    {
                        "statusCode": md_probe.get("status_code"),
                        "contentType": content_type,
                        "weakFalsePositive": weak_false_positive,
                        "attempts": md_probe.get("attempts", []),
                    },
                    "Servir une représentation markdown/plain pour faciliter l'ingestion agentique.",
                    "https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Accept",
                )
            )

        if requested.get("botAccessControl", True):
            markers = ["web-bot-auth", "bot-auth", "ai-bot", "cf-aig", "cf-bot-management"]
            haystack = " ".join([link_header.lower(), body.lower(), str(home_headers).lower()])
            found = [m for m in markers if m in haystack]
            all_checks.append(
                _mk_check(
                    "bot_access_control.web_bot_auth",
                    "botAccessControl",
                    "Web Bot Auth discovery",
                    _safe_presence_status(bool(found), homepage),
                    {
                        "markersFound": found,
                        "securityHeaders": _pick_headers(httpx.Headers(home_headers), ["server", "cf-ray", "x-amzn-trace-id"]),
                        "attempts": homepage.get("attempts", []),
                    },
                    "Exposer un mécanisme explicite d'authentification/autorisation des bots.",
                    "https://blog.cloudflare.com/web-bot-auth/",
                )
            )

        if requested.get("protocolDiscovery", True):
            api_catalog = any(x in body.lower() for x in ["/openapi", "swagger", "api-docs", "application/vnd.oai.openapi"])
            oauth_probe = _probe(client, urljoin(normalized, "/.well-known/openid-configuration"), timeout_seconds=7.0, retries=2)
            oauth_pr_probe = _probe(client, urljoin(normalized, "/.well-known/oauth-protected-resource"), timeout_seconds=7.0, retries=2)
            oauth_discovery = bool(oauth_probe.get("ok") and oauth_probe.get("status_code") in (200, 204))
            oauth_pr = bool(oauth_pr_probe.get("ok") and oauth_pr_probe.get("status_code") in (200, 204))
            mcp_server_card = any(x in body.lower() for x in ["mcp", "server card", "model context protocol"])
            webmcp_probe = _probe(client, urljoin(normalized, "/.well-known/webmcp"), timeout_seconds=7.0, retries=2)
            a2a_probe = _probe(client, urljoin(normalized, "/.well-known/agent.json"), timeout_seconds=7.0, retries=2)
            webmcp = bool(webmcp_probe.get("ok") and webmcp_probe.get("status_code") in (200, 204))
            a2a_card = bool(a2a_probe.get("ok") and a2a_probe.get("status_code") in (200, 204))
            agent_skills = any(x in body.lower() for x in ["agent skills", "/skills", "skills.json"])

            all_checks.extend(
                [
                    _mk_check(
                        "protocol_discovery.api_catalog",
                        "protocolDiscovery",
                        "API catalog discovery",
                        _safe_presence_status(api_catalog, homepage),
                        {"homepageHints": api_catalog, "attempts": homepage.get("attempts", [])},
                        "Publier un catalogue API visible (OpenAPI/Swagger/Docs).",
                        "https://spec.openapis.org/oas/latest.html",
                    ),
                    _mk_check(
                        "protocol_discovery.oauth_discovery",
                        "protocolDiscovery",
                        "OAuth discovery",
                        _safe_presence_status(oauth_discovery, oauth_probe),
                        {
                            "wellKnown": "/.well-known/openid-configuration",
                            "statusCode": oauth_probe.get("status_code"),
                            "attempts": oauth_probe.get("attempts", []),
                        },
                        "Exposer les métadonnées OAuth/OpenID discovery.",
                        "https://openid.net/specs/openid-connect-discovery-1_0.html",
                    ),
                    _mk_check(
                        "protocol_discovery.oauth_protected_resource",
                        "protocolDiscovery",
                        "OAuth Protected Resource (RFC9728)",
                        _safe_presence_status(oauth_pr, oauth_pr_probe),
                        {
                            "wellKnown": "/.well-known/oauth-protected-resource",
                            "statusCode": oauth_pr_probe.get("status_code"),
                            "attempts": oauth_pr_probe.get("attempts", []),
                        },
                        "Exposer le endpoint RFC9728 pour ressources protégées.",
                        "https://www.rfc-editor.org/rfc/rfc9728",
                    ),
                    _mk_check(
                        "protocol_discovery.mcp_server_card",
                        "protocolDiscovery",
                        "MCP Server Card discovery",
                        _safe_presence_status(mcp_server_card, homepage),
                        {"homepageHints": mcp_server_card, "attempts": homepage.get("attempts", [])},
                        "Publier une server card MCP avec endpoints et capacités.",
                        "https://modelcontextprotocol.io/",
                    ),
                    _mk_check(
                        "protocol_discovery.webmcp",
                        "protocolDiscovery",
                        "WebMCP discovery",
                        _safe_presence_status(webmcp, webmcp_probe),
                        {
                            "wellKnown": "/.well-known/webmcp",
                            "statusCode": webmcp_probe.get("status_code"),
                            "attempts": webmcp_probe.get("attempts", []),
                        },
                        "Exposer un point d'entrée WebMCP documenté.",
                        "https://modelcontextprotocol.io/",
                    ),
                    _mk_check(
                        "protocol_discovery.a2a_agent_card",
                        "protocolDiscovery",
                        "A2A agent card discovery",
                        _safe_presence_status(a2a_card, a2a_probe),
                        {
                            "wellKnown": "/.well-known/agent.json",
                            "statusCode": a2a_probe.get("status_code"),
                            "attempts": a2a_probe.get("attempts", []),
                        },
                        "Publier une Agent Card A2A décrivant l'agent et ses actions.",
                        "https://a2aprotocol.ai/",
                    ),
                    _mk_check(
                        "protocol_discovery.agent_skills",
                        "protocolDiscovery",
                        "Agent skills discovery",
                        _safe_presence_status(agent_skills, homepage),
                        {"homepageHints": agent_skills, "attempts": homepage.get("attempts", [])},
                        "Rendre les skills agents découvrables via endpoint ou manifest.",
                        "https://modelcontextprotocol.io/",
                    ),
                ]
            )

        if requested.get("commerce", True):
            commerce_markers = [r"\bx402\b", r"\bucp\b", r"\bacp\b"]
            found_markers = [m for m in commerce_markers if re.search(m, body, re.IGNORECASE)]
            all_checks.append(
                _mk_check(
                    "commerce.x402_ucp_acp",
                    "commerce",
                    "Commerce agentique (x402/UCP/ACP)",
                    _safe_presence_status(bool(found_markers), homepage),
                    {"markersFound": found_markers, "attempts": homepage.get("attempts", [])},
                    "Exposer les primitives commerce machine-to-machine (x402/UCP/ACP).",
                    "https://x402.org/",
                )
            )

        # Re-expose simple WAF/Cloudflare signal without duplicating scanner logic.
        waf_headers = _pick_headers(httpx.Headers(home_headers), ["server", "cf-ray", "cf-cache-status", "x-sucuri-id"])
        if waf_headers:
            all_checks.append(
                _mk_check(
                    "discoverability.waf_signal_reuse",
                    "discoverability",
                    "Signal WAF/Cloudflare (réexposé)",
                    "na",
                    {"headers": waf_headers},
                    "Signal informatif réutilisé, non pondéré dans le score readiness.",
                    "https://developers.cloudflare.com/fundamentals/reference/http-headers/",
                )
            )

    total_weight = 0
    score_weight = 0.0
    by_category = {
        "discoverability": {"weight": 20, "obtained": 0.0, "max": 0.0},
        "contentAccessibility": {"weight": 20, "obtained": 0.0, "max": 0.0},
        "botAccessControl": {"weight": 20, "obtained": 0.0, "max": 0.0},
        "protocolDiscovery": {"weight": 30, "obtained": 0.0, "max": 0.0},
        "commerce": {"weight": 10, "obtained": 0.0, "max": 0.0},
    }
    cat_map = {
        "discoverability": "discoverability",
        "contentAccessibility": "contentAccessibility",
        "botAccessControl": "botAccessControl",
        "protocolDiscovery": "protocolDiscovery",
        "commerce": "commerce",
    }

    for chk in all_checks:
        cid = chk["id"]
        status = chk["status"]
        w = CHECK_WEIGHTS.get(cid, 0)
        if w <= 0 or status == "na":
            continue
        total_weight += w
        cat = cat_map.get(chk["category"], "discoverability")
        by_category[cat]["max"] += w
        if status == "pass":
            score_weight += w
            by_category[cat]["obtained"] += w
        elif status == "warn":
            score_weight += w * 0.5
            by_category[cat]["obtained"] += w * 0.5

    global_score = int(round((score_weight / total_weight) * 100)) if total_weight > 0 else 0
    grade = "A" if global_score >= 85 else "B" if global_score >= 70 else "C" if global_score >= 55 else "D"
    breakdown = {
        cat: int(round((val["obtained"] / val["max"]) * 100)) if val["max"] > 0 else 0
        for cat, val in by_category.items()
    }

    return {
        "jobId": str(uuid.uuid4()),
        "url": normalized,
        "domain": domain,
        "status": "completed",
        "score": {"global": global_score, "grade": grade, "breakdown": breakdown},
        "checks": all_checks,
        "generatedAt": now_iso(),
    }
