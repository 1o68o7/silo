"""
Utilitaires URL pour Silo: détection des paramètres, canonicalisation.
Permet de filtrer ou marquer les URLs avec query params (utm_*, fbclid, etc.).
"""
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


# Paramètres de tracking à ignorer pour la canonicalisation (dupliquent le contenu)
TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "fbclid", "gclid", "gclsrc", "dclid", "msclkid",  # Click IDs
    "ref", "source", "mc_cid", "mc_eid", "_ga",
})


def url_has_query_params(url: str) -> bool:
    """Vérifie si l'URL contient des paramètres de requête."""
    if not url or "?" not in url:
        return False
    parsed = urlparse(url)
    return bool(parsed.query)


def get_canonical_url(url: str, strip_tracking_only: bool = True) -> str:
    """
    Retourne une version canonicalisée de l'URL.
    - strip_tracking_only=True: supprime uniquement les paramètres de tracking (utm_*, fbclid, etc.)
    - strip_tracking_only=False: supprime tous les paramètres
    """
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=False)
    if strip_tracking_only:
        filtered = {k: v for k, v in params.items() if k.lower() not in TRACKING_PARAMS}
    else:
        filtered = {}
    if not filtered:
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", parsed.fragment))
    new_query = urlencode(
        [(k, v[0] if len(v) == 1 else v) for k, v in filtered.items()],
        doseq=True,
    )
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", new_query, parsed.fragment))


def should_exclude_from_crawl(url: str) -> bool:
    """
    Détermine si une URL doit être exclue du crawl/analyse.
    Par défaut: exclure les URLs avec des paramètres de requête.
    """
    return url_has_query_params(url)


# Codes langue supportés pour path_prefix (aligné avec get_theoretical_silo_from_url)
LANG_CODES = frozenset({"fr", "en", "de", "it", "es", "nl", "pt"})


def extract_lang_path_prefix(url: str) -> str | None:
    """
    Extrait le préfixe de chemin langue depuis l'URL si le premier segment est un code langue connu.
    Ex: https://site.com/fr/accus/... -> "/fr"
        https://site.com/en/page -> "/en"
        https://site.com/page -> None
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        segments = [s for s in path.split("/") if s]
        if segments and segments[0].lower() in LANG_CODES:
            return "/" + segments[0].lower()
        return None
    except Exception:
        return None


def url_matches_path_prefix(url: str, path_prefix: str | None) -> bool:
    """
    Vérifie si le path de l'URL commence par path_prefix.
    - path_prefix None ou "" : pas de restriction (retourne True)
    - path_prefix "/fr" : True si path commence par /fr (ex: /fr/accus/...)
    """
    if not path_prefix:
        return True
    try:
        parsed = urlparse(url)
        path = parsed.path or "/"
        if not path.startswith("/"):
            path = "/" + path
        prefix = path_prefix if path_prefix.startswith("/") else "/" + path_prefix
        return path == prefix or path.startswith(prefix + "/")
    except Exception:
        return False


def get_theoretical_silo_from_url(url: str) -> str:
    """
    Extrait un "silo théorique" depuis l'URL (structure des chemins).
    Premier segment significatif du path, hors codes langue (fr, en, etc.).
    Aligné avec le frontend (silo-utils.ts).
    """
    if not url:
        return "racine"
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        segments = [s for s in path.split("/") if s]
        for s in segments:
            if s.lower() not in LANG_CODES and len(s) > 1:
                return s
        return "racine" if segments else "default"
    except Exception:
        return "racine"
