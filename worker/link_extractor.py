"""
Extraction des liens avec contexte sémantique - Brief: LangExtract / fenêtre de texte.
Pour chaque lien: anchor, contexte autour (window), position dans le document.
"""
import re
from urllib.parse import urljoin, urlparse
from typing import List, Tuple

from lxml import html as lxml_html

from worker.url_utils import url_has_query_params


def get_links_with_context(
    html: str, base_url: str, context_window: int = 200, exclude_urls_with_params: bool = True
) -> List[Tuple[str, str, str, float]]:
    """
    Extrait les liens internes avec:
    - anchor text
    - contexte sémantique (fenêtre de texte autour du lien)
    - position_ratio (0-1, pour Reasonable Surfer: 1.5x si < 0.2)

    Returns: [(target_url, anchor, context_text, position_ratio), ...]
    """
    base_domain = urlparse(base_url).netloc
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return []
    body = tree.find(".//body") or tree
    if body is None:
        return []
    body_text = lxml_html.tostring(body, encoding="unicode", method="html")
    body_len = len(body_text)

    results = []
    seen = set()

    for elem in body.iter("a"):
        href = elem.get("href")
        if not href:
            continue
        full_url = urljoin(base_url, href.split("#")[0].strip())
        if not full_url.startswith("http") or urlparse(full_url).netloc != base_domain:
            continue
        if full_url == base_url:
            continue
        if full_url in seen:
            continue
        if exclude_urls_with_params and url_has_query_params(full_url):
            continue
        seen.add(full_url)

        anchor = "".join(elem.itertext()).strip()[:512] or "(lien)"

        # Position dans le document (pour Reasonable Surfer)
        elem_html = lxml_html.tostring(elem, encoding="unicode")
        pos = body_text.find(elem_html)
        position_ratio = pos / body_len if body_len > 0 else 0.5

        # Contexte: fenêtre de texte autour du lien
        start = max(0, pos - context_window)
        end = min(len(body_text), pos + len(elem_html) + context_window)
        context_raw = body_text[start:end]
        context_clean = re.sub(r"<[^>]+>", " ", context_raw)
        context_clean = re.sub(r"\s+", " ", context_clean).strip()[:500]

        results.append((full_url, anchor, context_clean or anchor, position_ratio))

    return results
