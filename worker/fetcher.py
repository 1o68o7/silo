"""
Module de fetch HTML - Brief: Scrapling + StealthyFetcher (évasion anti-bot).
Fallback sur Trafilatura si Scrapling indisponible.
Optimisation: fetch parallèle, async aiohttp (Trafilatura), timeout 25s.
"""
import os
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from typing import Optional

logger = logging.getLogger("silo-worker")

USE_STEALTHY = os.environ.get("SILO_USE_STEALTHY_FETCHER", "true").lower() == "true"
USE_ASYNC_FETCH = os.environ.get("SILO_USE_ASYNC_FETCH", "true").lower() == "true"
FETCH_TIMEOUT = int(os.environ.get("SILO_FETCH_TIMEOUT", "25"))
FETCH_PARALLEL_WORKERS = int(os.environ.get("SILO_FETCH_PARALLEL_WORKERS", "3"))


def _fetch_stealthy(url: str) -> Optional[str]:
    """Fetch via Scrapling (sans timeout, appelé dans un thread)."""
    from scrapling.fetchers import StealthyFetcher
    from lxml import html as lxml_html
    StealthyFetcher.adaptive = True
    page = StealthyFetcher.fetch(url, headless=True, network_idle=True)
    if not page:
        return None
    if hasattr(page, "html") and page.html:
        return page.html
    if hasattr(page, "root") and page.root is not None:
        return lxml_html.tostring(page.root, encoding="unicode")
    if hasattr(page, "content") and page.content:
        return str(page.content)
    return None


def _fetch_trafilatura(url: str) -> Optional[str]:
    """Fallback Trafilatura."""
    from trafilatura import fetch_url
    result = fetch_url(url)
    if result and isinstance(result, bytes):
        return result.decode("utf-8", errors="ignore")
    return result if isinstance(result, str) else None


def fetch_html(url: str) -> Optional[str]:
    """
    Récupère le HTML d'une URL avec timeout.
    Priorité: Scrapling StealthyFetcher. Fallback: Trafilatura.
    """
    def _do_fetch():
        if USE_STEALTHY:
            try:
                return _fetch_stealthy(url)
            except ImportError:
                logger.debug("Scrapling non installé, fallback Trafilatura")
            except Exception as e:
                logger.warning(f"StealthyFetcher échoué pour {url}: {e}, fallback Trafilatura")
        return _fetch_trafilatura(url)

    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_do_fetch)
            return future.result(timeout=FETCH_TIMEOUT)
    except FuturesTimeoutError:
        logger.warning(f"Fetch timeout ({FETCH_TIMEOUT}s) pour {url[:80]}...")
        return None


async def _fetch_one_aiohttp(session, url: str) -> tuple[str, Optional[str]]:
    """Fetch une URL via aiohttp (pour Trafilatura mode)."""
    import aiohttp
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as resp:
            if resp.status == 200:
                body = await resp.read()
                return (url, body.decode("utf-8", errors="ignore"))
    except asyncio.TimeoutError:
        logger.warning(f"Fetch timeout ({FETCH_TIMEOUT}s) pour {url[:80]}...")
    except Exception as e:
        logger.warning(f"Fetch {url[:60]}...: {e}")
    return (url, None)


async def _fetch_urls_async(urls: list) -> dict:
    """Fetch parallèle async (aiohttp) — plus efficace que threads pour I/O HTTP."""
    import aiohttp
    results = {}
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0 (compatible; SiloBot/1.0)"}
    ) as session:
        tasks = [_fetch_one_aiohttp(session, u) for u in urls]
        for url, html in await asyncio.gather(*tasks):
            if html:
                results[url] = html
    return results


def fetch_urls_parallel(urls: list, max_workers: int = None) -> dict:
    """
    Fetch plusieurs URLs en parallèle.
    Si USE_STEALTHY=false et USE_ASYNC_FETCH=true: aiohttp (async, plus rapide).
    Sinon: ThreadPoolExecutor + fetch_html (StealthyFetcher ou Trafilatura).
    """
    workers = max_workers if max_workers is not None else FETCH_PARALLEL_WORKERS
    if not urls:
        return {}

    if len(urls) == 1 or workers <= 1:
        results = {}
        for u in urls:
            h = fetch_html(u)
            if h:
                results[u] = h
        return results

    # Async aiohttp (Trafilatura uniquement — StealthyFetcher est bloquant)
    if not USE_STEALTHY and USE_ASYNC_FETCH:
        try:
            return asyncio.run(_fetch_urls_async(urls))
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Async fetch fallback: {e}")

    # Threads (StealthyFetcher ou fallback)
    results = {}
    with ThreadPoolExecutor(max_workers=min(workers, len(urls))) as ex:
        futures = {ex.submit(fetch_html, url): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                html = future.result(timeout=FETCH_TIMEOUT + 5)
                if html:
                    results[url] = html
            except FuturesTimeoutError:
                logger.warning(f"Fetch timeout ({FETCH_TIMEOUT}s) pour {url[:80]}...")
            except Exception as e:
                logger.warning(f"Fetch {url[:60]}...: {e}")
    return results
