"""
Service layer - CRUD pour projets, pages, edges.
"""
from datetime import datetime
from typing import Optional
import uuid

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import Project, Page, Edge, OpportunityRecord, ComputedOpportunity, EMBEDDING_DIM
from .db import get_engine, get_session, init_db


def ensure_db():
    """Initialise la DB si nécessaire."""
    try:
        init_db()
        return True
    except Exception:
        return False


def list_projects(session: Session) -> list[dict]:
    rows = session.query(Project).order_by(Project.created_at.desc()).all()
    return [
        {
            "id": p.id,
            "name": p.name,
            "seed_url": p.seed_url,
            "created_at": p.created_at.isoformat() + "Z" if p.created_at else "",
            "urls_count": p.urls_count or 0,
            "status": p.status or "idle",
        }
        for p in rows
    ]


def create_project(session: Session, name: str, seed_url: str) -> dict:
    pid = str(uuid.uuid4())[:8]
    p = Project(id=pid, name=name, seed_url=seed_url)
    session.add(p)
    session.commit()
    return {
        "id": p.id,
        "name": p.name,
        "seed_url": p.seed_url,
        "created_at": p.created_at.isoformat() + "Z",
        "urls_count": 0,
        "status": "idle",
    }


def get_project(session: Session, project_id: str) -> Optional[Project]:
    return session.query(Project).filter(Project.id == project_id).first()


def get_graph(session: Session, project_id: str, include_excluded: bool = False) -> dict:
    """
    Retourne nodes et edges pour l'API.
    Par défaut, exclut les pages avec paramètres (excluded=True ou URL avec query).
    """
    pages = session.query(Page).filter(Page.project_id == project_id).all()
    edges = session.query(Edge).filter(Edge.project_id == project_id).all()

    # Identifier les pages exclues (URL avec paramètres)
    from worker.url_utils import url_has_query_params
    excluded_ids = {
        p.id for p in pages
        if getattr(p, "excluded", False) or url_has_query_params(p.url) or "?" in (p.url or "")
    }
    excluded_count = len(excluded_ids)

    # Filtrer les pages exclues sauf si demandé
    if not include_excluded:
        pages_filtered = [p for p in pages if p.id not in excluded_ids]
    else:
        pages_filtered = pages

    def _filter_entities(entities):
        """Exclut les marqueurs internes (__FETCH_FAILED__, __NO_ENTITIES__) de l'API."""
        if not entities:
            return []
        return [e for e in (entities if isinstance(entities, list) else []) if e not in ("__FETCH_FAILED__", "__NO_ENTITIES__")]

    nodes = [
        {
            "id": p.id,
            "url": p.url,
            "title": p.title,
            "h1": p.h1,
            "page_rank": p.page_rank or 0.0,
            "depth": p.depth or 0,
            "silo_id": p.silo_id,
            "entities": _filter_entities(p.entities),
            "excluded": p.id in excluded_ids,
        }
        for p in pages_filtered
    ]
    # Filtrer les edges qui pointent vers des nœuds exclus
    node_ids = {n["id"] for n in nodes}
    edges_data = [
        {"source": e.source_id, "target": e.target_id, "weight": e.weight or 0.0, "anchor": e.anchor}
        for e in edges
        if e.source_id in node_ids and e.target_id in node_ids
    ]
    return {"nodes": nodes, "edges": edges_data, "excluded_count": excluded_count}


def get_silo_analysis(session: Session, project_id: str) -> Optional[dict]:
    """
    Analyse silos théorique vs réel (Phase 6).
    Retourne les métriques pré-calculées pour la page Analyse silos.
    """
    from worker.url_utils import get_theoretical_silo_from_url

    graph = get_graph(session, project_id, include_excluded=False)
    nodes = graph["nodes"]
    edges = graph["edges"]

    if not nodes:
        return None

    # Agrégation par silo théorique
    by_theoretical = {}
    for n in nodes:
        th = get_theoretical_silo_from_url(n.get("url"))
        if th not in by_theoretical:
            by_theoretical[th] = {"count": 0, "louvain_ids": set(), "louvain_counts": {}}
        entry = by_theoretical[th]
        entry["count"] += 1
        rid = n.get("silo_id") or "—"
        entry["louvain_ids"].add(rid)
        entry["louvain_counts"][rid] = entry["louvain_counts"].get(rid, 0) + 1

    # Agrégation par silo réel (Louvain)
    by_real = {}
    for n in nodes:
        rid = n.get("silo_id") or "—"
        if rid not in by_real:
            by_real[rid] = {"count": 0, "theoretical_ids": set(), "theoretical_counts": {}}
        entry = by_real[rid]
        entry["count"] += 1
        th = get_theoretical_silo_from_url(n.get("url"))
        entry["theoretical_ids"].add(th)
        entry["theoretical_counts"][th] = entry["theoretical_counts"].get(th, 0) + 1

    # Liens internes
    node_to_theo = {n["id"]: get_theoretical_silo_from_url(n.get("url")) for n in nodes}
    node_to_real = {n["id"]: n.get("silo_id") or "—" for n in nodes}

    internal_links_theo = {}
    internal_links_real = {}
    for e in edges:
        src, tgt = e["source"], e["target"]
        th_s, th_t = node_to_theo.get(src), node_to_theo.get(tgt)
        r_s, r_t = node_to_real.get(src), node_to_real.get(tgt)
        if th_s and th_t and th_s == th_t:
            internal_links_theo[th_s] = internal_links_theo.get(th_s, 0) + 1
        if r_s and r_t and r_s == r_t:
            internal_links_real[r_s] = internal_links_real.get(r_s, 0) + 1

    # Dominant Louvain par segment théorique
    dominant_by_theoretical = {}
    for th, entry in by_theoretical.items():
        m = entry["louvain_counts"]
        dominant = max(m, key=m.get) if m else "—"
        dominant_by_theoretical[th] = dominant

    # Cohérence par segment théorique
    coherence_by_theoretical = {}
    for th, entry in by_theoretical.items():
        dom = dominant_by_theoretical.get(th, "—")
        coherent = entry["louvain_counts"].get(dom, 0)
        coherence_by_theoretical[th] = (coherent / entry["count"] * 100) if entry["count"] else 0

    # Score global
    total_pages = sum(e["count"] for e in by_theoretical.values())
    global_coherence = (
        sum(coherence_by_theoretical.get(th, 0) * by_theoretical[th]["count"] for th in by_theoretical)
        / total_pages
        if total_pages else 0
    )

    # Pages incohérentes
    inconsistent_pages = []
    for n in nodes:
        th = get_theoretical_silo_from_url(n.get("url"))
        dom = dominant_by_theoretical.get(th, "—")
        rid = n.get("silo_id") or "—"
        if dom != "—" and rid != "—" and rid != dom:
            inconsistent_pages.append({
                "id": n["id"],
                "url": n.get("url", ""),
                "theoretical": th,
                "real": rid,
            })

    # Matrice théorique → réel
    all_louvain = sorted(
        set(by_real.keys())
        | set(rid for entry in by_theoretical.values() for rid in entry["louvain_ids"])
    )
    matrix_theo_to_real = []
    for th in sorted(by_theoretical.keys()):
        entry = by_theoretical[th]
        counts = dict(entry["louvain_counts"])
        matrix_theo_to_real.append({
            "theo": th,
            "counts": counts,
            "coherence": coherence_by_theoretical.get(th, 0),
        })

    # Sérialiser les sets en listes pour JSON
    return {
        "by_theoretical": {
            th: {
                "count": e["count"],
                "louvain_ids": list(e["louvain_ids"]),
                "louvain_counts": e["louvain_counts"],
            }
            for th, e in by_theoretical.items()
        },
        "by_real": {
            rid: {
                "count": e["count"],
                "theoretical_ids": list(e["theoretical_ids"]),
                "theoretical_counts": e["theoretical_counts"],
            }
            for rid, e in by_real.items()
        },
        "internal_links_theoretical": internal_links_theo,
        "internal_links_real": internal_links_real,
        "coherence_by_theoretical": coherence_by_theoretical,
        "dominant_by_theoretical": dominant_by_theoretical,
        "global_coherence": round(global_coherence, 1),
        "inconsistent_pages": inconsistent_pages[:100],
        "matrix_theo_to_real": matrix_theo_to_real,
        "all_louvain_ids": all_louvain,
    }


def get_crawl_status(session: Session, project_id: str) -> dict:
    p = get_project(session, project_id)
    if not p:
        return None
    count = session.query(Page).filter(Page.project_id == project_id).count()
    total = max(count, 1)
    processed = count if p.status == "done" else count
    return {
        "project_id": project_id,
        "status": p.status or "idle",
        "urls_discovered": p.urls_count or 0,
        "urls_processed": processed,
        "progress_percent": 100.0 if p.status == "done" else (processed / total * 100) if total else 0,
        "message": None,
    }


def update_project_status(session: Session, project_id: str, status: str, urls_count: int = None):
    p = get_project(session, project_id)
    if p:
        p.status = status
        if urls_count is not None:
            p.urls_count = urls_count
        session.commit()


def delete_project(session: Session, project_id: str) -> bool:
    """Supprime un projet et ses pages/edges (cascade)."""
    p = get_project(session, project_id)
    if p:
        session.delete(p)
        session.commit()
        return True
    return False


def get_embeddings_status(session: Session, project_id: str, page_id: str = None) -> dict:
    """Retourne le statut des embeddings (nécessaires pour les opportunités)."""
    total = session.query(Page).filter(Page.project_id == project_id).count()
    with_emb = session.query(Page).filter(
        Page.project_id == project_id,
        Page.embedding.isnot(None),
    ).count()
    result = {
        "total_pages": total,
        "pages_with_embedding": with_emb,
        "has_embeddings": with_emb > 0,
    }
    if page_id:
        page = session.query(Page).filter(
            Page.project_id == project_id,
            Page.id == page_id,
        ).first()
        result["page_has_embedding"] = page is not None and page.embedding is not None
    return result


def get_ner_status(session: Session, project_id: str) -> dict:
    """Retourne le statut NER (pages avec entités détectées)."""
    from sqlalchemy import func
    total = session.query(Page).filter(Page.project_id == project_id).count()
    with_entities = session.query(Page).filter(
        Page.project_id == project_id,
        Page.entities.isnot(None),
        func.jsonb_array_length(Page.entities) > 0,
    ).count()
    return {
        "total_pages": total,
        "pages_with_entities": with_entities,
    }


def _cosine_sim(a: list, b: list) -> float:
    """Similarité cosine entre deux vecteurs."""
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


# Taille du voisinage pour l'analyse locale (topologique + sémantique)
NEIGHBORHOOD_K = 4

# Boost de score quand deux URLs partagent des entités NER
NER_OVERLAP_BOOST = 0.05


def _get_top_semantic_neighbors_pgvector(
    session: Session, project_id: str, page_id: str, k: int, exclude_linked: bool = False
) -> list[dict]:
    """
    Top-k voisins sémantiques via pgvector (ORDER BY embedding <=>).
    Retourne [{id, url, similarity}, ...] triés par similarité décroissante.
    """
    # Filtre SQL: excluded, url sans paramètres
    excl_filter = """
      AND (p2.excluded = FALSE OR p2.excluded IS NULL)
      AND p2.url NOT LIKE '%?%'
    """
    if exclude_linked:
        excl_filter += """
      AND NOT EXISTS (
        SELECT 1 FROM edges e
        WHERE e.project_id = :project_id
          AND ((e.source_id = :page_id AND e.target_id = p2.id)
               OR (e.target_id = :page_id AND e.source_id = p2.id))
      )
    """

    sql = text(f"""
        SELECT p2.id, p2.url,
               1 - (p2.embedding <=> p1.embedding) AS similarity
        FROM pages p1
        JOIN pages p2 ON p2.project_id = p1.project_id
          AND p2.id != p1.id
          AND p2.embedding IS NOT NULL
        WHERE p1.id = :page_id
          AND p1.project_id = :project_id
          AND p1.embedding IS NOT NULL
          {excl_filter}
        ORDER BY p2.embedding <=> p1.embedding
        LIMIT :k
    """)
    rows = session.execute(sql, {"page_id": page_id, "project_id": project_id, "k": k}).fetchall()
    return [
        {"id": r.id, "url": r.url or "", "similarity": round(float(r.similarity), 4)}
        for r in rows
    ]


def _get_topological_neighbors(
    session: Session, project_id: str, page_id: str, k: int
) -> list[str]:
    """
    IDs des k premiers voisins topologiques (inbound + outbound).
    Retourne une liste d'IDs de pages (sans doublon, ordre arbitraire).
    """
    edges = session.query(Edge).filter(Edge.project_id == project_id).all()
    neighbor_ids = []
    for e in edges:
        if e.source_id == page_id:
            neighbor_ids.append(e.target_id)
        elif e.target_id == page_id:
            neighbor_ids.append(e.source_id)
    # Dédupliquer et limiter à k
    seen = set()
    result = []
    for nid in neighbor_ids:
        if nid not in seen and nid != page_id:
            seen.add(nid)
            result.append(nid)
            if len(result) >= k:
                break
    return result


_INVALID_ENTITIES = {"__FETCH_FAILED__", "__NO_ENTITIES__"}


def _filter_valid_entities(entities: list) -> list:
    """Exclut les marqueurs internes, retourne une liste pour affichage."""
    if not entities:
        return []
    return [e for e in (entities if isinstance(entities, list) else []) if e and e not in _INVALID_ENTITIES]


def _entities_overlap_score(entities_a: list, entities_b: list) -> float:
    """
    Score de chevauchement NER (0 à NER_OVERLAP_BOOST).
    Exclut __FETCH_FAILED__, __NO_ENTITIES__.
    """
    if not entities_a or not entities_b:
        return 0.0
    set_a = set(_filter_valid_entities(entities_a))
    set_b = set(_filter_valid_entities(entities_b))
    overlap = len(set_a & set_b)
    if overlap == 0:
        return 0.0
    return min(NER_OVERLAP_BOOST, overlap * 0.02)


def _get_silo_mismatch_alert(
    url: str, silo_theoretical: str, silo_louvain: Optional[str]
) -> Optional[str]:
    """
    Alerte si l'URL est dans un silo théorique A mais Louvain la place en B.
    """
    if not silo_louvain or silo_louvain == "—":
        return None
    if silo_theoretical == silo_louvain:
        return None
    return f"Re-maillage: structure={silo_theoretical}, cluster Louvain={silo_louvain}"


def get_opportunities(session: Session, project_id: str, min_similarity: float = 0.9) -> list[dict]:
    """
    Gap Analysis: paires de pages avec similarité sémantique forte (>= min_similarity)
    mais sans lien physique entre elles.
    Approche voisinage: pgvector top-k par page (évite O(n²)), dédupliqué.
    """
    from worker.url_utils import url_has_query_params

    pages = session.query(Page).filter(
        Page.project_id == project_id,
        Page.embedding.isnot(None),
    ).all()
    pages = [
        p
        for p in pages
        if not (getattr(p, "excluded", False) or url_has_query_params(p.url or "") or "?" in (p.url or ""))
    ]
    page_ids = [p.id for p in pages]
    pages_by_id = {p.id: p for p in pages}

    seen_pairs = set()
    pairs = []
    k_global = 50  # Plus de voisins pour la vue globale

    for page_id in page_ids:
        neighbors = _get_top_semantic_neighbors_pgvector(
            session, project_id, page_id, k=k_global, exclude_linked=True
        )
        for nb in neighbors:
            if nb["similarity"] < min_similarity:
                continue
            other_id = nb["id"]
            pair_key = tuple(sorted([page_id, other_id]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            src, tgt = pair_key
            src_p = pages_by_id.get(src)
            tgt_p = pages_by_id.get(tgt)
            pairs.append({
                "source": src,
                "target": tgt,
                "similarity": nb["similarity"],
                "source_url": src_p.url if src_p else "",
                "target_url": tgt_p.url if tgt_p else "",
            })

    return pairs


def _extract_zone_texte(content_text: Optional[str], max_chars: int = 300) -> str:
    """
    Extrait la zone de texte (paragraphe) la plus pertinente pour insérer un lien.
    Utilise le premier paragraphe substantiel du contenu.
    """
    if not content_text or not content_text.strip():
        return ""
    # Découper par paragraphes (double saut de ligne ou \n\n)
    paragraphs = [p.strip() for p in content_text.split("\n\n") if len(p.strip()) > 50]
    if not paragraphs:
        # Fallback: premier bloc de texte
        text = content_text.strip()[:max_chars]
        return text + ("..." if len(content_text) > max_chars else "")
    # Retourner le premier paragraphe significatif (hors intro courte)
    for p in paragraphs:
        if len(p) >= 80:
            return (p[:max_chars] + "...") if len(p) > max_chars else p
    return paragraphs[0][:max_chars] + ("..." if len(paragraphs[0]) > max_chars else "")


def _suggest_phrase_ancre(target_h1: Optional[str], target_title: Optional[str], entities: list) -> str:
    """
    Propose une phrase d'ancrage pour le lien, basée sur H1, titre ou entités NER.
    """
    if target_h1 and len(target_h1.strip()) > 0:
        # H1 prioritaire, tronquer si trop long (idéal 3-8 mots)
        words = target_h1.strip().split()[:8]
        return " ".join(words)
    if target_title and len(target_title.strip()) > 0:
        words = target_title.strip().split()[:8]
        return " ".join(words)
    if entities:
        valid = [e for e in entities if e and e not in ("__FETCH_FAILED__", "__NO_ENTITIES__")]
        if valid:
            return valid[0]
    return "en savoir plus"


def get_opportunities_with_script(
    session: Session, project_id: str, min_similarity: float = 0.9
) -> list[dict]:
    """
    Opportunités enrichies avec zone_texte et phrase_ancre_proposee (script de maillage).
    """
    pairs = get_opportunities(session, project_id, min_similarity)
    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}

    result = []
    for p in pairs:
        source_page = pages_by_id.get(p["source"])
        target_page = pages_by_id.get(p["target"])
        zone = _extract_zone_texte(source_page.content_text if source_page else None)
        entities = target_page.entities if target_page and target_page.entities else []
        phrase = _suggest_phrase_ancre(
            target_page.h1 if target_page else None,
            target_page.title if target_page else None,
            entities,
        )
        result.append({
            **p,
            "zone_texte": zone,
            "phrase_ancre_proposee": phrase,
            "project_id": project_id,
        })
    return result


# Seuil minimal stocké pour les opportunités pré-calculées (permet filtrage 0.7-1.0 à la lecture)
COMPUTED_OPP_MIN_SIMILARITY = 0.7


def get_computed_opportunities_status(session: Session, project_id: str) -> Optional[dict]:
    """Retourne le statut des opportunités pré-calculées (count, computed_at) ou None si vide."""
    from sqlalchemy import func
    row = session.query(
        func.count(ComputedOpportunity.id).label("count"),
        func.max(ComputedOpportunity.computed_at).label("computed_at"),
    ).filter(ComputedOpportunity.project_id == project_id).first()
    if not row or (row.count or 0) == 0:
        return None
    return {"count": row.count, "computed_at": row.computed_at.isoformat() + "Z" if row.computed_at else None}


def get_computed_opportunities(
    session: Session, project_id: str, min_similarity: float = 0.85
) -> list[dict]:
    """Lit les opportunités pré-calculées depuis la BDD (filtrage par seuil)."""
    rows = session.query(ComputedOpportunity).filter(
        ComputedOpportunity.project_id == project_id,
        ComputedOpportunity.similarity >= min_similarity,
    ).order_by(ComputedOpportunity.similarity.desc()).all()
    return [
        {
            "source": r.source_page_id,
            "target": r.target_page_id,
            "similarity": round(r.similarity, 4),
            "source_url": r.source_url or "",
            "target_url": r.target_url or "",
        }
        for r in rows
    ]


def enrich_computed_opportunities_with_script(
    session: Session, project_id: str, pairs: list[dict]
) -> list[dict]:
    """Enrichit les paires avec zone_texte, phrase_ancre, NER, silos et embedding."""
    if not pairs:
        return []
    from worker.url_utils import get_theoretical_silo_from_url

    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}
    result = []
    for p in pairs:
        source_page = pages_by_id.get(p["source"])
        target_page = pages_by_id.get(p["target"])
        zone = _extract_zone_texte(source_page.content_text if source_page else None)
        entities = target_page.entities if target_page and target_page.entities else []
        phrase = _suggest_phrase_ancre(
            target_page.h1 if target_page else None,
            target_page.title if target_page else None,
            entities,
        )
        ent_src = _filter_valid_entities(source_page.entities if source_page else [])
        ent_tgt = _filter_valid_entities(target_page.entities if target_page else [])
        ent_shared = list(set(ent_src) & set(ent_tgt))
        src_url = p.get("source_url") or (source_page.url if source_page else "")
        tgt_url = p.get("target_url") or (target_page.url if target_page else "")

        result.append({
            **p,
            "zone_texte": zone,
            "phrase_ancre_proposee": phrase,
            "project_id": project_id,
            "entities_source": ent_src,
            "entities_target": ent_tgt,
            "entities_shared": ent_shared,
            "silo_theoretical_source": get_theoretical_silo_from_url(src_url) if src_url else "—",
            "silo_theoretical_target": get_theoretical_silo_from_url(tgt_url) if tgt_url else "—",
            "silo_louvain_source": source_page.silo_id if source_page and source_page.silo_id else "—",
            "silo_louvain_target": target_page.silo_id if target_page and target_page.silo_id else "—",
            "embedding_dim": EMBEDDING_DIM,
        })
    return result


def run_compute_and_store_opportunities(session: Session, project_id: str) -> dict:
    """
    Calcule les opportunités via pgvector (top-k par page) et les stocke en BDD.
    Approche voisinage: évite O(n²), dédupliqué, par batch de pages.
    """
    from worker.url_utils import url_has_query_params

    pages = session.query(Page).filter(
        Page.project_id == project_id,
        Page.embedding.isnot(None),
    ).all()
    pages = [
        p
        for p in pages
        if not (getattr(p, "excluded", False) or url_has_query_params(p.url or "") or "?" in (p.url or ""))
    ]
    if len(pages) < 2:
        return {"ok": False, "pairs_stored": 0, "error": "not_enough_pages"}

    session.query(ComputedOpportunity).filter(ComputedOpportunity.project_id == project_id).delete()
    session.commit()

    seen_pairs = set()
    total_stored = 0
    k_per_page = 50
    BATCH_COMMIT = 500

    for page in pages:
        neighbors = _get_top_semantic_neighbors_pgvector(
            session, project_id, page.id, k=k_per_page, exclude_linked=True
        )
        batch_records = []
        for nb in neighbors:
            if nb["similarity"] < COMPUTED_OPP_MIN_SIMILARITY:
                continue
            other_id = nb["id"]
            pair_key = tuple(sorted([page.id, other_id]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            if pair_key[0] == page.id:
                src_url = page.url or ""
                tgt_url = nb.get("url", "")
            else:
                src_url = nb.get("url", "")
                tgt_url = page.url or ""

            batch_records.append(
                ComputedOpportunity(
                    project_id=project_id,
                    source_page_id=pair_key[0],
                    target_page_id=pair_key[1],
                    similarity=round(nb["similarity"], 4),
                    source_url=src_url,
                    target_url=tgt_url,
                )
            )

        for rec in batch_records:
            session.add(rec)
        total_stored += len(batch_records)

        if total_stored > 0 and total_stored % BATCH_COMMIT < len(batch_records):
            session.commit()

    if total_stored > 0:
        session.commit()

    return {"ok": True, "pairs_stored": total_stored}


def get_page(session: Session, project_id: str, page_id: str) -> Optional[dict]:
    """Retourne une page avec toutes ses données (inspecteur)."""
    p = session.query(Page).filter(
        Page.project_id == project_id,
        Page.id == page_id,
    ).first()
    if not p:
        return None
    entities = p.entities or []
    entities = [e for e in (entities if isinstance(entities, list) else []) if e not in ("__FETCH_FAILED__", "__NO_ENTITIES__")]
    return {
        "id": p.id,
        "url": p.url,
        "title": p.title,
        "h1": p.h1,
        "page_rank": p.page_rank or 0.0,
        "depth": p.depth or 0,
        "silo_id": p.silo_id,
        "entities": entities,
    }


def get_page_links(session: Session, project_id: str, page_id: str) -> dict:
    """Retourne les liens entrants et sortants d'une page (vue macro)."""
    edges = session.query(Edge).filter(Edge.project_id == project_id).all()
    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}

    outgoing = []
    incoming = []
    for e in edges:
        if e.source_id == page_id:
            target = pages_by_id.get(e.target_id)
            if target:
                outgoing.append({
                    "page_id": target.id,
                    "url": target.url,
                    "title": target.title,
                    "weight": e.weight or 0.0,
                    "anchor": e.anchor,
                })
        if e.target_id == page_id:
            source = pages_by_id.get(e.source_id)
            if source:
                incoming.append({
                    "page_id": source.id,
                    "url": source.url,
                    "title": source.title,
                    "weight": e.weight or 0.0,
                    "anchor": e.anchor,
                })
    return {"outgoing": outgoing, "incoming": incoming}


def get_opportunities_for_page(
    session: Session, project_id: str, page_id: str, min_similarity: float = 0.9
) -> list[dict]:
    """
    Opportunités où la page est source OU cible.
    Approche voisinage : top-k sémantique via pgvector, boost NER, alerte silo mismatch.
    """
    from worker.url_utils import get_theoretical_silo_from_url

    page = session.query(Page).filter(
        Page.project_id == project_id,
        Page.id == page_id,
        Page.embedding.isnot(None),
    ).first()
    if not page or page.embedding is None:
        return []

    # Top-k voisins sémantiques sans lien (pgvector)
    k = max(NEIGHBORHOOD_K * 2, 20)  # Plus de candidats pour filtrer par seuil
    neighbors = _get_top_semantic_neighbors_pgvector(
        session, project_id, page_id, k=k, exclude_linked=True
    )
    if not neighbors:
        return []

    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}
    source_page = pages_by_id.get(page_id)
    source_entities = (source_page.entities or []) if source_page else []

    silo_theo = get_theoretical_silo_from_url(page.url or "")
    silo_louvain = page.silo_id or "—"
    silo_alert = _get_silo_mismatch_alert(page.url or "", silo_theo, silo_louvain)

    result = []
    for nb in neighbors:
        if nb["similarity"] < min_similarity:
            continue
        other_id = nb["id"]
        other_page = pages_by_id.get(other_id)
        other_entities = (other_page.entities or []) if other_page else []

        # Boost NER si entités partagées
        score = nb["similarity"] + _entities_overlap_score(source_entities, other_entities)
        if score < min_similarity:
            continue

        src_id, tgt_id = (page_id, other_id) if page_id < other_id else (other_id, page_id)
        src_url = page.url if src_id == page_id else (other_page.url if other_page else "")
        tgt_url = other_page.url if tgt_id == other_id else (page.url if page else "")

        # zone_texte = contenu de la page source (où insérer le lien)
        zone_page = pages_by_id.get(src_id)
        phrase_page = pages_by_id.get(tgt_id)
        zone = _extract_zone_texte(zone_page.content_text if zone_page else None)
        phrase = _suggest_phrase_ancre(
            phrase_page.h1 if phrase_page else None,
            phrase_page.title if phrase_page else None,
            phrase_page.entities if phrase_page else [],
        )

        ner_boost = _entities_overlap_score(source_entities, other_entities)
        entities_shared = list(
            set(_filter_valid_entities(source_entities)) & set(_filter_valid_entities(other_entities))
        )

        src_page = pages_by_id.get(src_id)
        tgt_page = pages_by_id.get(tgt_id)

        pair = {
            "source": src_id,
            "target": tgt_id,
            "similarity": round(score, 4),
            "similarity_base": round(nb["similarity"], 4),
            "source_url": src_url,
            "target_url": tgt_url,
            "zone_texte": zone,
            "phrase_ancre_proposee": phrase,
            "project_id": project_id,
            "entities_source": _filter_valid_entities(src_page.entities if src_page else []),
            "entities_target": _filter_valid_entities(tgt_page.entities if tgt_page else []),
            "entities_shared": entities_shared,
            "ner_boost": round(ner_boost, 4),
            "silo_theoretical_source": get_theoretical_silo_from_url(src_url) if src_url else "—",
            "silo_theoretical_target": get_theoretical_silo_from_url(tgt_url) if tgt_url else "—",
            "silo_louvain_source": src_page.silo_id if src_page and src_page.silo_id else "—",
            "silo_louvain_target": tgt_page.silo_id if tgt_page and tgt_page.silo_id else "—",
            "embedding_dim": EMBEDDING_DIM,
        }
        if silo_alert:
            pair["silo_mismatch_alert"] = silo_alert
        result.append(pair)

    return result


def get_similarity_stats_for_page(
    session: Session, project_id: str, page_id: str
) -> dict:
    """
    Retourne la similarité max pour cette page avec les pages non liées.
    Utilise pgvector (top-1) pour performance.
    """
    page = session.query(Page).filter(
        Page.project_id == project_id,
        Page.id == page_id,
        Page.embedding.isnot(None),
    ).first()
    if not page or page.embedding is None:
        return {"max_similarity": None, "pages_compared": 0, "hint": "no_embedding"}

    neighbors = _get_top_semantic_neighbors_pgvector(
        session, project_id, page_id, k=1, exclude_linked=True
    )
    max_sim = neighbors[0]["similarity"] if neighbors else 0.0
    return {
        "max_similarity": round(max_sim, 4) if max_sim > 0 else None,
        "pages_compared": 1 if neighbors else 0,
    }


def get_top_similar_pairs_for_page(
    session: Session, project_id: str, page_id: str, limit: int = 10
) -> list[dict]:
    """
    Top N paires les plus proches (sans lien) pour cette page, quel que soit le seuil.
    Utilise pgvector pour performance (ORDER BY embedding <=>).
    """
    page = session.query(Page).filter(
        Page.project_id == project_id,
        Page.id == page_id,
        Page.embedding.isnot(None),
    ).first()
    if not page or page.embedding is None:
        return []

    neighbors = _get_top_semantic_neighbors_pgvector(
        session, project_id, page_id, k=limit, exclude_linked=True
    )
    if not neighbors:
        return []

    from worker.url_utils import get_theoretical_silo_from_url

    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}
    result = []
    for nb in neighbors:
        other_id = nb["id"]
        other_page = pages_by_id.get(other_id)

        src_id, tgt_id = (page_id, other_id) if page_id < other_id else (other_id, page_id)
        src_url = page.url if src_id == page_id else (other_page.url if other_page else "")
        tgt_url = other_page.url if tgt_id == other_id else (page.url if page else "")

        zone_page = page if src_id == page_id else other_page
        phrase_page = page if tgt_id == page_id else other_page
        zone = _extract_zone_texte(zone_page.content_text if zone_page else None)
        phrase = _suggest_phrase_ancre(
            phrase_page.h1 if phrase_page else None,
            phrase_page.title if phrase_page else None,
            phrase_page.entities if phrase_page else [],
        )

        src_page = page if src_id == page_id else other_page
        tgt_page = page if tgt_id == page_id else other_page
        ent_src = _filter_valid_entities(src_page.entities if src_page else [])
        ent_tgt = _filter_valid_entities(tgt_page.entities if tgt_page else [])
        ent_shared = list(set(ent_src) & set(ent_tgt))

        result.append({
            "source": src_id,
            "target": tgt_id,
            "similarity": nb["similarity"],
            "source_url": src_url,
            "target_url": tgt_url,
            "zone_texte": zone,
            "phrase_ancre_proposee": phrase,
            "project_id": project_id,
            "entities_source": ent_src,
            "entities_target": ent_tgt,
            "entities_shared": ent_shared,
            "silo_theoretical_source": get_theoretical_silo_from_url(src_url) if src_url else "—",
            "silo_theoretical_target": get_theoretical_silo_from_url(tgt_url) if tgt_url else "—",
            "silo_louvain_source": src_page.silo_id if src_page and src_page.silo_id else "—",
            "silo_louvain_target": tgt_page.silo_id if tgt_page and tgt_page.silo_id else "—",
            "embedding_dim": EMBEDDING_DIM,
        })
    return result


def save_opportunity_records(session: Session, project_id: str, pairs: list[dict]) -> list[dict]:
    """Enregistre des opportunités en BDD (stockage indéfini)."""
    saved = []
    for p in pairs:
        rec = OpportunityRecord(
            project_id=project_id,
            source_page_id=p["source"],
            target_page_id=p["target"],
            similarity=p.get("similarity", 0.0),
            zone_texte=p.get("zone_texte"),
            phrase_ancre_proposee=p.get("phrase_ancre_proposee"),
        )
        session.add(rec)
        session.flush()
        saved.append({
            "id": rec.id,
            "source": p["source"],
            "target": p["target"],
            "similarity": rec.similarity,
            "zone_texte": rec.zone_texte,
            "phrase_ancre_proposee": rec.phrase_ancre_proposee,
        })
    session.commit()
    return saved


def list_opportunity_records(session: Session, project_id: str, page_id: str = None) -> list[dict]:
    """Liste les opportunités enregistrées, optionnellement filtrées par page."""
    q = session.query(OpportunityRecord).filter(OpportunityRecord.project_id == project_id)
    if page_id:
        q = q.filter(
            (OpportunityRecord.source_page_id == page_id) |
            (OpportunityRecord.target_page_id == page_id)
        )
    records = q.order_by(OpportunityRecord.created_at.desc()).all()
    pages_by_id = {p.id: p for p in session.query(Page).filter(Page.project_id == project_id).all()}
    result = []
    for r in records:
        src = pages_by_id.get(r.source_page_id)
        tgt = pages_by_id.get(r.target_page_id)
        result.append({
            "id": r.id,
            "source": r.source_page_id,
            "target": r.target_page_id,
            "source_url": src.url if src else "",
            "target_url": tgt.url if tgt else "",
            "similarity": r.similarity,
            "zone_texte": r.zone_texte,
            "phrase_ancre_proposee": r.phrase_ancre_proposee,
            "created_at": r.created_at.isoformat() + "Z" if r.created_at else "",
        })
    return result


def delete_opportunity_record(session: Session, project_id: str, record_id: int) -> bool:
    """Supprime une opportunité enregistrée."""
    rec = session.query(OpportunityRecord).filter(
        OpportunityRecord.project_id == project_id,
        OpportunityRecord.id == record_id,
    ).first()
    if rec:
        session.delete(rec)
        session.commit()
        return True
    return False
