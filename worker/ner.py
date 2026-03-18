"""
Extraction des entités NER - Brief: spaCy fr_core_news_lg ou fr_core_news_sm.
Filtre les entités de type prix (MONEY, montants) non pertinentes pour le SEO.
SILO_SPACY_MODEL: fr_core_news_sm (plus rapide) ou fr_core_news_lg (plus précis, défaut).
"""
import os
import re
import logging
from typing import List, Optional

logger = logging.getLogger("silo-worker")

_nlp = None
SPACY_MODEL = os.environ.get("SILO_SPACY_MODEL", "fr_core_news_lg")

# Patterns pour exclure les entités de type prix / montants
_PRICE_PATTERNS = [
    re.compile(r"^\d+[\.,]?\d*\s*(CHF|€|\$|EUR|USD|GBP|fr\.?|francs?)\b", re.I),
    re.compile(r"\b(CHF|€|\$)\s*\d+[\.,]?\d*", re.I),
    re.compile(r"^\s*prix\s+[\d\s.,€$CHF]+", re.I),
    re.compile(r"[\d\s.,]+\s*(CHF|€|\$)\s*(en\s+stock|du\s+mois)?", re.I),
    re.compile(r"^\d+[\.,]\d{2}\s*(CHF|€|\$)"),  # 22,90 CHF
    re.compile(r"en\s+stock$", re.I),
    re.compile(r"^\d+\s*(CHF|€|\$)\s*$", re.I),
]


def _is_price_entity(text: str) -> bool:
    """Vérifie si l'entité ressemble à un prix (à exclure)."""
    if not text or len(text) < 3:
        return True
    t = text.strip()
    for pat in _PRICE_PATTERNS:
        if pat.search(t):
            return True
    # Montants purs (ex: "22,90", "99.00")
    if re.match(r"^\d+[\.,]\d{2}$", t):
        return True
    return False


def get_nlp():
    """Charge le modèle spaCy (lazy). SILO_SPACY_MODEL: fr_core_news_sm (rapide) ou fr_core_news_lg (précis)."""
    global _nlp
    if _nlp is None:
        try:
            import spacy
            _nlp = spacy.load(SPACY_MODEL)
            logger.info(f"Modèle spaCy chargé: {SPACY_MODEL}")
        except OSError:
            try:
                import spacy
                import subprocess
                subprocess.run(["python", "-m", "spacy", "download", SPACY_MODEL], check=False)
                _nlp = spacy.load(SPACY_MODEL)
                logger.info(f"Modèle spaCy chargé: {SPACY_MODEL}")
            except Exception as e:
                logger.warning(f"spaCy {SPACY_MODEL} non disponible: {e}")
                _nlp = False
    return _nlp if _nlp else None


def extract_entities(text: str, max_entities: int = 20) -> List[str]:
    """
    Extrait les entités nommées (PER, ORG, LOC, MISC...) via spaCy.
    Exclut les entités de type prix (MONEY, montants CHF/€/$).
    """
    nlp = get_nlp()
    if not nlp or not text or len(text) < 10:
        return []
    try:
        doc = nlp(text[:5000])
        entities = []
        for ent in doc.ents:
            if len(ent.text) <= 1:
                continue
            # Exclure MONEY et les entités ressemblant à des prix
            if ent.label_ == "MONEY" or _is_price_entity(ent.text):
                continue
            entities.append(ent.text.strip())
        return list(dict.fromkeys(entities))[:max_entities]  # déduplique en gardant l'ordre
    except Exception as e:
        logger.debug(f"NER erreur: {e}")
        return []


def extract_entities_batch(texts: List[str], max_entities: int = 20) -> List[List[str]]:
    """
    Extrait les entités pour plusieurs textes en batch via nlp.pipe (plus rapide).
    Retourne une liste de listes d'entités, une par texte d'entrée.
    """
    nlp = get_nlp()
    if not nlp or not texts:
        return [[] for _ in texts]
    results = []
    try:
        for doc in nlp.pipe((t[:5000] if t else "" for t in texts), batch_size=50):
            entities = []
            for ent in doc.ents:
                if len(ent.text) <= 1:
                    continue
                if ent.label_ == "MONEY" or _is_price_entity(ent.text):
                    continue
                entities.append(ent.text.strip())
            results.append(list(dict.fromkeys(entities))[:max_entities])
        return results
    except Exception as e:
        logger.debug(f"NER batch erreur: {e}")
        return [[] for _ in texts]


def anchor_contains_entity(anchor: str, target_entities: List[str]) -> bool:
    """Vérifie si l'ancre contient une entité NER de la page cible (bonus +20%)."""
    if not anchor or not target_entities:
        return False
    anchor_lower = anchor.lower()
    for ent in target_entities:
        if ent and len(ent) > 1 and ent.lower() in anchor_lower:
            return True
    return False
