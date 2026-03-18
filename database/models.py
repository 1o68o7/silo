"""
Schéma PostgreSQL + pgvector pour Silo.
EMBEDDING_DIM: 384 (e5-small) ou 1024 (e5-large). Migration requise si changement.
"""
import os
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    ForeignKey,
    Text,
    Boolean,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declarative_base
from pgvector.sqlalchemy import Vector

Base = declarative_base()

# Dimension des embeddings: e5-small=384, e5-large=1024
_embedding_model = os.environ.get("SILO_EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
EMBEDDING_DIM = 384 if "small" in _embedding_model.lower() else 1024


class Project(Base):
    __tablename__ = "projects"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    seed_url = Column(String(2048), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default="idle")  # idle | crawling | done | error
    urls_count = Column(Integer, default=0)

    pages = relationship("Page", back_populates="project", cascade="all, delete-orphan")
    edges = relationship("Edge", back_populates="project", cascade="all, delete-orphan")


class Page(Base):
    __tablename__ = "pages"

    id = Column(String(64), primary_key=True)  # hash of URL
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    url = Column(String(2048), nullable=False)
    title = Column(String(512))
    h1 = Column(String(512))
    depth = Column(Integer, default=0)
    page_rank = Column(Float, default=0.0)
    silo_id = Column(String(20))  # Louvain community id
    embedding = Column(Vector(EMBEDDING_DIM))  # e5-small=384, e5-large=1024
    entities = Column(JSONB, default=list)  # NER entities
    content_text = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    excluded = Column(Boolean, default=False)  # True si URL avec paramètres (à filtrer de l'analyse)

    project = relationship("Project", back_populates="pages")
    out_edges = relationship("Edge", foreign_keys="Edge.source_id", back_populates="source_page")
    in_edges = relationship("Edge", foreign_keys="Edge.target_id", back_populates="target_page")


class Edge(Base):
    __tablename__ = "edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    source_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    target_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    weight = Column(Float, default=0.0)
    anchor = Column(String(512))
    context_embedding = Column(Vector(EMBEDDING_DIM))  # e5-small=384, e5-large=1024

    project = relationship("Project", back_populates="edges")
    source_page = relationship("Page", foreign_keys=[source_id], back_populates="out_edges")
    target_page = relationship("Page", foreign_keys=[target_id])


class CrawlQueue(Base):
    """File d'attente Redis simulée en BDD (fallback) ou métadonnées."""
    __tablename__ = "crawl_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), nullable=False)
    url = Column(String(2048), nullable=False)
    depth = Column(Integer, default=0)
    status = Column(String(20), default="pending")  # pending | processing | done | error
    created_at = Column(DateTime, default=datetime.utcnow)


class OpportunityRecord(Base):
    """Opportunités de maillage stockées indéfiniment jusqu'à suppression par l'utilisateur."""
    __tablename__ = "opportunity_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    source_page_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    target_page_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    similarity = Column(Float, nullable=False)
    zone_texte = Column(Text)
    phrase_ancre_proposee = Column(String(512))
    created_at = Column(DateTime, default=datetime.utcnow)


class ComputedOpportunity(Base):
    """Opportunités pré-calculées pour la vue Toutes (global). Évite le calcul O(n²) à chaque requête."""
    __tablename__ = "computed_opportunities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    source_page_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    target_page_id = Column(String(64), ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    similarity = Column(Float, nullable=False)
    source_url = Column(Text)
    target_url = Column(Text)
    zone_texte = Column(Text)
    phrase_ancre_proposee = Column(String(512))
    computed_at = Column(DateTime, default=datetime.utcnow)
