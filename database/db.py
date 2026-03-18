"""
Connexion PostgreSQL et session SQLAlchemy.
SILO_DB_POOL_SIZE / SILO_DB_MAX_OVERFLOW: pool de connexions (prod).
Engine singleton pour éviter d'épuiser max_connections PostgreSQL.
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from .models import Base

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://admin:password_secret@localhost:5433/semantic_cocoon",
)
POOL_SIZE = int(os.environ.get("SILO_DB_POOL_SIZE", "10"))
MAX_OVERFLOW = int(os.environ.get("SILO_DB_MAX_OVERFLOW", "20"))
POOL_TIMEOUT = int(os.environ.get("SILO_DB_POOL_TIMEOUT", "60"))

_engine = None


def get_engine():
    """Retourne l'engine singleton (évite d'épuiser max_connections PostgreSQL)."""
    global _engine
    if _engine is None:
        use_null_pool = "localhost" in DATABASE_URL
        kwargs = {
            "pool_pre_ping": True,
            "pool_recycle": 300,
            "pool_timeout": POOL_TIMEOUT,
            "poolclass": NullPool if use_null_pool else None,
        }
        if not use_null_pool:
            kwargs["pool_size"] = POOL_SIZE
            kwargs["max_overflow"] = MAX_OVERFLOW
        _engine = create_engine(DATABASE_URL, **kwargs)
    return _engine


def init_db(engine=None):
    """Crée les tables et l'extension pgvector."""
    eng = engine or get_engine()
    with eng.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.commit()
    Base.metadata.create_all(eng)
    # Migration: ajouter colonne excluded si absente (URLs avec paramètres)
    # lock_timeout évite de bloquer au démarrage si une tx longue tient la table
    with eng.connect() as conn:
        conn.execute(text("SET lock_timeout = '5s'"))
        conn.execute(text(
            "ALTER TABLE pages ADD COLUMN IF NOT EXISTS excluded BOOLEAN DEFAULT FALSE"
        ))
        conn.commit()
    return eng


def get_session() -> Session:
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
