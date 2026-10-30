"""
    Accès simple aux tables de la couche de restitution (PostgreSQL) depuis pandas.

    import sys; sys.path.append("/home/jovyan/work")
    from commun.donnees import lire, executer

    horaires = lire("gold_trajets_horaires")
    top = lire("SELECT zone, courses FROM gold_zones ORDER BY courses DESC LIMIT 10")
"""
import os
import subprocess
import sys

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "deces")
PG_USER = os.getenv("PG_USER", "projet")
PG_PASSWORD = os.getenv("PG_PASSWORD", "projet")

_moteur = None


def _installer(*paquets):
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", *paquets], check=True)


def moteur():
    """Connexion SQLAlchemy à PostgreSQL (créée une seule fois)."""
    global _moteur
    if _moteur is None:
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            _installer("psycopg2-binary")
        import sqlalchemy
        _moteur = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:5432/{PG_DB}")
    return _moteur


def lire(table_ou_requete):
    """Renvoie un DataFrame pandas à partir d'un nom de table ou d'une requête SQL."""
    import pandas as pd
    texte = table_ou_requete.strip()
    requete = texte if " " in texte else f"SELECT * FROM {texte}"
    return pd.read_sql(requete, moteur())


def executer(sql):
    """Exécute une ou plusieurs instructions SQL (création de vues, etc.)."""
    import sqlalchemy
    with moteur().begin() as conn:
        for instruction in [s for s in sql.split(";") if s.strip()]:
            conn.execute(sqlalchemy.text(instruction))