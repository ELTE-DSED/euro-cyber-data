"""Create embedding and similarity tables."""

from src.utils.database import db, ensure_database_exists

SKILL_EMBEDDING_TABLE = "skill_embedding"
ECSF_EMBEDDING_TABLE = "ecsf_tks_embedding"
SIMILARITY_TABLE = "skill_ecsf_similarity"


def ensure_tables_exist():
    skill_embedding_sql = f"""
    CREATE TABLE IF NOT EXISTS {SKILL_EMBEDDING_TABLE} (
        skill_id INTEGER REFERENCES skill_dim(id),
        model_name TEXT NOT NULL,
        embedding DOUBLE PRECISION[] NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (skill_id, model_name)
    )
    """

    ecsf_embedding_sql = f"""
    CREATE TABLE IF NOT EXISTS {ECSF_EMBEDDING_TABLE} (
        tks_id TEXT REFERENCES ecsf_tks(id),
        model_name TEXT NOT NULL,
        embedding DOUBLE PRECISION[] NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (tks_id, model_name)
    )
    """

    similarity_sql = f"""
    CREATE TABLE IF NOT EXISTS {SIMILARITY_TABLE} (
        skill_id INTEGER REFERENCES skill_dim(id),
        tks_id TEXT REFERENCES ecsf_tks(id),
        model_name TEXT NOT NULL,
        similarity DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (skill_id, tks_id, model_name)
    )
    """

    db.execute_query(skill_embedding_sql, fetch=False)
    db.execute_query(ecsf_embedding_sql, fetch=False)
    db.execute_query(similarity_sql, fetch=False)


def main():
    created = ensure_database_exists()
    if created:
        print("Created database")
    else:
        print("Database already exists")

    ensure_tables_exist()
    print("Embedding and similarity tables are ready")


if __name__ == "__main__":
    main()
