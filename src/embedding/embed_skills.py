"""Generate SBERT embeddings for normalized job skills."""

import argparse
from typing import List, Dict

from sentence_transformers import SentenceTransformer

from src.utils.database import db, ensure_database_exists

SKILL_DIM_TABLE = "skill_dim"
SKILL_EMBEDDING_TABLE = "skill_embedding"


def load_pending_skills(model_name: str, limit: int | None) -> List[Dict]:
    limit_clause = "" if limit is None else "LIMIT %(limit)s"
    query = f"""
    SELECT s.id, s.normalized_skill
    FROM {SKILL_DIM_TABLE} s
    LEFT JOIN {SKILL_EMBEDDING_TABLE} e
      ON e.skill_id = s.id AND e.model_name = %(model_name)s
    WHERE e.skill_id IS NULL
    ORDER BY s.id
    {limit_clause}
    """
    params = {"model_name": model_name, "limit": limit}
    return db.execute_query(query, params)


def insert_embeddings(model_name: str, rows: List[Dict], embeddings):
    insert_sql = f"""
    INSERT INTO {SKILL_EMBEDDING_TABLE} (
        skill_id,
        model_name,
        embedding
    ) VALUES (
        %(skill_id)s,
        %(model_name)s,
        %(embedding)s
    )
    ON CONFLICT (skill_id, model_name) DO NOTHING
    """

    payload = [
        {
            "skill_id": row["id"],
            "model_name": model_name,
            "embedding": embedding.tolist() if hasattr(embedding, "tolist") else embedding,
        }
        for row, embedding in zip(rows, embeddings)
    ]
    db.execute_many(insert_sql, payload, batch_size=500)


def embed_skills(model_name: str, batch_size: int, limit: int | None):
    rows = load_pending_skills(model_name, limit)
    if not rows:
        print("No new skills to embed")
        return

    model = SentenceTransformer(model_name)
    texts = [row["normalized_skill"] for row in rows]
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    insert_embeddings(model_name, rows, embeddings)
    print(f"Inserted {len(rows)} skill embeddings")


def parse_args():
    parser = argparse.ArgumentParser(description="Embed normalized skills using SBERT")
    parser.add_argument(
        "--model-name",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="SentenceTransformer model name",
    )
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def main():
    created = ensure_database_exists()
    if created:
        print("Created database")
    else:
        print("Database already exists")

    args = parse_args()
    embed_skills(args.model_name, args.batch_size, args.limit)


if __name__ == "__main__":
    main()
