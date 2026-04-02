"""Generate SBERT embeddings for ECSF TKS text."""

import argparse
from typing import List, Dict

from sentence_transformers import SentenceTransformer

from src.utils.database import db, ensure_database_exists

ECSF_TEXT_TABLE = "ecsf_tks_text"
ECSF_EMBEDDING_TABLE = "ecsf_tks_embedding"


def load_pending_tks(model_name: str, limit: int | None) -> List[Dict]:
    limit_clause = "" if limit is None else "LIMIT %(limit)s"
    query = f"""
    SELECT t.tks_id, t.embedding_text
    FROM {ECSF_TEXT_TABLE} t
    LEFT JOIN {ECSF_EMBEDDING_TABLE} e
      ON e.tks_id = t.tks_id AND e.model_name = %(model_name)s
    WHERE e.tks_id IS NULL
    ORDER BY t.tks_id
    {limit_clause}
    """
    params = {"model_name": model_name, "limit": limit}
    return db.execute_query(query, params)


def insert_embeddings(model_name: str, rows: List[Dict], embeddings):
    insert_sql = f"""
    INSERT INTO {ECSF_EMBEDDING_TABLE} (
        tks_id,
        model_name,
        embedding
    ) VALUES (
        %(tks_id)s,
        %(model_name)s,
        %(embedding)s
    )
    ON CONFLICT (tks_id, model_name) DO NOTHING
    """

    payload = [
        {
            "tks_id": row["tks_id"],
            "model_name": model_name,
            "embedding": embedding.tolist() if hasattr(embedding, "tolist") else embedding,
        }
        for row, embedding in zip(rows, embeddings)
    ]
    db.execute_many(insert_sql, payload, batch_size=500)


def embed_ecsf(model_name: str, batch_size: int, limit: int | None):
    rows = load_pending_tks(model_name, limit)
    if not rows:
        print("No new ECSF entries to embed")
        return

    model = SentenceTransformer(model_name)
    texts = [row["embedding_text"] for row in rows]
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    insert_embeddings(model_name, rows, embeddings)
    print(f"Inserted {len(rows)} ECSF embeddings")


def parse_args():
    parser = argparse.ArgumentParser(description="Embed ECSF TKS text using SBERT")
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
    embed_ecsf(args.model_name, args.batch_size, args.limit)


if __name__ == "__main__":
    main()
