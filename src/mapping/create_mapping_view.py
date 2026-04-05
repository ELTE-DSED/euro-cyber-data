"""Create SQL view for interpretable skill to ECSF mappings."""

from src.utils.database import db, ensure_database_exists

VIEW_NAME = "vw_skill_ecsf_mapping"


def create_view():
    view_sql = f"""
    CREATE OR REPLACE VIEW {VIEW_NAME} AS
    SELECT
        s.id AS skill_id,
        s.normalized_skill AS normalized_skill,
        sim.tks_id AS tks_id,
        t.description AS tks_description,
        sim.similarity AS similarity,
        sim.model_name AS model_name,
        ROW_NUMBER() OVER (
            PARTITION BY s.id, sim.model_name
            ORDER BY sim.similarity DESC
        ) AS rank
    FROM skill_ecsf_similarity sim
    INNER JOIN skill_dim s
        ON s.id = sim.skill_id
    INNER JOIN ecsf_tks t
        ON t.id = sim.tks_id
    """

    db.execute_query(view_sql, fetch=False)


def main():
    created = ensure_database_exists()
    if created:
        print("Created database")
    else:
        print("Database already exists")

    create_view()
    print(f"View ready: {VIEW_NAME}")


if __name__ == "__main__":
    main()
