# tests/test_load.py
"""
Integration tests for the Load step.

These tests require:
- Postgres running
- airbnb_dw database created
- sql/schema.sql already applied
- .env configured with DB credentials

Focus:
- run_load completes without error
- Dimension and fact tables in Postgres are populated
"""

from sqlalchemy import text

from src.pipeline.extract import run_extract
from src.pipeline.validate import run_validate
from src.pipeline.transform import run_transform
from src.pipeline.load import run_load
from src.utils.db_connector import get_engine


def test_load_populates_database(config_path):
    """
    Full pipeline: extract → validate → transform → load.

    After load:
    - dim tables and fact_reviews should have > 0 rows in Postgres.
    """
    listings_raw, reviews_raw = run_extract(config_path=config_path)
    listings_v, reviews_v = run_validate(
        listings_raw.copy(), reviews_raw.copy(), config_path=config_path
    )
    dims, facts = run_transform(listings_v, reviews_v)

    # Execute load (will truncate + reload)
    run_load(dims, facts, config_path=config_path)

    # Now query DB to check row counts
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT
                  (SELECT COUNT(*) FROM dim_date)          AS dim_date_rows,
                  (SELECT COUNT(*) FROM dim_neighborhood)  AS dim_neighborhood_rows,
                  (SELECT COUNT(*) FROM dim_host)          AS dim_host_rows,
                  (SELECT COUNT(*) FROM dim_listing)       AS dim_listing_rows,
                  (SELECT COUNT(*) FROM fact_reviews)      AS fact_reviews_rows
                """
            )
        )
        row = result.fetchone()

    assert row.dim_date_rows > 0, "dim_date is empty after load"
    assert row.dim_neighborhood_rows > 0, "dim_neighborhood is empty after load"
    assert row.dim_host_rows > 0, "dim_host is empty after load"
    assert row.dim_listing_rows > 0, "dim_listing is empty after load"
    assert row.fact_reviews_rows > 0, "fact_reviews is empty after load"
