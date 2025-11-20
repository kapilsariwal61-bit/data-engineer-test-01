# src/pipeline/load.py
import yaml
from sqlalchemy import text
from src.utils.db_connector import get_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_load(
    dims: dict,
    facts: dict,
    config_path: str = "src/config/config.yaml",
):
    """
    Load step:
    - Idempotent: truncate all DW tables and full-refresh load.
    - Load dimensions first, then fact table.
    - Uses surrogate keys generated in transform.
    """
    logger.info("Starting load step")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    tables_cfg = config["tables"]

    engine = get_engine()

    dim_date_df = dims["dim_date"]
    dim_neighborhood_df = dims["dim_neighborhood"]
    dim_host_df = dims["dim_host"]
    dim_listing_df = dims["dim_listing"]
    fact_reviews_df = facts["fact_reviews"]

    with engine.begin() as conn:
        logger.info("Truncating DW tables for full-refresh (idempotent load)")
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    {fact},
                    {dim_listing},
                    {dim_host},
                    {dim_neighborhood},
                    {dim_date}
                RESTART IDENTITY CASCADE
                """.format(
                    fact=tables_cfg["fact_reviews"],
                    dim_listing=tables_cfg["dim_listing"],
                    dim_host=tables_cfg["dim_host"],
                    dim_neighborhood=tables_cfg["dim_neighborhood"],
                    dim_date=tables_cfg["dim_date"],
                )
            )
        )

        logger.info("Loading dim_date")
        dim_date_df.to_sql(
            tables_cfg["dim_date"],
            conn,
            if_exists="append",
            index=False,
        )

        logger.info("Loading dim_neighborhood")
        dim_neighborhood_df.to_sql(
            tables_cfg["dim_neighborhood"],
            conn,
            if_exists="append",
            index=False,
        )

        logger.info("Loading dim_host (SCD2-ready structure, single snapshot)")
        dim_host_df.to_sql(
            tables_cfg["dim_host"],
            conn,
            if_exists="append",
            index=False,
        )

        logger.info("Loading dim_listing")
        dim_listing_df.to_sql(
            tables_cfg["dim_listing"],
            conn,
            if_exists="append",
            index=False,
        )

        logger.info("Loading fact_reviews")
        fact_reviews_df.to_sql(
            tables_cfg["fact_reviews"],
            conn,
            if_exists="append",
            index=False,
        )

    logger.info("Load step complete")
