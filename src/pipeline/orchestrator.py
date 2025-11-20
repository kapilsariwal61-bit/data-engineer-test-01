# src/pipeline/orchestrator.py
from src.pipeline.extract import run_extract
from src.pipeline.validate import run_validate
from src.pipeline.transform import run_transform
from src.pipeline.load import run_load
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_pipeline(config_path: str = "src/config/config.yaml"):
    logger.info("=== Airbnb DW Pipeline START ===")

    # 1) Extract
    listings, reviews = run_extract(config_path=config_path)

    # 2) Validate
    listings, reviews = run_validate(
        listings, reviews, config_path=config_path
    )

    # 3) Transform
    dims, facts = run_transform(listings, reviews)

    # 4) Load
    run_load(dims, facts, config_path=config_path)

    logger.info("=== Airbnb DW Pipeline END ===")


if __name__ == "__main__":
    run_pipeline()
