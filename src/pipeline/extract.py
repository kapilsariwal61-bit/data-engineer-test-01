# src/pipeline/extract.py
from pathlib import Path
import pandas as pd
import yaml
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_extract(config_path: str = "src/config/config.yaml"):
    """
    Extract step:
    - Read listings.csv and reviews.csv from paths in config.
    - Return raw Pandas DataFrames.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    listings_path = Path(config["data_paths"]["listings"])
    reviews_path = Path(config["data_paths"]["reviews"])

    logger.info(f"Extracting listings from {listings_path}")
    logger.info(f"Extracting reviews from {reviews_path}")

    listings = pd.read_csv(listings_path)
    reviews = pd.read_csv(reviews_path)

    logger.info(f"Listings rows: {len(listings)}, Reviews rows: {len(reviews)}")
    return listings, reviews
    