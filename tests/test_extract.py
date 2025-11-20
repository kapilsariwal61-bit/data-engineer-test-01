# tests/test_extract.py
"""
Tests for the Extract step.

Focus:
- run_extract returns non-empty DataFrames
- Expected columns are present
"""

import pandas as pd

from src.pipeline.extract import run_extract


def test_extract_returns_dataframes(config_path):
    """run_extract should return two non-empty Pandas DataFrames."""
    listings, reviews = run_extract(config_path=config_path)

    assert isinstance(listings, pd.DataFrame)
    assert isinstance(reviews, pd.DataFrame)
    assert not listings.empty, "Listings dataframe is empty"
    assert not reviews.empty, "Reviews dataframe is empty"


def test_extract_expected_columns_listings(config_path):
    """Listings dataframe should have all required columns from the raw CSV."""
    listings, _ = run_extract(config_path=config_path)

    expected_cols = {
        "id",
        "name",
        "host_id",
        "host_name",
        "neighbourhood_group",
        "neighbourhood",
        "latitude",
        "longitude",
        "room_type",
        "price",
        "minimum_nights",
        "number_of_reviews",
        "last_review",
        "reviews_per_month",
        "calculated_host_listings_count",
        "availability_365",
        "number_of_reviews_ltm",
        "license",
    }

    missing = expected_cols - set(listings.columns)
    assert not missing, f"Listings missing expected columns: {missing}"


def test_extract_expected_columns_reviews(config_path):
    """Reviews dataframe should have all required columns from the raw CSV."""
    _, reviews = run_extract(config_path=config_path)

    expected_cols = {"listing_id", "date"}
    missing = expected_cols - set(reviews.columns)
    assert not missing, f"Reviews missing expected columns: {missing}"
