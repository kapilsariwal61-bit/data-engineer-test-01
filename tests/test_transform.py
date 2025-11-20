# tests/test_transform.py
"""
Tests for the Transform step.

Focus:
- All dimension and fact DataFrames are created
- Surrogate keys exist and are unique
- Fact table keys point to existing dimension keys
"""

import numpy as np

from src.pipeline.extract import run_extract
from src.pipeline.validate import run_validate
from src.pipeline.transform import run_transform


def _run_full_transform(config_path):
    """Helper: extract → validate → transform."""
    listings_raw, reviews_raw = run_extract(config_path=config_path)
    listings_v, reviews_v = run_validate(
        listings_raw.copy(), reviews_raw.copy(), config_path=config_path
    )
    dims, facts = run_transform(listings_v, reviews_v)
    return dims, facts


def test_transform_outputs_exist(config_path):
    """Transform should return all four dims and fact_reviews."""
    dims, facts = _run_full_transform(config_path)

    for key in ["dim_date", "dim_neighborhood", "dim_host", "dim_listing"]:
        assert key in dims, f"{key} not returned from transform"
        assert not dims[key].empty, f"{key} is empty"

    assert "fact_reviews" in facts, "fact_reviews not returned from transform"
    assert not facts["fact_reviews"].empty, "fact_reviews is empty"


def test_transform_surrogate_keys_unique(config_path):
    """Surrogate keys in each dimension should be unique."""
    dims, _ = _run_full_transform(config_path)

    for dim_name, key_col in [
        ("dim_date", "date_sk"),
        ("dim_neighborhood", "neighborhood_sk"),
        ("dim_host", "host_sk"),
        ("dim_listing", "listing_sk"),
    ]:
        dim = dims[dim_name]
        duplicated = dim[key_col].duplicated().sum()
        assert duplicated == 0, f"{dim_name}.{key_col} contains duplicates"


def test_transform_fact_fk_integrity(config_path):
    """Fact foreign keys should reference existing dim keys."""
    dims, facts = _run_full_transform(config_path)

    dim_listing = dims["dim_listing"]
    dim_host = dims["dim_host"]
    dim_date = dims["dim_date"]
    fact_reviews = facts["fact_reviews"]

    listing_keys = set(dim_listing["listing_sk"].unique())
    host_keys = set(dim_host["host_sk"].unique())
    date_keys = set(dim_date["date_sk"].unique())

    assert fact_reviews["listing_sk"].isin(listing_keys).all(), \
        "fact_reviews.listing_sk references non-existing dim_listing keys"

    assert fact_reviews["host_sk"].isin(host_keys).all(), \
        "fact_reviews.host_sk references non-existing dim_host keys"

    assert fact_reviews["date_sk"].isin(date_keys).all(), \
        "fact_reviews.date_sk references non-existing dim_date keys"


def test_transform_basic_business_sanity(config_path):
    """
    Basic sanity checks on transformed values:
    - price is positive for listings used in facts
    - review_count in fact_reviews is always 1
    """
    dims, facts = _run_full_transform(config_path)

    dim_listing = dims["dim_listing"]
    fact_reviews = facts["fact_reviews"]

    # Join fact → listing for price
    merged = fact_reviews.merge(
        dim_listing[["listing_sk", "price"]],
        on="listing_sk",
        how="left",
    )

    assert (merged["price"] > 0).all(), "Found non-positive prices in fact listings"

    # review_count should be exactly 1 for every fact row
    assert (fact_reviews["review_count"] == 1).all(), \
        "fact_reviews.review_count should be 1 for each row"
