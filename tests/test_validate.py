# tests/test_validate.py
"""
Tests for the Validate step.

Focus:
- Critical IDs are non-null after validation
- No orphan reviews (listing_id must exist in listings)
- Latitude/longitude and availability ranges are respected
- Data quality report is generated and contains check results
"""

import json
from pathlib import Path

from src.pipeline.extract import run_extract
from src.pipeline.validate import run_validate


def test_validate_basic_quality(config_path):
    """After validation, IDs should be non-null and row counts reduced or equal."""
    listings_raw, reviews_raw = run_extract(config_path=config_path)
    listings_v, reviews_v = run_validate(
        listings_raw.copy(), reviews_raw.copy(), config_path=config_path
    )

    # IDs must be non-null
    assert listings_v["id"].isna().sum() == 0, "Null ids still present in listings"
    assert reviews_v["listing_id"].isna().sum() == 0, "Null listing_id still present in reviews"

    # Should not increase row counts
    assert len(listings_v) <= len(listings_raw)
    assert len(reviews_v) <= len(reviews_raw)


def test_validate_no_orphan_reviews(config_path):
    """Every review.listing_id must exist in listings.id after validation."""
    listings_raw, reviews_raw = run_extract(config_path=config_path)
    listings_v, reviews_v = run_validate(
        listings_raw.copy(), reviews_raw.copy(), config_path=config_path
    )

    valid_listing_ids = set(listings_v["id"].unique())
    invalid_mask = ~reviews_v["listing_id"].isin(valid_listing_ids)
    invalid_count = int(invalid_mask.sum())

    assert invalid_count == 0, f"Found {invalid_count} orphan reviews after validation"


def test_validate_ranges_and_report(config_path, project_root):
    """
    Validate should:
    - drop rows with invalid latitude/longitude or availability_365
    - create a JSON data quality report with checks
    """
    _, _ = run_validate(*run_extract(config_path=config_path), config_path=config_path)

    # Read config to find report path
    from yaml import safe_load

    with open(config_path) as f:
        cfg = safe_load(f)
    report_path = Path(cfg["output"]["data_quality_report"])

    assert report_path.exists(), "data_quality_report.json was not created"

    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert "checks" in report, "Report must contain a 'checks' list"
    assert isinstance(report["checks"], list)
    assert len(report["checks"]) > 0, "Expected at least one validation check recorded"

    # Make sure some range-related checks are present
    check_names = {c["name"] for c in report["checks"]}
    expected_some = {
        "latitude_in_range",
        "longitude_in_range",
        "availability_in_range",
    }
    assert expected_some & check_names, (
        "Expected latitude/longitude/availability range checks in report, "
        f"found only: {check_names}"
    )
