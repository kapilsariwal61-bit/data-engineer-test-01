# src/pipeline/validate.py
import json
from pathlib import Path
import yaml
import pandas as pd
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _check_columns_present(df: pd.DataFrame, required_cols, check_name, checks):
    missing = [c for c in required_cols if c not in df.columns]
    status = "pass" if not missing else "fail"
    checks.append(
        {
            "name": check_name,
            "status": status,
            "missing_columns": missing,
        }
    )
    if missing:
        logger.warning(f"{check_name} failed. Missing columns: {missing}")


def _check_not_null(df, column, check_name, checks):
    null_count = df[column].isna().sum()
    status = "pass" if null_count == 0 else "fail"
    checks.append(
        {
            "name": check_name,
            "status": status,
            "null_count": int(null_count),
        }
    )
    if null_count > 0:
        logger.warning(f"{check_name} failed. Nulls in {column}: {null_count}")


def _check_unique(df, column, check_name, checks):
    dup_count = df[column].duplicated().sum()
    status = "pass" if dup_count == 0 else "fail"
    checks.append(
        {
            "name": check_name,
            "status": status,
            "duplicate_count": int(dup_count),
        }
    )
    if dup_count > 0:
        logger.warning(f"{check_name} failed. Duplicates in {column}: {dup_count}")


def run_validate(
    listings: pd.DataFrame,
    reviews: pd.DataFrame,
    config_path: str = "src/config/config.yaml",
):
    """
    Validate step:
    - Schema checks
    - Null / uniqueness checks
    - Value range checks
    - FK checks (reviews.listing_id in listings.id)
    - Write data_quality_report.json
    - Return cleaned DataFrames with bad rows removed where possible
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    validation_cfg = config.get("validation", {})
    report_path = Path(config["output"]["data_quality_report"])
    report_path.parent.mkdir(parents=True, exist_ok=True)

    checks = []

    # --- Expected columns ---
    listings_required = [
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
    ]
    reviews_required = ["listing_id", "date"]

    _check_columns_present(
        listings, listings_required, "listings_columns_present", checks
    )
    _check_columns_present(
        reviews, reviews_required, "reviews_columns_present", checks
    )

    # Only keep the expected columns (drop unexpected extras)
    listings = listings[listings_required].copy()
    reviews = reviews[reviews_required].copy()

    # --- Null checks ---
    _check_not_null(listings, "id", "listings_id_not_null", checks)
    _check_not_null(reviews, "listing_id", "reviews_listing_id_not_null", checks)

    # Drop rows with null IDs
    listings = listings[~listings["id"].isna()].copy()
    reviews = reviews[~reviews["listing_id"].isna()].copy()

    # --- Uniqueness checks ---
    _check_unique(listings, "id", "listings_id_unique", checks)

    # Deduplicate listings on id (keep first)
    before = len(listings)
    listings = listings.drop_duplicates(subset=["id"], keep="first")
    after = len(listings)
    if before != after:
        logger.warning(
            f"Deduplicated listings on id. Before={before}, After={after}"
        )

    # --- Value range checks ---
    # Convert some fields to numeric
    listings["availability_365"] = pd.to_numeric(
        listings["availability_365"], errors="coerce"
    )
    listings["latitude"] = pd.to_numeric(listings["latitude"], errors="coerce")
    listings["longitude"] = pd.to_numeric(listings["longitude"], errors="coerce")

    min_price = validation_cfg.get("min_price", 1)
    max_avail = validation_cfg.get("max_availability", 365)
    min_lat = validation_cfg.get("min_latitude", -90)
    max_lat = validation_cfg.get("max_latitude", 90)
    min_lon = validation_cfg.get("min_longitude", -180)
    max_lon = validation_cfg.get("max_longitude", 180)

    # Price will be cleaned later; here just check non-empty
    price_nulls = listings["price"].isna().sum()
    checks.append(
        {
            "name": "price_not_null_raw",
            "status": "pass" if price_nulls == 0 else "fail",
            "null_count": int(price_nulls),
        }
    )

    # Latitude/Longitude range
    invalid_lat = ~listings["latitude"].between(min_lat, max_lat)
    invalid_lon = ~listings["longitude"].between(min_lon, max_lon)

    checks.append(
        {
            "name": "latitude_in_range",
            "status": "pass" if invalid_lat.sum() == 0 else "fail",
            "invalid_count": int(invalid_lat.sum()),
        }
    )
    checks.append(
        {
            "name": "longitude_in_range",
            "status": "pass" if invalid_lon.sum() == 0 else "fail",
            "invalid_count": int(invalid_lon.sum()),
        }
    )

    # Drop rows with invalid lat/lon
    bad_geo = invalid_lat | invalid_lon
    if bad_geo.sum() > 0:
        logger.warning(f"Dropping {int(bad_geo.sum())} listings with bad lat/lon")
        listings = listings[~bad_geo].copy()

    # Availability range
    invalid_avail = ~listings["availability_365"].between(0, max_avail)
    checks.append(
        {
            "name": "availability_in_range",
            "status": "pass" if invalid_avail.sum() == 0 else "fail",
            "invalid_count": int(invalid_avail.sum()),
        }
    )
    if invalid_avail.sum() > 0:
        logger.warning(
            f"Dropping {int(invalid_avail.sum())} listings with invalid availability"
        )
        listings = listings[~invalid_avail].copy()

    # --- FK check: reviews listing_id exists in listings.id ---
    valid_listing_ids = set(listings["id"].unique())
    fk_valid_mask = reviews["listing_id"].isin(valid_listing_ids)
    fk_invalid_count = (~fk_valid_mask).sum()
    checks.append(
        {
            "name": "reviews_listing_id_fk",
            "status": "pass" if fk_invalid_count == 0 else "fail",
            "orphan_reviews": int(fk_invalid_count),
        }
    )
    if fk_invalid_count > 0:
        logger.warning(
            f"Dropping {int(fk_invalid_count)} reviews with unknown listing_id"
        )
        reviews = reviews[fk_valid_mask].copy()

    # --- Summarise report ---
    report = {
        "listings_row_count_after_validation": int(len(listings)),
        "reviews_row_count_after_validation": int(len(reviews)),
        "checks": checks,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Data quality report written to {report_path}")
    return listings, reviews
