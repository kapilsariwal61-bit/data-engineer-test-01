# src/pipeline/transform.py
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def clean_price(series: pd.Series) -> pd.Series:
    """Remove currency symbols/commas and convert to float."""
    return (
        series.astype(str)
        .str.replace(r"[^0-9.]", "", regex=True)
        .replace("", "0")
        .astype(float)
    )


def classify_price_tier(price: pd.Series) -> pd.Series:
    """Simple price banding: budget / mid / premium."""
    return pd.cut(
        price,
        bins=[-1, 50, 150, float("inf")],
        labels=["budget", "mid", "premium"],
    )


def run_transform(
    listings: pd.DataFrame,
    reviews: pd.DataFrame,
):
    """
    Transform step:
    - Clean fields
    - Feature engineering
    - Build star-schema tables as DataFrames:
      dim_date, dim_neighborhood, dim_host (SCD2-ready), dim_listing, fact_reviews
    """
    logger.info("Starting transform step")

    listings = listings.copy()
    reviews = reviews.copy()

    # --- Basic renaming ---
    listings.rename(columns={"id": "listing_id"}, inplace=True)

    # --- Clean prices & numeric fields ---
    listings["price"] = clean_price(listings["price"])
    listings["minimum_nights"] = pd.to_numeric(
        listings["minimum_nights"], errors="coerce"
    ).fillna(0)
    listings["number_of_reviews"] = pd.to_numeric(
        listings["number_of_reviews"], errors="coerce"
    ).fillna(0)
    listings["reviews_per_month"] = pd.to_numeric(
        listings["reviews_per_month"], errors="coerce"
    ).fillna(0)
    listings["number_of_reviews_ltm"] = pd.to_numeric(
        listings["number_of_reviews_ltm"], errors="coerce"
    ).fillna(0)

    # --- Dates ---
    reviews["date"] = pd.to_datetime(reviews["date"], errors="coerce")

    # --- Feature engineering ---
    # Simple occupancy proxy using reviews_per_month (totally configurable)
    listings["estimated_occupancy_rate"] = (
        listings["reviews_per_month"] / 5
    ).clip(lower=0, upper=1)

    listings["estimated_monthly_revenue"] = (
        listings["price"] * listings["estimated_occupancy_rate"] * 30
    )

    listings["price_tier"] = classify_price_tier(listings["price"])

    # --- dim_date ---
    dim_date = (
        reviews[["date"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"date": "full_date"})
        .sort_values("full_date")
        .reset_index(drop=True)
    )
    dim_date["date_sk"] = dim_date.index + 1
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["day_of_week"] = dim_date["full_date"].dt.weekday + 1  # 1=Mon
    dim_date["day_name"] = dim_date["full_date"].dt.day_name()

    # --- dim_neighborhood ---
    dim_neighborhood = (
        listings[["neighbourhood_group", "neighbourhood"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_neighborhood["neighborhood_sk"] = dim_neighborhood.index + 1

    # --- dim_host (SCD2-ready structure, single version for now) ---
    dim_host = (
        listings[
            ["host_id", "host_name", "calculated_host_listings_count"]
        ]
        .drop_duplicates(subset=["host_id"])
        .reset_index(drop=True)
    )
    dim_host["host_sk"] = dim_host.index + 1
    run_date = pd.Timestamp.today().normalize()
    dim_host["valid_from"] = run_date
    dim_host["valid_to"] = pd.NaT
    dim_host["is_current"] = True

    # --- dim_listing ---
    dim_listing = listings.merge(
        dim_neighborhood,
        on=["neighbourhood_group", "neighbourhood"],
        how="left",
    )

    dim_listing = dim_listing[
        [
            "listing_id",
            "name",
            "host_id",
            "neighborhood_sk",
            "room_type",
            "price",
            "minimum_nights",
            "availability_365",
            "number_of_reviews",
            "reviews_per_month",
            "number_of_reviews_ltm",
            "license",
            "latitude",
            "longitude",
            "estimated_occupancy_rate",
            "estimated_monthly_revenue",
            "price_tier",
        ]
    ].drop_duplicates(subset=["listing_id"]).reset_index(drop=True)

    dim_listing["listing_sk"] = dim_listing.index + 1

    # --- fact_reviews ---
    fact_reviews = reviews.merge(
        dim_listing[["listing_id", "listing_sk", "host_id"]],
        on="listing_id",
        how="left",
    )

    fact_reviews = fact_reviews.merge(
        dim_host[["host_id", "host_sk"]],
        on="host_id",
        how="left",
    )

    fact_reviews = fact_reviews.merge(
        dim_date[["date_sk", "full_date"]],
        left_on="date",
        right_on="full_date",
        how="left",
    )

    fact_reviews["review_count"] = 1

    fact_reviews = fact_reviews[
        ["listing_sk", "host_sk", "date_sk", "review_count"]
    ].dropna(subset=["listing_sk", "host_sk", "date_sk"])

    # --- Build dicts to pass to load ---
    dims = {
        "dim_date": dim_date[
            [
                "date_sk",
                "full_date",
                "year",
                "month",
                "day",
                "month_name",
                "day_of_week",
                "day_name",
            ]
        ],
        "dim_neighborhood": dim_neighborhood[
            ["neighborhood_sk", "neighbourhood_group", "neighbourhood"]
        ],
        "dim_host": dim_host[
            [
                "host_sk",
                "host_id",
                "host_name",
                "calculated_host_listings_count",
                "valid_from",
                "valid_to",
                "is_current",
            ]
        ],
        "dim_listing": dim_listing[
            [
                "listing_sk",
                "listing_id",
                "name",
                "host_id",
                "neighborhood_sk",
                "room_type",
                "price",
                "minimum_nights",
                "availability_365",
                "number_of_reviews",
                "reviews_per_month",
                "number_of_reviews_ltm",
                "license",
                "latitude",
                "longitude",
                # "estimated_occupancy_rate",
                # "estimated_monthly_revenue",
                # "price_tier",
            ]
        ],
    }

    facts = {
        "fact_reviews": fact_reviews
    }

    logger.info(
        f"Transform complete. dim_listing={len(dim_listing)}, fact_reviews={len(fact_reviews)}"
    )
    return dims, facts
