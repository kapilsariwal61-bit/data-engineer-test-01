/*
  03_market_opportunities.sql

  Purpose:
    Identify neighbourhoods with high demand but limited supply.

  Definitions:
    Demand:
      - reviews_last_12m = number of reviews in the last 365 days.
    Supply:
      - active_listings = count of listings with availability_365 > 0.
    Opportunity:
      - demand_per_listing = reviews_last_12m / active_listings
      - opportunity_score = demand_per_listing scaled for readability.

  Output columns:
    neighbourhood_group
    neighbourhood
    demand_score          -- reviews_last_12m
    supply_score          -- active_listings
    opportunity_score
    recommended_action
*/

WITH base_listings AS (
    SELECT
        dl.listing_sk,
        dl.listing_id,
        dl.availability_365,
        dl.price,
        dl.room_type,
        dn.neighbourhood_group,
        dn.neighbourhood
    FROM dim_listing dl
    JOIN dim_neighborhood dn
      ON dl.neighborhood_sk = dn.neighborhood_sk
),

demand AS (
    -- Count reviews per neighbourhood in last 12 months
    SELECT
        b.neighbourhood_group,
        b.neighbourhood,
        COUNT(*) AS reviews_last_12m
    FROM fact_reviews fr
    JOIN dim_date dd
      ON fr.date_sk = dd.date_sk
    JOIN base_listings b
      ON fr.listing_sk = b.listing_sk
    WHERE dd.full_date >= (current_date - INTERVAL '365 days')
    GROUP BY
        b.neighbourhood_group,
        b.neighbourhood
),

supply AS (
    -- Count active listings and compute some price/availability context
    SELECT
        neighbourhood_group,
        neighbourhood,
        COUNT(*) AS active_listings,
        AVG(price) AS avg_price,
        AVG(availability_365) AS avg_availability_365
    FROM base_listings
    WHERE availability_365 > 0
    GROUP BY
        neighbourhood_group,
        neighbourhood
),

combined AS (
    SELECT
        s.neighbourhood_group,
        s.neighbourhood,
        COALESCE(d.reviews_last_12m, 0) AS reviews_last_12m,
        s.active_listings,
        s.avg_price,
        s.avg_availability_365
    FROM supply s
    LEFT JOIN demand d
      ON s.neighbourhood_group = d.neighbourhood_group
     AND s.neighbourhood       = d.neighbourhood
),

scored AS (
    SELECT
        neighbourhood_group,
        neighbourhood,
        reviews_last_12m,
        active_listings,
        avg_price,
        avg_availability_365,
        CASE
            WHEN active_listings = 0
                THEN NULL
            ELSE reviews_last_12m::numeric / active_listings
        END AS demand_per_listing
    FROM combined
),

ranked AS (
    SELECT
        neighbourhood_group,
        neighbourhood,
        reviews_last_12m,
        active_listings,
        avg_price,
        avg_availability_365,
        demand_per_listing,
        -- Simple scaling: multiply by 10 so numbers are more readable.
        CASE
            WHEN demand_per_listing IS NULL
                THEN 0
            ELSE ROUND(demand_per_listing * 10, 2)
        END AS opportunity_score
    FROM scored
)

SELECT
    neighbourhood_group,
    neighbourhood,
    reviews_last_12m AS demand_score,
    active_listings  AS supply_score,
    opportunity_score,
    CASE
        WHEN opportunity_score >= 50 AND active_listings < 50
            THEN 'increase_supply'
        WHEN opportunity_score >= 50 AND active_listings >= 50
            THEN 'high_demand_competitive'
        WHEN opportunity_score BETWEEN 20 AND 50
            THEN 'moderate_opportunity'
        ELSE 'low_demand'
    END AS recommended_action
FROM ranked
ORDER BY
    opportunity_score DESC,
    reviews_last_12m DESC;
