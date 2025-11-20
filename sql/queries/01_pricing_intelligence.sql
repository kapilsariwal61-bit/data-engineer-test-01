/*
  01_pricing_intelligence.sql

  Purpose:
    Identify listings that are significantly over/under-priced relative to
    similar listings in the same neighbourhood_group + neighbourhood + room_type.

  Output:
    listing_id, name, neighbourhood_group, neighbourhood, room_type,
    current_price, market_average, price_difference_pct, recommendation
*/

WITH base AS (
    SELECT
        dl.listing_id,
        dl.name,
        dn.neighbourhood_group,
        dn.neighbourhood,
        dl.room_type,
        dl.price::numeric AS current_price
    FROM dim_listing dl
    JOIN dim_neighborhood dn
      ON dl.neighborhood_sk = dn.neighborhood_sk
    WHERE dl.price IS NOT NULL
),

peer_stats AS (
    SELECT
        neighbourhood_group,
        neighbourhood,
        room_type,
        AVG(current_price) AS market_average
    FROM base
    GROUP BY
        neighbourhood_group,
        neighbourhood,
        room_type
),

pricing AS (
    SELECT
        b.listing_id,
        b.name,
        b.neighbourhood_group,
        b.neighbourhood,
        b.room_type,
        b.current_price,
        ps.market_average,
        CASE
            WHEN ps.market_average IS NULL OR ps.market_average = 0
                THEN NULL
            ELSE ROUND(
                    (b.current_price - ps.market_average)
                    / ps.market_average * 100.0,
                    2
                 )
        END AS price_difference_pct
    FROM base b
    JOIN peer_stats ps
      ON b.neighbourhood_group = ps.neighbourhood_group
     AND b.neighbourhood       = ps.neighbourhood
     AND b.room_type           = ps.room_type
)

SELECT
    listing_id,
    name,
    neighbourhood_group,
    neighbourhood,
    room_type,
    current_price,
    ROUND(market_average, 2)      AS market_average,
    price_difference_pct,
    CASE
        WHEN price_difference_pct IS NULL
            THEN 'unknown'
        WHEN price_difference_pct <= -20
            THEN 'underpriced'
        WHEN price_difference_pct >=  20
            THEN 'overpriced'
        ELSE 'fair'
    END AS recommendation
FROM pricing
ORDER BY
    ABS(COALESCE(price_difference_pct, 0)) DESC,
    neighbourhood_group,
    neighbourhood;
