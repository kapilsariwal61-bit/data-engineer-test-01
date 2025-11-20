/*
  02_host_performance.sql

  Purpose:
    Rank hosts using a composite performance score based on:
      - total estimated monthly revenue
      - review activity (last 12 months of data)
      - portfolio size (# of listings)

  Assumptions:
    estimated_monthly_revenue_per_listing ≈ price * reviews_per_month * 30

  Output:
    host_id, host_name, performance_score, ranking, key_metrics_breakdown (JSON)
*/

-- Use last 12 months relative to the max date we have in dim_date
WITH max_date AS (
    SELECT MAX(full_date) AS max_full_date
    FROM dim_date
),

host_listings AS (
    SELECT
        dh.host_id,
        dh.host_name,
        COUNT(DISTINCT dl.listing_id) AS listing_count,
        COALESCE(
            SUM(
                (dl.price::numeric)
                * COALESCE(dl.reviews_per_month, 0)::numeric
                * 30
            ),
            0
        ) AS total_estimated_monthly_revenue
    FROM dim_listing dl
    JOIN dim_host dh
      ON dl.host_id = dh.host_id
     AND dh.is_current = TRUE
    GROUP BY
        dh.host_id,
        dh.host_name
),

host_reviews_last_12m AS (
    SELECT
        dh.host_id,
        COUNT(*) AS reviews_last_12m
    FROM fact_reviews fr
    JOIN dim_date dd
      ON fr.date_sk = dd.date_sk
    JOIN dim_host dh
      ON fr.host_sk = dh.host_sk
     AND dh.is_current = TRUE
    CROSS JOIN max_date md
    WHERE dd.full_date >= md.max_full_date - INTERVAL '365 days'
    GROUP BY
        dh.host_id
),

combined AS (
    SELECT
        hl.host_id,
        hl.host_name,
        hl.listing_count,
        hl.total_estimated_monthly_revenue,
        COALESCE(hr.reviews_last_12m, 0) AS reviews_last_12m
    FROM host_listings hl
    LEFT JOIN host_reviews_last_12m hr
      ON hl.host_id = hr.host_id
),

normalized AS (
    SELECT
        c.*,
        MAX(total_estimated_monthly_revenue) OVER () AS max_revenue,
        MAX(reviews_last_12m) OVER ()               AS max_reviews,
        MAX(listing_count) OVER ()                  AS max_listings
    FROM combined c
),

scored AS (
    SELECT
        host_id,
        host_name,
        listing_count,
        total_estimated_monthly_revenue,
        reviews_last_12m,
        CASE
            WHEN max_revenue > 0
                THEN total_estimated_monthly_revenue / max_revenue
            ELSE 0
        END AS revenue_score,
        CASE
            WHEN max_reviews > 0
                THEN reviews_last_12m::numeric / max_reviews
            ELSE 0
        END AS review_activity_score,
        CASE
            WHEN max_listings > 0
                THEN listing_count::numeric / max_listings
            ELSE 0
        END AS portfolio_score
    FROM normalized
),

final AS (
    SELECT
        host_id,
        host_name,
        listing_count,
        total_estimated_monthly_revenue,
        reviews_last_12m,
        ROUND(
            0.5 * revenue_score +
            0.3 * review_activity_score +
            0.2 * portfolio_score,
            3
        ) AS performance_score,
        jsonb_build_object(
            'revenue_score',           ROUND(revenue_score, 3),
            'review_activity_score',   ROUND(review_activity_score, 3),
            'portfolio_score',         ROUND(portfolio_score, 3),
            'listing_count',           listing_count,
            'reviews_last_12m',        reviews_last_12m,
            'total_estimated_monthly_revenue',
                                      ROUND(total_estimated_monthly_revenue, 2)
        ) AS key_metrics_breakdown
    FROM scored
)

SELECT
    host_id,
    host_name,
    performance_score,
    RANK() OVER (ORDER BY performance_score DESC) AS ranking,
    key_metrics_breakdown
FROM final
ORDER BY
    ranking,
    host_id;
    