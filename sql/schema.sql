-- 1) Date Dimension
CREATE TABLE dim_date (
    date_sk      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    year         INT  NOT NULL,
    month        INT  NOT NULL,
    day          INT  NOT NULL,
    month_name   TEXT,
    day_of_week  INT,
    day_name     TEXT
);

-- 2) Neighborhood Dimension
CREATE TABLE dim_neighborhood (
    neighborhood_sk     SERIAL PRIMARY KEY,
    neighbourhood_group TEXT,
    neighbourhood       TEXT,
    UNIQUE (neighbourhood_group, neighbourhood)
);

-- 3) Host Dimension (SCD Type 2)
CREATE TABLE dim_host (
    host_sk                         SERIAL PRIMARY KEY,
    host_id                         BIGINT NOT NULL,  -- natural key from source
    host_name                       TEXT,
    calculated_host_listings_count  INT,
    valid_from                      DATE NOT NULL,
    valid_to                        DATE,
    is_current                      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_host_host_id ON dim_host(host_id);

-- 4) Listing Dimension
CREATE TABLE dim_listing (
    listing_sk                 SERIAL PRIMARY KEY,
    listing_id                 BIGINT NOT NULL,  -- source 'id'
    name                       TEXT,
    host_id                    BIGINT NOT NULL,
    neighborhood_sk            INT REFERENCES dim_neighborhood(neighborhood_sk),
    room_type                  TEXT,
    price                      NUMERIC,
    minimum_nights             INT,
    availability_365           INT,
    number_of_reviews          INT,
    reviews_per_month          NUMERIC,
    number_of_reviews_ltm      INT,
    license                    TEXT,
    latitude                   NUMERIC,
    longitude                  NUMERIC
);

CREATE INDEX idx_dim_listing_listing_id ON dim_listing(listing_id);
CREATE INDEX idx_dim_listing_host_id ON dim_listing(host_id);

-- 5) Reviews Fact Table
CREATE TABLE fact_reviews (
    review_sk    SERIAL PRIMARY KEY,
    listing_sk   INT NOT NULL REFERENCES dim_listing(listing_sk),
    host_sk      INT NOT NULL REFERENCES dim_host(host_sk),
    date_sk      INT NOT NULL REFERENCES dim_date(date_sk),
    review_count INT NOT NULL DEFAULT 1
);

CREATE INDEX idx_fact_reviews_listing_sk ON fact_reviews(listing_sk);
CREATE INDEX idx_fact_reviews_host_sk    ON fact_reviews(host_sk);
CREATE INDEX idx_fact_reviews_date_sk    ON fact_reviews(date_sk);
