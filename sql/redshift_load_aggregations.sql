-- =============================================================================
-- Redshift Load: Aggregations (Trends & Source Activity)
-- =============================================================================
-- Loads monthly trends and source activity from Glue Job 03 outputs
-- Input: S3 CSV files from Job 03
-- Output: core.agg_topic_monthly_trends, core.agg_news_site_monthly_activity

-- =============================================================================
-- STEP 1: Create temporary staging tables
-- =============================================================================

DROP TABLE IF EXISTS stg_topic_trends;
CREATE TEMP TABLE stg_topic_trends (
    year_month          VARCHAR(7),
    topic_name          VARCHAR(100),
    content_type        VARCHAR(20),
    content_count       VARCHAR(20),
    ingest_date         VARCHAR(50),
    calculated_at       VARCHAR(50)
);

DROP TABLE IF EXISTS stg_source_activity;
CREATE TEMP TABLE stg_source_activity (
    year_month          VARCHAR(7),
    news_site           VARCHAR(200),
    content_type        VARCHAR(20),
    content_count       VARCHAR(20),
    ingest_date         VARCHAR(50),
    calculated_at       VARCHAR(50)
);

-- =============================================================================
-- STEP 2: COPY from S3 to staging
-- =============================================================================

-- Load topic trends
COPY stg_topic_trends (
    year_month, topic_name, content_type, content_count, 
    ingest_date, calculated_at
)
FROM 's3://monokera-bucket/data/gold/trends/topic_counts_csv/ingest_date={{ ingest_date }}/'
IAM_ROLE '{{ redshift_iam_role }}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
TRUNCATECOLUMNS;

-- Load source activity
COPY stg_source_activity (
    year_month, news_site, content_type, content_count, 
    ingest_date, calculated_at
)
FROM 's3://monokera-bucket/data/gold/trends/source_activity_csv/ingest_date={{ ingest_date }}/'
IAM_ROLE '{{ redshift_iam_role }}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
TRUNCATECOLUMNS;

-- =============================================================================
-- STEP 3: Insert topic trends to core (UPSERT logic)
-- =============================================================================

BEGIN TRANSACTION;

-- Delete existing records for this ingest_date to avoid duplicates
DELETE FROM core.agg_topic_monthly_trends
WHERE ingest_date = TRY_CAST('{{ ingest_date }}' AS DATE);

-- Insert new trends
INSERT INTO core.agg_topic_monthly_trends (
    year_month,
    topic_name,
    content_type,
    content_count,
    ingest_date,
    calculated_at
)
SELECT 
    year_month,
    topic_name,
    content_type,
    TRY_CAST(content_count AS INTEGER) AS content_count,
    TRY_CAST(ingest_date AS DATE) AS ingest_date,
    TRY_CAST(calculated_at AS TIMESTAMP) AS calculated_at
FROM stg_topic_trends
WHERE year_month IS NOT NULL 
    AND topic_name IS NOT NULL 
    AND content_type IS NOT NULL;

COMMIT;

-- =============================================================================
-- STEP 4: Insert source activity to core (UPSERT logic)
-- =============================================================================

BEGIN TRANSACTION;

-- Delete existing records for this ingest_date to avoid duplicates
DELETE FROM core.agg_news_site_monthly_activity
WHERE ingest_date = TRY_CAST('{{ ingest_date }}' AS DATE);

-- Insert new activity
INSERT INTO core.agg_news_site_monthly_activity (
    year_month,
    news_site,
    content_type,
    content_count,
    ingest_date,
    calculated_at
)
SELECT 
    year_month,
    news_site,
    content_type,
    TRY_CAST(content_count AS INTEGER) AS content_count,
    TRY_CAST(ingest_date AS DATE) AS ingest_date,
    TRY_CAST(calculated_at AS TIMESTAMP) AS calculated_at
FROM stg_source_activity
WHERE year_month IS NOT NULL 
    AND news_site IS NOT NULL 
    AND content_type IS NOT NULL;

COMMIT;

-- =============================================================================
-- Verification Queries
-- =============================================================================

-- Check topic trends counts by ingest_date
SELECT 
    'Topic Trends' AS table_name,
    ingest_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT year_month) AS distinct_months,
    COUNT(DISTINCT topic_name) AS distinct_topics,
    COUNT(DISTINCT content_type) AS distinct_content_types
FROM core.agg_topic_monthly_trends
WHERE ingest_date = TRY_CAST('{{ ingest_date }}' AS DATE)
GROUP BY ingest_date

UNION ALL

-- Check source activity counts by ingest_date
SELECT 
    'Source Activity' AS table_name,
    ingest_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT year_month) AS distinct_months,
    COUNT(DISTINCT news_site) AS distinct_sources,
    COUNT(DISTINCT content_type) AS distinct_content_types
FROM core.agg_news_site_monthly_activity
WHERE ingest_date = TRY_CAST('{{ ingest_date }}' AS DATE)
GROUP BY ingest_date;
