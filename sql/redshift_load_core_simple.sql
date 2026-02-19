-- =============================================================================
-- Redshift Load: COPY to Staging + Simple INSERT to Core
-- =============================================================================

-- =============================================================================
-- STEP 1: COPY from S3 Gold to Staging
-- =============================================================================

-- Truncate staging table
TRUNCATE TABLE stg.stg_content_items;

-- COPY articles
COPY stg.stg_content_items (
    id, content_id, content_type, source_system, title, url, summary, image_url, 
    news_site, featured, published_at, updated_at, keywords, entities, topic_name,
    published_year, published_month, published_day, authors_json, launches_json, 
    events_json, ingest_date, ingest_run_id, record_hash, processed_at
)
FROM 's3://monokera-bucket/data/gold/content_enriched_csv/articles/ingest_date={{ ingest_date }}/'
IAM_ROLE '{{ redshift_iam_role }}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
TRUNCATECOLUMNS;

-- COPY blogs
COPY stg.stg_content_items (
    id, content_id, content_type, source_system, title, url, summary, image_url, 
    news_site, featured, published_at, updated_at, keywords, entities, topic_name,
    published_year, published_month, published_day, authors_json, launches_json, 
    events_json, ingest_date, ingest_run_id, record_hash, processed_at
)
FROM 's3://monokera-bucket/data/gold/content_enriched_csv/blogs/ingest_date={{ ingest_date }}/'
IAM_ROLE '{{ redshift_iam_role }}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
TRUNCATECOLUMNS;

-- COPY reports
COPY stg.stg_content_items (
    id, content_id, content_type, source_system, title, url, summary, image_url, 
    news_site, featured, published_at, updated_at, keywords, entities, topic_name,
    published_year, published_month, published_day, authors_json, launches_json, 
    events_json, ingest_date, ingest_run_id, record_hash, processed_at
)
FROM 's3://monokera-bucket/data/gold/content_enriched_csv/reports/ingest_date={{ ingest_date }}/'
IAM_ROLE '{{ redshift_iam_role }}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
TRUNCATECOLUMNS;

-- =============================================================================
-- STEP 2: MERGE to core.content_items with SCD Type 2
-- =============================================================================

BEGIN TRANSACTION;

-- 2A: Expire existing records that have changed
UPDATE core.content_items AS target
SET 
    valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE,
    updated_db_at = CURRENT_TIMESTAMP
FROM stg.stg_content_items AS source
WHERE 
    target.content_id = source.content_id
    AND target.content_type = source.content_type
    AND target.is_current = TRUE
    AND target.record_hash != source.record_hash;

-- 2B: Insert new versions (changed records) and brand new records
INSERT INTO core.content_items (
    content_id,
    content_type,
    title,
    url,
    image_url,
    news_site,
    summary,
    published_at,
    updated_at,
    published_year,
    published_month,
    published_day,
    keywords,
    entities,
    topic_name,
    authors_json,
    launches_json,
    events_json,
    featured,
    source_system,
    record_hash,
    version,
    valid_from,
    valid_to,
    is_current
)
SELECT 
    stg.content_id,
    stg.content_type,
    stg.title,
    stg.url,
    stg.image_url,
    stg.news_site,
    stg.summary,
    TRY_CAST(NULLIF(stg.published_at, '') AS TIMESTAMP) AS published_at,
    TRY_CAST(NULLIF(stg.updated_at, '') AS TIMESTAMP) AS updated_at,
    TRY_CAST(NULLIF(stg.published_year, '') AS INTEGER) AS published_year,
    TRY_CAST(NULLIF(stg.published_month, '') AS INTEGER) AS published_month,
    TRY_CAST(NULLIF(stg.published_day, '') AS INTEGER) AS published_day,
    stg.keywords,
    stg.entities,
    stg.topic_name,
    stg.authors_json,
    stg.launches_json,
    stg.events_json,
    CASE WHEN stg.featured IN ('True', 'true', '1') THEN TRUE ELSE FALSE END AS featured,
    stg.source_system,
    stg.record_hash,
    COALESCE(core.version, 0) + 1 AS version,
    CURRENT_TIMESTAMP AS valid_from,
    '9999-12-31 23:59:59' AS valid_to,
    TRUE AS is_current
FROM stg.stg_content_items AS stg
LEFT JOIN (
    SELECT content_id, content_type, MAX(version) AS version
    FROM core.content_items
    GROUP BY content_id, content_type
) AS core
    ON stg.content_id = core.content_id
    AND stg.content_type = core.content_type
WHERE NOT EXISTS (
    SELECT 1
    FROM core.content_items AS existing
    WHERE existing.content_id = stg.content_id
        AND existing.content_type = stg.content_type
        AND existing.is_current = TRUE
        AND existing.record_hash = stg.record_hash
);

COMMIT;

-- =============================================================================
-- STEP 3: Refresh core.ref_news_sites
-- =============================================================================

-- Insert new news sites
INSERT INTO core.ref_news_sites (
    news_site_name,
    first_seen_date,
    last_seen_date,
    total_content_count
)
SELECT 
    news_site,
    TRY_CAST(NULLIF(MIN(ingest_date), '') AS DATE) AS first_seen_date,
    TRY_CAST(NULLIF(MAX(ingest_date), '') AS DATE) AS last_seen_date,
    COUNT(*) AS total_content_count
FROM stg.stg_content_items
WHERE news_site IS NOT NULL AND news_site != ''
GROUP BY news_site
EXCEPT
SELECT news_site_name, first_seen_date, last_seen_date, total_content_count
FROM core.ref_news_sites;

-- Update existing news sites
UPDATE core.ref_news_sites AS target
SET 
    last_seen_date = source.last_seen_date,
    total_content_count = target.total_content_count + source.new_count,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT 
        news_site,
        TRY_CAST(NULLIF(MAX(ingest_date), '') AS DATE) AS last_seen_date,
        COUNT(*) AS new_count
    FROM stg.stg_content_items
    WHERE news_site IS NOT NULL AND news_site != ''
    GROUP BY news_site
) AS source
WHERE target.news_site_name = source.news_site;

-- =============================================================================
-- STEP 4: Refresh core.ref_topics
-- =============================================================================

-- Truncate and reload all topics (to remove any invalid entries like years)
TRUNCATE TABLE core.ref_topics;

-- Insert all distinct topics
INSERT INTO core.ref_topics (topic_name)
SELECT DISTINCT topic_name
FROM stg.stg_content_items
WHERE topic_name IS NOT NULL AND topic_name != '';

-- =============================================================================
-- Verification Queries
-- =============================================================================

-- Count by content type in core (current vs historical)
SELECT 
    content_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN is_current = FALSE THEN 1 ELSE 0 END) AS historical_records
FROM core.content_items
GROUP BY content_type
ORDER BY content_type;

-- Count news sites and topics
SELECT 
    'news_sites' AS ref_table,
    COUNT(*) AS total_count
FROM core.ref_news_sites
UNION ALL
SELECT 
    'topics' AS ref_table,
    COUNT(*) AS total_count
FROM core.ref_topics
ORDER BY ref_table;

