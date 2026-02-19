CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;

-- =========================
-- STG
-- =========================

DROP TABLE IF EXISTS stg.stg_content_items CASCADE;
CREATE TABLE stg.stg_content_items (
    id                  VARCHAR(50),
    content_id          VARCHAR(50),
    content_type        VARCHAR(20),
    title               VARCHAR(5000),
    url                 VARCHAR(5000),
    image_url           VARCHAR(5000),
    news_site           VARCHAR(500),
    summary             VARCHAR(20000),
    published_at        VARCHAR(100),
    updated_at          VARCHAR(100),
    processed_at        VARCHAR(100),
    published_year      VARCHAR(10),
    published_month     VARCHAR(10),
    published_day       VARCHAR(10),
    keywords            VARCHAR(10000),
    entities            VARCHAR(10000),
    topic_name          VARCHAR(200),
    authors_json        VARCHAR(20000),
    launches_json       VARCHAR(20000),
    events_json         VARCHAR(20000),
    featured            VARCHAR(10),
    source_system       VARCHAR(100),
    ingest_date         VARCHAR(50),
    ingest_run_id       VARCHAR(200),
    record_hash         VARCHAR(100)
)
DISTSTYLE AUTO
SORTKEY (ingest_date, content_type);

-- =========================
-- CORE
-- =========================

DROP TABLE IF EXISTS core.content_items CASCADE;
CREATE TABLE core.content_items (
    content_pk          BIGINT IDENTITY(1,1) PRIMARY KEY,
    content_id          VARCHAR(50) NOT NULL,
    content_type        VARCHAR(20) NOT NULL,
    title               VARCHAR(1000) NOT NULL,
    url                 VARCHAR(2000) NOT NULL,
    image_url           VARCHAR(2000),
    news_site           VARCHAR(200),
    summary             VARCHAR(10000),
    published_at        TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP,
    published_year      INTEGER,
    published_month     INTEGER,
    published_day       INTEGER,
    keywords            VARCHAR(5000),
    entities            VARCHAR(2000),
    topic_name          VARCHAR(100),
    authors_json        VARCHAR(10000),
    launches_json       VARCHAR(10000),
    events_json         VARCHAR(10000),
    featured            BOOLEAN DEFAULT FALSE,
    source_system       VARCHAR(50),
    record_hash         VARCHAR(64),
    version             INTEGER NOT NULL DEFAULT 1,
    valid_from          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to            TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_db_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (content_type, published_at, is_current);

ALTER TABLE core.content_items
ADD CONSTRAINT uq_content_items_version
UNIQUE (content_id, content_type, version);

DROP TABLE IF EXISTS core.ref_news_sites CASCADE;
CREATE TABLE core.ref_news_sites (
    news_site_id        INTEGER IDENTITY(1,1) PRIMARY KEY,
    news_site_name      VARCHAR(200) NOT NULL,
    website_url         VARCHAR(2000),
    description         VARCHAR(1000),
    is_active           BOOLEAN DEFAULT TRUE,
    first_seen_date     DATE,
    last_seen_date      DATE,
    total_content_count INTEGER DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL;

ALTER TABLE core.ref_news_sites
ADD CONSTRAINT uq_ref_news_sites_name
UNIQUE (news_site_name);

DROP TABLE IF EXISTS core.ref_topics CASCADE;
CREATE TABLE core.ref_topics (
    topic_id            INTEGER IDENTITY(1,1) PRIMARY KEY,
    topic_name          VARCHAR(100) NOT NULL,
    description         VARCHAR(1000),
    parent_topic_id     INTEGER,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL;

ALTER TABLE core.ref_topics
ADD CONSTRAINT uq_ref_topics_name
UNIQUE (topic_name);

-- ============================================================================
-- ELIMINATED: Relationship tables (rel_*) - not used in pipeline
-- - rel_content_authors
-- - rel_content_launches  
-- - rel_content_events
-- Reason: JSON arrays (authors_json, launches_json, events_json) are mostly empty
-- ============================================================================

DROP TABLE IF EXISTS core.agg_topic_monthly_trends CASCADE;
CREATE TABLE core.agg_topic_monthly_trends (
    trend_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month          VARCHAR(7) NOT NULL,
    topic_name          VARCHAR(100) NOT NULL,
    content_type        VARCHAR(20) NOT NULL,
    content_count       INTEGER NOT NULL,
    ingest_date         DATE NOT NULL,
    calculated_at       TIMESTAMP NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (year_month, topic_name, content_type, ingest_date);

ALTER TABLE core.agg_topic_monthly_trends
ADD CONSTRAINT uq_topic_trends
UNIQUE (year_month, topic_name, content_type, ingest_date);

DROP TABLE IF EXISTS core.agg_news_site_monthly_activity CASCADE;
CREATE TABLE core.agg_news_site_monthly_activity (
    activity_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month          VARCHAR(7) NOT NULL,
    news_site           VARCHAR(200) NOT NULL,
    content_type        VARCHAR(20) NOT NULL,
    content_count       INTEGER NOT NULL,
    ingest_date         DATE NOT NULL,
    calculated_at       TIMESTAMP NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (year_month, news_site, content_type, ingest_date);

ALTER TABLE core.agg_news_site_monthly_activity
ADD CONSTRAINT uq_source_activity
UNIQUE (year_month, news_site, content_type, ingest_date);

DROP TABLE IF EXISTS core.meta_pipeline_runs CASCADE;
CREATE TABLE core.meta_pipeline_runs (
    run_id              VARCHAR(100) PRIMARY KEY,
    run_type            VARCHAR(50),
    run_status          VARCHAR(20),
    ingest_date         DATE,
    records_processed   INTEGER,
    records_failed      INTEGER,
    error_message       VARCHAR(10000),
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,
    duration_seconds    INTEGER,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (started_at);

-- ============================================================================
-- ELIMINATED: meta_api_info_runs (complex file parsing not needed)
-- ============================================================================

DROP TABLE IF EXISTS core.meta_data_quality_checks CASCADE;
CREATE TABLE core.meta_data_quality_checks (
    check_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    check_name          VARCHAR(200) NOT NULL,
    check_type          VARCHAR(50),
    target_table        VARCHAR(200),
    check_result        VARCHAR(20),
    check_value         NUMERIC(18,2),
    threshold_value     NUMERIC(18,2),
    error_message       VARCHAR(5000),
    ingest_date         DATE,
    checked_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (checked_at);

SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname IN ('stg', 'core')
ORDER BY schemaname, tablename;
