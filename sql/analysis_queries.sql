-- =============================================================================
-- SpaceNews Data Pipeline - SQL Analysis Queries
-- Comprehensive query collection for data exploration and visualization
-- =============================================================================
-- Author: Fernán Marsiglia
-- Date: Febrero 2026
-- Redshift Serverless: monokera-pipeline
-- =============================================================================

-- ÍNDICE DE QUERIES:
-- 1. CONTENT VOLUME & DISTRIBUTION (KPIs básicos)
-- 2. NEWS SOURCES ANALYSIS (Análisis de fuentes)
-- 3. TOPIC TRENDS (Análisis de tópicos)
-- 4. TEMPORAL ANALYSIS (Tendencias temporales)
-- 5. ENTITY & KEYWORD ANALYSIS (Análisis de entidades y keywords)
-- 6. DATA QUALITY & FRESHNESS (Calidad de datos)
-- 7. AGGREGATIONS (Uso de tablas agregadas)
-- 8. DASHBOARD QUERIES (Queries para visualización)
-- 9. MONITORING & ALERTS (Monitoreo del pipeline)
-- 10. ADVANCED ANALYTICS (Análisis avanzado)

-- =============================================================================
-- 1. CONTENT VOLUME & DISTRIBUTION
-- =============================================================================

-- 1.1: Total content by type (KPI principal)
-- Uso: Dashboard principal - Total de contenido por tipo
SELECT 
    content_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT content_id) AS unique_content,
    MIN(published_at) AS earliest,
    MAX(published_at) AS latest,
    DATEDIFF(day, MIN(published_at), MAX(published_at)) AS days_span,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type
ORDER BY total_records DESC;


-- 1.2: Daily content volume (last 30 days)
-- Uso: Gráfico de líneas - Volumen diario de content
SELECT 
    DATE_TRUNC('day', published_at) AS publish_date,
    content_type,
    COUNT(*) AS content_count
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY publish_date, content_type
ORDER BY publish_date DESC, content_type;


-- 1.3: Content distribution by topic
-- Uso: Pie chart - Distribución de contenido por tópico
SELECT 
    topic_name,
    content_type,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')  -- Exclude years
GROUP BY topic_name, content_type
ORDER BY content_count DESC;


-- 1.4: Content summary statistics
-- Uso: Tarjetas KPI - Estadísticas de longitud de contenido
SELECT 
    content_type,
    COUNT(*) AS total_content,
    ROUND(AVG(LENGTH(title)), 0) AS avg_title_length,
    ROUND(AVG(LENGTH(summary)), 0) AS avg_summary_length,
    MIN(LENGTH(title)) AS min_title_length,
    MAX(LENGTH(title)) AS max_title_length,
    MIN(LENGTH(summary)) AS min_summary_length,
    MAX(LENGTH(summary)) AS max_summary_length
FROM core.content_items
WHERE 
    is_current = TRUE
    AND title IS NOT NULL
    AND summary IS NOT NULL
GROUP BY content_type
ORDER BY total_content DESC;


-- 1.5: Featured content analysis
-- Uso: KPI - Contenido destacado vs normal
SELECT 
    content_type,
    featured,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY content_type), 2) AS percentage_within_type
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type, featured
ORDER BY content_type, featured DESC;


-- 1.6: Content with images vs without
-- Uso: Bar chart - Contenido con/sin imágenes
SELECT 
    content_type,
    CASE 
        WHEN image_url IS NOT NULL THEN 'With Image'
        ELSE 'No Image'
    END AS has_image,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY content_type), 2) AS percentage
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type, has_image
ORDER BY content_type, has_image;


-- =============================================================================
-- 2. NEWS SOURCES ANALYSIS
-- =============================================================================

-- 2.1: Top 20 most prolific news sources
-- Uso: Bar chart - Fuentes más activas
SELECT 
    news_site,
    COUNT(*) AS article_count,
    COUNT(DISTINCT topic_name) AS topics_covered,
    MIN(published_at) AS first_article,
    MAX(published_at) AS latest_article,
    DATEDIFF(day, MIN(published_at), MAX(published_at)) AS active_days,
    ROUND(AVG(LENGTH(summary)), 0) AS avg_summary_length
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
GROUP BY news_site
ORDER BY article_count DESC
LIMIT 20;


-- 2.2: News source activity by month (last 12 months)
-- Uso: Heatmap - Actividad mensual de fuentes
SELECT 
    TO_CHAR(published_at, 'YYYY-MM') AS year_month,
    news_site,
    COUNT(*) AS content_count
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
    AND published_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY year_month, news_site
ORDER BY year_month DESC, content_count DESC;


-- 2.3: Sources by topic specialization
-- Uso: Matrix - Especialización de fuentes por tópico
SELECT 
    news_site,
    topic_name,
    COUNT(*) AS article_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY news_site), 2) AS pct_of_source_output
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY news_site, topic_name
HAVING COUNT(*) >= 5
ORDER BY news_site, article_count DESC;


-- 2.4: Source diversity index (how many topics each source covers)
-- Uso: Scatter plot - Diversidad vs volumen
SELECT 
    news_site,
    COUNT(DISTINCT topic_name) AS topic_diversity,
    COUNT(*) AS total_articles,
    ROUND(COUNT(DISTINCT topic_name) * 1.0 / NULLIF(COUNT(*), 0), 3) AS diversity_ratio
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY news_site
HAVING COUNT(*) >= 10
ORDER BY topic_diversity DESC, total_articles DESC;


-- 2.5: Source publishing frequency (articles per day)
-- Uso: Table - Frecuencia de publicación
SELECT 
    news_site,
    COUNT(*) AS total_articles,
    DATEDIFF(day, MIN(published_at), MAX(published_at)) + 1 AS active_days,
    ROUND(COUNT(*) * 1.0 / NULLIF(DATEDIFF(day, MIN(published_at), MAX(published_at)) + 1, 0), 2) AS articles_per_day,
    MIN(published_at) AS first_article,
    MAX(published_at) AS latest_article
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
GROUP BY news_site
HAVING COUNT(*) >= 50
ORDER BY articles_per_day DESC;


-- =============================================================================
-- 3. TOPIC TRENDS
-- =============================================================================

-- 3.1: Top topics overall
-- Uso: Bar chart - Tópicos más populares
SELECT 
    topic_name,
    COUNT(*) AS content_count,
    COUNT(DISTINCT news_site) AS sources_covering,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    MIN(published_at) AS first_mention,
    MAX(published_at) AS latest_mention
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY topic_name
ORDER BY content_count DESC;


-- 3.2: Topics by content type matrix
-- Uso: Heatmap - Tópicos x Tipo de contenido
SELECT 
    topic_name,
    content_type,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*) OVER (PARTITION BY topic_name), 2) AS pct_within_topic
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY topic_name, content_type
ORDER BY topic_name, content_count DESC;


-- 3.3: Trending topics (last 7 days vs previous 7 days)
-- Uso: Table con delta - Tópicos en tendencia
WITH last_week AS (
    SELECT 
        topic_name,
        COUNT(*) AS count_last_7d
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND topic_name IS NOT NULL
        AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
        AND published_at >= CURRENT_DATE - INTERVAL '7 days'
        AND published_at < CURRENT_DATE
    GROUP BY topic_name
),
prev_week AS (
    SELECT 
        topic_name,
        COUNT(*) AS count_prev_7d
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND topic_name IS NOT NULL
        AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
        AND published_at >= CURRENT_DATE - INTERVAL '14 days'
        AND published_at < CURRENT_DATE - INTERVAL '7 days'
    GROUP BY topic_name
)
SELECT 
    COALESCE(lw.topic_name, pw.topic_name) AS topic_name,
    COALESCE(lw.count_last_7d, 0) AS last_7_days,
    COALESCE(pw.count_prev_7d, 0) AS previous_7_days,
    COALESCE(lw.count_last_7d, 0) - COALESCE(pw.count_prev_7d, 0) AS absolute_change,
    CASE 
        WHEN pw.count_prev_7d = 0 OR pw.count_prev_7d IS NULL THEN NULL
        ELSE ROUND((COALESCE(lw.count_last_7d, 0) - COALESCE(pw.count_prev_7d, 0)) * 100.0 / pw.count_prev_7d, 2)
    END AS percent_change
FROM last_week lw
FULL OUTER JOIN prev_week pw ON lw.topic_name = pw.topic_name
WHERE COALESCE(lw.count_last_7d, 0) + COALESCE(pw.count_prev_7d, 0) > 0
ORDER BY absolute_change DESC;


-- 3.4: Topic emergence (topics appearing for first time recently)
-- Uso: Table - Nuevos tópicos detectados
SELECT 
    topic_name,
    MIN(published_at) AS first_appearance,
    COUNT(*) AS article_count,
    COUNT(DISTINCT news_site) AS sources,
    DATEDIFF(day, MIN(published_at), CURRENT_DATE) AS days_since_first
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY topic_name
HAVING MIN(published_at) >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY first_appearance DESC;


-- =============================================================================
-- 4. TEMPORAL ANALYSIS  
-- =============================================================================

-- 4.1: Monthly content volume trend (last 24 months)
-- Uso: Line chart - Tendencia mensual de contenido
SELECT 
    TO_CHAR(published_at, 'YYYY-MM') AS year_month,
    content_type,
    COUNT(*) AS content_count,
    COUNT(DISTINCT news_site) AS active_sources,
    COUNT(DISTINCT topic_name) AS topics_covered
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY year_month, content_type
ORDER BY year_month DESC, content_type;


-- 4.2: Weekly content volume (last 12 weeks)
-- Uso: Bar chart - Volumen semanal
SELECT 
    DATE_TRUNC('week', published_at) AS week_start,
    COUNT(*) AS total_content,
    COUNT(DISTINCT news_site) AS active_sources,
    ROUND(COUNT(*) * 1.0 / 7, 1) AS avg_per_day
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY week_start
ORDER BY week_start DESC;


-- 4.3: Day of week analysis (publishing patterns)
-- Uso: Bar chart - Patrón de publicación por día de semana
SELECT 
    TO_CHAR(published_at, 'Day') AS day_of_week,
    EXTRACT(dow FROM published_at) AS day_number,  -- 0=Sunday, 6=Saturday
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(COUNT(*)) OVER (), 0) AS avg_per_day
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY day_of_week, day_number
ORDER BY day_number;


-- 4.4: Hour of day analysis (when content is published)
-- Uso: Heatmap - Hora de publicación
SELECT 
    EXTRACT(hour FROM published_at) AS hour_of_day,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- 4.5: Monthly growth rate (MoM)
-- Uso: Table con % growth - Tasa de crecimiento mensual
WITH monthly_counts AS (
    SELECT 
        TO_CHAR(published_at, 'YYYY-MM') AS year_month,
        COUNT(*) AS content_count
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND published_at >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY year_month
),
with_lag AS (
    SELECT 
        year_month,
        content_count,
        LAG(content_count, 1) OVER (ORDER BY year_month) AS prev_month_count
    FROM monthly_counts
)
SELECT 
    year_month,
    content_count AS current_month,
    prev_month_count AS previous_month,
    content_count - prev_month_count AS absolute_change,
    CASE 
        WHEN prev_month_count IS NULL OR prev_month_count = 0 THEN NULL
        ELSE ROUND((content_count - prev_month_count) * 100.0 / prev_month_count, 2)
    END AS growth_rate_pct
FROM with_lag
ORDER BY year_month DESC;


-- =============================================================================
-- 5. ENTITY & KEYWORD ANALYSIS
-- =============================================================================

-- 5.1: Top 50 most mentioned entities
-- Uso: Word cloud - Entidades más mencionadas
WITH entity_exploded AS (
    SELECT 
        content_pk,
        content_type,
        topic_name,
        TRIM(SPLIT_PART(entities, ',', n.n)) AS entity
    FROM core.content_items
    CROSS JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) n
    WHERE 
        is_current = TRUE
        AND entities IS NOT NULL
        AND SPLIT_PART(entities, ',', n.n) != ''
)
SELECT 
    entity,
    COUNT(DISTINCT content_pk) AS mention_count,
    COUNT(DISTINCT content_type) AS content_types_count,
    COUNT(DISTINCT topic_name) AS topics_count
FROM entity_exploded
WHERE 
    entity != ''
    AND LENGTH(entity) >= 3
GROUP BY entity
ORDER BY mention_count DESC
LIMIT 50;


-- 5.2: Top 100 keywords
-- Uso: Tag cloud - Keywords más frecuentes
WITH keyword_exploded AS (
    SELECT 
        content_pk,
        topic_name,
        TRIM(SPLIT_PART(keywords, ',', n.n)) AS keyword
    FROM core.content_items
    CROSS JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) n
    WHERE 
        is_current = TRUE
        AND keywords IS NOT NULL
        AND SPLIT_PART(keywords, ',', n.n) != ''
)
SELECT 
    keyword,
    COUNT(DISTINCT content_pk) AS frequency,
    COUNT(DISTINCT topic_name) AS topics_used_in
FROM keyword_exploded
WHERE 
    keyword != '' 
    AND LENGTH(keyword) >= 3
GROUP BY keyword
ORDER BY frequency DESC
LIMIT 100;


-- 5.3: Keywords by topic  
-- Uso: Table - Keywords por tópico específico
WITH keyword_exploded AS (
    SELECT 
        topic_name,
        TRIM(SPLIT_PART(keywords, ',', n.n)) AS keyword
    FROM core.content_items
    CROSS JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) n
    WHERE 
        is_current = TRUE
        AND keywords IS NOT NULL
        AND topic_name IS NOT NULL
        AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
        AND SPLIT_PART(keywords, ',', n.n) != ''
)
SELECT 
    topic_name,
    keyword,
    COUNT(*) AS frequency
FROM keyword_exploded
WHERE 
    keyword != '' 
    AND LENGTH(keyword) >= 3
GROUP BY topic_name, keyword
HAVING COUNT(*) >= 5
ORDER BY topic_name, frequency DESC;


-- 5.4: Entity co-occurrence (entities that appear together)
-- Uso: Network graph - Relaciones entre entidades
WITH entity_pairs AS (
    SELECT 
        content_pk,
        TRIM(SPLIT_PART(entities, ',', n1.n)) AS entity1,
        TRIM(SPLIT_PART(entities, ',', n2.n)) AS entity2
    FROM core.content_items
    CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) n1
    CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) n2
    WHERE 
        is_current = TRUE
        AND entities IS NOT NULL
        AND SPLIT_PART(entities, ',', n1.n) != ''
        AND SPLIT_PART(entities, ',', n2.n) != ''
        AND n1.n < n2.n  -- Avoid self-pairs and duplicates
)
SELECT 
    entity1,
    entity2,
    COUNT(DISTINCT content_pk) AS co_occurrences
FROM entity_pairs
WHERE 
    entity1 != '' 
    AND entity2 != ''
    AND LENGTH(entity1) >= 3
    AND LENGTH(entity2) >= 3
GROUP BY entity1, entity2
HAVING COUNT(DISTINCT content_pk) >= 5
ORDER BY co_occurrences DESC
LIMIT 100;


-- ============================================================================
-- 6. DATA QUALITY & FRESHNESS
-- =============================================================================

-- 6.1: Freshness check (how recent is the data?)
-- Uso: KPI cards - Frescura de los datos
SELECT 
    content_type,
    MAX(published_at) AS latest_published,
    MAX(ingest_date) AS latest_ingested,
    DATEDIFF(hour, MAX(published_at), CURRENT_TIMESTAMP) AS hours_since_latest_publish,
    DATEDIFF(hour, MAX(ingest_date), CURRENT_DATE) AS days_since_latest_ingest
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type
ORDER BY content_type;


-- 6.2: Completeness check (NULL fields analysis)
-- Uso: Bar chart - Porcentaje de campos nulos


WITH nulls_analysis AS (
    SELECT 'title' AS field_name,
           COUNT(CASE WHEN title IS NULL THEN 1 END) AS null_count,
           COUNT(*) AS total_count
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'image_url',
           COUNT(CASE WHEN image_url IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'summary',
           COUNT(CASE WHEN summary IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'topic_name',
           COUNT(CASE WHEN topic_name IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'entities',
           COUNT(CASE WHEN entities IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'keywords',
           COUNT(CASE WHEN keywords IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
    UNION ALL
    SELECT 'news_site',
           COUNT(CASE WHEN news_site IS NULL THEN 1 END),
           COUNT(*)
    FROM core.content_items WHERE is_current = TRUE
)
SELECT 
    field_name,
    null_count,
    total_count,
    total_count - null_count AS non_null_count,
    ROUND(null_count * 100.0 / total_count, 2) AS null_percentage,
    ROUND((total_count - null_count) * 100.0 / total_count, 2) AS completeness_pct
FROM nulls_analysis
ORDER BY null_percentage DESC;


-- 6.3: Duplicate detection (should be 0 with SCD Type 2)
-- Uso: Alert - Detectar duplicados anómalos
SELECT 
    content_id,
    content_type,
    COUNT(*) AS current_versions,
    MAX(version) AS latest_version
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_id, content_type
HAVING COUNT(*) > 1
ORDER BY current_versions DESC;


-- 6.4: Version history analysis (content update patterns)
-- Uso: Bar chart - Frecuencia de actualizaciones
SELECT 
    version_count,
    COUNT(*) AS content_with_this_many_versions
FROM (
    SELECT 
        content_id,
        content_type,
        COUNT(*) AS version_count
    FROM core.content_items
    GROUP BY content_id, content_type
) versions
GROUP BY version_count
ORDER BY version_count;


-- 6.5: Ingest quality by date
-- Uso: Line chart - Calidad de ingesta por fecha
SELECT 
    ingest_date,
    content_type,
    COUNT(*) AS records_ingested,
    COUNT(DISTINCT content_id) AS unique_content,
    COUNT(*) - COUNT(DISTINCT content_id) AS duplicates_in_batch
FROM core.content_items
WHERE 
    ingest_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY ingest_date, content_type
ORDER BY ingest_date DESC, content_type;


-- 6.6: SCD Type 2 audit (recently updated records)
-- Uso: Table - Auditoría de cambios recientes
SELECT 
    content_id,
    content_type,
    version,
    valid_from,
    valid_to,
    is_current,
    title
FROM core.content_items
WHERE 
    valid_from >= CURRENT_DATE - INTERVAL '7 days'
    OR valid_to >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY valid_from DESC
LIMIT 100;


-- =============================================================================
-- 7. AGGREGATIONS (Using Pre-computed Tables)
-- =============================================================================

-- 7.1: Topic trends from aggregation table (last 12 months)
-- Uso: Line chart multi-series - Tendencias de tópicos
SELECT 
    year_month,
    topic_name,
    SUM(content_count) AS total_content,
    ROUND(AVG(content_count), 1) AS avg_per_type
FROM core.agg_topic_monthly_trends
WHERE 
    ingest_date = (SELECT MAX(ingest_date) FROM core.agg_topic_monthly_trends)
    AND year_month >= TO_CHAR(CURRENT_DATE - INTERVAL '12 months', 'YYYY-MM')
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY year_month, topic_name
ORDER BY year_month DESC, total_content DESC;


-- 7.2: Source activity trends from aggregation table
-- Uso: Stacked bar chart - Actividad de fuentes por mes
SELECT 
    year_month,
    news_site,
    SUM(content_count) AS total_content
FROM core.agg_news_site_monthly_activity
WHERE 
    ingest_date = (SELECT MAX(ingest_date) FROM core.agg_news_site_monthly_activity)
    AND year_month >= TO_CHAR(CURRENT_DATE - INTERVAL '12 months', 'YYYY-MM')
GROUP BY year_month, news_site
ORDER BY year_month DESC, total_content DESC;


-- 7.3: Topic growth rate (month-over-month from agg table)
-- Uso: Table con crecimiento - MoM growth
WITH monthly_topic AS (
    SELECT 
        year_month,
        topic_name,
        SUM(content_count) AS content_count
    FROM core.agg_topic_monthly_trends
    WHERE 
        ingest_date = (SELECT MAX(ingest_date) FROM core.agg_topic_monthly_trends)
        AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
    GROUP BY year_month, topic_name
),
with_lag AS (
    SELECT 
        year_month,
        topic_name,
        content_count,
        LAG(content_count, 1) OVER (PARTITION BY topic_name ORDER BY year_month) AS prev_month_count
    FROM monthly_topic
)
SELECT 
    year_month,
    topic_name,
    content_count AS current_month,
    prev_month_count AS previous_month,
    content_count - COALESCE(prev_month_count, 0) AS absolute_change,
    CASE 
        WHEN prev_month_count IS NULL OR prev_month_count = 0 THEN NULL
        ELSE ROUND((content_count - prev_month_count) * 100.0 / prev_month_count, 2)
    END AS growth_rate_pct
FROM with_lag
WHERE year_month >= TO_CHAR(CURRENT_DATE - INTERVAL '6 months', 'YYYY-MM')
ORDER BY year_month DESC, growth_rate_pct DESC NULLS LAST;


-- 7.4: Top performers (highest volume months per topic)
-- Uso: Leaderboard - Top meses por tópico
SELECT 
    topic_name,
    year_month,
    SUM(content_count) AS total_content,
    RANK() OVER (PARTITION BY topic_name ORDER BY SUM(content_count) DESC) AS rank_within_topic
FROM core.agg_topic_monthly_trends
WHERE 
    ingest_date = (SELECT MAX(ingest_date) FROM core.agg_topic_monthly_trends)
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY topic_name, year_month
QUALIFY rank_within_topic <= 3
ORDER BY topic_name, rank_within_topic;


-- =============================================================================
-- 8. DASHBOARD QUERIES (High-Level KPIs)
-- =============================================================================

-- 8.1: Executive summary (single-row KPIs)
-- Uso: KPI cards en dashboard principal
SELECT 
    (SELECT COUNT(*) FROM core.content_items WHERE is_current = TRUE) AS total_content,
    (SELECT COUNT(DISTINCT content_id) FROM core.content_items WHERE is_current = TRUE) AS unique_content_items,
    (SELECT COUNT(DISTINCT news_site) FROM core.content_items WHERE is_current = TRUE) AS active_sources,
    (SELECT COUNT(DISTINCT topic_name) FROM core.content_items WHERE is_current = TRUE AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')) AS topics_covered,
    (SELECT MAX(published_at) FROM core.content_items WHERE is_current = TRUE) AS latest_content_date,
    (SELECT MAX(ingest_date) FROM core.content_items WHERE is_current = TRUE) AS latest_ingest_date,
    (SELECT COUNT(*) FROM core.content_items WHERE is_current = TRUE AND published_at >= CURRENT_DATE - INTERVAL '7 days') AS content_last_7_days,
    (SELECT COUNT(*) FROM core.content_items WHERE is_current = TRUE AND published_at >= CURRENT_DATE - INTERVAL '30 days') AS content_last_30_days;


-- 8.2: Last 7 days detailed summary
-- Uso: Daily breakdown table
SELECT 
    DATE_TRUNC('day', published_at) AS date,
    COUNT(*) AS content_count,
    COUNT(DISTINCT news_site) AS active_sources,
    COUNT(DISTINCT topic_name) AS topics_covered,
    SUM(CASE WHEN featured = TRUE THEN 1 ELSE 0 END) AS featured_count
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date
ORDER BY date DESC;


-- 8.3: Content type distribution (for pie chart)
-- Uso: Pie chart - Distribución por tipo
SELECT 
    content_type,
    COUNT(*) AS content_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type
ORDER BY content_count DESC;


-- 8.4: Top 10 topics (for bar chart)
-- Uso: Horizontal bar chart - Top tópicos
SELECT 
    topic_name,
    COUNT(*) AS content_count,
    COUNT(DISTINCT news_site) AS sources_covering
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
GROUP BY topic_name
ORDER BY content_count DESC
LIMIT 10;


-- 8.5: Top 10 sources (for bar chart)
-- Uso: Horizontal bar chart - Top fuentes
SELECT 
    news_site,
    COUNT(*) AS content_count,
    COUNT(DISTINCT topic_name) AS topics_covered
FROM core.content_items
WHERE 
    is_current = TRUE
    AND news_site IS NOT NULL
GROUP BY news_site
ORDER BY content_count DESC
LIMIT 10;


-- 8.6: Recent highlights (featured content)
-- Uso: Featured content carousel
SELECT 
    title,
    content_type,
    news_site,
    topic_name,
    published_at,
    url,
    image_url,
    summary
FROM core.content_items
WHERE 
    is_current = TRUE
    AND featured = TRUE
    AND image_url IS NOT NULL
ORDER BY published_at DESC
LIMIT 10;


-- 8.7: Daily content heatmap (last 90 days)
-- Uso: Calendar heatmap
SELECT 
    DATE(published_at) AS date,
    COUNT(*) AS content_count,
    CASE 
        WHEN COUNT(*) >= 100 THEN 'Very High'
        WHEN COUNT(*) >= 50 THEN 'High'
        WHEN COUNT(*) >= 20 THEN 'Medium'
        ELSE 'Low'
    END AS activity_level
FROM core.content_items
WHERE 
    is_current = TRUE
    AND published_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE(published_at)
ORDER BY date DESC;


-- =============================================================================
-- 9. MONITORING & ALERTS
-- =============================================================================

-- 9.1: Pipeline runs monitoring
-- Uso: Monitor table - Estado de ejecuciones recientes
SELECT 
    run_date,
    pipeline_name,
    status,
    records_processed,
    execution_time_seconds,
    ROUND(records_processed * 1.0 / NULLIF(execution_time_seconds, 0), 2) AS records_per_second,
    error_message,
    created_at
FROM core.meta_pipeline_runs
ORDER BY run_date DESC, created_at DESC
LIMIT 50;


-- 9.2: Failed pipeline runs (alert query)
-- Uso: Alert - Ejecuciones fallidas recientes
SELECT 
    run_date,
    pipeline_name,
    status,
    error_message,
    execution_time_seconds,
    created_at,
    DATEDIFF(hour, created_at, CURRENT_TIMESTAMP) AS hours_ago
FROM core.meta_pipeline_runs
WHERE 
    status = 'failed'
    AND run_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY created_at DESC;


-- 9.3: Data quality checks monitoring
-- Uso: Monitor table - Chequeos de calidad
SELECT 
    check_date,
    check_name,
    check_result,
    records_checked,
    records_failed,
    ROUND(records_failed * 100.0 / NULLIF(records_checked, 0), 2) AS failure_rate_pct,
    error_details,
    created_at
FROM core.meta_data_quality_checks
ORDER BY check_date DESC, created_at DESC
LIMIT 50;


-- 9.4: Failed quality checks (alert query)
-- Uso: Alert - Chequeos de calidad fallidos
SELECT 
    check_date,
    check_name,
    check_result,
    records_checked,
    records_failed,
    error_details
FROM core.meta_data_quality_checks
WHERE 
    check_result = 'failed'
    AND check_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY check_date DESC;


-- 9.5: Ingestion freshness alert
-- Uso: Alert - Datos desactualizados
SELECT 
    content_type,
    MAX(ingest_date) AS latest_ingest_date,
    DATEDIFF(day, MAX(ingest_date), CURRENT_DATE) AS days_since_last_ingest,
    CASE 
        WHEN DATEDIFF(day, MAX(ingest_date), CURRENT_DATE) > 2 THEN 'ALERT: Stale Data'
        WHEN DATEDIFF(day, MAX(ingest_date), CURRENT_DATE) > 1 THEN 'WARNING: Data Aging'
        ELSE 'OK'
    END AS freshness_status
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type
ORDER BY days_since_last_ingest DESC;


-- 9.6: Volume anomaly detection (current vs avg)
-- Uso: Alert - Anomalías en volumen de datos
WITH daily_counts AS (
    SELECT 
        DATE(published_at) AS date,
        COUNT(*) AS daily_count
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND published_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(published_at)
),
statistics AS (
    SELECT 
        AVG(daily_count) AS avg_daily,
        STDDEV(daily_count) AS stddev_daily
    FROM daily_counts
)
SELECT 
    dc.date,
    dc.daily_count,
    st.avg_daily,
    st.stddev_daily,
    ROUND((dc.daily_count - st.avg_daily) / NULLIF(st.stddev_daily, 0), 2) AS z_score,
    CASE 
        WHEN ABS((dc.daily_count - st.avg_daily) / NULLIF(st.stddev_daily, 0)) > 2 THEN 'ANOMALY'
        WHEN ABS((dc.daily_count - st.avg_daily) / NULLIF(st.stddev_daily, 0)) > 1.5 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS status
FROM daily_counts dc
CROSS JOIN statistics st
WHERE dc.date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY dc.date DESC;


-- =============================================================================
-- 10. ADVANCED ANALYTICS
-- =============================================================================

-- 10.1: Topic correlation matrix (topics that appear together)
-- Uso: Correlation matrix - Tópicos relacionados
WITH topic_pairs AS (
    SELECT 
        c1.topic_name AS topic1,
        c2.topic_name AS topic2,
        COUNT(*) AS co_occurrence_count
    FROM core.content_items c1
    INNER JOIN core.content_items c2
        ON DATE(c1.published_at) = DATE(c2.published_at)
        AND c1.news_site = c2.news_site
        AND c1.topic_name < c2.topic_name  -- Avoid duplicates
    WHERE 
        c1.is_current = TRUE
        AND c2.is_current = TRUE
        AND c1.topic_name IS NOT NULL
        AND c2.topic_name IS NOT NULL
        AND c1.topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
        AND c2.topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
    GROUP BY topic1, topic2
)
SELECT 
    topic1,
    topic2,
    co_occurrence_count,
    RANK() OVER (ORDER BY co_occurrence_count DESC) AS rank
FROM topic_pairs
WHERE co_occurrence_count >= 10
ORDER BY co_occurrence_count DESC
LIMIT 50;


-- 10.2: Source content velocity (articles per day trend)
-- Uso: Line chart - Velocidad de publicación por fuente
WITH daily_source_counts AS (
    SELECT 
        DATE(published_at) AS date,
        news_site,
        COUNT(*) AS daily_articles
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND published_at >= CURRENT_DATE - INTERVAL '90 days'
        AND news_site IS NOT NULL
    GROUP BY date, news_site
)
SELECT 
    news_site,
    AVG(daily_articles) AS avg_articles_per_day,
    MIN(daily_articles) AS min_daily,
    MAX(daily_articles) AS max_daily,
    STDDEV(daily_articles) AS stddev_articles,
    COUNT(DISTINCT date) AS active_days
FROM daily_source_counts
GROUP BY news_site
HAVING COUNT(DISTINCT date) >= 30
ORDER BY avg_articles_per_day DESC
LIMIT 20;


-- 10.3: Topic seasonality analysis (which months are topics popular)
-- Uso: Heatmap - Estacionalidad por tópico
SELECT 
    topic_name,
    EXTRACT(month FROM published_at) AS month,
    TO_CHAR(published_at, 'Month') AS month_name,
    COUNT(*) AS article_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY topic_name), 2) AS pct_of_topic_total
FROM core.content_items
WHERE 
    is_current = TRUE
    AND topic_name IS NOT NULL
    AND topic_name NOT IN ('2018', '2019', '2020', '2021', '2022', '2023', '2024')
    AND published_at >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY topic_name, month, month_name
ORDER BY topic_name, month;


-- 10.4: Content lifecycle analysis (time from publish to ingest)
-- Uso: Histogram - Latencia de ingesta
SELECT 
    content_type,
    DATEDIFF(day, published_at, ingest_date) AS days_to_ingest,
    COUNT(*) AS content_count
FROM core.content_items
WHERE 
    is_current = TRUE
    AND ingest_date IS NOT NULL
    AND published_at IS NOT NULL
    AND DATEDIFF(day, published_at, ingest_date) >= 0
    AND DATEDIFF(day, published_at, ingest_date) <= 365
GROUP BY content_type, days_to_ingest
ORDER BY content_type, days_to_ingest;


-- 10.5: Source reliability score (consistency, coverage, freshness)
-- Uso: Ranking table - Score de fuentes
WITH source_metrics AS (
    SELECT 
        news_site,
        COUNT(*) AS total_articles,
        COUNT(DISTINCT DATE(published_at)) AS active_days,
        COUNT(DISTINCT topic_name) AS topics_covered,
        DATEDIFF(day, MIN(published_at), MAX(published_at)) + 1 AS days_span,
        -- Consistency: what % of days they published
        ROUND(COUNT(DISTINCT DATE(published_at)) * 100.0 / NULLIF(DATEDIFF(day, MIN(published_at), MAX(published_at)) + 1, 0), 2) AS consistency_pct,
        -- Coverage: how many topics
        COUNT(DISTINCT topic_name) AS coverage_score,
        -- Freshness: how recent
        DATEDIFF(day, MAX(published_at), CURRENT_DATE) AS days_since_last
    FROM core.content_items
    WHERE 
        is_current = TRUE
        AND news_site IS NOT NULL
        AND published_at >= CURRENT_DATE - INTERVAL '180 days'
    GROUP BY news_site
)
SELECT 
    news_site,
    total_articles,
    active_days,
    topics_covered,
    consistency_pct,
    days_since_last,
    -- Weighted reliability score (0-100)
    ROUND(
        (consistency_pct * 0.4) +  -- 40% weight on consistency
        (LEAST(topics_covered * 10, 100) * 0.3) +  -- 30% weight on coverage (max 10 topics)
        (GREATEST(100 - days_since_last * 10, 0) * 0.3),  -- 30% weight on freshness
        2
    ) AS reliability_score
FROM source_metrics
WHERE total_articles >= 10
ORDER BY reliability_score DESC
LIMIT 30;


-- =============================================================================
-- TESTING NEW QUERIES
-- =============================================================================
-- Use this section to test new query ideas before adding them above

-- Example: Test a specific date range
-- SELECT * FROM core.content_items 
-- WHERE published_at BETWEEN '2026-01-01' AND '2026-02-01' 
-- LIMIT 10;


-- =============================================================================
-- QUERY PERFORMANCE NOTES
-- =============================================================================
-- Para mejorar performance de queries frecuentes:
--
-- 1. Crear vistas materializadas para queries complejas:
--    CREATE MATERIALIZED VIEW mv_daily_summary AS
--    SELECT DATE(published_at) AS date, COUNT(*) AS count 
--    FROM core.content_items WHERE is_current = TRUE GROUP BY date;
--
-- 2. Agregar índices en columnas frecuentemente filtradas:
--    - published_at (ya particionado por ingest_date)
--    - content_type
--    - topic_name
--    - news_site
--
-- 3. Usar tablas de agregación pre-calculadas cuando sea posible:
--    - core.agg_topic_monthly_trends
--    - core.agg_news_site_monthly_activity
--
-- 4. Limitar queries con LIMIT o WHERE clauses restrictivas
--
-- 5. Evitar CROSS JOIN en tablas grandes sin filtros
--
-- =============================================================================
-- FIN DEL DOCUMENTO
-- =============================================================================

