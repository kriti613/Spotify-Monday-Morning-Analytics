-- ================================================================
-- SPOTIFY ANALYTICS PIPELINE
-- Phase 3: Transform — Staging → Analytics Star Schema
-- ================================================================
-- Execution order MATTERS because of foreign key dependencies:
--
--   1. dim_artists   (no FK dependencies)
--   2. dim_users     (no FK dependencies)
--   3. dim_songs     (depends on dim_artists → artist_key)
--   4. fact_streams  (depends on all three dims + dim_date)
--
-- Each block follows the same pattern:
--   a) INSERT new records that don't exist yet (upsert logic)
--   b) Use the source's natural key (artist_id, user_id etc.)
--      to look up the surrogate key (artist_key, user_key etc.)
--   c) Apply cleaning rules (TRIM, LOWER, COALESCE, validation)
--
-- Idempotency: every INSERT uses ON CONFLICT DO NOTHING on the
-- UNIQUE natural key column. Running this script twice will NOT
-- create duplicate rows — it just skips already-loaded records.
-- ================================================================


-- ================================================================
-- STEP 1 — POPULATE dim_artists
--
-- Cleaning rules applied here:
--   - TRIM whitespace from name and genre
--   - NULLIF('') turns empty strings into proper NULLs
--   - UPPER(country) enforces consistent country codes
-- ================================================================

INSERT INTO analytics.dim_artists (
    artist_id,
    name,
    country,
    primary_genre
)
SELECT
    src.artist_id,
    TRIM(src.name)                        AS name,
    UPPER(TRIM(src.country))              AS country,
    LOWER(TRIM(src.primary_genre))        AS primary_genre
FROM staging.stg_artists src

-- Skip any artist already in the analytics table.
-- This makes the transform safe to re-run (idempotent).
ON CONFLICT (artist_id) DO NOTHING;


-- ================================================================
-- STEP 2 — POPULATE dim_users
--
-- Key decision: email is deliberately excluded — PII stays in
-- staging and never flows into the analytics layer.
--
-- Cleaning rules:
--   - subscription_type is LOWER + validated against known values.
--     Any unexpected value ('trial', NULL, '') becomes 'unknown'.
--   - Only active users (is_active = TRUE) are loaded.
--     Inactive users skew engagement metrics.
-- ================================================================

INSERT INTO analytics.dim_users (
    user_id,
    country_code,
    subscription_type,
    age_group,
    signup_date
)
SELECT
    src.user_id,
    UPPER(TRIM(src.country_code))         AS country_code,

    -- Validate subscription_type — only allow known values
    CASE
        WHEN LOWER(TRIM(src.subscription_type)) IN ('free', 'premium')
        THEN LOWER(TRIM(src.subscription_type))
        ELSE 'unknown'
    END                                   AS subscription_type,

    src.age_group,
    src.signup_date

FROM staging.stg_users src
WHERE src.is_active = TRUE          -- Exclude churned/inactive users

ON CONFLICT (user_id) DO NOTHING;


-- ================================================================
-- STEP 3 — POPULATE dim_songs
--
-- This step joins stg_songs → dim_artists to resolve artist_key.
-- That's the JOIN pattern that makes a star schema work:
-- the natural key (artist_id VARCHAR) from staging maps to the
-- surrogate key (artist_key INTEGER) in the analytics layer.
--
-- Cleaning:
--   - Songs with no matching artist are excluded (LEFT JOIN + WHERE)
--     rather than allowed in with a NULL FK — broken references
--     in a fact table cause silent errors in analytics.
--   - duration_ms < 10000 (under 10 seconds) are likely bad records.
--   - genre is normalised to lowercase.
-- ================================================================

INSERT INTO analytics.dim_songs (
    song_id,
    title,
    artist_key,
    genre,
    subgenre,
    duration_ms,
    release_date,
    is_explicit
)
SELECT
    src.song_id,
    TRIM(src.title)                       AS title,
    da.artist_key,                        -- Surrogate key resolved here
    LOWER(TRIM(src.genre))                AS genre,
    LOWER(TRIM(NULLIF(src.subgenre, ''))) AS subgenre,
    src.duration_ms,
    src.release_date,
    COALESCE(src.is_explicit, FALSE)      AS is_explicit

FROM staging.stg_songs src

-- Resolve artist natural key → surrogate key
INNER JOIN analytics.dim_artists da
    ON da.artist_id = src.artist_id

-- Exclude suspiciously short songs (data quality filter)
WHERE src.duration_ms > 10000

ON CONFLICT (song_id) DO NOTHING;


-- ================================================================
-- STEP 4 — POPULATE fact_streams
--
-- The most complex transform. For every raw stream event we need to:
--   1. Resolve user_id      → user_key   (JOIN dim_users)
--   2. Resolve song_id      → song_key   (JOIN dim_songs)
--   3. Resolve song's artist→ artist_key (JOIN dim_songs)
--   4. Resolve date         → date_key   (JOIN dim_date)
--   5. Calculate completion_pct          (derived metric)
--   6. Validate and clean numeric fields
--
-- Filters:
--   - Events with no matching user OR song are dropped (INNER JOIN).
--     Orphan events can't be analysed and skew all metrics.
--   - Events where duration_ms > song_duration_ms + 5s are dropped.
--     These are impossible values — sensor/logging bugs.
--   - Negative durations are dropped.
--
-- completion_pct:
--   Percentage of the song the user actually heard.
--   Formula: (duration_ms / song_duration_ms) * 100, capped at 100.
--   Stored as NUMERIC(5,2) so values like 87.35 are exact.
--   Used in business question #2 (skip analysis) and #3 (engagement).
-- ================================================================

INSERT INTO analytics.fact_streams (
    event_id,
    user_key,
    song_key,
    artist_key,
    date_key,
    listened_at,
    duration_ms,
    song_duration_ms,
    was_skipped,
    completion_pct,
    platform,
    country_code
)
SELECT
    src.event_id,
    du.user_key,                          -- Surrogate key
    ds.song_key,                          -- Surrogate key
    ds.artist_key,                        -- Denormalised onto fact for faster queries
    dd.date_key,                          -- Integer YYYYMMDD

    src.listened_at,

    -- Clean duration: must be positive
    GREATEST(src.duration_ms, 0)          AS duration_ms,
    GREATEST(src.song_duration_ms, 1)     AS song_duration_ms,  -- Avoid div/0

    COALESCE(src.was_skipped, FALSE)      AS was_skipped,

    -- completion_pct: how much of the song did they hear?
    -- LEAST(..., 100) caps at 100% — some events show slightly over due to
    -- buffering/timing differences in the app.
    ROUND(
        LEAST(
            (GREATEST(src.duration_ms, 0)::NUMERIC
             / GREATEST(src.song_duration_ms, 1)::NUMERIC) * 100,
            100.00
        ),
        2
    )                                     AS completion_pct,

    LOWER(TRIM(src.platform))             AS platform,
    UPPER(TRIM(src.country_code))         AS country_code

FROM staging.stg_stream_events src

-- Resolve user natural key → surrogate key
INNER JOIN analytics.dim_users du
    ON du.user_id = src.user_id

-- Resolve song natural key → surrogate key (also gives us artist_key)
INNER JOIN analytics.dim_songs ds
    ON ds.song_id = src.song_id

-- Resolve date: cast listened_at to DATE then to YYYYMMDD integer
INNER JOIN analytics.dim_date dd
    ON dd.date_key = TO_CHAR(src.listened_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER

-- Data quality filters
WHERE src.duration_ms >= 0
  AND src.duration_ms <= src.song_duration_ms + 5000   -- Allow 5s buffer

-- Idempotency: skip events already loaded
ON CONFLICT (event_id) DO NOTHING;


-- ================================================================
-- POST-TRANSFORM VALIDATION
-- Run immediately after the inserts to verify results.
-- ================================================================

-- 1. Row counts across all analytics tables
SELECT 'dim_artists'  AS table_name, COUNT(*) AS row_count FROM analytics.dim_artists  UNION ALL
SELECT 'dim_users',                  COUNT(*)              FROM analytics.dim_users     UNION ALL
SELECT 'dim_songs',                  COUNT(*)              FROM analytics.dim_songs     UNION ALL
SELECT 'dim_date',                   COUNT(*)              FROM analytics.dim_date      UNION ALL
SELECT 'fact_streams',               COUNT(*)              FROM analytics.fact_streams
ORDER BY table_name;


-- 2. Staging vs analytics row counts — how many rows were filtered out?
SELECT
    'stream_events' AS source,
    stg.total       AS staging_rows,
    fct.total       AS analytics_rows,
    stg.total - fct.total AS filtered_out,
    ROUND((fct.total::NUMERIC / stg.total) * 100, 2) AS pct_loaded
FROM
    (SELECT COUNT(*) AS total FROM staging.stg_stream_events) stg,
    (SELECT COUNT(*) AS total FROM analytics.fact_streams)    fct;


-- 3. Sample of fact_streams — verify surrogate keys and completion_pct
SELECT
    fs.stream_key,
    du.subscription_type,
    da.name              AS artist_name,
    ds.title             AS song_title,
    ds.genre,
    fs.was_skipped,
    fs.completion_pct,
    fs.platform,
    fs.country_code,
    dd.day_name,
    dd.week_number
FROM analytics.fact_streams  fs
JOIN analytics.dim_users   du ON du.user_key   = fs.user_key
JOIN analytics.dim_songs   ds ON ds.song_key   = fs.song_key
JOIN analytics.dim_artists da ON da.artist_key = fs.artist_key
JOIN analytics.dim_date    dd ON dd.date_key   = fs.date_key
LIMIT 10;


-- 4. Completion pct distribution — should show most plays are full listens
SELECT
    CASE
        WHEN completion_pct < 25  THEN 'a) 0–25%   (early bail)'
        WHEN completion_pct < 50  THEN 'b) 25–50%  (halfway)'
        WHEN completion_pct < 75  THEN 'c) 50–75%  (most of it)'
        WHEN completion_pct < 95  THEN 'd) 75–95%  (almost full)'
        ELSE                           'e) 95–100% (full listen)'
    END                                          AS completion_bucket,
    COUNT(*)                                     AS plays,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM analytics.fact_streams
GROUP BY completion_bucket
ORDER BY completion_bucket;


-- 5. Skip rate by genre (preview of business question #2)
SELECT
    ds.genre,
    COUNT(*)                                                     AS total_plays,
    SUM(CASE WHEN fs.was_skipped THEN 1 ELSE 0 END)             AS skips,
    ROUND(AVG(CASE WHEN fs.was_skipped THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_rate_pct,
    ROUND(AVG(fs.completion_pct), 1)                            AS avg_completion_pct
FROM analytics.fact_streams  fs
JOIN analytics.dim_songs    ds ON ds.song_key = fs.song_key
GROUP BY ds.genre
ORDER BY skip_rate_pct DESC;
