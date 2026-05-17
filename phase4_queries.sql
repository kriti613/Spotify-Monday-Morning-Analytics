-- ================================================================
-- SPOTIFY ANALYTICS PIPELINE
-- Phase 4: Business Queries — The Monday Morning Report
-- ================================================================
-- These 4 queries answer the exact business questions the data
-- team needs every week. Each one runs against the analytics
-- star schema built in Phase 3.
--
-- Every query is written to be:
--   - Self-contained: runs independently, no temp tables needed
--   - Parameterisable: the date window is isolated in a CTE so
--     you can change "last 7 days" to "last 30 days" in one place
--   - Explained: comments say WHY, not just what
-- ================================================================


-- ================================================================
-- QUERY 1: Top 10 Trending Songs — Last 7 Days
-- ================================================================
-- Business question: "What were the top 10 trending songs
-- globally last week?"
--
-- "Trending" definition: most unique listeners (not just plays).
-- Using unique listeners instead of raw play count prevents a
-- single user on repeat from inflating a song's ranking.
-- We also show play_count and avg_completion_pct to help the
-- team understand whether people are actually listening or
-- just pressing play.
--
-- The week window uses dim_date.date_key so it's fast (indexed
-- integer comparison) instead of a slow timestamp range scan.
-- ================================================================

WITH last_7_days AS (
    SELECT date_key
    FROM analytics.dim_date
    WHERE full_date >= CURRENT_DATE - INTERVAL '7 days'
      AND full_date <  CURRENT_DATE
)
SELECT
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT fs.user_key) DESC) AS rank,
    da.name                                                        AS artist,
    ds.title                                                       AS song,
    ds.genre,
    COUNT(DISTINCT fs.user_key)                                    AS unique_listeners,
    COUNT(*)                                                       AS total_plays,
    ROUND(AVG(fs.completion_pct), 1)                              AS avg_completion_pct,
    SUM(CASE WHEN fs.was_skipped THEN 1 ELSE 0 END)               AS skip_count,
    ROUND(
        SUM(CASE WHEN fs.was_skipped THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                              AS skip_rate_pct
FROM analytics.fact_streams  fs
JOIN analytics.dim_songs     ds ON ds.song_key   = fs.song_key
JOIN analytics.dim_artists   da ON da.artist_key = fs.artist_key
WHERE fs.date_key IN (SELECT date_key FROM last_7_days)
GROUP BY da.name, ds.title, ds.genre
ORDER BY unique_listeners DESC
LIMIT 10;


-- ================================================================
-- QUERY 2: Genre Skip Rates — Recommendation Quality Signal
-- ================================================================
-- Business question: "Which genres have the highest skip rates?"
--
-- High skip rate = the recommendation engine is placing wrong
-- songs in front of users. This tells the product team where
-- to focus model improvements.
--
-- We split by subscription type too because free users skip more
-- (ads interrupting flow, less invested) — mixing them together
-- would hide the true recommendation quality signal.
--
-- avg_listen_depth_pct is the average completion_pct on
-- NON-skipped songs only — how deeply people listen when they
-- do commit. A low depth even on non-skips means the songs
-- are too long or lose quality in the second half.
-- ================================================================

SELECT
    ds.genre,
    COUNT(*)                                                           AS total_plays,
    ROUND(AVG(CASE WHEN fs.was_skipped THEN 1.0 ELSE 0.0 END)*100,2) AS overall_skip_rate_pct,

    -- Split skip rate by subscription type
    ROUND(
        AVG(CASE WHEN du.subscription_type = 'free'    AND fs.was_skipped THEN 1.0
                 WHEN du.subscription_type = 'free'    AND NOT fs.was_skipped THEN 0.0
                 ELSE NULL END) * 100, 2
    )                                                                  AS free_skip_rate_pct,
    ROUND(
        AVG(CASE WHEN du.subscription_type = 'premium' AND fs.was_skipped THEN 1.0
                 WHEN du.subscription_type = 'premium' AND NOT fs.was_skipped THEN 0.0
                 ELSE NULL END) * 100, 2
    )                                                                  AS premium_skip_rate_pct,

    -- How deeply do committed listeners hear each genre?
    ROUND(
        AVG(CASE WHEN NOT fs.was_skipped THEN fs.completion_pct END), 1
    )                                                                  AS avg_listen_depth_pct,

    ROUND(AVG(ds.duration_ms) / 1000.0, 0)                           AS avg_song_duration_sec

FROM analytics.fact_streams  fs
JOIN analytics.dim_songs     ds ON ds.song_key   = fs.song_key
JOIN analytics.dim_users     du ON du.user_key   = fs.user_key
GROUP BY ds.genre
ORDER BY overall_skip_rate_pct DESC;


-- ================================================================
-- QUERY 3: Free vs Premium Listening Behaviour
-- ================================================================
-- Business question: "How does listening behavior differ between
-- Free and Premium users?"
--
-- This surfaces the value of upgrading — a key metric for
-- the growth/monetisation team. We want to show concrete
-- behavioural differences, not just skip rate.
--
-- Metrics shown:
--   avg_plays_per_user    — engagement depth
--   avg_completion_pct    — how fully they listen
--   skip_rate_pct         — recommendation fit / patience
--   platform_mix          — what devices they use (mobile vs web)
--   peak_hour             — when they listen most
--   unique_genres_per_user— how wide their taste is
-- ================================================================

WITH user_stats AS (
    SELECT
        fs.user_key,
        du.subscription_type,
        COUNT(*)                                                      AS plays,
        ROUND(AVG(fs.completion_pct), 2)                             AS avg_completion,
        ROUND(AVG(CASE WHEN fs.was_skipped THEN 1.0 ELSE 0.0 END)*100, 2) AS skip_rate,
        COUNT(DISTINCT ds.genre)                                      AS unique_genres
    FROM analytics.fact_streams  fs
    JOIN analytics.dim_users     du ON du.user_key = fs.user_key
    JOIN analytics.dim_songs     ds ON ds.song_key = fs.song_key
    GROUP BY fs.user_key, du.subscription_type
),
platform_stats AS (
    SELECT
        du.subscription_type,
        fs.platform,
        COUNT(*) AS play_count,
        ROW_NUMBER() OVER (
            PARTITION BY du.subscription_type
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM analytics.fact_streams fs
    JOIN analytics.dim_users    du ON du.user_key = fs.user_key
    GROUP BY du.subscription_type, fs.platform
),
peak_hour AS (
    SELECT
        du.subscription_type,
        EXTRACT(HOUR FROM fs.listened_at)::INT AS hour,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (
            PARTITION BY du.subscription_type
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM analytics.fact_streams fs
    JOIN analytics.dim_users    du ON du.user_key = fs.user_key
    GROUP BY du.subscription_type, EXTRACT(HOUR FROM fs.listened_at)
)
SELECT
    us.subscription_type,
    COUNT(DISTINCT us.user_key)              AS total_users,
    ROUND(AVG(us.plays), 0)                  AS avg_plays_per_user,
    ROUND(AVG(us.avg_completion), 1)         AS avg_completion_pct,
    ROUND(AVG(us.skip_rate), 1)              AS skip_rate_pct,
    ROUND(AVG(us.unique_genres), 1)          AS avg_unique_genres,
    MAX(CASE WHEN ps.rn = 1 THEN ps.platform END)  AS top_platform,
    MAX(CASE WHEN ph.rn = 1 THEN ph.hour END)       AS peak_hour_utc
FROM user_stats             us
LEFT JOIN platform_stats ps ON ps.subscription_type = us.subscription_type
LEFT JOIN peak_hour      ph ON ph.subscription_type = us.subscription_type
GROUP BY us.subscription_type
ORDER BY us.subscription_type;


-- ================================================================
-- QUERY 4: Regional Breakout Artists
-- ================================================================
-- Business question: "Which artists are breaking out regionally?"
--
-- A "breakout" artist is one who is growing fast in a specific
-- country — high unique listener count relative to how long
-- they've been in the catalog.
--
-- Method: compare plays in the last 30 days vs plays in the
-- 30 days before that (days 31–60). A breakout_score > 1.5
-- means they more than 50% more listeners recently.
--
-- We also calculate a concentration_pct: what % of their global
-- plays come from this one country. High concentration = they
-- are a regional phenomenon, not yet global.
-- ================================================================

WITH date_windows AS (
    SELECT
        date_key,
        CASE
            WHEN full_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'recent'
            WHEN full_date >= CURRENT_DATE - INTERVAL '60 days' THEN 'previous'
        END AS window_label
    FROM analytics.dim_date
    WHERE full_date >= CURRENT_DATE - INTERVAL '60 days'
      AND full_date <  CURRENT_DATE
),
regional_plays AS (
    SELECT
        da.name                           AS artist,
        da.primary_genre                  AS genre,
        fs.country_code,
        dw.window_label,
        COUNT(DISTINCT fs.user_key)       AS unique_listeners
    FROM analytics.fact_streams  fs
    JOIN analytics.dim_artists   da ON da.artist_key = fs.artist_key
    JOIN date_windows            dw ON dw.date_key   = fs.date_key
    GROUP BY da.name, da.primary_genre, fs.country_code, dw.window_label
),
pivoted AS (
    SELECT
        artist,
        genre,
        country_code,
        SUM(CASE WHEN window_label = 'recent'   THEN unique_listeners ELSE 0 END) AS recent_listeners,
        SUM(CASE WHEN window_label = 'previous' THEN unique_listeners ELSE 0 END) AS prev_listeners
    FROM regional_plays
    GROUP BY artist, genre, country_code
),
global_totals AS (
    SELECT
        da.name                     AS artist,
        COUNT(DISTINCT fs.user_key) AS global_listeners
    FROM analytics.fact_streams fs
    JOIN analytics.dim_artists  da ON da.artist_key = fs.artist_key
    JOIN date_windows           dw ON dw.date_key   = fs.date_key
    WHERE dw.window_label = 'recent'
    GROUP BY da.name
)
SELECT
    p.artist,
    p.genre,
    p.country_code,
    p.recent_listeners,
    p.prev_listeners,
    -- How much did they grow? NULL if no previous data
    ROUND(
        p.recent_listeners::NUMERIC
        / NULLIF(p.prev_listeners, 0), 2
    )                                                AS breakout_score,
    -- What fraction of their global listeners are in this country?
    ROUND(
        p.recent_listeners * 100.0
        / NULLIF(gt.global_listeners, 0), 1
    )                                               AS regional_concentration_pct
FROM pivoted             p
JOIN global_totals       gt ON gt.artist = p.artist
WHERE p.recent_listeners  >= 20         -- Minimum threshold: avoid noise from 1-2 listeners
  AND p.prev_listeners    >= 5          -- Must have existed in previous period
  AND p.recent_listeners::NUMERIC
      / NULLIF(p.prev_listeners, 0) > 1.3  -- At least 30% growth
ORDER BY breakout_score DESC, recent_listeners DESC
LIMIT 15;
