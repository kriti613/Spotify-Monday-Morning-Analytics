-- ================================================================
-- SPOTIFY ANALYTICS PIPELINE
-- Phase 1: Database Schema Design
-- ================================================================
-- Architecture: ELT (Extract → Load → Transform)
-- Database:     PostgreSQL
-- Schemas:      staging (raw data)  |  analytics (star schema)
--
-- Two schemas = two layers:
--   staging.*   Raw data, exactly as it arrives. Never modified.
--               This is our safety net — if transforms break,
--               we can always re-run from here.
--
--   analytics.* Cleaned, modelled, query-ready. This is what
--               analysts and dashboards query against.
-- ================================================================


-- ================================================================
-- SETUP: Create the two schemas
-- ================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;


-- ================================================================
-- STAGING LAYER
--
-- Rules for this layer:
--   1. No foreign keys — raw data can have orphan records
--   2. No NOT NULL on most columns — raw data can be messy
--   3. Every table gets a loaded_at timestamp for debugging
--   4. Data types are permissive (VARCHAR not ENUM)
--   5. Never UPDATE or DELETE rows here — append only
-- ================================================================

-- Raw listening events from Spotify's app servers.
-- Each row = one play event (user pressed play on a song).
-- In the real world this arrives as JSON log files every hour.
CREATE TABLE staging.stg_stream_events (
    event_id          VARCHAR(36),          -- UUID from source system
    user_id           VARCHAR(36),          -- Who listened
    song_id           VARCHAR(36),          -- What they listened to
    listened_at       TIMESTAMPTZ,          -- When the play started (with timezone)
    duration_ms       INTEGER,              -- How many ms they actually listened
    song_duration_ms  INTEGER,              -- Full length of the song in ms
    was_skipped       BOOLEAN,              -- Did they press skip?
    skip_at_ms        INTEGER,              -- If skipped, at what ms?
    platform          VARCHAR(20),          -- ios | android | web | desktop
    country_code      CHAR(2),              -- ISO 3166-1 alpha-2 (US, GB, BR...)
    loaded_at         TIMESTAMPTZ DEFAULT NOW()  -- When WE loaded this row
);

-- Raw user records from Spotify's user management service.
-- In the real world this is a nightly CSV export or DB replica.
CREATE TABLE staging.stg_users (
    user_id             VARCHAR(36),
    email               VARCHAR(255),         -- PII — never move to analytics layer
    country_code        CHAR(2),
    subscription_type   VARCHAR(20),          -- free | premium
    age_group           VARCHAR(20),          -- 18-24 | 25-34 | 35-44 | 45+
    signup_date         DATE,
    is_active           BOOLEAN,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Raw song metadata from Spotify's catalog API.
CREATE TABLE staging.stg_songs (
    song_id         VARCHAR(36),
    title           VARCHAR(500),
    artist_id       VARCHAR(36),
    album_id        VARCHAR(36),
    genre           VARCHAR(100),
    subgenre        VARCHAR(100),
    duration_ms     INTEGER,
    release_date    DATE,
    is_explicit     BOOLEAN,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Raw artist records from Spotify's catalog API.
CREATE TABLE staging.stg_artists (
    artist_id         VARCHAR(36),
    name              VARCHAR(500),
    country           CHAR(2),
    primary_genre     VARCHAR(100),
    monthly_listeners INTEGER,
    loaded_at         TIMESTAMPTZ DEFAULT NOW()
);


-- ================================================================
-- ANALYTICS LAYER — STAR SCHEMA
--
-- Design pattern: Star Schema
--   - One FACT table at the center (fact_streams)
--   - Multiple DIMENSION tables around it (dim_*)
--
-- Fact table:      Stores measurable EVENTS. One row per thing
--                  that happened. Has timestamps, numbers (metrics),
--                  and FK keys pointing to dimension tables.
--
-- Dimension tables: Store DESCRIPTIVE attributes about entities.
--                   Who are our users? What songs exist? When?
--
-- Surrogate keys (user_key, song_key etc):
--   We use SERIAL integers instead of the original VARCHAR UUIDs
--   as primary keys. Why?
--     - Integer joins are ~3x faster than VARCHAR joins
--     - Protects us if source IDs ever change format
--     - Allows SCD (Slowly Changing Dimensions) patterns later
--   The original source ID is kept as a UNIQUE column (natural key).
-- ================================================================


-- dim_date: A row for every single calendar day.
-- Pre-populated once with a date generation script (Phase 2).
-- Why not just store timestamps and use EXTRACT()?
--   - Pre-calculated columns (week_number, is_weekend) make
--     GROUP BY queries 10x simpler and faster.
--   - Analysts can join on date_key without knowing SQL date functions.
--   - Enables "week over week" and "same week last year" logic easily.
CREATE TABLE analytics.dim_date (
    date_key        INTEGER      PRIMARY KEY,   -- YYYYMMDD e.g. 20240115
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,      -- 1, 2, 3, 4
    month           SMALLINT     NOT NULL,      -- 1–12
    month_name      VARCHAR(12)  NOT NULL,      -- January, February...
    week_number     SMALLINT     NOT NULL,      -- ISO week 1–53
    day_of_week     SMALLINT     NOT NULL,      -- 1=Monday, 7=Sunday
    day_name        VARCHAR(10)  NOT NULL,      -- Monday, Tuesday...
    is_weekend      BOOLEAN      NOT NULL
);

-- dim_artists: One row per unique artist. Cleaned, deduplicated.
CREATE TABLE analytics.dim_artists (
    artist_key      SERIAL        PRIMARY KEY,  -- Surrogate key (our internal ID)
    artist_id       VARCHAR(36)   NOT NULL UNIQUE, -- Natural key from source
    name            VARCHAR(500)  NOT NULL,
    country         CHAR(2),
    primary_genre   VARCHAR(100),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- dim_users: One row per unique user. Note: NO email here — PII stays in staging.
CREATE TABLE analytics.dim_users (
    user_key            SERIAL       PRIMARY KEY,
    user_id             VARCHAR(36)  NOT NULL UNIQUE,
    country_code        CHAR(2),
    subscription_type   VARCHAR(20),  -- free | premium (cleaned + validated)
    age_group           VARCHAR(20),
    signup_date         DATE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- dim_songs: One row per unique song.
-- References dim_artists via artist_key — this is the "snowflake" relationship.
CREATE TABLE analytics.dim_songs (
    song_key        SERIAL        PRIMARY KEY,
    song_id         VARCHAR(36)   NOT NULL UNIQUE,
    title           VARCHAR(500)  NOT NULL,
    artist_key      INTEGER       NOT NULL REFERENCES analytics.dim_artists(artist_key),
    genre           VARCHAR(100),
    subgenre        VARCHAR(100),
    duration_ms     INTEGER,
    release_date    DATE,
    is_explicit     BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- fact_streams: THE most important table. One row per play event.
-- This is what all 4 business questions will query against.
--
-- Key metrics stored here:
--   duration_ms     — how long the user listened
--   completion_pct  — percentage of song heard (calculated on load)
--   was_skipped     — the key signal for recommendation quality
--
-- event_id is UNIQUE to prevent duplicate loads (idempotency).
-- If we accidentally run the pipeline twice, no duplicate rows.
CREATE TABLE analytics.fact_streams (
    stream_key        BIGSERIAL    PRIMARY KEY,
    event_id          VARCHAR(36)  NOT NULL UNIQUE,  -- Dedup guard
    user_key          INTEGER      NOT NULL REFERENCES analytics.dim_users(user_key),
    song_key          INTEGER      NOT NULL REFERENCES analytics.dim_songs(song_key),
    artist_key        INTEGER      NOT NULL REFERENCES analytics.dim_artists(artist_key),
    date_key          INTEGER      NOT NULL REFERENCES analytics.dim_date(date_key),
    listened_at       TIMESTAMPTZ  NOT NULL,
    duration_ms       INTEGER,
    song_duration_ms  INTEGER,
    was_skipped       BOOLEAN      NOT NULL DEFAULT FALSE,
    completion_pct    NUMERIC(5,2),  -- e.g. 87.50 means 87.5% of song heard
    platform          VARCHAR(20),
    country_code      CHAR(2),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);


-- ================================================================
-- INDEXES
--
-- We create indexes on the exact columns our 4 business questions
-- will filter and group by. No guessing — we design indexes for
-- known query patterns.
--
-- PostgreSQL will use these automatically when relevant.
-- ================================================================

-- Business Q1: Top 10 trending songs last week
--   → needs fast lookup by date_key and grouping by song_key
CREATE INDEX idx_fact_streams_date_key    ON analytics.fact_streams(date_key);
CREATE INDEX idx_fact_streams_song_key    ON analytics.fact_streams(song_key);

-- Business Q2: Skip rate by genre
--   → needs fast filter on was_skipped + join to dim_songs for genre
CREATE INDEX idx_fact_streams_was_skipped ON analytics.fact_streams(was_skipped);

-- Business Q3: Listening behavior by subscription type
--   → needs join on user_key to get subscription_type from dim_users
CREATE INDEX idx_fact_streams_user_key    ON analytics.fact_streams(user_key);

-- Business Q4: Regional breakout artists
--   → needs grouping by artist_key and country_code
CREATE INDEX idx_fact_streams_artist_key  ON analytics.fact_streams(artist_key);
CREATE INDEX idx_fact_streams_country     ON analytics.fact_streams(country_code);

-- Range queries on actual timestamp (dashboards often say "last 7 days")
CREATE INDEX idx_fact_streams_listened_at ON analytics.fact_streams(listened_at);

-- Supporting indexes on dimension tables
CREATE INDEX idx_dim_songs_genre          ON analytics.dim_songs(genre);
CREATE INDEX idx_dim_users_sub_type       ON analytics.dim_users(subscription_type);
CREATE INDEX idx_dim_artists_country      ON analytics.dim_artists(country);
CREATE INDEX idx_dim_date_week            ON analytics.dim_date(year, week_number);


-- ================================================================
-- VERIFICATION QUERIES
-- Run these after setup to confirm everything was created.
-- ================================================================

-- Check staging tables exist
SELECT table_name, table_schema
FROM information_schema.tables
WHERE table_schema = 'staging'
ORDER BY table_name;

-- Check analytics tables exist
SELECT table_name, table_schema
FROM information_schema.tables
WHERE table_schema = 'analytics'
ORDER BY table_name;

-- Check all foreign keys are in place
SELECT
    tc.table_name        AS table_name,
    kcu.column_name      AS fk_column,
    ccu.table_name       AS references_table,
    ccu.column_name      AS references_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'analytics'
ORDER BY tc.table_name;
