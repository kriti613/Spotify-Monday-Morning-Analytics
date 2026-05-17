"""
Spotify Analytics Pipeline — Phase 2: Extract & Load
=====================================================
Reads the 4 raw JSON source files and loads them into
the PostgreSQL staging tables (staging.stg_*).

What this script does:
  1. Connects to PostgreSQL using environment config
  2. Generates the dim_date table (date spine — run once)
  3. Loads artists  → staging.stg_artists
  4. Loads songs    → staging.stg_songs
  5. Loads users    → staging.stg_users
  6. Loads events   → staging.stg_stream_events (in batches)

Key engineering decisions:
  - TRUNCATE staging tables before each load (full refresh pattern)
    Safe because staging is always re-loadable from source files.
  - Batch inserts (executemany) — far faster than one INSERT at a time.
    321k rows would take ~5min row-by-row; batching does it in seconds.
  - All DB operations in a single transaction per table.
    If anything fails mid-load, the whole table rolls back cleanly.
  - Logging to both console and a log file for auditability.
"""

import getpass
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras  # for execute_values (bulk insert)

PROJECT_ROOT = Path(__file__).resolve().parent

# ── Logging setup ─────────────────────────────────────────────
# Writes to console AND to a log file.
# In production you'd ship these logs to something like Datadog.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "pipeline.log", mode="w"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
# In production, pull from environment variables or a secrets manager.
# Never hardcode passwords in real codebases!
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "spotify_pipeline"),
    "user":     os.getenv("DB_USER",     getpass.getuser()),
    "password": os.getenv("DB_PASSWORD", ""),
}

RAW_DATA_DIR = str(PROJECT_ROOT / "spotify_dataset")
BATCH_SIZE   = 5_000   # Rows per INSERT batch for stream_events


# ═══════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════

def get_connection():
    """Create and return a psycopg2 database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False   # We control commits explicitly
        return conn
    except psycopg2.OperationalError as e:
        log.error(f"Could not connect to database: {e}")
        sys.exit(1)


def load_json(filename):
    """Load a JSON file from the raw data directory."""
    path = os.path.join(RAW_DATA_DIR, filename)
    log.info(f"Reading {filename}...")
    with open(path, "r") as f:
        data = json.load(f)
    log.info(f"  → {len(data):,} records loaded from file")
    return data


def run_load(conn, table, columns, rows, truncate=True):
    """
    Generic bulk loader.
    - Optionally TRUNCATEs the target table first
    - Uses psycopg2.extras.execute_values for fast batch inserts
    - Wraps everything in a transaction; rolls back on any error

    Args:
        conn:     psycopg2 connection
        table:    fully qualified table name e.g. 'staging.stg_users'
        columns:  list of column names matching the tuples in rows
        rows:     list of tuples, one per row to insert
        truncate: if True, clear the table before loading
    """
    cols_str        = ", ".join(columns)
    placeholders    = ", ".join(["%s"] * len(columns))
    insert_sql      = f"INSERT INTO {table} ({cols_str}) VALUES %s"

    start = time.time()
    try:
        with conn.cursor() as cur:
            if truncate:
                log.info(f"  Truncating {table}...")
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")

            # execute_values sends rows in pages — much faster than executemany
            psycopg2.extras.execute_values(
                cur, insert_sql, rows,
                template=None,
                page_size=BATCH_SIZE,
            )
            conn.commit()

        elapsed = time.time() - start
        log.info(f"  ✓ Inserted {len(rows):,} rows into {table} in {elapsed:.2f}s")

    except Exception as e:
        conn.rollback()
        log.error(f"  ✗ Failed to load {table}: {e}")
        raise


# ═══════════════════════════════════════════════════════════════
# STEP 0 — POPULATE dim_date (run once, not a staging table)
#
# We generate every date from 2020-01-01 to 2027-12-31.
# This covers our 90-day history window plus future dates.
# The date_key is an integer in YYYYMMDD format (e.g. 20240115).
# Storing it as an integer means: fast joins, easy range filters,
# and readable values when you scan the table.
# ═══════════════════════════════════════════════════════════════

def populate_dim_date(conn):
    log.info("Populating analytics.dim_date...")

    # Check if already populated (idempotent — safe to re-run)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.dim_date")
        count = cur.fetchone()[0]
    if count > 0:
        log.info(f"  dim_date already has {count:,} rows — skipping")
        return

    rows = []
    start = date(2020, 1, 1)
    end   = date(2027, 12, 31)
    delta = timedelta(days=1)
    current = start

    MONTH_NAMES = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday",
                 "Friday", "Saturday", "Sunday"]

    while current <= end:
        date_key    = int(current.strftime("%Y%m%d"))   # 20240115
        quarter     = (current.month - 1) // 3 + 1
        week_number = current.isocalendar()[1]
        day_of_week = current.weekday() + 1             # 1=Mon, 7=Sun
        is_weekend  = current.weekday() >= 5

        rows.append((
            date_key,
            current,
            current.year,
            quarter,
            current.month,
            MONTH_NAMES[current.month],
            week_number,
            day_of_week,
            DAY_NAMES[current.weekday()],
            is_weekend,
        ))
        current += delta

    columns = [
        "date_key", "full_date", "year", "quarter", "month",
        "month_name", "week_number", "day_of_week", "day_name", "is_weekend",
    ]
    run_load(conn, "analytics.dim_date", columns, rows, truncate=False)


# ═══════════════════════════════════════════════════════════════
# STEP 1 — LOAD ARTISTS
# ═══════════════════════════════════════════════════════════════

def load_artists(conn):
    log.info("Loading artists → staging.stg_artists")
    data = load_json("artists.json")

    rows = [
        (
            r["artist_id"],
            r["name"],
            r.get("country"),
            r.get("primary_genre"),
            r.get("monthly_listeners"),
        )
        for r in data
    ]

    run_load(
        conn,
        "staging.stg_artists",
        ["artist_id", "name", "country", "primary_genre", "monthly_listeners"],
        rows,
    )


# ═══════════════════════════════════════════════════════════════
# STEP 2 — LOAD SONGS
# ═══════════════════════════════════════════════════════════════

def load_songs(conn):
    log.info("Loading songs → staging.stg_songs")
    data = load_json("songs.json")

    rows = [
        (
            r["song_id"],
            r["title"],
            r["artist_id"],
            None,                    # album_id — not in our source data
            r.get("genre"),
            r.get("subgenre"),
            r.get("duration_ms"),
            r.get("release_date"),
            r.get("is_explicit", False),
        )
        for r in data
    ]

    run_load(
        conn,
        "staging.stg_songs",
        ["song_id", "title", "artist_id", "album_id", "genre",
         "subgenre", "duration_ms", "release_date", "is_explicit"],
        rows,
    )


# ═══════════════════════════════════════════════════════════════
# STEP 3 — LOAD USERS
# ═══════════════════════════════════════════════════════════════

def load_users(conn):
    log.info("Loading users → staging.stg_users")
    data = load_json("users.json")

    rows = [
        (
            r["user_id"],
            r.get("email"),              # PII — staging only, never goes to analytics
            r.get("country_code"),
            r.get("subscription_type"),
            r.get("age_group"),
            r.get("signup_date"),
            r.get("is_active", True),
        )
        for r in data
    ]

    run_load(
        conn,
        "staging.stg_users",
        ["user_id", "email", "country_code", "subscription_type",
         "age_group", "signup_date", "is_active"],
        rows,
    )


# ═══════════════════════════════════════════════════════════════
# STEP 4 — LOAD STREAM EVENTS
#
# This is the big one: 321k rows.
# We load in batches defined by BATCH_SIZE to avoid memory issues.
# Each batch is its own commit — so if something fails at row
# 200k, the first 200k rows are already committed and we can
# resume from the checkpoint.
# ═══════════════════════════════════════════════════════════════

def load_stream_events(conn):
    log.info("Loading stream events → staging.stg_stream_events")
    data = load_json("stream_events.json")

    # TRUNCATE first (outside the batch loop) to clear old data
    with conn.cursor() as cur:
        log.info("  Truncating staging.stg_stream_events...")
        cur.execute("TRUNCATE TABLE staging.stg_stream_events")
        conn.commit()

    columns = [
        "event_id", "user_id", "song_id", "listened_at",
        "duration_ms", "song_duration_ms", "was_skipped",
        "skip_at_ms", "platform", "country_code",
    ]

    total_rows    = len(data)
    inserted      = 0
    total_start   = time.time()

    # Split into batches and load one batch at a time
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch = data[batch_start : batch_start + BATCH_SIZE]

        rows = [
            (
                r["event_id"],
                r["user_id"],
                r["song_id"],
                r["listened_at"],
                r.get("duration_ms"),
                r.get("song_duration_ms"),
                r.get("was_skipped", False),
                r.get("skip_at_ms"),
                r.get("platform"),
                r.get("country_code"),
            )
            for r in batch
        ]

        insert_sql = f"""
            INSERT INTO staging.stg_stream_events ({', '.join(columns)})
            VALUES %s
        """
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, insert_sql, rows, page_size=BATCH_SIZE
            )
        conn.commit()

        inserted += len(batch)
        pct = inserted / total_rows * 100
        log.info(f"  Batch loaded: {inserted:>7,} / {total_rows:,} rows  ({pct:.1f}%)")

    elapsed = time.time() - total_start
    rate    = total_rows / elapsed
    log.info(f"  ✓ All {total_rows:,} stream events loaded in {elapsed:.1f}s  ({rate:,.0f} rows/sec)")


# ═══════════════════════════════════════════════════════════════
# VALIDATION — Row counts + quick sanity checks after loading
# ═══════════════════════════════════════════════════════════════

def validate_load(conn):
    log.info("─" * 55)
    log.info("Running post-load validation...")

    checks = [
        # (description, SQL query, expected condition description)
        (
            "Staging row counts",
            """
            SELECT 'stg_artists'      AS tbl, COUNT(*) FROM staging.stg_artists     UNION ALL
            SELECT 'stg_songs',              COUNT(*) FROM staging.stg_songs        UNION ALL
            SELECT 'stg_users',              COUNT(*) FROM staging.stg_users        UNION ALL
            SELECT 'stg_stream_events',      COUNT(*) FROM staging.stg_stream_events
            ORDER BY tbl
            """,
        ),
        (
            "dim_date range",
            "SELECT MIN(full_date), MAX(full_date), COUNT(*) FROM analytics.dim_date",
        ),
        (
            "Null event_ids in staging (should be 0)",
            "SELECT COUNT(*) FROM staging.stg_stream_events WHERE event_id IS NULL",
        ),
        (
            "Orphan song_ids in events (songs not in catalog)",
            """
            SELECT COUNT(*) FROM staging.stg_stream_events e
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.stg_songs s WHERE s.song_id = e.song_id
            )
            """,
        ),
        (
            "Skip rate by subscription type (sanity: free > premium)",
            """
            SELECT
                u.subscription_type,
                ROUND(AVG(CASE WHEN e.was_skipped THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_pct,
                COUNT(*) AS total_events
            FROM staging.stg_stream_events e
            JOIN staging.stg_users u ON u.user_id = e.user_id
            GROUP BY u.subscription_type
            ORDER BY u.subscription_type
            """,
        ),
    ]

    with conn.cursor() as cur:
        for description, sql in checks:
            log.info(f"\n  CHECK: {description}")
            cur.execute(sql)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            # Print header
            header = "  " + "  |  ".join(f"{c:<25}" for c in col_names)
            log.info(header)
            log.info("  " + "-" * (len(header) - 2))
            for row in rows:
                line = "  " + "  |  ".join(f"{str(v):<25}" for v in row)
                log.info(line)


# ═══════════════════════════════════════════════════════════════
# MAIN — Orchestrates all steps in order
# ═══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 55)
    log.info("SPOTIFY PIPELINE — Phase 2: Extract & Load")
    log.info("=" * 55)

    pipeline_start = time.time()
    conn = get_connection()
    log.info(f"Connected to database: {DB_CONFIG['dbname']}")

    try:
        populate_dim_date(conn)   # Step 0: date spine (analytics layer)
        load_artists(conn)        # Step 1
        load_songs(conn)          # Step 2
        load_users(conn)          # Step 3
        load_stream_events(conn)  # Step 4 (big one)
        validate_load(conn)       # Step 5: sanity checks

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise
    finally:
        conn.close()

    total_elapsed = time.time() - pipeline_start
    log.info("=" * 55)
    log.info(f"Pipeline complete in {total_elapsed:.1f}s")
    log.info("Next step → Phase 3: run transform SQL")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
