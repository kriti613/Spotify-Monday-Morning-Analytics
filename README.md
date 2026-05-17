# Spotify Streaming Analytics — ELT Pipeline

An end-to-end ELT data pipeline built with **Python** and **PostgreSQL**, modelling 321,000+ streaming events into a star schema that answers four real business questions a music analytics team would ask every week.

---

## What this project demonstrates

| Skill | Where it appears |
|---|---|
| ELT pipeline architecture | Full extract → load → transform flow across 3 stages |
| Dimensional modelling | Star schema: 1 fact table + 4 dimension tables |
| PostgreSQL schema design | Surrogate keys, FK constraints, indexes tuned to query patterns |
| Batch data loading | psycopg2 `execute_values`, batched commits, idempotent upserts |
| SQL transforms | CTEs, window functions, conditional aggregation, date spines |
| Data quality | Null handling, orphan detection, impossible-value filters |
| Data governance | PII isolation — email never leaves the staging layer |
| Synthetic data engineering | Realistic behavioural patterns with Faker + weighted distributions |

---

## Business problem

Spotify's data team needs to answer four questions every Monday morning:

1. **Trending songs** — What were the top 10 songs globally last week, by unique listeners?
2. **Recommendation quality** — Which genres have the highest skip rates, split by subscription tier?
3. **Subscription value** — How does listening behaviour differ between Free and Premium users?
4. **Regional breakout** — Which artists are growing fastest in specific countries?

---

## Architecture

```
Source files (JSON)
        │
        ▼
┌───────────────────┐
│   staging schema  │   Raw data, append-only, PII lives here
│                   │
│  stg_stream_events│
│  stg_users        │
│  stg_songs        │
│  stg_artists      │
└─────────┬─────────┘
          │  SQL transforms (phase3_transform.sql)
          ▼
┌───────────────────┐
│  analytics schema │   Clean, query-ready star schema
│                   │
│  fact_streams ◄───┼── one row per play event
│  dim_users        │
│  dim_songs        │
│  dim_artists      │
│  dim_date         │
└─────────┬─────────┘
          │  Business queries (phase4_queries.sql)
          ▼
    Monday report
```

### Why ELT, not ETL?

With PostgreSQL as the destination, transforms run as SQL inside the database — faster and simpler than transforming in Python before loading. Raw data is preserved in staging, so transforms are replayable without re-extracting from the source.

---

## Project structure

```
spotify-case-study/
│
├── spotify_dataset/
│   ├── generate_data.py      # Synthetic data generator (Faker)
│   ├── stream_events.json    # Generated locally (not in git — too large)
│   ├── users.json
│   ├── songs.json
│   └── artists.json
│
├── phase1_schema.sql         # Creates staging + analytics schemas
├── phase2_load.py            # Python loader: JSON → staging tables
├── phase3_transform.sql      # SQL transforms: staging → star schema
├── phase4_queries.sql        # 4 business queries (the Monday report)
├── requirements.txt
├── pipeline.log              # Written by phase2_load.py (gitignored)
└── README.md
```

---

## Data model

### Staging layer (`staging.*`)

| Table | Rows | Description |
|---|---|---|
| `stg_stream_events` | 321,265 | Raw play events — one row per listen |
| `stg_users` | 1,000 | User records including email (PII) |
| `stg_songs` | 500 | Song metadata from catalog |
| `stg_artists` | 150 | Artist records |

### Analytics layer (`analytics.*`) — star schema

| Table | Rows | Type | Description |
|---|---|---|---|
| `fact_streams` | 275,575 | Fact | One row per valid play event |
| `dim_users` | 856 | Dimension | Active users, no PII |
| `dim_songs` | 500 | Dimension | Song attributes + genre |
| `dim_artists` | 150 | Dimension | Artist attributes |
| `dim_date` | 2,922 | Dimension | Date spine (2020–2027) |

**45,690 rows filtered during transform** — events with impossible durations, orphan user/song references, and inactive user records. All documented in `phase3_transform.sql`.

### fact_streams — key columns

| Column | Type | Description |
|---|---|---|
| `stream_key` | BIGSERIAL | Surrogate PK |
| `event_id` | VARCHAR UNIQUE | Natural key from source — dedup guard |
| `user_key` | INT FK | → dim_users |
| `song_key` | INT FK | → dim_songs |
| `artist_key` | INT FK | → dim_artists (denormalised for speed) |
| `date_key` | INT FK | → dim_date (YYYYMMDD integer) |
| `completion_pct` | NUMERIC(5,2) | % of song heard, capped at 100 |
| `was_skipped` | BOOLEAN | Core signal for recommendation quality |

---

## How to run (clone → full pipeline)

Use these steps to reproduce the project on a fresh machine after cloning from GitHub.

### Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.9+ |
| PostgreSQL | 14+ (`psql`, `createdb` on your PATH) |

### 1. Clone and enter the project

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd spotify-case-study   # use your repo folder name
```

### 2. Install PostgreSQL

**macOS (Homebrew)**

```bash
brew install postgresql@16
brew services start postgresql@16
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"   # add to ~/.zshrc to persist
```

**Ubuntu / Debian**

```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo service postgresql start
```

**Windows** — install [PostgreSQL](https://www.postgresql.org/download/windows/) and ensure `psql` is on your PATH.

Verify PostgreSQL is running:

```bash
pg_isready
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the database

```bash
createdb spotify_pipeline
```

If `createdb` fails with “role does not exist”, create a matching PostgreSQL user or set `DB_USER` (see [Environment variables](#environment-variables-optional) below).

### 5. Generate source data

JSON files live in `spotify_dataset/` and are **not committed to git** (`stream_events.json` is ~125 MB). Generate them once:

```bash
python3 spotify_dataset/generate_data.py
```

This writes four files (~321k stream events, takes 1–3 minutes). Re-run only if you want fresh synthetic data.

### 6. Run the pipeline (in order)

From the **project root** (where `phase1_schema.sql` lives):

```bash
# Phase 1 — schemas, staging tables, analytics star-schema DDL
psql -d spotify_pipeline -f phase1_schema.sql

# Phase 2 — load JSON into staging + populate dim_date
python3 phase2_load.py

# Phase 3 — transform staging → analytics (star schema)
psql -d spotify_pipeline -f phase3_transform.sql

# Phase 4 — Monday report (4 business queries)
psql -d spotify_pipeline -f phase4_queries.sql
```

**Phase 4 is the main output** — four result tables printed in the terminal (trending songs, skip rates by genre, free vs premium, regional breakouts).

### What you should see

| Step | Success signal |
|------|----------------|
| Phase 2 | `Pipeline complete in ~5–15s`; `pipeline.log` shows 321,265 stream events loaded |
| Phase 3 | `fact_streams` ≈ **275,575** rows; ~45,690 events filtered as bad/orphan |
| Phase 4 | Ranked top-10 songs, genre skip table, free/premium comparison, breakout artists |

Example Phase 4 headline (your numbers should match if you use the default seed in `generate_data.py`):

- **Q1:** BTS — *The Last* — 250 unique listeners  
- **Q2:** Metal — highest skip rate (~23.9%)  
- **Q3:** Premium users ~2.6× more plays than free  
- **Q4:** Makayla Christian (afrobeats, FR) — ~2.18× breakout score  

### Re-run safely

All transforms use `ON CONFLICT DO NOTHING`, so you can re-run Phase 3–4 without duplicating analytics rows. Phase 2 truncates staging tables before each load.

To reset everything:

```bash
dropdb spotify_pipeline && createdb spotify_pipeline
# then repeat steps 5–6
```

### Environment variables (optional)

`phase2_load.py` connects to `localhost` as your **OS username** by default (typical for Homebrew Postgres on Mac). Override if needed:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=spotify_pipeline
export DB_USER=your_postgres_user
export DB_PASSWORD=your_password   # leave unset for local peer/trust auth

python3 phase2_load.py
```

### Troubleshooting

| Problem | Fix |
|---------|-----|
| `psql: command not found` | Install PostgreSQL and add its `bin` directory to `PATH` (see step 2). |
| `Could not connect to database` | Run `brew services start postgresql@16` (Mac) or `sudo service postgresql start` (Linux). |
| `role "postgres" does not exist` (Mac) | Use your Mac username: `export DB_USER=$(whoami)` — this is the default in `phase2_load.py`. |
| `No such file: artists.json` | Run `python3 spotify_dataset/generate_data.py` first (step 5). |
| Phase 4 empty / wrong dates | Transforms use `CURRENT_DATE`; results for “last 7 days” depend on when you run the pipeline. |

---

## Key results

### Q1 — Top trending song (last 7 days)
BTS "The Last" — 250 unique listeners, 62.6% avg completion

### Q2 — Highest skip rate genre
Metal at 23.87% overall — but the free/premium gap tells the real story:
Free users skip metal at 31.6% vs premium at 19.9% — a 12-point gap that
signals the recommendation engine over-serves metal to users who won't commit.

### Q3 — Free vs Premium behaviour

| Metric | Free | Premium |
|---|---|---|
| Avg plays per user | 189 | 497 |
| Avg completion % | 56.9% | 67.1% |
| Skip rate | 31.7% | 18.2% |
| Top platform | Android | Android |

Premium users listen **2.6× more** and finish **18% more** of each song.

### Q4 — Top regional breakout
Makayla Christian (afrobeats, France) — 2.18× growth score, up from
11 listeners to 24 in 30 days. Early-stage signal before mainstream breakout.

---

## Engineering decisions worth noting

**Surrogate keys over natural keys** — `artist_key INTEGER` joins are ~3× faster
than `artist_id VARCHAR(36)`. Source IDs are preserved as UNIQUE natural keys.

**`ON CONFLICT DO NOTHING` on all inserts** — every transform is idempotent.
Re-running the pipeline never produces duplicate rows.

**`execute_values` for bulk loading** — sends 5,000 rows per round-trip vs one
row at a time. Achieved 41,786 rows/sec loading 321k events.

**`dim_date` pre-populated** — `week_number`, `is_weekend`, `day_name` are
computed once at setup. Analysts write `GROUP BY week_number` instead of
`EXTRACT(WEEK FROM listened_at)` in every query.

**`artist_key` denormalised onto `fact_streams`** — avoids a join through
`dim_songs` for every artist-level aggregation. Small storage cost, large
query speed gain.

**PII isolation** — `email` exists in `stg_users` (staging) only. The transform
that populates `dim_users` explicitly excludes it. There is no migration path
for email into the analytics layer.

---

## What I'd add with more time

- **Incremental loading** — replace full TRUNCATE/reload with timestamp-based
  delta loads using a `loaded_at` watermark
- **dbt models** — replace raw SQL transforms with dbt for lineage, testing,
  and documentation
- **Airflow DAG** — schedule the pipeline and add retry/alerting logic
- **Great Expectations** — add a data quality framework with explicit contracts
  on each staging table
- **Partitioning** — partition `fact_streams` by month for query performance
  at 100M+ row scale

---

## Dependencies

```
psycopg2-binary>=2.9
faker>=24.0
python-dotenv>=1.0
```
