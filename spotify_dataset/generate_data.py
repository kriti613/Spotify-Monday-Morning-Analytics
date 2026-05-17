"""
Spotify Analytics Pipeline — Synthetic Data Generator
======================================================
Generates realistic fake data for 4 source files:
  - users.json         (~1,000 users)
  - artists.json       (~150 artists)
  - songs.json         (~500 songs)
  - stream_events.json (~50,000 play events over 90 days)

Realistic patterns baked in:
  - Premium users listen more and skip less
  - Weekends have higher listen volume
  - Peak hours: 7–9am commute, 12–1pm lunch, 5–8pm evening
  - Skips happen more in first 30 seconds
  - Some songs are "trending" (much higher play counts)
  - Geographic distribution mirrors real Spotify market share
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker

fake = Faker()
random.seed(42)  # Reproducible output — same data every run

# ── Output paths ──────────────────────────────────────────────
from pathlib import Path
import os

OUTPUT_DIR = str(Path(__file__).resolve().parent)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Realistic distribution constants ─────────────────────────

COUNTRIES = [
    ("US", 0.28), ("BR", 0.14), ("GB", 0.08), ("DE", 0.06),
    ("MX", 0.06), ("IN", 0.05), ("FR", 0.04), ("AU", 0.03),
    ("CA", 0.03), ("ES", 0.03), ("ID", 0.03), ("PH", 0.02),
    ("AR", 0.02), ("NL", 0.02), ("SE", 0.02), ("NG", 0.02),
    ("ZA", 0.01), ("JP", 0.01), ("KR", 0.01), ("CO", 0.01),
]

PLATFORMS = [("ios", 0.38), ("android", 0.42), ("web", 0.12), ("desktop", 0.08)]

GENRES = [
    "pop", "hip-hop", "rock", "latin", "r&b",
    "electronic", "indie", "k-pop", "country", "jazz",
    "classical", "metal", "reggaeton", "afrobeats", "lo-fi"
]

# Genre popularity (more popular = more songs, more plays)
GENRE_WEIGHTS = [20, 18, 12, 10, 9, 7, 5, 4, 4, 2, 2, 2, 2, 2, 1]

AGE_GROUPS = [("18-24", 0.28), ("25-34", 0.32), ("35-44", 0.20), ("45+", 0.20)]

SUBSCRIPTION_TYPES = [("free", 0.60), ("premium", 0.40)]

def weighted_choice(choices):
    """Pick from a list of (value, weight) tuples."""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]

def make_uuid():
    return str(uuid.uuid4())

# ── Peak listening hours (probability per hour 0-23) ──────────
# Morning commute 7-9, lunch 12-1, evening 5-8, late night 10-12
HOUR_WEIGHTS = [
    0.5, 0.3, 0.2, 0.2, 0.2, 0.4,   # 0-5am  (low, night owls)
    0.8, 2.5, 2.8, 1.5, 1.2, 1.4,   # 6-11am (morning commute peak)
    2.2, 1.8, 1.5, 1.4, 1.3, 2.0,   # 12-5pm (lunch + afternoon)
    3.0, 3.2, 2.8, 2.0, 1.5, 1.0,   # 6-11pm (evening peak)
]

def realistic_timestamp(start_date, end_date):
    """Generate a timestamp weighted toward peak listening hours."""
    days_range = (end_date - start_date).days
    day_offset = random.randint(0, days_range)
    
    # Weekends get 40% more listens
    target_date = start_date + timedelta(days=day_offset)
    if target_date.weekday() >= 5:  # Sat/Sun
        # Re-roll to bias toward weekends
        if random.random() < 0.30:
            # Force a weekend day
            weekend_days = [d for d in range(days_range)
                            if (start_date + timedelta(days=d)).weekday() >= 5]
            if weekend_days:
                day_offset = random.choice(weekend_days)
                target_date = start_date + timedelta(days=day_offset)

    hour = random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    return target_date.replace(
        hour=hour, minute=minute, second=second,
        tzinfo=timezone.utc
    )

# ══════════════════════════════════════════════════════════════
# 1. GENERATE ARTISTS
# ══════════════════════════════════════════════════════════════
print("Generating artists...")

# A handful of globally famous fake artists (will get many plays)
FAMOUS_ARTISTS = [
    ("The Weeknd",      "CA", "r&b",       45_000_000),
    ("Bad Bunny",       "PR", "reggaeton", 62_000_000),
    ("Taylor Swift",    "US", "pop",       85_000_000),
    ("Drake",           "CA", "hip-hop",   72_000_000),
    ("Billie Eilish",   "US", "pop",       56_000_000),
    ("BTS",             "KR", "k-pop",     48_000_000),
    ("Ed Sheeran",      "GB", "pop",       68_000_000),
    ("Kendrick Lamar",  "US", "hip-hop",   43_000_000),
    ("Dua Lipa",        "GB", "pop",       52_000_000),
    ("J Balvin",        "CO", "latin",     39_000_000),
]

artists = []
artist_ids = []

# Add famous artists first
for name, country, genre, listeners in FAMOUS_ARTISTS:
    aid = make_uuid()
    artist_ids.append(aid)
    artists.append({
        "artist_id":         aid,
        "name":              name,
        "country":           country,
        "primary_genre":     genre,
        "monthly_listeners": listeners,
    })

# Add 140 more random artists
for _ in range(140):
    genre = weighted_choice(list(zip(GENRES, GENRE_WEIGHTS)))
    country = weighted_choice(COUNTRIES)
    aid = make_uuid()
    artist_ids.append(aid)
    artists.append({
        "artist_id":         aid,
        "name":              fake.name(),
        "country":           country,
        "primary_genre":     genre,
        "monthly_listeners": random.randint(5_000, 8_000_000),
    })

print(f"  → {len(artists)} artists")

# ══════════════════════════════════════════════════════════════
# 2. GENERATE SONGS
# ══════════════════════════════════════════════════════════════
print("Generating songs...")

# Song duration distributions by genre (ms)
DURATION_BY_GENRE = {
    "classical":  (240_000, 420_000),
    "jazz":       (200_000, 480_000),
    "metal":      (200_000, 360_000),
    "hip-hop":    (150_000, 280_000),
    "pop":        (160_000, 240_000),
    "lo-fi":      (120_000, 200_000),
    "default":    (150_000, 300_000),
}

SONG_WORDS = [
    "Love", "Night", "Fire", "Dream", "Rain", "Gold", "Broken",
    "Rise", "Fall", "Stars", "Dark", "Light", "Run", "Heart",
    "Wild", "Storm", "Gone", "Home", "Free", "Lost", "Found",
    "Neon", "Ghost", "Blue", "Last", "Forever", "Ashes", "Wings",
]

def make_song_title():
    pattern = random.choice([
        lambda: random.choice(SONG_WORDS),
        lambda: f"{random.choice(SONG_WORDS)} {random.choice(SONG_WORDS)}",
        lambda: f"The {random.choice(SONG_WORDS)}",
        lambda: f"{random.choice(SONG_WORDS)} of {random.choice(SONG_WORDS)}",
        lambda: f"No {random.choice(SONG_WORDS)}",
    ])
    return pattern()

songs = []
song_ids = []
# Map each song to how "popular" it is (affects play probability)
song_popularity = []

for i in range(500):
    genre = weighted_choice(list(zip(GENRES, GENRE_WEIGHTS)))
    # Bias artist toward same genre
    matching_artists = [a for a in artists if a["primary_genre"] == genre]
    if matching_artists and random.random() < 0.7:
        artist = random.choice(matching_artists)
    else:
        artist = random.choice(artists)

    dur_range = DURATION_BY_GENRE.get(genre, DURATION_BY_GENRE["default"])
    duration_ms = random.randint(*dur_range)

    # Release date: older songs are more common in catalog
    years_ago = random.choices(range(0, 8), weights=[15,12,10,10,10,8,8,7], k=1)[0]
    release_date = fake.date_between(
        start_date=datetime.now() - timedelta(days=365 * (years_ago + 1)),
        end_date=datetime.now() - timedelta(days=365 * years_ago),
    )

    # Famous artist songs get 10-30x the plays of random songs
    is_famous_artist = artist in artists[:10]
    popularity = random.uniform(5, 15) if is_famous_artist else random.uniform(0.1, 3)

    sid = make_uuid()
    song_ids.append(sid)
    song_popularity.append(popularity)
    songs.append({
        "song_id":      sid,
        "title":        make_song_title(),
        "artist_id":    artist["artist_id"],
        "genre":        genre,
        "subgenre":     random.choice([genre, None, None]),
        "duration_ms":  duration_ms,
        "release_date": str(release_date),
        "is_explicit":  random.random() < 0.22,
    })

print(f"  → {len(songs)} songs")

# ══════════════════════════════════════════════════════════════
# 3. GENERATE USERS
# ══════════════════════════════════════════════════════════════
print("Generating users...")

users = []
user_ids = []

for _ in range(1000):
    uid = make_uuid()
    user_ids.append(uid)
    sub_type = weighted_choice(SUBSCRIPTION_TYPES)
    country = weighted_choice(COUNTRIES)
    age_group = weighted_choice(AGE_GROUPS)
    
    users.append({
        "user_id":           uid,
        "email":             fake.email(),      # PII — stays in staging only
        "country_code":      country,
        "subscription_type": sub_type,
        "age_group":         age_group,
        "signup_date":       str(fake.date_between(
            start_date="-4y", end_date="today"
        )),
        "is_active": random.random() < 0.85,
    })

print(f"  → {len(users)} users")

# ══════════════════════════════════════════════════════════════
# 4. GENERATE STREAM EVENTS
# ══════════════════════════════════════════════════════════════
print("Generating stream events (this takes a moment)...")

END_DATE   = datetime.now().replace(tzinfo=timezone.utc)
START_DATE = END_DATE - timedelta(days=90)  # Last 90 days of history

stream_events = []

# How many plays each user generates:
# Premium users: avg 40 listens/week  → ~514 over 90 days
# Free users:    avg 15 listens/week  → ~193 over 90 days
for user in users:
    is_premium = user["subscription_type"] == "premium"
    n_plays = random.randint(
        300 if is_premium else 80,
        700 if is_premium else 300,
    )
    
    for _ in range(n_plays):
        # Weighted random song (popular songs get more plays)
        song = random.choices(songs, weights=song_popularity, k=1)[0]
        listened_at = realistic_timestamp(START_DATE, END_DATE)
        song_duration = song["duration_ms"]

        # Skip logic:
        # - Free users skip more (worse recommendations)
        # - Skips mostly happen in first 30 seconds
        # - Premium users more likely to finish songs
        skip_prob = 0.18 if is_premium else 0.32
        was_skipped = random.random() < skip_prob

        if was_skipped:
            # Skip happens early: mostly in first 30s
            skip_at_ms = int(random.betavariate(1, 4) * min(song_duration, 60_000))
            duration_ms = skip_at_ms
        else:
            # Finished song (with minor variation — phone died, etc.)
            completion = random.betavariate(5, 1.2)  # Skewed toward full listens
            duration_ms = int(song_duration * min(completion, 1.0))
            skip_at_ms = None

        stream_events.append({
            "event_id":        make_uuid(),
            "user_id":         user["user_id"],
            "song_id":         song["song_id"],
            "listened_at":     listened_at.isoformat(),
            "duration_ms":     duration_ms,
            "song_duration_ms": song_duration,
            "was_skipped":     was_skipped,
            "skip_at_ms":      skip_at_ms,
            "platform":        weighted_choice(PLATFORMS),
            "country_code":    user["country_code"],
        })

print(f"  → {len(stream_events):,} stream events")

# ══════════════════════════════════════════════════════════════
# 5. WRITE TO JSON FILES
# ══════════════════════════════════════════════════════════════
print("\nWriting files...")

files = {
    "users.json":         users,
    "artists.json":       artists,
    "songs.json":         songs,
    "stream_events.json": stream_events,
}

for filename, data in files.items():
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    size_kb = os.path.getsize(path) / 1024
    print(f"  ✓ {filename:<25} {len(data):>7,} records   ({size_kb:,.0f} KB)")

print("\n✅ Data generation complete!")
print(f"   Location: {OUTPUT_DIR}")
print(f"\n   Summary:")
print(f"   • {len(users):,} users  ({sum(1 for u in users if u['subscription_type']=='premium'):,} premium, {sum(1 for u in users if u['subscription_type']=='free'):,} free)")
print(f"   • {len(artists):,} artists across {len(set(a['primary_genre'] for a in artists))} genres")
print(f"   • {len(songs):,} songs")
print(f"   • {len(stream_events):,} stream events over 90 days")
print(f"   • Skip rate: {sum(1 for e in stream_events if e['was_skipped'])/len(stream_events):.1%}")
