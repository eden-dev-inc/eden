"""
ADAM Demo — Data Initializer
Loads T-ECD (T-Tech E-commerce Cross-Domain) dataset into all databases.
All domains share user_id and brand_id, enabling meaningful cross-DB queries.

Source: https://huggingface.co/datasets/t-tech/T-ECD

Databases & Domains:
  PostgreSQL  ← users + brands (shared catalogs) + marketplace events (OLTP)
  MongoDB     ← retail events + items (document store)
  Redis       ← offers events + items (real-time cache, leaderboards)
  ClickHouse  ← marketplace events (OLAP analytics, same data as PG)
  Weaviate    ← reviews (vector search on review embeddings)
"""

import os
import re
import time
import logging
from datetime import datetime
from pathlib import Path
import random

import numpy as np
import pandas as pd

# --- Local data directory (bundled Parquet files) ---
DATA_DIR = Path(__file__).parent / "data"

# --- Config ---
POSTGRES_URL = os.environ["POSTGRES_URL"]
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")
REDIS_URL = os.environ["REDIS_URL"]
CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8323))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "eden")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "eden")
WEAVIATE_URL = os.environ["WEAVIATE_URL"]
VERTICAL = os.environ.get("VERTICAL", "retail").lower()

WEAVIATE_LIMIT = int(os.environ.get("WEAVIATE_LIMIT", "10000"))
STONEBREAKER_DOC_LIMITS = {
    "demo": 250,
    "small": 500,
    "medium": 1000,
    "large": 2000,
    "massive": 3500,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("adam-init")

DISABLE_CLICKHOUSE_INIT = os.environ.get("DISABLE_CLICKHOUSE_INIT", "1").lower() in {
    "1",
    "true",
    "yes",
}

# ── Scale limits for T-ECD dataset loading ──────────────────────
# Controls how many rows to load from each bundled parquet file.
# None means load all rows (no limit).
TECD_SCALE_LIMITS = {
    "demo":    {"users": 5_000,   "brands": None, "items": 2_000,   "events": 50_000,  "reviews": 2_000},
    "small":   {"users": 50_000,  "brands": None, "items": 10_000,  "events": 200_000, "reviews": 5_000},
    "medium":  {"users": 500_000, "brands": None, "items": 50_000,  "events": 1_000_000, "reviews": 10_000},
    "large":   None,  # no limits
    "massive": None,  # no limits
}

SCALE = os.environ.get("SCALE", "demo").lower()


def _row_limit(category: str) -> int | None:
    """Return the row limit for a parquet category at the current scale, or None for unlimited."""
    limits = TECD_SCALE_LIMITS.get(SCALE)
    if limits is None:
        return None
    return limits.get(category)

# --- HuggingFace download config (fallback if local files missing) ---
TECD_REPO = "t-tech/T-ECD"
TECD_BASE = "dataset/small"
TECD_MAX_DAYS = 5


def load_parquet(filename, limit: int | None = None):
    """Load a bundled Parquet file from data/, optionally limiting rows."""
    path = DATA_DIR / filename
    if path.exists():
        log.info(f"  Loading from local file: {filename}")
        df = pd.read_parquet(path)
        if limit is not None and len(df) > limit:
            log.info(f"  Scale={SCALE}: limiting {len(df):,} rows → {limit:,}")
            df = df.head(limit)
        return df
    return None


def download_parquet(remote_path):
    """Download a Parquet file from HuggingFace and return as DataFrame."""
    from huggingface_hub import hf_hub_download
    local = hf_hub_download(repo_id=TECD_REPO, filename=remote_path, repo_type="dataset")
    return pd.read_parquet(local)


def extract_day(df):
    """Extract integer day from the timestamp column (timedelta)."""
    if "timestamp" in df.columns:
        ts = df["timestamp"]
        if pd.api.types.is_timedelta64_dtype(ts):
            df["event_day"] = ts.dt.days
        else:
            df["event_day"] = 0
    else:
        df["event_day"] = 0
    return df


class LogProgress:
    """Log-friendly progress tracker."""
    def __init__(self, desc, total=None, log_every=10):
        self.desc = desc
        self.total = total
        self.count = 0
        self.log_every = log_every
        self.last_pct = -1
        self.start = time.time()

    def update(self, n=1):
        self.count += n
        if self.total and self.total > 0:
            pct = int(100 * self.count / self.total)
            if pct >= self.last_pct + self.log_every:
                self.last_pct = pct
                elapsed = time.time() - self.start
                log.info(f"  [{self.desc}] {self.count:,}/{self.total:,} ({pct}%) — {elapsed:.1f}s")

    def finish(self):
        elapsed = time.time() - self.start
        if self.total:
            log.info(f"  [{self.desc}] Done: {self.count:,}/{self.total:,} in {elapsed:.1f}s")
        else:
            log.info(f"  [{self.desc}] Done: {self.count:,} rows in {elapsed:.1f}s")


def safe_int(val, default=0):
    try:
        if pd.isna(val):
            return default
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_str(val, default=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val)


def safe_year(val, default=0):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    if isinstance(val, pd.Timestamp):
        return int(val.year)
    try:
        text = str(val)
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
        return int(val)
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════
# 1. PostgreSQL — Shared catalogs + Marketplace events (OLTP)
# ═══════════════════════════════════════════════════════════════

def load_postgres():
    import psycopg2
    from psycopg2.extras import execute_values

    log.info("Loading T-ECD data into PostgreSQL (users, brands, marketplace)...")

    conn = psycopg2.connect(POSTGRES_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Check if already loaded
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] > 0:
        log.info("PostgreSQL already populated, skipping")
        conn.close()
        return

    batch_size = 5000

    # --- Load users catalog ---
    users_df = load_parquet("tecd_users.parquet", limit=_row_limit("users"))
    if users_df is None:
        log.info("  Downloading users from HuggingFace...")
        users_df = download_parquet(f"{TECD_BASE}/users.pq")
        limit = _row_limit("users")
        if limit is not None and len(users_df) > limit:
            users_df = users_df.head(limit)

    log.info(f"  Loading {len(users_df):,} users...")
    user_rows = []
    for _, r in users_df.iterrows():
        user_rows.append((
            safe_int(r["user_id"]),
            safe_int(r.get("region"), None),
            safe_int(r.get("socdem_cluster"), None),
        ))

    for i in range(0, len(user_rows), batch_size):
        execute_values(cur,
            "INSERT INTO users (user_id, region, socdem_cluster) VALUES %s ON CONFLICT DO NOTHING",
            user_rows[i:i+batch_size])
    log.info(f"  {len(user_rows):,} users loaded")

    # --- Load brands catalog ---
    brands_df = load_parquet("tecd_brands.parquet", limit=_row_limit("brands"))
    if brands_df is None:
        log.info("  Downloading brands from HuggingFace...")
        brands_df = download_parquet(f"{TECD_BASE}/brands.pq")
        limit = _row_limit("brands")
        if limit is not None and len(brands_df) > limit:
            brands_df = brands_df.head(limit)

    log.info(f"  Loading {len(brands_df):,} brands...")
    brand_rows = [(safe_int(r["brand_id"]),) for _, r in brands_df.iterrows()]

    for i in range(0, len(brand_rows), batch_size):
        execute_values(cur,
            "INSERT INTO brands (brand_id) VALUES %s ON CONFLICT DO NOTHING",
            brand_rows[i:i+batch_size])
    log.info(f"  {len(brand_rows):,} brands loaded")

    # --- Load marketplace items ---
    items_df = load_parquet("tecd_marketplace_items.parquet", limit=_row_limit("items"))
    if items_df is None:
        try:
            log.info("  Downloading marketplace items from HuggingFace...")
            items_df = download_parquet(f"{TECD_BASE}/marketplace/items.pq")
            limit = _row_limit("items")
            if limit is not None and len(items_df) > limit:
                items_df = items_df.head(limit)
        except Exception as e:
            log.warning(f"  No marketplace items file: {e}")

    if items_df is not None:
        log.info(f"  Loading {len(items_df):,} marketplace items...")
        item_rows = []
        for _, r in items_df.iterrows():
            item_rows.append((
                safe_str(r["item_id"]),
                safe_int(r.get("brand_id"), None),
                safe_str(r.get("category", "")),
                safe_str(r.get("subcategory", "")),
                safe_float(r.get("price"), None),
            ))

        for i in range(0, len(item_rows), batch_size):
            execute_values(cur,
                """INSERT INTO marketplace_items (item_id, brand_id, category, subcategory, price)
                   VALUES %s ON CONFLICT DO NOTHING""",
                item_rows[i:i+batch_size])
        log.info(f"  {len(item_rows):,} marketplace items loaded")

    # --- Load marketplace events ---
    events_df = load_parquet("tecd_marketplace_events.parquet", limit=_row_limit("events"))
    if events_df is None:
        log.info("  Downloading marketplace events from HuggingFace...")
        frames = []
        for day in range(TECD_MAX_DAYS):
            try:
                frames.append(download_parquet(f"{TECD_BASE}/marketplace/events/{day}.pq"))
            except Exception:
                break
        if frames:
            events_df = pd.concat(frames, ignore_index=True)
            limit = _row_limit("events")
            if limit is not None and len(events_df) > limit:
                events_df = events_df.head(limit)

    if events_df is not None:
        events_df = extract_day(events_df)
        total = len(events_df)
        log.info(f"  Loading {total:,} marketplace events...")
        progress = LogProgress("PG marketplace events", total=total)

        event_batch = []
        for _, r in events_df.iterrows():
            event_batch.append((
                safe_int(r.get("user_id"), None),
                safe_str(r.get("item_id", "")),
                safe_str(r.get("action_type", "")),
                safe_str(r.get("subdomain", "")),
                safe_str(r.get("os", "")),
                safe_int(r.get("event_day", 0)),
            ))

            if len(event_batch) >= batch_size:
                execute_values(cur,
                    """INSERT INTO marketplace_events
                       (user_id, item_id, action_type, subdomain, os, event_day)
                       VALUES %s""",
                    event_batch)
                progress.update(len(event_batch))
                event_batch = []

        if event_batch:
            execute_values(cur,
                """INSERT INTO marketplace_events
                   (user_id, item_id, action_type, subdomain, os, event_day)
                   VALUES %s""",
                event_batch)
            progress.update(len(event_batch))
        progress.finish()

    conn.close()


# ═══════════════════════════════════════════════════════════════
# 2. MongoDB — Retail events + items (document store)
# ═══════════════════════════════════════════════════════════════

def load_mongodb():
    from pymongo import MongoClient

    log.info("Loading T-ECD retail data into MongoDB...")

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]

    # --- Retail items ---
    items_col = db["retail_items"]
    if items_col.count_documents({}) > 0:
        log.info("MongoDB already populated, skipping")
        client.close()
        return

    items_df = load_parquet("tecd_retail_items.parquet", limit=_row_limit("items"))
    if items_df is None:
        try:
            log.info("  Downloading retail items from HuggingFace...")
            items_df = download_parquet(f"{TECD_BASE}/retail/items.pq")
            limit = _row_limit("items")
            if limit is not None and len(items_df) > limit:
                items_df = items_df.head(limit)
        except Exception as e:
            log.warning(f"  No retail items file: {e}")

    if items_df is not None:
        # Drop embedding column if present (large, not needed for queries)
        for col in ("embedding", "embeddings"):
            if col in items_df.columns:
                items_df = items_df.drop(columns=[col])

        log.info(f"  Loading {len(items_df):,} retail items...")
        docs = []
        for _, r in items_df.iterrows():
            doc = {col: _to_native(r[col]) for col in items_df.columns}
            docs.append(doc)
            if len(docs) >= 2000:
                items_col.insert_many(docs)
                docs = []
        if docs:
            items_col.insert_many(docs)
        items_col.create_index("item_id")
        items_col.create_index("brand_id")
        items_col.create_index("category")
        log.info(f"  {items_col.count_documents({}):,} retail items loaded")

    # --- Retail events ---
    events_col = db["retail_events"]
    events_df = load_parquet("tecd_retail_events.parquet", limit=_row_limit("events"))
    if events_df is None:
        log.info("  Downloading retail events from HuggingFace...")
        frames = []
        for day in range(TECD_MAX_DAYS):
            try:
                frames.append(download_parquet(f"{TECD_BASE}/retail/events/{day}.pq"))
            except Exception:
                break
        if frames:
            events_df = pd.concat(frames, ignore_index=True)
            limit = _row_limit("events")
            if limit is not None and len(events_df) > limit:
                events_df = events_df.head(limit)

    if events_df is not None:
        events_df = extract_day(events_df)
        # Drop raw timestamp (timedelta not BSON-compatible)
        if "timestamp" in events_df.columns:
            events_df = events_df.drop(columns=["timestamp"])

        total = len(events_df)
        log.info(f"  Loading {total:,} retail events into MongoDB...")
        progress = LogProgress("Retail events", total=total)

        docs = []
        for _, r in events_df.iterrows():
            doc = {col: _to_native(r[col]) for col in events_df.columns}
            docs.append(doc)
            progress.update()

            if len(docs) >= 2000:
                events_col.insert_many(docs)
                docs = []

        if docs:
            events_col.insert_many(docs)
        progress.finish()

        events_col.create_index("user_id")
        events_col.create_index("item_id")
        events_col.create_index("action_type")
        events_col.create_index("event_day")

    log.info(f"  {events_col.count_documents({}):,} retail events loaded into MongoDB")
    client.close()


def _to_native(val):
    """Convert numpy/pandas types to Python native types for MongoDB."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    if isinstance(val, pd.Timedelta):
        return val.total_seconds()
    if pd.isna(val):
        return None
    return val


# ═══════════════════════════════════════════════════════════════
# 3. Redis — Offers events + items (real-time cache)
# ═══════════════════════════════════════════════════════════════

def load_redis():
    import redis as r

    log.info("Loading T-ECD offers data into Redis...")

    client = r.from_url(REDIS_URL, decode_responses=True)

    if client.dbsize() > 0:
        log.info("Redis already populated, skipping")
        client.close()
        return

    pipe = client.pipeline()

    # --- Load offer items as hashes ---
    items_df = load_parquet("tecd_offers_items.parquet", limit=_row_limit("items"))
    if items_df is None:
        try:
            log.info("  Downloading offers items from HuggingFace...")
            items_df = download_parquet(f"{TECD_BASE}/offers/items.pq")
            limit = _row_limit("items")
            if limit is not None and len(items_df) > limit:
                items_df = items_df.head(limit)
        except Exception as e:
            log.warning(f"  No offers items: {e}")

    if items_df is not None:
        log.info(f"  Loading {len(items_df):,} offer items...")
        for _, r_row in items_df.iterrows():
            item_id = safe_str(r_row.get("item_id", ""))
            item_data = {}
            for col in items_df.columns:
                if col in ("embedding", "embeddings"):
                    continue
                val = r_row[col]
                if not (isinstance(val, float) and pd.isna(val)):
                    item_data[col] = str(val)
            if item_data:
                pipe.hset(f"offer:item:{item_id}", mapping=item_data)

    # --- Load offer events and build aggregates ---
    events_df = load_parquet("tecd_offers_events.parquet", limit=_row_limit("events"))
    if events_df is None:
        log.info("  Downloading offers events from HuggingFace...")
        frames = []
        for day in range(TECD_MAX_DAYS):
            try:
                frames.append(download_parquet(f"{TECD_BASE}/offers/events/{day}.pq"))
            except Exception:
                break
        if frames:
            events_df = pd.concat(frames, ignore_index=True)
            limit = _row_limit("events")
            if limit is not None and len(events_df) > limit:
                events_df = events_df.head(limit)

    if events_df is not None:
        log.info(f"  Processing {len(events_df):,} offer events into Redis structures...")

        # Build leaderboards from events
        item_engagement = {}
        user_offer_counts = {}
        action_counts = {}

        for _, r_row in events_df.iterrows():
            user_id = safe_int(r_row.get("user_id"), 0)
            action = safe_str(r_row.get("action_type", "view"))
            item_id = safe_str(r_row.get("item_id", ""))

            item_engagement[item_id] = item_engagement.get(item_id, 0) + 1
            user_offer_counts[user_id] = user_offer_counts.get(user_id, 0) + 1
            action_counts[action] = action_counts.get(action, 0) + 1

        # Store leaderboards
        log.info("  Building offer leaderboards...")
        for item_id, score in item_engagement.items():
            pipe.zadd("leaderboard:offer_items", {item_id: score})

        for user_id, count in list(user_offer_counts.items())[:50000]:
            pipe.zadd("leaderboard:user_offer_activity", {f"user:{user_id}": count})

        # Store aggregate stats
        for action, count in action_counts.items():
            pipe.set(f"stats:offers:{action}", count)

        pipe.set("stats:total_offer_events", len(events_df))
        pipe.set("stats:unique_offer_users", len(user_offer_counts))
        pipe.set("stats:unique_offer_items", len(item_engagement))

    # --- User sessions keyed by shared user_ids ---
    log.info("  Generating user sessions from offer data...")

    if events_df is not None and "user_id" in events_df.columns:
        sample_users = events_df["user_id"].dropna().unique()[:5000]
    else:
        sample_users = range(1, 5001)

    for user_id in sample_users:
        uid = safe_int(user_id)
        session = {
            "user_id": str(uid),
            "last_active": datetime.now().isoformat(),
            "offer_views": str(random.randint(0, 50)),
            "offer_clicks": str(random.randint(0, 20)),
            "loyalty_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        }
        pipe.hset(f"session:{uid}", mapping=session)

    pipe.execute()
    log.info(f"  {client.dbsize():,} keys loaded into Redis")
    client.close()


# ═══════════════════════════════════════════════════════════════
# 4. ClickHouse — Marketplace events (OLAP analytics)
# ═══════════════════════════════════════════════════════════════

def load_clickhouse():
    import clickhouse_connect

    log.info("Loading T-ECD marketplace events into ClickHouse (analytics)...")

    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    count = ch.query("SELECT count() FROM analytics.marketplace_events").result_rows[0][0]
    if count > 0:
        log.info("ClickHouse already populated, skipping")
        ch.close()
        return

    # Load the same marketplace events used for PostgreSQL (OLTP → OLAP replication)
    events_df = load_parquet("tecd_marketplace_events.parquet", limit=_row_limit("events"))
    if events_df is None:
        log.info("  Downloading marketplace events from HuggingFace...")
        frames = []
        for day in range(TECD_MAX_DAYS):
            try:
                frames.append(download_parquet(f"{TECD_BASE}/marketplace/events/{day}.pq"))
            except Exception:
                break
        if frames:
            events_df = pd.concat(frames, ignore_index=True)
            limit = _row_limit("events")
            if limit is not None and len(events_df) > limit:
                events_df = events_df.head(limit)

    if events_df is None:
        log.warning("  No marketplace events available for ClickHouse")
        ch.close()
        return

    events_df = extract_day(events_df)
    total = len(events_df)
    log.info(f"  Loading {total:,} marketplace events into ClickHouse...")
    progress = LogProgress("CH marketplace events", total=total)

    ch_columns = ["user_id", "item_id", "action_type", "subdomain", "os", "event_day"]

    batch_size = 50000
    batch = []
    for _, row in events_df.iterrows():
        batch.append([
            safe_int(row.get("user_id"), 0),
            safe_str(row.get("item_id", "")),
            safe_str(row.get("action_type", "")),
            safe_str(row.get("subdomain", "")),
            safe_str(row.get("os", "")),
            safe_int(row.get("event_day", 0)),
        ])

        if len(batch) >= batch_size:
            ch.insert("analytics.marketplace_events", batch, column_names=ch_columns)
            progress.update(len(batch))
            batch = []

    if batch:
        ch.insert("analytics.marketplace_events", batch, column_names=ch_columns)
        progress.update(len(batch))
    progress.finish()

    # Populate daily action summary
    ch.command("""
        INSERT INTO analytics.daily_action_summary
        SELECT
            event_day,
            action_type,
            subdomain,
            count() AS event_count,
            uniq(user_id) AS unique_users
        FROM analytics.marketplace_events
        GROUP BY event_day, action_type, subdomain
    """)

    count = ch.query("SELECT count() FROM analytics.marketplace_events").result_rows[0][0]
    log.info(f"  {count:,} events loaded into ClickHouse")
    ch.close()


# ═══════════════════════════════════════════════════════════════
# 5. Weaviate — Reviews (vector search)
# ═══════════════════════════════════════════════════════════════

def load_weaviate():
    import weaviate
    from weaviate.classes.config import Configure, Property, DataType

    log.info("Loading T-ECD reviews into Weaviate for vector search...")

    client = weaviate.connect_to_custom(
        http_host=WEAVIATE_URL.replace("http://", "").split(":")[0],
        http_port=int(WEAVIATE_URL.split(":")[-1]),
        http_secure=False,
        grpc_host=WEAVIATE_URL.replace("http://", "").split(":")[0],
        grpc_port=50051,
        grpc_secure=False,
    )

    # Check if already populated
    if client.collections.exists("Review"):
        collection = client.collections.get("Review")
        resp = collection.aggregate.over_all(total_count=True)
        if resp.total_count > 0:
            log.info("Weaviate already populated, skipping")
            client.close()
            return

    # Create Review collection
    if not client.collections.exists("Review"):
        client.collections.create(
            name="Review",
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="user_id", data_type=DataType.INT),
                Property(name="brand_id", data_type=DataType.INT),
                Property(name="rating", data_type=DataType.INT),
                Property(name="event_day", data_type=DataType.INT),
            ],
        )

    # Load embedding model — reviews don't have pretrained embeddings in the
    # stripped parquet, so we generate them from a text representation
    log.info("  Loading sentence-transformers model (all-MiniLM-L6-v2)...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")

    reviews_df = load_parquet("tecd_reviews.parquet", limit=_row_limit("reviews"))
    if reviews_df is None:
        log.info("  Downloading reviews from HuggingFace...")
        from huggingface_hub import hf_hub_download, list_repo_tree
        prefix = f"{TECD_BASE}/reviews/"
        try:
            entries = list_repo_tree(TECD_REPO, path_in_repo=prefix, repo_type="dataset")
            day_files = sorted([e.rfilename for e in entries if e.rfilename.endswith(".pq")])[:TECD_MAX_DAYS]
        except Exception:
            day_files = []

        frames = []
        for fpath in day_files:
            try:
                local = hf_hub_download(repo_id=TECD_REPO, filename=fpath, repo_type="dataset")
                frames.append(pd.read_parquet(local))
            except Exception:
                pass
        if frames:
            reviews_df = pd.concat(frames, ignore_index=True)
            limit = _row_limit("reviews")
            if limit is not None and len(reviews_df) > limit:
                reviews_df = reviews_df.head(limit)

    if reviews_df is None:
        log.warning("  No review data available, generating synthetic")
        _load_weaviate_synthetic_reviews(client, model)
    else:
        reviews_df = extract_day(reviews_df)
        review_collection = client.collections.get("Review")

        limit = min(len(reviews_df), WEAVIATE_LIMIT)
        log.info(f"  Embedding {limit:,} reviews...")

        # Generate text representations for embedding
        # (T-ECD reviews have rating + brand_id but no raw text)
        rating_labels = {
            1: "terrible awful worst experience never again",
            2: "poor disappointed below expectations not good",
            3: "average okay acceptable nothing special mediocre",
            4: "good satisfied happy quality recommended",
            5: "excellent outstanding amazing best perfect love",
        }

        batch_size = 100
        progress = LogProgress("Review embeddings", total=limit)

        for start in range(0, limit, batch_size):
            end = min(start + batch_size, limit)
            chunk = reviews_df.iloc[start:end]

            texts = []
            for _, r in chunk.iterrows():
                rating = safe_int(r.get("rating", 3))
                rating = max(1, min(5, rating))
                brand_id = safe_int(r.get("brand_id", 0))
                texts.append(f"Brand {brand_id} review: {rating_labels[rating]}")

            embeddings = model.encode(texts, show_progress_bar=False)

            with review_collection.batch.dynamic() as batch:
                for j, (_, r) in enumerate(chunk.iterrows()):
                    batch.add_object(
                        properties={
                            "user_id": safe_int(r.get("user_id"), 0),
                            "brand_id": safe_int(r.get("brand_id"), 0),
                            "rating": safe_int(r.get("rating", 0)),
                            "event_day": safe_int(r.get("event_day", 0)),
                        },
                        vector=embeddings[j].tolist(),
                    )
            progress.update(end - start)
        progress.finish()

    if VERTICAL == "stonebreaker":
        _load_stonebreaker_documents(client, model)

    client.close()


def _stonebreaker_document_limit() -> int:
    return int(
        os.environ.get(
            "STONEBREAKER_DOC_LIMIT",
            str(STONEBREAKER_DOC_LIMITS.get(SCALE, STONEBREAKER_DOC_LIMITS["demo"])),
        )
    )


def _stonebreaker_localfs_output_dir() -> Path:
    return Path(
        os.environ.get(
            "STONEBREAKER_LOCALFS_OUTPUT_DIR",
            str(DATA_DIR / "stonebreaker" / "localfs"),
        )
    )


def _stonebreaker_slug(value: str, fallback: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return text or fallback


def _materialize_stonebreaker_localfs(docs_df: pd.DataFrame):
    outdir = _stonebreaker_localfs_output_dir()
    outdir.mkdir(parents=True, exist_ok=True)

    manifest_rows = []
    for idx, (_, row) in enumerate(docs_df.iterrows(), start=1):
        company = safe_str(row.get("company_name", "")) or "unknown-company"
        symbol = safe_str(row.get("company_symbol", "")) or "na"
        year = safe_year(row.get("report_year"), 0)
        page = safe_int(row.get("page_number"), 0)
        doc_id = safe_str(row.get("id") or row.get("context_id") or f"doc-{idx:04d}")
        question = safe_str(row.get("question", ""))
        context = safe_str(row.get("context", ""))

        company_slug = _stonebreaker_slug(company, f"company-{idx:04d}")
        symbol_slug = _stonebreaker_slug(symbol, "na")
        file_slug = _stonebreaker_slug(doc_id, f"doc-{idx:04d}")
        relative_dir = Path(company_slug) / f"{max(year, 0):04d}"
        target_dir = outdir / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{symbol_slug}-p{page:04d}-{file_slug}.md"
        relative_path = relative_dir / filename
        doc_path = outdir / relative_path
        doc_path.write_text(
            "\n".join(
                [
                    f"# {company}",
                    "",
                    f"- Symbol: {symbol}",
                    f"- Report year: {year}",
                    f"- Page number: {page}",
                    f"- Dataset: {os.environ.get('STONEBREAKER_DOC_DATASET', 'G4KMU/t2-ragbench')}",
                    f"- Subset: {os.environ.get('STONEBREAKER_DOC_SUBSET', 'ConvFinQA')}",
                    f"- Document ID: {doc_id}",
                    "",
                    "## Question",
                    question or "(none)",
                    "",
                    "## Context",
                    context or "(none)",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        manifest_rows.append(
            {
                "doc_id": doc_id,
                "relative_path": relative_path.as_posix(),
                "company_name": company,
                "company_symbol": symbol,
                "report_year": year,
                "page_number": page,
                "question": question,
            }
        )

    pd.DataFrame(manifest_rows).to_csv(outdir / "manifest.csv", index=False)
    log.info(
        "  Materialized %s stonebreaker localfs documents under %s",
        len(manifest_rows),
        outdir,
    )


def _load_stonebreaker_documents(client, model):
    from datasets import load_dataset
    from weaviate.classes.config import Configure, Property, DataType

    collection_name = "BenchmarkDocument"
    if not client.collections.exists(collection_name):
        client.collections.create(
            name=collection_name,
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="doc_id", data_type=DataType.TEXT),
                Property(name="dataset", data_type=DataType.TEXT),
                Property(name="subset", data_type=DataType.TEXT),
                Property(name="company_name", data_type=DataType.TEXT),
                Property(name="company_symbol", data_type=DataType.TEXT),
                Property(name="report_year", data_type=DataType.INT),
                Property(name="page_number", data_type=DataType.INT),
                Property(name="company_sector", data_type=DataType.TEXT),
                Property(name="company_industry", data_type=DataType.TEXT),
                Property(name="file_name", data_type=DataType.TEXT),
                Property(name="question", data_type=DataType.TEXT),
                Property(name="context_snippet", data_type=DataType.TEXT),
            ],
        )

    collection = client.collections.get(collection_name)
    existing = collection.aggregate.over_all(total_count=True)
    existing_total = existing.total_count or 0
    localfs_manifest = _stonebreaker_localfs_output_dir() / "manifest.csv"

    if existing_total > 0 and localfs_manifest.exists():
        log.info(
            "  Stonebreaker document corpus already populated (%s docs) and localfs manifest exists, skipping",
            existing_total,
        )
        return

    doc_path = DATA_DIR / "stonebreaker" / "benchmark_documents.parquet"
    limit = _stonebreaker_document_limit()

    if doc_path.exists():
        docs_df = pd.read_parquet(doc_path)
    else:
        log.info("  Downloading stonebreaker benchmark documents from Hugging Face...")
        ds = load_dataset(
            os.environ.get("STONEBREAKER_DOC_DATASET", "G4KMU/t2-ragbench"),
            name=os.environ.get("STONEBREAKER_DOC_SUBSET", "ConvFinQA"),
            split=os.environ.get("STONEBREAKER_DOC_SPLIT", "turn_0"),
        )
        docs_df = pd.DataFrame(ds)

    if len(docs_df) > limit:
        docs_df = docs_df.head(limit)

    if docs_df.empty:
        log.warning("  No stonebreaker benchmark documents available")
        return

    _materialize_stonebreaker_localfs(docs_df)

    if existing_total > 0:
        log.info(
            "  Stonebreaker document corpus already populated (%s docs), skipped re-embedding after refreshing localfs",
            existing_total,
        )
        return

    log.info(f"  Embedding {len(docs_df):,} stonebreaker benchmark documents...")
    progress = LogProgress("Stonebreaker docs", total=len(docs_df))
    batch_size = 50

    for start in range(0, len(docs_df), batch_size):
        end = min(start + batch_size, len(docs_df))
        chunk = docs_df.iloc[start:end]
        texts = []
        payloads = []

        for _, row in chunk.iterrows():
            question = safe_str(row.get("question", ""))
            context = safe_str(row.get("context", ""))
            snippet = context[:4000]
            texts.append(f"{question}\n\n{snippet}")
            payloads.append(
                {
                    "doc_id": safe_str(row.get("id") or row.get("context_id") or ""),
                    "dataset": os.environ.get("STONEBREAKER_DOC_DATASET", "G4KMU/t2-ragbench"),
                    "subset": os.environ.get("STONEBREAKER_DOC_SUBSET", "ConvFinQA"),
                    "company_name": safe_str(row.get("company_name", "")),
                    "company_symbol": safe_str(row.get("company_symbol", "")),
                    "report_year": safe_year(row.get("report_year"), 0),
                    "page_number": safe_int(row.get("page_number"), 0),
                    "company_sector": safe_str(row.get("company_sector", "")),
                    "company_industry": safe_str(row.get("company_industry", "")),
                    "file_name": safe_str(row.get("file_name", "")),
                    "question": question,
                    "context_snippet": snippet,
                }
            )

        embeddings = model.encode(texts, show_progress_bar=False)
        with collection.batch.dynamic() as batch:
            for idx, payload in enumerate(payloads):
                batch.add_object(properties=payload, vector=embeddings[idx].tolist())
        progress.update(end - start)
    progress.finish()


def _load_weaviate_synthetic_reviews(client, model):
    """Generate synthetic reviews with embeddings if T-ECD reviews unavailable."""
    from weaviate.classes.config import Configure, Property, DataType

    if not client.collections.exists("Review"):
        client.collections.create(
            name="Review",
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="user_id", data_type=DataType.INT),
                Property(name="brand_id", data_type=DataType.INT),
                Property(name="rating", data_type=DataType.INT),
                Property(name="event_day", data_type=DataType.INT),
            ],
        )

    review_collection = client.collections.get("Review")
    sentiments = [
        "Terrible experience, would not recommend",
        "Poor quality, very disappointed",
        "Average product, nothing special",
        "Good value for money, satisfied",
        "Excellent quality, highly recommended",
        "Fast delivery, great packaging",
        "Product broke after a week",
        "Best purchase I ever made",
        "Not worth the price",
        "Decent but could be better",
    ]

    log.info("  Generating 2000 synthetic reviews...")
    batch_size = 100
    progress = LogProgress("Synthetic reviews", total=2000)

    for start in range(0, 2000, batch_size):
        texts = []
        metadata = []
        for i in range(start, min(start + batch_size, 2000)):
            texts.append(random.choice(sentiments))
            metadata.append({
                "user_id": random.randint(1, 100000),
                "brand_id": random.randint(1, 10000),
                "rating": random.randint(1, 5),
                "event_day": random.randint(0, 200),
            })

        embeddings = model.encode(texts, show_progress_bar=False)
        with review_collection.batch.dynamic() as batch:
            for j in range(len(texts)):
                batch.add_object(
                    properties=metadata[j],
                    vector=embeddings[j].tolist(),
                )
        progress.update(len(texts))
    progress.finish()


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    start = time.time()
    log.info("=" * 60)
    log.info("ADAM Demo — T-ECD Data Initialization")
    log.info(f"Scale: {SCALE}")
    log.info("All domains share user_id and brand_id for cross-DB queries")
    log.info("=" * 60)

    steps = [
        ("PostgreSQL", load_postgres),
        ("MongoDB", load_mongodb),
        ("Redis", load_redis),
        ("ClickHouse", load_clickhouse),
        ("Weaviate", load_weaviate),
    ]

    if DISABLE_CLICKHOUSE_INIT:
        steps = [step for step in steps if step[0] != "ClickHouse"]
        log.warning("Skipping ClickHouse load because DISABLE_CLICKHOUSE_INIT is enabled")

    for name, fn in steps:
        log.info(f"\n{'─' * 40}")
        log.info(f"Loading {name}...")
        log.info(f"{'─' * 40}")
        try:
            fn()
        except Exception as e:
            log.error(f"Failed to load {name}: {e}", exc_info=True)

    elapsed = time.time() - start
    log.info(f"\n{'=' * 60}")
    log.info(f"Data initialization complete in {elapsed:.1f}s")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
