"""
ADAM Demo — Synthetic E-Commerce Data Generator
Generates millions of associated records across all 5 databases.

All entities share user_id, product_id, order_id, and brand_id
for meaningful cross-database queries via Eden.

Database Distribution:
  PostgreSQL  ← Users, orders, order_items, invoices, payments, coupons, categories (OLTP)
  MongoDB     ← Products, shopping_carts, shipments, user_addresses, wishlists (documents)
  Redis       ← Sessions, inventory, leaderboards, price_cache, cart_cache (real-time)
  ClickHouse  ← Clickstream, purchase_events, revenue_daily, funnel_events (OLAP)
  Weaviate    ← Product embeddings, review embeddings (vector search)

Usage:
  python generate_ecommerce_data.py                    # Default: ~1M total records
  python generate_ecommerce_data.py --scale large      # ~5M total records
  python generate_ecommerce_data.py --scale massive    # ~20M total records
  SCALE=large python generate_ecommerce_data.py        # Via env var
"""

import os
import time
import random
import hashlib
import logging
import argparse
from datetime import datetime, timedelta

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ecommerce-gen")

DISABLE_CLICKHOUSE_INIT = os.environ.get("DISABLE_CLICKHOUSE_INIT", "1").lower() in {
    "1",
    "true",
    "yes",
}

# ── Config ──────────────────────────────────────────────────────
POSTGRES_URL = os.environ.get("POSTGRES_URL", "postgresql://eden:eden@localhost:5632/ecommerce")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://eden:eden@localhost:27217")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6579")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8323))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "eden")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "eden")
WEAVIATE_URL = os.environ.get("WEAVIATE_URL", "http://localhost:8280")
WEAVIATE_LIMIT = int(os.environ.get("WEAVIATE_LIMIT", "10000"))

# ── Scale Profiles ──────────────────────────────────────────────
SCALES = {
    "demo": {
        "users": 1_000,
        "products": 500,
        "orders": 5_000,
        "carts": 1_000,
        "clickstream": 50_000,
        "reviews": 2_000,
        "coupons": 100,
        "shipments": 4_500,
        "searches": 10_000,
    },
    "small": {
        "users": 10_000,
        "products": 5_000,
        "orders": 50_000,
        "carts": 8_000,
        "clickstream": 500_000,
        "reviews": 20_000,
        "coupons": 500,
        "shipments": 45_000,
        "searches": 100_000,
    },
    "medium": {
        "users": 100_000,
        "products": 25_000,
        "orders": 500_000,
        "carts": 50_000,
        "clickstream": 5_000_000,
        "reviews": 100_000,
        "coupons": 2_000,
        "shipments": 450_000,
        "searches": 1_000_000,
    },
    "large": {
        "users": 500_000,
        "products": 100_000,
        "orders": 2_000_000,
        "carts": 200_000,
        "clickstream": 20_000_000,
        "reviews": 500_000,
        "coupons": 5_000,
        "shipments": 1_800_000,
        "searches": 5_000_000,
    },
    "massive": {
        "users": 1_000_000,
        "products": 250_000,
        "orders": 5_000_000,
        "carts": 500_000,
        "clickstream": 50_000_000,
        "reviews": 1_000_000,
        "coupons": 10_000,
        "shipments": 4_500_000,
        "searches": 10_000_000,
    },
}

# ── Reference Data ──────────────────────────────────────────────
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Kevin", "Carol", "Brian", "Amanda", "George", "Dorothy", "Timothy", "Melissa",
    "Ronald", "Deborah", "Edward", "Stephanie", "Jason", "Rebecca", "Jeffrey", "Sharon",
    "Ryan", "Laura", "Jacob", "Cynthia", "Gary", "Kathleen", "Nicholas", "Amy",
    "Wei", "Yuki", "Raj", "Fatima", "Olga", "Hans", "Pierre", "Aisha",
    "Carlos", "Mei", "Ahmed", "Ingrid", "Hiroshi", "Priya", "Lars", "Sophia",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Chen", "Wang", "Li", "Zhang", "Liu", "Kim", "Park", "Tanaka", "Sato", "Patel",
    "Singh", "Kumar", "Müller", "Schmidt", "Fischer", "Rossi", "Colombo", "Dubois",
]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "protonmail.com", "icloud.com"]

CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras", "Smartwatches", "Speakers", "Monitors"],
    "Clothing": ["Men's Shirts", "Women's Dresses", "Jeans", "Jackets", "Shoes", "Activewear", "Accessories", "Underwear"],
    "Home & Kitchen": ["Furniture", "Cookware", "Bedding", "Lighting", "Storage", "Decor", "Appliances", "Cleaning"],
    "Sports & Outdoors": ["Fitness", "Camping", "Cycling", "Running", "Swimming", "Team Sports", "Hiking", "Yoga"],
    "Beauty & Personal Care": ["Skincare", "Haircare", "Makeup", "Fragrance", "Bath & Body", "Oral Care", "Shaving", "Nail Care"],
    "Books & Media": ["Fiction", "Non-Fiction", "Textbooks", "Comics", "Audiobooks", "E-books", "Magazines", "Music"],
    "Toys & Games": ["Board Games", "Action Figures", "Puzzles", "Educational", "Outdoor Toys", "Video Games", "Dolls", "Building Sets"],
    "Food & Grocery": ["Snacks", "Beverages", "Organic", "Frozen", "Dairy", "Bakery", "Condiments", "Health Foods"],
    "Automotive": ["Parts", "Accessories", "Tools", "Electronics", "Tires", "Oils & Fluids", "Exterior", "Interior"],
    "Pet Supplies": ["Dog Food", "Cat Food", "Toys", "Grooming", "Health", "Beds", "Leashes", "Aquarium"],
}

BRAND_NAMES = [
    "TechNova", "UrbanPeak", "EcoVibe", "SwiftEdge", "LuxCraft", "PureForm",
    "ZenithGear", "VoltStream", "ArcticBloom", "SolarFlare", "NexGen", "VividPulse",
    "IronClad", "CrystalWave", "ThunderBolt", "SilkThread", "MountainRise", "OceanBreeze",
    "FireForge", "GoldenLeaf", "SkyBound", "DeepRoot", "BrightPath", "StormCrest",
    "NovaCore", "PrimeLine", "DuskRider", "FrostBite", "RapidFlow", "TrueNorth",
    "BlueHaven", "RedShift", "GreenMile", "SilverArc", "CoralReef", "AmberWood",
    "JetStream", "MoonRise", "SunPeak", "StarLight", "CloudNine", "DesertHawk",
    "TidalWave", "PolarStar", "ThunderPeak", "CedarGrove", "MapleLeaf", "WildRose",
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East", "Africa", "Oceania"]
COUNTRIES = {
    "North America": ["US", "CA", "MX"],
    "Europe": ["GB", "DE", "FR", "IT", "ES", "NL", "SE", "PL"],
    "Asia Pacific": ["JP", "CN", "IN", "KR", "AU", "SG", "TH", "VN"],
    "Latin America": ["BR", "AR", "CO", "CL", "PE"],
    "Middle East": ["AE", "SA", "IL", "TR"],
    "Africa": ["ZA", "NG", "KE", "EG"],
    "Oceania": ["AU", "NZ"],
}
CITIES = [
    "New York", "Los Angeles", "Chicago", "London", "Paris", "Berlin", "Tokyo", "Sydney",
    "Toronto", "Mumbai", "São Paulo", "Dubai", "Singapore", "Seoul", "Amsterdam", "Stockholm",
    "Barcelona", "Milan", "Mexico City", "Bangkok", "Lagos", "Nairobi", "Auckland", "Cape Town",
]

ORDER_STATUSES = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "returned", "refunded"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer", "crypto", "gift_card"]
PAYMENT_STATUSES = ["pending", "authorized", "captured", "failed", "refunded", "partially_refunded"]
SHIPPING_CARRIERS = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics", "Royal Mail", "Canada Post", "DPD"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
OS_TYPES = ["iOS", "Android", "Windows", "macOS", "Linux"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "begin_checkout",
               "purchase", "search", "wishlist_add", "review_submit", "coupon_apply"]
COUPON_TYPES = ["percentage", "fixed_amount", "free_shipping", "buy_one_get_one"]
SHIPMENT_STATUSES = ["label_created", "picked_up", "in_transit", "out_for_delivery", "delivered", "exception", "returned"]
STOCK_CHANGE_REASONS = ["sale", "restock", "return", "adjustment", "damage", "transfer_in", "transfer_out"]
WAREHOUSES = ["US-EAST", "US-WEST", "EU-CENTRAL", "APAC-SOUTH", "APAC-EAST"]
ABANDONMENT_REASONS = [
    "high_shipping", "found_cheaper", "just_browsing", "payment_failed",
    "out_of_stock", "slow_delivery", "complicated_checkout", "changed_mind",
    "price_too_high", "missing_coupon",
]
SEARCH_QUERIES = [
    "wireless headphones", "men's running shoes", "laptop stand", "yoga mat",
    "protein powder", "USB-C cable", "gaming mouse", "leather wallet",
    "stainless steel water bottle", "bluetooth speaker", "desk lamp",
    "winter jacket", "organic coffee", "phone case", "backpack for travel",
    "air fryer", "electric toothbrush", "mechanical keyboard", "running shorts",
    "sunglasses", "moisturizer", "kids toys", "dog food grain free",
    "smart watch", "noise cancelling earbuds", "camping tent", "cast iron skillet",
    "resistance bands", "face wash", "graphic novel", "puzzle 1000 piece",
    "trail running shoes", "espresso machine", "standing desk converter",
    "hiking boots waterproof", "kindle case", "wool socks", "dumbbell set",
    "plant pot ceramic", "baby monitor", "beard trimmer", "tennis racket",
    "cookware set", "swim goggles", "art supplies", "vitamin D supplements",
    "reusable bags", "cat scratching post", "monitor arm", "bath towels",
]

# Sentiment templates for review embeddings
REVIEW_TEXTS = {
    1: [
        "Terrible product, broke after one day. Complete waste of money.",
        "Worst purchase ever. Do not buy this product under any circumstances.",
        "Extremely disappointed. Quality is nonexistent. Returning immediately.",
        "Awful experience. Product arrived damaged and customer service was unhelpful.",
    ],
    2: [
        "Below average quality. Not worth the price they charge for it.",
        "Disappointing purchase. Expected much better based on the description.",
        "Poor build quality. Feels cheap and flimsy. Would not recommend.",
        "Product works but barely. Many issues and frustrations with daily use.",
    ],
    3: [
        "Decent product for the price. Nothing special but gets the job done.",
        "Average quality. Meets basic expectations but nothing more than that.",
        "It's okay. Some pros and cons. Might look for alternatives next time.",
        "Acceptable product. Works as described but room for improvement.",
    ],
    4: [
        "Good product overall. Well made and reliable. Happy with my purchase.",
        "Very satisfied with this purchase. Great value for the money spent.",
        "Quality product that works well. Would recommend to friends and family.",
        "Impressed with the build quality and features. Solid purchase.",
    ],
    5: [
        "Outstanding product! Exceeded all expectations. Best purchase this year.",
        "Absolutely love this product. Perfect quality and amazing value.",
        "Incredible quality and performance. Highly recommend to everyone.",
        "Five stars! Exceptional product. Will definitely buy from this brand again.",
    ],
}


class ProgressTracker:
    """Log-friendly progress tracker for batch operations."""
    def __init__(self, desc, total, log_every_pct=10):
        self.desc = desc
        self.total = total
        self.count = 0
        self.log_every_pct = log_every_pct
        self.last_pct = -1
        self.start = time.time()

    def update(self, n=1):
        self.count += n
        if self.total > 0:
            pct = int(100 * self.count / self.total)
            if pct >= self.last_pct + self.log_every_pct:
                self.last_pct = pct
                elapsed = time.time() - self.start
                rate = self.count / elapsed if elapsed > 0 else 0
                log.info(f"  [{self.desc}] {self.count:,}/{self.total:,} ({pct}%) — {elapsed:.1f}s ({rate:,.0f}/s)")

    def finish(self):
        elapsed = time.time() - self.start
        rate = self.count / elapsed if elapsed > 0 else 0
        log.info(f"  [{self.desc}] Done: {self.count:,} in {elapsed:.1f}s ({rate:,.0f}/s)")


# ── Shared ID generators ───────────────────────────────────────

class IDPool:
    """Pre-generates shared IDs used across all databases."""
    def __init__(self, scale):
        self.num_users = scale["users"]
        self.num_products = scale["products"]
        self.num_orders = scale["orders"]
        self.num_brands = len(BRAND_NAMES)
        self.num_categories = len(CATEGORIES)

        # Shared ID ranges (consistent across all databases)
        self.user_ids = list(range(1, self.num_users + 1))
        self.product_ids = list(range(1, self.num_products + 1))
        self.order_ids = list(range(1, self.num_orders + 1))
        self.brand_ids = list(range(1, self.num_brands + 1))

        # Pre-assign products to brands and categories
        self.product_brand = {}
        self.product_category = {}
        self.product_subcategory = {}
        self.product_price = {}
        cat_list = list(CATEGORIES.keys())

        rng = random.Random(42)
        for pid in self.product_ids:
            self.product_brand[pid] = rng.choice(self.brand_ids)
            cat = rng.choice(cat_list)
            self.product_category[pid] = cat
            self.product_subcategory[pid] = rng.choice(CATEGORIES[cat])
            # Price follows a log-normal distribution (realistic)
            self.product_price[pid] = round(rng.lognormvariate(3.5, 1.2), 2)
            self.product_price[pid] = max(0.99, min(self.product_price[pid], 9999.99))

        # Pre-assign users to regions
        self.user_region = {}
        self.user_country = {}
        for uid in self.user_ids:
            region = rng.choice(REGIONS)
            self.user_region[uid] = region
            self.user_country[uid] = rng.choice(COUNTRIES[region])

        # Pre-assign orders to users (power-law: some users order much more)
        weights = np.array([1.0 / (i ** 0.5) for i in range(1, self.num_users + 1)])
        weights /= weights.sum()
        self.order_user = {}
        order_users = rng.choices(self.user_ids, weights=weights.tolist(), k=self.num_orders)
        for i, oid in enumerate(self.order_ids):
            self.order_user[oid] = order_users[i]

        # Pre-assign order line items (shared across PG, CH, Redis)
        # This ensures the SAME products appear in PG order_items, CH purchase_events,
        # and Redis leaderboards — so cross-database queries return consistent results.
        log.info("  Pre-computing order→product mappings...")
        self.order_items = {}       # order_id → [(product_id, quantity, unit_price)]
        self.order_day = {}         # order_id → event_day (0..365)
        self.order_payment = {}     # order_id → payment_method
        self.order_total = {}       # order_id → total amount

        for oid in self.order_ids:
            num_items = rng.choices([1, 2, 3, 4, 5, 6], weights=[40, 30, 15, 8, 5, 2])[0]
            items = []
            subtotal = 0
            for pid in rng.sample(self.product_ids, min(num_items, len(self.product_ids))):
                qty = rng.choices([1, 2, 3, 4, 5], weights=[60, 25, 10, 3, 2])[0]
                price = self.product_price[pid]
                line_total = round(price * qty, 2)
                subtotal += line_total
                items.append((pid, qty, price))
            self.order_items[oid] = items
            self.order_day[oid] = rng.randint(0, 365)
            self.order_payment[oid] = rng.choice(PAYMENT_METHODS)
            tax = round(subtotal * rng.uniform(0.05, 0.12), 2)
            shipping = round(rng.choice([0, 4.99, 7.99, 9.99, 12.99, 14.99]), 2) if subtotal < 100 else 0
            self.order_total[oid] = round(subtotal + tax + shipping, 2)

        # Pre-compute aggregate stats (used by Redis leaderboards & counters)
        log.info("  Pre-computing aggregate stats...")
        self.user_spend = {}        # user_id → total spend
        self.product_sales = {}     # product_id → units sold
        self.product_revenue = {}   # product_id → total revenue
        for oid in self.order_ids:
            uid = self.order_user[oid]
            self.user_spend[uid] = self.user_spend.get(uid, 0) + self.order_total[oid]
            for pid, qty, price in self.order_items[oid]:
                self.product_sales[pid] = self.product_sales.get(pid, 0) + qty
                self.product_revenue[pid] = self.product_revenue.get(pid, 0) + round(price * qty, 2)

        # Reference date for timestamps
        self.base_date = datetime(2025, 1, 1)
        self.date_range_days = 365


def rand_date(pool, rng):
    """Generate a random datetime within the data range."""
    days = rng.randint(0, pool.date_range_days)
    seconds = rng.randint(0, 86399)
    return pool.base_date + timedelta(days=days, seconds=seconds)


def rand_email(first, last, uid, rng):
    """Generate a deterministic but realistic email."""
    domain = rng.choice(DOMAINS)
    tag = uid % 10000
    return f"{first.lower()}.{last.lower()}{tag}@{domain}"


# ═══════════════════════════════════════════════════════════════
# 1. PostgreSQL — Users, Orders, Invoices, Payments, Coupons
# ═════════════════════════════════════════════���═════════════════

def generate_postgres(pool, scale):
    import psycopg2
    from psycopg2.extras import execute_values

    log.info("=" * 60)
    log.info("Generating PostgreSQL data (users, orders, invoices, payments, coupons)...")
    log.info("=" * 60)

    conn = psycopg2.connect(POSTGRES_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Check if synthetic data already loaded
    cur.execute("SELECT COUNT(*) FROM orders")
    if cur.fetchone()[0] > 0:
        log.info("PostgreSQL orders table already populated, skipping synthetic generation")
        conn.close()
        return

    batch_size = 5000
    rng = random.Random(42)

    # ── Users (upsert — may already exist from T-ECD) ──
    log.info(f"  Generating {pool.num_users:,} users...")
    progress = ProgressTracker("PG users", pool.num_users)
    user_batch = []
    for uid in pool.user_ids:
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        email = rand_email(first, last, uid, rng)
        created = rand_date(pool, rng)
        tier = rng.choice(LOYALTY_TIERS)
        region_idx = REGIONS.index(pool.user_region[uid])
        user_batch.append((
            uid, first, last, email, tier,
            region_idx, rng.randint(0, 9),
            created.isoformat(),
        ))
        if len(user_batch) >= batch_size:
            execute_values(cur,
                """INSERT INTO users (user_id, first_name, last_name, email, loyalty_tier, region, socdem_cluster, created_at)
                   VALUES %s ON CONFLICT (user_id) DO UPDATE SET
                   first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
                   email=EXCLUDED.email, loyalty_tier=EXCLUDED.loyalty_tier,
                   created_at=EXCLUDED.created_at""",
                user_batch)
            progress.update(len(user_batch))
            user_batch = []
    if user_batch:
        execute_values(cur,
            """INSERT INTO users (user_id, first_name, last_name, email, loyalty_tier, region, socdem_cluster, created_at)
               VALUES %s ON CONFLICT (user_id) DO UPDATE SET
               first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
               email=EXCLUDED.email, loyalty_tier=EXCLUDED.loyalty_tier,
               created_at=EXCLUDED.created_at""",
            user_batch)
        progress.update(len(user_batch))
    progress.finish()

    # ── Brands (upsert) ──
    log.info(f"  Generating {pool.num_brands} brands...")
    brand_batch = [(bid, BRAND_NAMES[bid - 1]) for bid in pool.brand_ids]
    execute_values(cur,
        "INSERT INTO brands (brand_id, brand_name) VALUES %s ON CONFLICT (brand_id) DO UPDATE SET brand_name=EXCLUDED.brand_name",
        brand_batch)

    # ── Categories ──
    log.info(f"  Generating {sum(len(v) for v in CATEGORIES.values())} categories...")
    cat_batch = []
    cat_id = 1
    for cat, subcats in CATEGORIES.items():
        for sub in subcats:
            cat_batch.append((cat_id, cat, sub))
            cat_id += 1
    execute_values(cur,
        "INSERT INTO categories (category_id, category_name, subcategory_name) VALUES %s ON CONFLICT DO NOTHING",
        cat_batch)

    # ── Coupons ──
    num_coupons = scale["coupons"]
    log.info(f"  Generating {num_coupons:,} coupons...")
    coupon_batch = []
    for i in range(1, num_coupons + 1):
        code = f"{''.join(rng.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
        coupon_type = rng.choice(COUPON_TYPES)
        if coupon_type == "percentage":
            discount_value = rng.choice([5, 10, 15, 20, 25, 30, 40, 50])
        elif coupon_type == "fixed_amount":
            discount_value = rng.choice([5, 10, 15, 20, 25, 50, 100])
        else:
            discount_value = 0
        min_purchase = rng.choice([0, 25, 50, 75, 100, 150, 200])
        max_uses = rng.choice([100, 500, 1000, 5000, 10000])
        used_count = rng.randint(0, max_uses)
        start_date = rand_date(pool, rng)
        end_date = start_date + timedelta(days=rng.randint(7, 90))
        active = rng.random() > 0.3
        coupon_batch.append((
            i, code, coupon_type, discount_value, min_purchase,
            max_uses, used_count, start_date.date().isoformat(),
            end_date.date().isoformat(), active,
        ))
    execute_values(cur,
        """INSERT INTO coupons (coupon_id, code, coupon_type, discount_value, min_purchase,
           max_uses, used_count, start_date, end_date, is_active)
           VALUES %s ON CONFLICT DO NOTHING""",
        coupon_batch)

    # ── Orders + Order Items + Invoices + Payments ──
    # Uses pre-computed order→product mappings from IDPool so the SAME
    # relationships appear in ClickHouse purchase_events and Redis leaderboards.
    num_orders = scale["orders"]
    log.info(f"  Generating {num_orders:,} orders with items, invoices, and payments...")
    progress = ProgressTracker("PG orders", num_orders)

    order_batch = []
    item_batch = []
    invoice_batch = []
    payment_batch = []
    item_counter = 0

    for oid in pool.order_ids:
        uid = pool.order_user[oid]
        status = rng.choice(ORDER_STATUSES)
        order_date = pool.base_date + timedelta(days=pool.order_day[oid],
                                                 seconds=rng.randint(0, 86399))
        total = pool.order_total[oid]

        # Use pre-computed line items (same products as CH purchase_events)
        subtotal = 0
        for pid, qty, unit_price in pool.order_items[oid]:
            item_counter += 1
            line_total = round(unit_price * qty, 2)
            subtotal += line_total
            item_batch.append((item_counter, oid, pid, qty, unit_price, line_total))

        tax = round(total - subtotal if total > subtotal else subtotal * 0.08, 2)
        shipping_cost = round(max(0, total - subtotal - tax), 2)
        coupon_id = rng.randint(1, num_coupons) if rng.random() < 0.15 else None

        order_batch.append((
            oid, uid, status, subtotal, tax, shipping_cost, total,
            coupon_id, order_date.isoformat(),
            pool.user_country[uid], pool.user_region[uid],
        ))

        # Invoice (1:1 with order)
        invoice_batch.append((
            oid, oid, uid, total, tax,
            "paid" if status in ("delivered", "shipped", "processing", "confirmed") else "pending",
            order_date.isoformat(),
            (order_date + timedelta(days=30)).isoformat(),
        ))

        # Payment (1:1 with order) — uses same payment method as IDPool
        pay_method = pool.order_payment[oid]
        pay_status = "captured" if status in ("delivered", "shipped", "processing") else rng.choice(PAYMENT_STATUSES)
        payment_batch.append((
            oid, oid, uid, total, pay_method, pay_status,
            f"txn_{hashlib.md5(f'{oid}'.encode()).hexdigest()[:16]}",
            order_date.isoformat(),
        ))

        if len(order_batch) >= batch_size:
            execute_values(cur,
                """INSERT INTO orders (order_id, user_id, status, subtotal, tax, shipping_cost,
                   total, coupon_id, created_at, country, region)
                   VALUES %s ON CONFLICT DO NOTHING""",
                order_batch)
            execute_values(cur,
                """INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, line_total)
                   VALUES %s ON CONFLICT DO NOTHING""",
                item_batch)
            execute_values(cur,
                """INSERT INTO invoices (invoice_id, order_id, user_id, total, tax, status, issued_at, due_at)
                   VALUES %s ON CONFLICT DO NOTHING""",
                invoice_batch)
            execute_values(cur,
                """INSERT INTO payments (payment_id, order_id, user_id, amount, method, status, transaction_ref, paid_at)
                   VALUES %s ON CONFLICT DO NOTHING""",
                payment_batch)
            progress.update(len(order_batch))
            order_batch, item_batch, invoice_batch, payment_batch = [], [], [], []

    # Flush remaining
    if order_batch:
        execute_values(cur,
            """INSERT INTO orders (order_id, user_id, status, subtotal, tax, shipping_cost,
               total, coupon_id, created_at, country, region)
               VALUES %s ON CONFLICT DO NOTHING""",
            order_batch)
        execute_values(cur,
            """INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, line_total)
               VALUES %s ON CONFLICT DO NOTHING""",
            item_batch)
        execute_values(cur,
            """INSERT INTO invoices (invoice_id, order_id, user_id, total, tax, status, issued_at, due_at)
               VALUES %s ON CONFLICT DO NOTHING""",
            invoice_batch)
        execute_values(cur,
            """INSERT INTO payments (payment_id, order_id, user_id, amount, method, status, transaction_ref, paid_at)
               VALUES %s ON CONFLICT DO NOTHING""",
            payment_batch)
        progress.update(len(order_batch))
    progress.finish()

    log.info(f"  Total order items generated: {item_counter:,}")
    conn.close()


# ═══════════════════════════════════════════════════════════════
# 2. MongoDB — Products, Carts, Shipments, Addresses, Wishlists
# ═══════════════════════════════════════════════════════════════

def generate_mongodb(pool, scale):
    from pymongo import MongoClient

    log.info("=" * 60)
    log.info("Generating MongoDB data (products, carts, shipments, wishlists)...")
    log.info("=" * 60)

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    rng = random.Random(43)

    # ── Products (rich documents) ──
    products_col = db["products"]
    if products_col.count_documents({}) == 0:
        log.info(f"  Generating {pool.num_products:,} product documents...")
        progress = ProgressTracker("Mongo products", pool.num_products)
        docs = []
        for pid in pool.product_ids:
            cat = pool.product_category[pid]
            subcat = pool.product_subcategory[pid]
            brand_id = pool.product_brand[pid]
            price = pool.product_price[pid]

            doc = {
                "product_id": pid,
                "brand_id": brand_id,
                "brand_name": BRAND_NAMES[brand_id - 1],
                "name": f"{BRAND_NAMES[brand_id - 1]} {subcat} #{pid}",
                "description": f"High-quality {subcat.lower()} from {BRAND_NAMES[brand_id - 1]}. "
                               f"Part of our {cat} collection.",
                "category": cat,
                "subcategory": subcat,
                "price": price,
                "compare_at_price": round(price * rng.uniform(1.1, 1.5), 2) if rng.random() < 0.3 else None,
                "currency": "USD",
                "sku": f"SKU-{cat[:3].upper()}-{pid:07d}",
                "weight_kg": round(rng.uniform(0.1, 25.0), 2),
                "dimensions": {
                    "length_cm": round(rng.uniform(5, 100), 1),
                    "width_cm": round(rng.uniform(5, 80), 1),
                    "height_cm": round(rng.uniform(2, 60), 1),
                },
                "tags": rng.sample(
                    ["bestseller", "new-arrival", "sale", "premium", "eco-friendly",
                     "limited-edition", "trending", "clearance", "exclusive", "gift-idea"],
                    k=rng.randint(0, 4)
                ),
                "attributes": {
                    "color": rng.choice(["Black", "White", "Blue", "Red", "Green", "Gray", "Navy", "Brown"]),
                    "material": rng.choice(["Cotton", "Polyester", "Metal", "Plastic", "Wood", "Leather", "Glass", "Ceramic"]),
                },
                "rating_avg": round(rng.uniform(1.0, 5.0), 1),
                "rating_count": rng.randint(0, 5000),
                "is_active": rng.random() > 0.05,
                "created_at": rand_date(pool, rng).isoformat(),
                "updated_at": rand_date(pool, rng).isoformat(),
            }
            docs.append(doc)
            if len(docs) >= 2000:
                products_col.insert_many(docs)
                progress.update(len(docs))
                docs = []
        if docs:
            products_col.insert_many(docs)
            progress.update(len(docs))
        progress.finish()

        products_col.create_index("product_id", unique=True)
        products_col.create_index("brand_id")
        products_col.create_index("category")
        products_col.create_index("subcategory")
        products_col.create_index("price")
        products_col.create_index("tags")
    else:
        log.info("  Products collection already populated, skipping")

    # ── Shopping Carts ──
    # Converted carts link to real orders for the SAME user, with the
    # SAME products that appear in PG order_items and CH purchase_events.
    carts_col = db["shopping_carts"]
    num_carts = scale["carts"]
    if carts_col.count_documents({}) == 0:
        log.info(f"  Generating {num_carts:,} shopping carts...")
        progress = ProgressTracker("Mongo carts", num_carts)

        # Build user→orders lookup for realistic cart→order conversion
        user_orders = {}
        for oid in pool.order_ids:
            uid = pool.order_user[oid]
            if uid not in user_orders:
                user_orders[uid] = []
            user_orders[uid].append(oid)

        docs = []
        for i in range(1, num_carts + 1):
            status = rng.choices(
                ["active", "abandoned", "converted", "expired"],
                weights=[30, 40, 25, 5]
            )[0]

            converted_order_id = None
            if status == "converted":
                # Pick a user who actually has orders, and link to one of their real orders
                uid = rng.choice(list(user_orders.keys()))
                converted_order_id = rng.choice(user_orders[uid])
                # Cart items mirror the actual order items
                cart_products_data = pool.order_items[converted_order_id]
                items = []
                cart_total = 0
                for pid, qty, price in cart_products_data:
                    line_total = round(price * qty, 2)
                    cart_total += line_total
                    items.append({
                        "product_id": pid,
                        "quantity": qty,
                        "unit_price": price,
                        "line_total": line_total,
                    })
            else:
                uid = rng.choice(pool.user_ids)
                num_items = rng.choices([1, 2, 3, 4, 5, 6, 7, 8], weights=[25, 30, 20, 12, 7, 3, 2, 1])[0]
                cart_pids = rng.sample(pool.product_ids, min(num_items, len(pool.product_ids)))
                items = []
                cart_total = 0
                for pid in cart_pids:
                    qty = rng.choices([1, 2, 3], weights=[70, 25, 5])[0]
                    price = pool.product_price[pid]
                    line_total = round(price * qty, 2)
                    cart_total += line_total
                    items.append({
                        "product_id": pid,
                        "quantity": qty,
                        "unit_price": price,
                        "line_total": line_total,
                    })

            created = rand_date(pool, rng)
            doc = {
                "cart_id": i,
                "user_id": uid,
                "status": status,
                "items": items,
                "item_count": len(items),
                "total": round(cart_total, 2),
                "currency": "USD",
                "created_at": created.isoformat(),
                "updated_at": (created + timedelta(minutes=rng.randint(1, 4320))).isoformat(),
                "converted_order_id": converted_order_id,
            }
            docs.append(doc)
            if len(docs) >= 2000:
                carts_col.insert_many(docs)
                progress.update(len(docs))
                docs = []
        if docs:
            carts_col.insert_many(docs)
            progress.update(len(docs))
        progress.finish()

        carts_col.create_index("cart_id", unique=True)
        carts_col.create_index("user_id")
        carts_col.create_index("status")
    else:
        log.info("  Shopping carts already populated, skipping")

    # ── Shipments ──
    shipments_col = db["shipments"]
    num_shipments = scale["shipments"]
    if shipments_col.count_documents({}) == 0:
        log.info(f"  Generating {num_shipments:,} shipments...")
        progress = ProgressTracker("Mongo shipments", num_shipments)
        docs = []
        shipped_orders = [oid for oid in pool.order_ids if rng.random() < (num_shipments / len(pool.order_ids) * 1.1)]
        shipped_orders = shipped_orders[:num_shipments]
        # Pad if needed
        while len(shipped_orders) < num_shipments:
            shipped_orders.append(rng.choice(pool.order_ids))

        for i, oid in enumerate(shipped_orders, 1):
            uid = pool.order_user[oid]
            carrier = rng.choice(SHIPPING_CARRIERS)
            tracking = f"{carrier[:3].upper()}{rng.randint(100000000, 999999999)}"
            ship_date = rand_date(pool, rng)
            status = rng.choice(SHIPMENT_STATUSES)

            events = []
            current = ship_date
            for s in SHIPMENT_STATUSES:
                events.append({
                    "status": s,
                    "timestamp": current.isoformat(),
                    "location": rng.choice(CITIES),
                })
                if s == status:
                    break
                current += timedelta(hours=rng.randint(4, 48))

            doc = {
                "shipment_id": i,
                "order_id": oid,
                "user_id": uid,
                "carrier": carrier,
                "tracking_number": tracking,
                "status": status,
                "estimated_delivery": (ship_date + timedelta(days=rng.randint(2, 14))).isoformat(),
                "actual_delivery": (ship_date + timedelta(days=rng.randint(2, 10))).isoformat() if status == "delivered" else None,
                "weight_kg": round(rng.uniform(0.2, 30.0), 2),
                "destination_country": pool.user_country[uid],
                "destination_city": rng.choice(CITIES),
                "tracking_events": events,
                "created_at": ship_date.isoformat(),
            }
            docs.append(doc)
            if len(docs) >= 2000:
                shipments_col.insert_many(docs)
                progress.update(len(docs))
                docs = []
        if docs:
            shipments_col.insert_many(docs)
            progress.update(len(docs))
        progress.finish()

        shipments_col.create_index("shipment_id", unique=True)
        shipments_col.create_index("order_id")
        shipments_col.create_index("user_id")
        shipments_col.create_index("tracking_number")
        shipments_col.create_index("status")
    else:
        log.info("  Shipments already populated, skipping")

    # ── Wishlists ──
    wishlists_col = db["wishlists"]
    if wishlists_col.count_documents({}) == 0:
        num_wishlists = pool.num_users // 3  # ~33% of users have wishlists
        log.info(f"  Generating {num_wishlists:,} wishlists...")
        progress = ProgressTracker("Mongo wishlists", num_wishlists)
        docs = []
        wishlist_users = rng.sample(pool.user_ids, min(num_wishlists, pool.num_users))
        for i, uid in enumerate(wishlist_users, 1):
            num_items = rng.randint(1, 20)
            items = rng.sample(pool.product_ids, min(num_items, len(pool.product_ids)))
            doc = {
                "wishlist_id": i,
                "user_id": uid,
                "product_ids": items,
                "item_count": len(items),
                "created_at": rand_date(pool, rng).isoformat(),
            }
            docs.append(doc)
            if len(docs) >= 2000:
                wishlists_col.insert_many(docs)
                progress.update(len(docs))
                docs = []
        if docs:
            wishlists_col.insert_many(docs)
            progress.update(len(docs))
        progress.finish()

        wishlists_col.create_index("wishlist_id", unique=True)
        wishlists_col.create_index("user_id")
    else:
        log.info("  Wishlists already populated, skipping")

    # ── User Addresses ──
    addresses_col = db["user_addresses"]
    if addresses_col.count_documents({}) == 0:
        log.info(f"  Generating user addresses...")
        progress = ProgressTracker("Mongo addresses", pool.num_users)
        docs = []
        for uid in pool.user_ids:
            num_addrs = rng.choices([1, 2, 3], weights=[60, 30, 10])[0]
            for j in range(num_addrs):
                doc = {
                    "user_id": uid,
                    "address_type": ["shipping", "billing", "both"][min(j, 2)],
                    "is_default": j == 0,
                    "street": f"{rng.randint(1, 9999)} {rng.choice(LAST_NAMES)} {rng.choice(['St', 'Ave', 'Blvd', 'Dr', 'Ln', 'Ct'])}",
                    "city": rng.choice(CITIES),
                    "state": f"State-{rng.randint(1, 50)}",
                    "postal_code": f"{rng.randint(10000, 99999)}",
                    "country": pool.user_country[uid],
                    "phone": f"+1{rng.randint(2000000000, 9999999999)}",
                }
                docs.append(doc)
            if len(docs) >= 2000:
                addresses_col.insert_many(docs)
                progress.update(num_addrs)
                docs = []
        if docs:
            addresses_col.insert_many(docs)
        progress.finish()

        addresses_col.create_index("user_id")
    else:
        log.info("  User addresses already populated, skipping")

    # ── Stock Levels (historical stock change events per product) ──
    stock_col = db["stock_levels"]
    if stock_col.count_documents({}) == 0:
        # Generate ~5 stock events per product (restock, sales, adjustments)
        num_events = pool.num_products * 5
        log.info(f"  Generating {num_events:,} stock level events...")
        progress = ProgressTracker("Mongo stock_levels", num_events)
        docs = []

        for pid in pool.product_ids:
            # Start with initial restock
            current_stock = rng.randint(50, 2000)
            warehouse = rng.choice(WAREHOUSES)
            event_date = pool.base_date + timedelta(days=rng.randint(0, 30))

            doc = {
                "product_id": pid,
                "warehouse": warehouse,
                "change_type": "restock",
                "quantity_change": current_stock,
                "quantity_after": current_stock,
                "reason": "initial_stock",
                "reference_id": None,
                "recorded_at": event_date.isoformat(),
            }
            docs.append(doc)

            # Subsequent events: sales draw down, restocks bring back up
            num_changes = rng.randint(3, 6)
            for _ in range(num_changes):
                event_date += timedelta(days=rng.randint(1, 60), hours=rng.randint(0, 23))
                change_type = rng.choices(
                    STOCK_CHANGE_REASONS,
                    weights=[45, 25, 10, 8, 5, 4, 3]
                )[0]

                if change_type in ("sale", "damage", "transfer_out"):
                    qty = -min(rng.randint(1, 50), current_stock)
                elif change_type in ("restock", "return", "transfer_in"):
                    qty = rng.randint(10, 500)
                else:  # adjustment
                    qty = rng.randint(-20, 20)

                current_stock = max(0, current_stock + qty)

                # Link sales to actual orders when possible
                ref_id = None
                if change_type == "sale" and pool.product_sales.get(pid, 0) > 0:
                    # Pick a random order that contains this product
                    for oid in rng.sample(pool.order_ids, min(20, len(pool.order_ids))):
                        if any(p == pid for p, _, _ in pool.order_items[oid]):
                            ref_id = oid
                            break

                doc = {
                    "product_id": pid,
                    "warehouse": warehouse,
                    "change_type": change_type,
                    "quantity_change": qty,
                    "quantity_after": current_stock,
                    "reason": change_type,
                    "reference_id": ref_id,
                    "recorded_at": event_date.isoformat(),
                }
                docs.append(doc)

            if len(docs) >= 2000:
                stock_col.insert_many(docs)
                progress.update(len(docs))
                docs = []

        if docs:
            stock_col.insert_many(docs)
            progress.update(len(docs))
        progress.finish()

        stock_col.create_index("product_id")
        stock_col.create_index("warehouse")
        stock_col.create_index("change_type")
        stock_col.create_index("recorded_at")
    else:
        log.info("  Stock levels already populated, skipping")

    client.close()


# ═══════════════════════════════════════════════════════════════
# 3. Redis — Sessions, Inventory, Leaderboards, Caches
# ═══════════════════════════════════════════════════════════════

def generate_redis(pool, scale):
    import redis as r

    log.info("=" * 60)
    log.info("Generating Redis data (sessions, inventory, leaderboards, caches)...")
    log.info("=" * 60)

    client = r.from_url(REDIS_URL, decode_responses=True)
    rng = random.Random(44)

    # Check if already loaded (look for our marker key)
    if client.exists("_ecommerce_gen_complete"):
        log.info("Redis already populated with synthetic data, skipping")
        client.close()
        return

    pipe = client.pipeline()
    flush_every = 5000

    def maybe_flush(counter):
        if counter % flush_every == 0:
            pipe.execute()

    counter = 0

    # ── User Sessions ──
    num_sessions = min(pool.num_users, 50000)  # Active sessions
    log.info(f"  Generating {num_sessions:,} user sessions...")
    progress = ProgressTracker("Redis sessions", num_sessions)
    session_users = rng.sample(pool.user_ids, num_sessions)
    for uid in session_users:
        session = {
            "user_id": str(uid),
            "session_id": hashlib.md5(f"session:{uid}:{rng.randint(0,999999)}".encode()).hexdigest(),
            "started_at": rand_date(pool, rng).isoformat(),
            "last_active": rand_date(pool, rng).isoformat(),
            "device": rng.choice(DEVICE_TYPES),
            "browser": rng.choice(BROWSERS),
            "os": rng.choice(OS_TYPES),
            "ip_address": f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
            "country": pool.user_country[uid],
            "page_views": str(rng.randint(1, 100)),
            "cart_value": str(round(rng.uniform(0, 500), 2)),
            "loyalty_tier": rng.choice(LOYALTY_TIERS),
        }
        pipe.hset(f"session:{uid}", mapping=session)
        pipe.expire(f"session:{uid}", rng.randint(1800, 86400))
        counter += 1
        maybe_flush(counter)
        progress.update()
    progress.finish()

    # ── Product Inventory (real-time stock levels) ──
    log.info(f"  Generating {pool.num_products:,} inventory records...")
    progress = ProgressTracker("Redis inventory", pool.num_products)
    for pid in pool.product_ids:
        stock = rng.choices(
            [0, rng.randint(1, 5), rng.randint(6, 50), rng.randint(51, 500), rng.randint(501, 10000)],
            weights=[5, 10, 40, 35, 10]
        )[0]
        inv = {
            "product_id": str(pid),
            "stock": str(stock),
            "reserved": str(rng.randint(0, max(1, stock // 10))),
            "warehouse": rng.choice(["US-EAST", "US-WEST", "EU-CENTRAL", "APAC-SOUTH", "APAC-EAST"]),
            "reorder_point": str(rng.randint(5, 50)),
            "last_restocked": rand_date(pool, rng).isoformat(),
        }
        pipe.hset(f"inventory:{pid}", mapping=inv)
        counter += 1
        maybe_flush(counter)

        # Low stock alert set
        if stock <= 5:
            pipe.sadd("alerts:low_stock", str(pid))
        if stock == 0:
            pipe.sadd("alerts:out_of_stock", str(pid))
        progress.update()
    progress.finish()

    # ── Price Cache ──
    log.info(f"  Caching {pool.num_products:,} product prices...")
    for pid in pool.product_ids:
        pipe.set(f"price:{pid}", str(pool.product_price[pid]))
        counter += 1
        maybe_flush(counter)

    # ── Leaderboards (Sorted Sets) ──
    # Built from pre-computed order data — same aggregates as PG and CH.
    log.info("  Building leaderboards from actual order data...")

    # Top spenders (from pre-computed user_spend)
    for uid, spend in pool.user_spend.items():
        pipe.zadd("leaderboard:top_spenders", {f"user:{uid}": round(spend, 2)})
        counter += 1
        maybe_flush(counter)

    # Top products by units sold (from pre-computed product_sales)
    for pid, units in pool.product_sales.items():
        pipe.zadd("leaderboard:top_products", {f"product:{pid}": units})
        counter += 1
        maybe_flush(counter)

    # Top brands by revenue (derived from actual product revenue)
    brand_revenue = {}
    for pid, rev in pool.product_revenue.items():
        bid = pool.product_brand[pid]
        brand_revenue[bid] = brand_revenue.get(bid, 0) + rev
    for bid, rev in brand_revenue.items():
        pipe.zadd("leaderboard:top_brands", {f"brand:{bid}": round(rev, 2)})

    # Top categories by units sold
    cat_sales = {}
    for pid, units in pool.product_sales.items():
        cat = pool.product_category[pid]
        cat_sales[cat] = cat_sales.get(cat, 0) + units
    for cat, units in cat_sales.items():
        pipe.zadd("leaderboard:top_categories", {cat: units})

    # ── User → Order index (Sets) ──
    log.info("  Building user→order indexes...")
    for oid in pool.order_ids:
        uid = pool.order_user[oid]
        pipe.sadd(f"user:orders:{uid}", str(oid))
        counter += 1
        maybe_flush(counter)

    # ── Real-time counters (from actual pre-computed data) ──
    log.info("  Setting up real-time counters...")
    total_revenue = round(sum(pool.order_total.values()), 2)
    pipe.set("stats:total_users", str(pool.num_users))
    pipe.set("stats:total_products", str(pool.num_products))
    pipe.set("stats:total_orders", str(len(pool.order_ids)))
    pipe.set("stats:total_revenue", str(total_revenue))
    pipe.set("stats:total_units_sold", str(sum(pool.product_sales.values())))

    # Daily order counts (from actual order_day assignments)
    daily_counts = {}
    for oid in pool.order_ids:
        day = pool.order_day[oid]
        daily_counts[day] = daily_counts.get(day, 0) + 1
    for day, count in daily_counts.items():
        pipe.zadd("stats:daily_orders", {str(day): count})

    # ── Cart cache (hot carts for fast checkout) ──
    num_hot_carts = min(scale["carts"] // 5, 10000)
    log.info(f"  Caching {num_hot_carts:,} active cart summaries...")
    for i in range(1, num_hot_carts + 1):
        uid = rng.choice(pool.user_ids)
        pipe.hset(f"cart:active:{uid}", mapping={
            "cart_id": str(i),
            "item_count": str(rng.randint(1, 8)),
            "total": str(round(rng.uniform(10, 500), 2)),
            "updated_at": rand_date(pool, rng).isoformat(),
        })
        counter += 1
        maybe_flush(counter)

    # ── Recently viewed products per user ──
    log.info("  Building recent product views...")
    for uid in rng.sample(pool.user_ids, min(20000, pool.num_users)):
        viewed = rng.sample(pool.product_ids, min(rng.randint(3, 15), len(pool.product_ids)))
        for pid in viewed:
            pipe.lpush(f"user:recent:{uid}", str(pid))
        pipe.ltrim(f"user:recent:{uid}", 0, 19)  # Keep last 20
        counter += 1
        maybe_flush(counter)

    # ── Abandoned Cart Tracking ──
    # Real-time abandonment state tied to MongoDB cart data.
    # ~40% of carts are abandoned (matching MongoDB cart distribution).
    num_abandoned = scale["carts"] * 40 // 100
    log.info(f"  Tracking {num_abandoned:,} abandoned carts...")
    progress = ProgressTracker("Redis abandoned carts", num_abandoned)

    for i in range(1, num_abandoned + 1):
        uid = rng.choice(pool.user_ids)
        cart_id = i
        created = rand_date(pool, rng)
        # Abandonment happens minutes to days after cart creation
        abandoned_at = created + timedelta(minutes=rng.randint(5, 4320))
        reason = rng.choice(ABANDONMENT_REASONS)
        total = round(rng.uniform(10, 800), 2)
        item_count = rng.randint(1, 8)

        # 25% get a recovery email, 10% of those actually recover
        recovery_sent = rng.random() < 0.25
        recovered = recovery_sent and rng.random() < 0.10
        recovered_order_id = ""
        if recovered:
            # Link to actual order for this user if possible
            for oid in pool.order_ids:
                if pool.order_user[oid] == uid:
                    recovered_order_id = str(oid)
                    break

        pipe.hset(f"cart:abandoned:{cart_id}", mapping={
            "cart_id": str(cart_id),
            "user_id": str(uid),
            "item_count": str(item_count),
            "total": str(total),
            "created_at": created.isoformat(),
            "abandoned_at": abandoned_at.isoformat(),
            "abandonment_reason": reason,
            "recovery_email_sent": "1" if recovery_sent else "0",
            "recovery_email_at": (abandoned_at + timedelta(hours=rng.randint(1, 24))).isoformat() if recovery_sent else "",
            "recovered": "1" if recovered else "0",
            "recovered_order_id": recovered_order_id,
        })
        counter += 1
        maybe_flush(counter)

        # Sorted set for abandonment by value (for "high-value abandoned carts" queries)
        pipe.zadd("abandoned_carts:by_value", {str(cart_id): total})
        # Sorted set by time (for "recently abandoned" queries)
        pipe.zadd("abandoned_carts:by_time", {str(cart_id): int(abandoned_at.timestamp())})
        # Per-reason counter
        pipe.hincrby("stats:abandonment_reasons", reason, 1)

        progress.update()
    progress.finish()

    # Abandonment summary stats
    pipe.set("stats:abandoned_carts", str(num_abandoned))

    # Marker to indicate completion
    pipe.set("_ecommerce_gen_complete", "1")
    pipe.execute()

    log.info(f"  {client.dbsize():,} total keys in Redis")
    client.close()


# ═══════════════════════════════════════════════════════════════
# 4. ClickHouse — Clickstream, Purchase Events, Revenue Analytics
# ═══════════════════════════════════════════════════════════════

def generate_clickhouse(pool, scale):
    import clickhouse_connect

    log.info("=" * 60)
    log.info("Generating ClickHouse data (clickstream, purchases, revenue)...")
    log.info("=" * 60)

    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    rng = random.Random(45)

    # ── Clickstream Events ──
    # ~30% of clickstream events reference products that the user actually ordered,
    # creating realistic browse→purchase funnels that are verifiable across databases.
    count = ch.query("SELECT count() FROM analytics.clickstream_events").result_rows[0][0]
    if count == 0:
        num_events = scale["clickstream"]
        log.info(f"  Generating {num_events:,} clickstream events...")
        progress = ProgressTracker("CH clickstream", num_events)

        columns = ["event_id", "user_id", "product_id", "event_type", "device_type",
                    "browser", "os", "country", "region", "session_id",
                    "referrer", "event_day", "event_hour"]
        batch = []
        batch_size = 50000
        referrers = ["google", "facebook", "instagram", "direct", "email", "twitter", "tiktok", "youtube", "bing", "affiliate"]

        # Build user→purchased_products for realistic clickstream
        user_purchased = {}
        for oid in pool.order_ids:
            uid = pool.order_user[oid]
            if uid not in user_purchased:
                user_purchased[uid] = set()
            for pid, _, _ in pool.order_items[oid]:
                user_purchased[uid].add(pid)

        for i in range(1, num_events + 1):
            uid = rng.choice(pool.user_ids)

            # 30% chance: browse a product this user actually ordered
            if rng.random() < 0.3 and uid in user_purchased and user_purchased[uid]:
                pid = rng.choice(list(user_purchased[uid]))
            else:
                pid = rng.choice(pool.product_ids)

            event_type = rng.choices(
                EVENT_TYPES,
                weights=[30, 25, 12, 5, 4, 3, 10, 5, 3, 3]
            )[0]
            day = rng.randint(0, pool.date_range_days)
            hour = rng.choices(range(24), weights=[
                1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 9,
                8, 7, 7, 8, 9, 9, 8, 7, 6, 4, 3, 2
            ])[0]

            batch.append([
                i, uid, pid, event_type,
                rng.choice(DEVICE_TYPES), rng.choice(BROWSERS), rng.choice(OS_TYPES),
                pool.user_country[uid], pool.user_region[uid],
                hashlib.md5(f"{uid}:{day}".encode()).hexdigest()[:16],
                rng.choice(referrers),
                day, hour,
            ])

            if len(batch) >= batch_size:
                ch.insert("analytics.clickstream_events", batch, column_names=columns)
                progress.update(len(batch))
                batch = []

        if batch:
            ch.insert("analytics.clickstream_events", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()
    else:
        log.info(f"  Clickstream already has {count:,} events, skipping")

    # ── Purchase Events ──
    # Uses the SAME pre-computed order→product mappings as PostgreSQL.
    # Each order line item becomes a purchase event row, so cross-DB queries
    # on order_id, user_id, product_id return consistent results.
    count = ch.query("SELECT count() FROM analytics.purchase_events").result_rows[0][0]
    if count == 0:
        total_items = sum(len(items) for items in pool.order_items.values())
        log.info(f"  Generating {total_items:,} purchase events (from {len(pool.order_ids):,} orders)...")
        progress = ProgressTracker("CH purchases", total_items)

        columns = ["order_id", "user_id", "product_id", "quantity", "unit_price",
                    "total", "payment_method", "country", "region", "brand_id",
                    "category", "event_day"]
        batch = []
        batch_size = 50000

        for oid in pool.order_ids:
            uid = pool.order_user[oid]
            day = pool.order_day[oid]
            pay_method = pool.order_payment[oid]

            for pid, qty, price in pool.order_items[oid]:
                line_total = round(price * qty, 2)
                batch.append([
                    oid, uid, pid, qty, price, line_total,
                    pay_method,
                    pool.user_country[uid], pool.user_region[uid],
                    pool.product_brand[pid], pool.product_category[pid],
                    day,
                ])

                if len(batch) >= batch_size:
                    ch.insert("analytics.purchase_events", batch, column_names=columns)
                    progress.update(len(batch))
                    batch = []

        if batch:
            ch.insert("analytics.purchase_events", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()

        # ── Pre-aggregate daily revenue ──
        log.info("  Building daily revenue aggregates...")
        ch.command("""
            INSERT INTO analytics.revenue_daily
            SELECT
                event_day,
                category,
                country,
                count() AS order_count,
                sum(total) AS revenue,
                uniq(user_id) AS unique_buyers,
                sum(quantity) AS units_sold
            FROM analytics.purchase_events
            GROUP BY event_day, category, country
        """)

        # ── Funnel events ──
        log.info("  Building conversion funnel data...")
        ch.command("""
            INSERT INTO analytics.funnel_events
            SELECT
                event_day,
                event_type AS step,
                device_type,
                country,
                count() AS event_count,
                uniq(user_id) AS unique_users
            FROM analytics.clickstream_events
            WHERE event_type IN ('page_view', 'product_view', 'add_to_cart', 'begin_checkout', 'purchase')
            GROUP BY event_day, event_type, device_type, country
        """)
    else:
        log.info(f"  Purchase events already has {count:,} rows, skipping")

    ch.close()


# ═══════════════════════════════════════════════════════════════
# 5. Weaviate — Product & Review Embeddings
# ═══════════════════════════════════════════════════════════════

def generate_weaviate(pool, scale):
    import weaviate
    from weaviate.classes.config import Configure, Property, DataType

    log.info("=" * 60)
    log.info("Generating Weaviate data (product & review embeddings)...")
    log.info("=" * 60)

    client = weaviate.connect_to_custom(
        http_host=WEAVIATE_URL.replace("http://", "").split(":")[0],
        http_port=int(WEAVIATE_URL.split(":")[-1]),
        http_secure=False,
        grpc_host=WEAVIATE_URL.replace("http://", "").split(":")[0],
        grpc_port=50051,
        grpc_secure=False,
    )

    rng = random.Random(46)

    # ── Product Embeddings ──
    if not client.collections.exists("Product"):
        client.collections.create(
            name="Product",
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="product_id", data_type=DataType.INT),
                Property(name="brand_id", data_type=DataType.INT),
                Property(name="name", data_type=DataType.TEXT),
                Property(name="category", data_type=DataType.TEXT),
                Property(name="subcategory", data_type=DataType.TEXT),
                Property(name="price", data_type=DataType.NUMBER),
                Property(name="rating_avg", data_type=DataType.NUMBER),
            ],
        )

    product_col = client.collections.get("Product")
    resp = product_col.aggregate.over_all(total_count=True)
    if resp.total_count == 0:
        log.info("  Loading sentence-transformers model...")
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")

        limit = min(pool.num_products, WEAVIATE_LIMIT)
        log.info(f"  Embedding {limit:,} products...")
        progress = ProgressTracker("Weaviate products", limit)

        batch_size = 100
        for start in range(0, limit, batch_size):
            end = min(start + batch_size, limit)
            texts = []
            product_data = []
            for pid in pool.product_ids[start:end]:
                cat = pool.product_category[pid]
                subcat = pool.product_subcategory[pid]
                brand = BRAND_NAMES[pool.product_brand[pid] - 1]
                text = f"{brand} {subcat} in {cat}. Price ${pool.product_price[pid]:.2f}"
                texts.append(text)
                product_data.append({
                    "product_id": pid,
                    "brand_id": pool.product_brand[pid],
                    "name": f"{brand} {subcat} #{pid}",
                    "category": cat,
                    "subcategory": subcat,
                    "price": pool.product_price[pid],
                    "rating_avg": round(rng.uniform(1.0, 5.0), 1),
                })

            embeddings = model.encode(texts, show_progress_bar=False)
            with product_col.batch.dynamic() as batch:
                for j in range(len(texts)):
                    batch.add_object(
                        properties=product_data[j],
                        vector=embeddings[j].tolist(),
                    )
            progress.update(end - start)
        progress.finish()
    else:
        log.info(f"  Product embeddings already exist ({resp.total_count:,}), skipping")

    # ── Review Embeddings ──
    if not client.collections.exists("ProductReview"):
        client.collections.create(
            name="ProductReview",
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="review_id", data_type=DataType.INT),
                Property(name="user_id", data_type=DataType.INT),
                Property(name="product_id", data_type=DataType.INT),
                Property(name="brand_id", data_type=DataType.INT),
                Property(name="rating", data_type=DataType.INT),
                Property(name="review_text", data_type=DataType.TEXT),
                Property(name="category", data_type=DataType.TEXT),
            ],
        )

    review_col = client.collections.get("ProductReview")
    resp = review_col.aggregate.over_all(total_count=True)
    if resp.total_count == 0:
        from sentence_transformers import SentenceTransformer
        try:
            model
        except NameError:
            log.info("  Loading sentence-transformers model...")
            model = SentenceTransformer("all-MiniLM-L6-v2")

        # Reviews come from users who actually purchased the product.
        # We sample from real order line items so user→product→brand
        # relationships are consistent with PG orders and CH purchases.
        num_reviews = min(scale["reviews"], WEAVIATE_LIMIT)
        log.info(f"  Embedding {num_reviews:,} reviews (from actual purchasers)...")
        progress = ProgressTracker("Weaviate reviews", num_reviews)

        # Build a flat list of (user_id, product_id) from actual orders
        purchase_pairs = []
        for oid in pool.order_ids:
            uid = pool.order_user[oid]
            for pid, _, _ in pool.order_items[oid]:
                purchase_pairs.append((uid, pid))
        # Shuffle and cap to review count
        rng.shuffle(purchase_pairs)
        if len(purchase_pairs) > num_reviews:
            purchase_pairs = purchase_pairs[:num_reviews]
        # Pad if needed (repeat some purchases)
        while len(purchase_pairs) < num_reviews:
            purchase_pairs.append(rng.choice(purchase_pairs))

        batch_size = 100
        for start in range(0, num_reviews, batch_size):
            end = min(start + batch_size, num_reviews)
            texts = []
            review_data = []
            for i in range(start, end):
                uid, pid = purchase_pairs[i]
                bid = pool.product_brand[pid]
                rating = rng.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0]
                text = rng.choice(REVIEW_TEXTS[rating])
                texts.append(text)
                review_data.append({
                    "review_id": i + 1,
                    "user_id": uid,
                    "product_id": pid,
                    "brand_id": bid,
                    "rating": rating,
                    "review_text": text,
                    "category": pool.product_category[pid],
                })

            embeddings = model.encode(texts, show_progress_bar=False)
            with review_col.batch.dynamic() as batch:
                for j in range(len(texts)):
                    batch.add_object(
                        properties=review_data[j],
                        vector=embeddings[j].tolist(),
                    )
            progress.update(end - start)
        progress.finish()
    else:
        log.info(f"  Review embeddings already exist ({resp.total_count:,}), skipping")

    # ── Search Query Embeddings (user search history) ──
    # Semantic search over what users searched for — enables "find similar searches"
    # and "users who searched for X also bought Y" style queries.
    if not client.collections.exists("SearchQuery"):
        client.collections.create(
            name="SearchQuery",
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                Property(name="search_id", data_type=DataType.INT),
                Property(name="user_id", data_type=DataType.INT),
                Property(name="query_text", data_type=DataType.TEXT),
                Property(name="results_count", data_type=DataType.INT),
                Property(name="clicked_product_id", data_type=DataType.INT),
                Property(name="clicked_position", data_type=DataType.INT),
                Property(name="device_type", data_type=DataType.TEXT),
                Property(name="country", data_type=DataType.TEXT),
                Property(name="converted", data_type=DataType.BOOL),
            ],
        )

    search_col = client.collections.get("SearchQuery")
    resp = search_col.aggregate.over_all(total_count=True)
    if resp.total_count == 0:
        from sentence_transformers import SentenceTransformer
        try:
            model
        except NameError:
            log.info("  Loading sentence-transformers model...")
            model = SentenceTransformer("all-MiniLM-L6-v2")

        num_searches = min(scale["searches"], WEAVIATE_LIMIT)
        log.info(f"  Embedding {num_searches:,} search queries...")
        progress = ProgressTracker("Weaviate searches", num_searches)

        # Build user→purchased_products for realistic click-through
        user_purchased = {}
        for oid in pool.order_ids:
            uid = pool.order_user[oid]
            if uid not in user_purchased:
                user_purchased[uid] = []
            for pid, _, _ in pool.order_items[oid]:
                user_purchased[uid].append(pid)

        batch_size = 100
        for start in range(0, num_searches, batch_size):
            end = min(start + batch_size, num_searches)
            texts = []
            search_data = []
            for i in range(start, end):
                uid = rng.choice(pool.user_ids)
                query_text = rng.choice(SEARCH_QUERIES)
                # Add variation: sometimes append category/brand modifiers
                if rng.random() < 0.3:
                    query_text += " " + rng.choice(["under $50", "best rated", "on sale", "free shipping", "premium"])
                if rng.random() < 0.15:
                    query_text += " " + rng.choice(BRAND_NAMES)

                results_count = rng.choices(
                    [0, rng.randint(1, 10), rng.randint(11, 50), rng.randint(51, 200)],
                    weights=[5, 20, 50, 25]
                )[0]

                # 40% of searches result in a click
                clicked_pid = 0
                clicked_pos = 0
                converted = False
                if results_count > 0 and rng.random() < 0.4:
                    # 30% click on something they actually bought
                    if rng.random() < 0.3 and uid in user_purchased and user_purchased[uid]:
                        clicked_pid = rng.choice(user_purchased[uid])
                        converted = True
                    else:
                        clicked_pid = rng.choice(pool.product_ids)
                    clicked_pos = rng.choices(
                        [1, 2, 3, rng.randint(4, 10), rng.randint(11, 50)],
                        weights=[35, 25, 15, 20, 5]
                    )[0]

                texts.append(query_text)
                search_data.append({
                    "search_id": i + 1,
                    "user_id": uid,
                    "query_text": query_text,
                    "results_count": results_count,
                    "clicked_product_id": clicked_pid,
                    "clicked_position": clicked_pos,
                    "device_type": rng.choice(DEVICE_TYPES),
                    "country": pool.user_country[uid],
                    "converted": converted,
                })

            embeddings = model.encode(texts, show_progress_bar=False)
            with search_col.batch.dynamic() as batch:
                for j in range(len(texts)):
                    batch.add_object(
                        properties=search_data[j],
                        vector=embeddings[j].tolist(),
                    )
            progress.update(end - start)
        progress.finish()
    else:
        log.info(f"  Search query embeddings already exist ({resp.total_count:,}), skipping")

    client.close()


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data across all databases")
    parser.add_argument("--scale", choices=SCALES.keys(),
                        default=os.environ.get("SCALE", "medium"),
                        help="Data scale profile (default: medium)")
    parser.add_argument("--skip", nargs="*", default=[],
                        choices=["postgres", "mongodb", "redis", "clickhouse", "weaviate"],
                        help="Skip specific databases")
    args = parser.parse_args()

    scale = SCALES[args.scale]
    skip = set(args.skip)
    if DISABLE_CLICKHOUSE_INIT:
        skip.add("clickhouse")
        log.warning("Skipping ClickHouse generation because DISABLE_CLICKHOUSE_INIT is enabled")

    start = time.time()
    log.info("=" * 60)
    log.info("E-Commerce Data Generator — Cross-Database")
    log.info(f"Scale: {args.scale}")
    log.info(f"  Users:       {scale['users']:>12,}")
    log.info(f"  Products:    {scale['products']:>12,}")
    log.info(f"  Orders:      {scale['orders']:>12,}")
    log.info(f"  Carts:       {scale['carts']:>12,}")
    log.info(f"  Clickstream: {scale['clickstream']:>12,}")
    log.info(f"  Reviews:     {scale['reviews']:>12,}")
    log.info(f"  Shipments:   {scale['shipments']:>12,}")
    log.info(f"  Searches:    {scale['searches']:>12,}")
    log.info(f"  Coupons:     {scale['coupons']:>12,}")
    log.info("=" * 60)

    log.info("Pre-generating shared ID pool...")
    pool = IDPool(scale)
    log.info(f"  ID pool ready ({pool.num_users:,} users, {pool.num_products:,} products, {len(pool.order_ids):,} orders)")

    steps = [
        ("postgres", "PostgreSQL", generate_postgres),
        ("mongodb", "MongoDB", generate_mongodb),
        ("redis", "Redis", generate_redis),
        ("clickhouse", "ClickHouse", generate_clickhouse),
        ("weaviate", "Weaviate", generate_weaviate),
    ]

    for key, name, fn in steps:
        if key in skip:
            log.info(f"\nSkipping {name} (--skip)")
            continue
        log.info(f"\n{'─' * 40}")
        log.info(f"Generating {name} data...")
        log.info(f"{'─' * 40}")
        try:
            fn(pool, scale)
        except Exception as e:
            log.error(f"Failed to generate {name} data: {e}", exc_info=True)

    elapsed = time.time() - start
    log.info(f"\n{'=' * 60}")
    log.info(f"Data generation complete in {elapsed:.1f}s")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
