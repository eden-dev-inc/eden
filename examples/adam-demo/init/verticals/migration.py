"""
Migration vertical — Financial Services Redis Migration

Populates the source Redis with realistic banking/trading data using
multiple Redis data types. The destination Redis starts empty.
During the demo, Eden's migration feature syncs data from source to dest.
"""

import os
import random
import logging
import json
from datetime import datetime, timedelta

from verticals.base import VerticalBase, DatabaseSilo, ProgressTracker

log = logging.getLogger("adam-init")

# Target ~1 GB at demo scale for a compelling migration demo
SCALE_LIMITS = {
    "demo":    {"accounts": 500_000, "sessions": 200_000, "orders": 2_000_000, "alerts": 100_000, "customers": 200_000},
    "small":   {"accounts": 100_000, "sessions": 50_000,  "orders": 500_000,   "alerts": 20_000,  "customers": 50_000},
    "medium":  {"accounts": 500_000, "sessions": 200_000, "orders": 2_000_000, "alerts": 100_000, "customers": 200_000},
    "large":   {"accounts": 1_000_000, "sessions": 500_000, "orders": 5_000_000, "alerts": 500_000, "customers": 500_000},
    "massive": {"accounts": 2_000_000, "sessions": 1_000_000, "orders": 10_000_000, "alerts": 1_000_000, "customers": 1_000_000},
}

MERCHANTS = ["Amazon", "Walmart", "Target", "Costco", "BestBuy", "HomeDepot", "Starbucks",
             "McDonalds", "Shell", "Chevron", "Delta", "United", "Hilton", "Marriott",
             "Netflix", "Spotify", "Apple", "Google", "Microsoft", "Tesla"]

CATEGORIES = ["groceries", "electronics", "dining", "travel", "gas", "entertainment",
              "healthcare", "utilities", "clothing", "home_improvement"]

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM", "BAC", "GS",
           "WFC", "C", "MS", "BRK.B", "V", "MA", "PYPL", "SQ", "COIN", "HOOD"]


class MigrationVertical(VerticalBase):
    name = "migration"
    description = "Financial Services Redis Migration"

    def silos(self):
        return [
            DatabaseSilo(
                name="redis_source",
                db_type="redis",
                description="Source Redis — Legacy on-prem financial data cache",
                url_env_var="REDIS_SOURCE_URL",
                eden_url_env_var="EDEN_REDIS_SOURCE_URL",
                team="Platform",
            ),
        ]

    def load_silo(self, silo, scale):
        if silo.name == "redis_source":
            self._load_source_redis(silo, scale)

    def _load_source_redis(self, silo, scale):
        import redis as r

        limits = SCALE_LIMITS.get(scale, SCALE_LIMITS["demo"])

        log.info("=" * 60)
        log.info("Populating source Redis with financial data...")
        log.info(f"Scale: {scale} — {limits}")
        log.info("=" * 60)

        url = os.environ.get(silo.url_env_var, "redis://localhost:6379")
        # Azure Redis requires TLS; skip cert verification to avoid CA issues in Docker
        ssl_kwargs = {}
        if url.startswith("rediss://"):
            ssl_kwargs["ssl_cert_reqs"] = None
        client = r.from_url(url, decode_responses=True, **ssl_kwargs)

        # Check if already populated (use DBSIZE; skip if > 100 keys)
        try:
            existing = client.dbsize()
            if existing > 100:
                log.info(f"Already populated ({existing:,} keys), skipping")
                client.close()
                return
        except Exception as e:
            log.warning(f"DBSIZE check failed ({e}), proceeding with load...")

        rng = random.Random(42)
        pipe = client.pipeline()
        batch_size = 5000
        count = 0

        # ── 1. Account balances (STRING) ──
        num_accounts = limits["accounts"]
        log.info(f"  Loading {num_accounts:,} account balances (STRING)...")
        progress = ProgressTracker("Account balances", num_accounts)
        for i in range(num_accounts):
            balance = round(rng.uniform(100, 500000), 2)
            pipe.set(f"balance:acct:{i}", str(balance))
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 2. Fraud scores (SORTED SET) ──
        log.info(f"  Loading {num_accounts:,} fraud risk scores (SORTED SET)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Fraud scores", num_accounts)
        count = 0
        for i in range(num_accounts):
            score = round(rng.uniform(0, 100), 2)
            pipe.zadd("fraud:scores", {f"acct:{i}": score})
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 3. Session tokens (HASH) ──
        num_sessions = limits["sessions"]
        log.info(f"  Loading {num_sessions:,} session tokens (HASH)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Sessions", num_sessions)
        count = 0
        for i in range(num_sessions):
            acct = rng.randint(0, num_accounts - 1)
            ip = f"{rng.randint(1,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
            pipe.hset(f"session:{i}", mapping={
                "account_id": str(acct),
                "ip": ip,
                "user_agent": rng.choice(["Chrome/120", "Firefox/121", "Safari/17", "Mobile/iOS", "Mobile/Android"]),
                "permissions": rng.choice(["read", "read,write", "read,write,admin"]),
                "last_active": (datetime.now() - timedelta(minutes=rng.randint(0, 1440))).isoformat(),
                "mfa_verified": str(rng.choice([True, True, True, False])).lower(),
            })
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 4. Transaction rate limiters (STRING with TTL) ──
        log.info(f"  Loading {num_accounts:,} rate limiters (STRING + TTL)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Rate limiters", num_accounts)
        count = 0
        for i in range(num_accounts):
            txn_count = rng.randint(0, 50)
            pipe.setex(f"ratelimit:acct:{i}", 3600, str(txn_count))
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 5. Market data feed (HASH) ──
        log.info(f"  Loading {len(SYMBOLS)} market data feeds (HASH)...")
        pipe = client.pipeline()
        for symbol in SYMBOLS:
            base_price = rng.uniform(10, 5000)
            spread = base_price * 0.001
            pipe.hset(f"market:{symbol}", mapping={
                "bid": str(round(base_price - spread, 2)),
                "ask": str(round(base_price + spread, 2)),
                "last": str(round(base_price, 2)),
                "volume": str(rng.randint(100000, 50000000)),
                "change_pct": str(round(rng.uniform(-5, 5), 2)),
                "updated_at": datetime.now().isoformat(),
            })
        pipe.execute()

        # ── 6. Pending fraud alerts (LIST) ──
        num_alerts = limits["alerts"]
        log.info(f"  Loading {num_alerts:,} fraud alerts (LIST)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Fraud alerts", num_alerts)
        count = 0
        for i in range(num_alerts):
            alert = json.dumps({
                "alert_id": i,
                "account_id": rng.randint(0, num_accounts - 1),
                "type": rng.choice(["suspicious_login", "large_transfer", "velocity_breach", "geo_anomaly", "device_change"]),
                "severity": rng.choice(["low", "medium", "high", "critical"]),
                "amount": round(rng.uniform(100, 50000), 2),
                "merchant": rng.choice(MERCHANTS),
                "timestamp": (datetime.now() - timedelta(hours=rng.randint(0, 72))).isoformat(),
            })
            pipe.rpush("alerts:fraud:pending", alert)
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 7. Recent transactions per account (LIST, capped) ──
        num_orders = limits["orders"]
        log.info(f"  Loading {num_orders:,} recent transactions (LIST per account)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Transactions", num_orders)
        count = 0
        for i in range(num_orders):
            acct = rng.randint(0, min(num_accounts - 1, 99999))
            txn = json.dumps({
                "txn_id": i,
                "amount": round(rng.lognormvariate(3, 1.5), 2),
                "merchant": rng.choice(MERCHANTS),
                "category": rng.choice(CATEGORIES),
                "currency": "USD",
                "status": rng.choice(["completed", "pending", "declined", "refunded"]),
                "channel": rng.choice(["online", "pos", "atm", "mobile", "wire"]),
                "location": f"{rng.choice(['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','Austin'])}, {rng.choice(['NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX'])}",
                "reference": f"TXN-{i:012d}",
                "timestamp": (datetime.now() - timedelta(hours=rng.randint(0, 720))).isoformat(),
            })
            pipe.lpush(f"txns:acct:{acct}", txn)
            pipe.ltrim(f"txns:acct:{acct}", 0, 49)  # keep last 50
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 8. Customer profiles (JSON-like HASH) ──
        num_customers = limits.get("customers", min(num_accounts, 50000))
        log.info(f"  Loading {num_customers:,} customer profiles (HASH)...")
        pipe = client.pipeline()
        progress = ProgressTracker("Customer profiles", num_customers)
        count = 0
        tiers = ["basic", "silver", "gold", "platinum", "private_banking"]
        for i in range(num_customers):
            pipe.hset(f"customer:{i}", mapping={
                "name": f"Customer {i}",
                "tier": rng.choices(tiers, weights=[40, 25, 20, 10, 5])[0],
                "credit_score": str(rng.randint(300, 850)),
                "account_age_days": str(rng.randint(30, 7300)),
                "total_assets": str(round(rng.lognormvariate(10, 2), 2)),
                "risk_rating": rng.choice(["low", "moderate", "elevated", "high"]),
                "kyc_verified": str(rng.choice([True, True, True, True, False])).lower(),
                "last_login": (datetime.now() - timedelta(days=rng.randint(0, 90))).isoformat(),
            })
            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = client.pipeline()
                progress.update(batch_size)
        pipe.execute()
        progress.update(count % batch_size)
        progress.finish()

        # ── 9. Leaderboards (SORTED SET) ──
        log.info("  Loading merchant leaderboards (SORTED SET)...")
        pipe = client.pipeline()
        for merchant in MERCHANTS:
            pipe.zadd("leaderboard:merchant_volume", {merchant: rng.randint(10000, 10000000)})
            pipe.zadd("leaderboard:merchant_fraud_rate", {merchant: round(rng.uniform(0, 5), 2)})
        # Top accounts by balance
        for i in range(min(num_accounts, 10000)):
            pipe.zadd("leaderboard:top_balances", {f"acct:{i}": round(rng.uniform(100, 500000), 2)})
        pipe.execute()

        # ── 10. Aggregate stats ──
        log.info("  Setting aggregate stats...")
        pipe = client.pipeline()
        pipe.set("stats:total_accounts", str(num_accounts))
        pipe.set("stats:total_sessions", str(num_sessions))
        pipe.set("stats:pending_alerts", str(num_alerts))
        pipe.set("stats:total_transactions_today", str(rng.randint(100000, 5000000)))
        pipe.set("stats:fraud_rate_pct", str(round(rng.uniform(0.1, 2.5), 2)))
        pipe.set("stats:avg_transaction_amount", str(round(rng.uniform(50, 500), 2)))
        pipe.set("stats:system_status", "operational")
        pipe.execute()

        total_keys = client.dbsize()
        log.info(f"  Source Redis populated: {total_keys:,} keys total")
        client.close()
