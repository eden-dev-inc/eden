"""
Tech / SaaS vertical.

Simulates a tech company with data scattered across 6 department silos:
  pg-network-security  (Postgres)   — SecOps: UNSW-NB15 network intrusion data (~2.5M flows)
  pg-saas-billing      (Postgres)   — Finance: SaaS subscriptions, invoices, API usage
  clickhouse-events    (ClickHouse) — Product: User behavior events (~285M from ecommerce-behavior)
  mongo-cve            (MongoDB)    — Security: CVE vulnerability records (~300K from NVD 1999-2025)
  redis-sessions       (Redis)      — Platform: Active sessions, rate limits, feature flags
  weaviate-logs        (Weaviate)   — SRE: Vulnerability & incident semantic search

HuggingFace datasets:
  - wwydmanski/UNSW-NB15                          (~2.5M network flows, 49 features)
  - kevykibbz/ecommerce-behavior-data...           (~285M user events)
  - stasvinokur/cve-and-cwe-dataset-1999-2025      (~300K CVE records)
  - pacovaldez/stackoverflow-questions             (~23M questions)
"""

import os
import random
import logging
from datetime import datetime, timedelta

from verticals.base import (
    VerticalBase, DatabaseSilo, ProgressTracker, load_local_or_stream,
    FIRST_NAMES, LAST_NAMES, rand_email, rand_date,
)

log = logging.getLogger("adam-init")

# Scale controls how much of each HF dataset to load
SCALE_LIMITS = {
    "demo":    {"network_flows": 50_000,   "events": 500_000,     "cves": 10_000,   "orgs": 1_000},
    "small":   {"network_flows": 100_000,  "events": 1_000_000,   "cves": 50_000,   "orgs": 5_000},
    "medium":  {"network_flows": 500_000,  "events": 10_000_000,  "cves": 150_000,  "orgs": 25_000},
    "large":   {"network_flows": 2_500_000, "events": 50_000_000, "cves": 300_000,  "orgs": 100_000},
    "massive": {"network_flows": 2_500_000, "events": 285_000_000, "cves": 300_000, "orgs": 500_000},
}

# SaaS-specific reference data
INDUSTRIES = ["Technology", "Finance", "Healthcare", "Retail", "Education", "Manufacturing", "Media", "Government"]
SIZE_TIERS = ["startup", "smb", "mid_market", "enterprise"]
PLAN_NAMES = ["free", "starter", "pro", "business", "enterprise"]
PLAN_PRICES = {"free": 0, "starter": 29, "pro": 99, "business": 299, "enterprise": 999}
SUB_STATUSES = ["active", "trial", "past_due", "cancelled", "paused"]
INVOICE_STATUSES = ["draft", "open", "paid", "void", "uncollectible"]
PAYMENT_METHODS = ["credit_card", "wire", "ach", "paypal"]
PAYMENT_STATUSES = ["succeeded", "failed", "refunded", "pending"]
USER_ROLES = ["admin", "developer", "viewer", "billing"]
API_SCOPES = [["read"], ["read", "write"], ["read", "write", "admin"]]


class TechVertical(VerticalBase):
    name = "tech"
    description = "Tech / SaaS"

    def silos(self) -> list[DatabaseSilo]:
        return [
            DatabaseSilo(
                name="pg_network_security",
                db_type="postgres",
                description="SecOps — Network intrusion detection (UNSW-NB15, 2.5M flows)",
                url_env_var="PG_NETWORK_SECURITY_URL",
                eden_url_env_var="EDEN_PG_NETWORK_SECURITY_URL",
                schema_file="tech/postgres_network_security.sql",
                hf_dataset="wwydmanski/UNSW-NB15",
                team="SecOps",
            ),
            DatabaseSilo(
                name="pg_saas_billing",
                db_type="postgres",
                description="Finance — SaaS subscriptions, invoices, API usage",
                url_env_var="PG_SAAS_BILLING_URL",
                eden_url_env_var="EDEN_PG_SAAS_BILLING_URL",
                schema_file="tech/postgres_saas_billing.sql",
                team="Finance",
            ),
            DatabaseSilo(
                name="ch_user_events",
                db_type="clickhouse",
                description="Product Analytics — User behavior event stream (285M events)",
                url_env_var="CLICKHOUSE_HOST",
                eden_url_env_var="EDEN_CLICKHOUSE_URL",
                schema_file="tech/clickhouse_events.sql",
                hf_dataset="kevykibbz/ecommerce-behavior-data-from-multi-category-store_oct-nov_2019",
                team="Product",
            ),
            DatabaseSilo(
                name="mongo_cve",
                db_type="mongo",
                description="Security — CVE vulnerability database (300K records, NVD 1999-2025)",
                url_env_var="MONGO_URL",
                eden_url_env_var="EDEN_MONGO_URL",
                hf_dataset="stasvinokur/cve-and-cwe-dataset-1999-2025",
                team="Security",
            ),
            DatabaseSilo(
                name="redis_sessions",
                db_type="redis",
                description="Platform — Active sessions, rate limits, feature flags",
                url_env_var="REDIS_URL",
                eden_url_env_var="EDEN_REDIS_URL",
                team="Platform",
            ),
            DatabaseSilo(
                name="weaviate_logs",
                db_type="weaviate",
                description="SRE — Incident & issue semantic search",
                url_env_var="WEAVIATE_URL",
                eden_url_env_var="EDEN_WEAVIATE_URL",
                team="SRE",
            ),
        ]

    def load_silo(self, silo: DatabaseSilo, scale: str):
        limits = SCALE_LIMITS.get(scale, SCALE_LIMITS["small"])
        if silo.name == "pg_network_security":
            self._load_network_security(silo, limits)
        elif silo.name == "pg_saas_billing":
            self._load_saas_billing(silo, limits)
        elif silo.name == "ch_user_events":
            self._load_user_events(silo, limits)
        elif silo.name == "mongo_cve":
            self._load_cve(silo, limits)
        elif silo.name == "redis_sessions":
            self._load_redis(silo, limits)
        elif silo.name == "weaviate_logs":
            self._load_weaviate(silo, limits)

    # ── Postgres #1: Network Security (UNSW-NB15) ────────────────

    def _load_network_security(self, silo: DatabaseSilo, limits: dict):
        import psycopg2
        from psycopg2.extras import execute_values

        log.info("=" * 60)
        log.info("Loading UNSW-NB15 network flows into pg-network-security...")
        log.info("=" * 60)

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM network_flows")
        if cur.fetchone()[0] > 0:
            log.info("Already populated, skipping")
            conn.close()
            return

        limit = limits["network_flows"]
        rows, total = load_local_or_stream("tech", "unsw_nb15.parquet", silo.hf_dataset, limit=limit)

        log.info(f"  Loading up to {limit:,} network flows...")
        progress = ProgressTracker("Network flows", total or limit)

        # Map UNSW-NB15 columns to our schema
        batch = []
        batch_size = 5000
        count = 0
        for row in rows:
            if count >= limit:
                break
            batch.append((
                row.get("srcip", ""), row.get("sport", 0),
                row.get("dstip", ""), row.get("dsport", 0),
                row.get("proto", ""), row.get("state", ""),
                row.get("dur", 0), row.get("sbytes", 0), row.get("dbytes", 0),
                row.get("sttl", 0), row.get("dttl", 0),
                row.get("sloss", 0), row.get("dloss", 0),
                row.get("service", ""), row.get("sload", 0), row.get("dload", 0),
                row.get("spkts", 0), row.get("dpkts", 0),
                row.get("swin", 0), row.get("dwin", 0),
                row.get("stcpb", 0), row.get("dtcpb", 0),
                row.get("smeansz", 0), row.get("dmeansz", 0),
                row.get("trans_depth", 0), row.get("res_bdy_len", 0),
                row.get("sjit", 0), row.get("djit", 0),
                row.get("sinpkt", 0), row.get("dinpkt", 0),
                row.get("tcprtt", 0), row.get("synack", 0), row.get("ackdat", 0),
                bool(row.get("is_sm_ips_ports", 0)),
                row.get("ct_state_ttl", 0), row.get("ct_flw_http_mthd", 0),
                bool(row.get("is_ftp_login", 0)), row.get("ct_ftp_cmd", 0),
                row.get("ct_srv_src", 0), row.get("ct_srv_dst", 0),
                row.get("ct_dst_ltm", 0), row.get("ct_src_ltm", 0),
                row.get("ct_src_dport_ltm", 0), row.get("ct_dst_sport_ltm", 0),
                row.get("ct_dst_src_ltm", 0),
                str(row.get("attack_cat", "Normal")).strip(),
                row.get("label", 0),
            ))
            count += 1
            if len(batch) >= batch_size:
                execute_values(cur, """INSERT INTO network_flows
                    (srcip, sport, dstip, dsport, proto, state, dur, sbytes, dbytes,
                     sttl, dttl, sloss, dloss, service, sload, dload, spkts, dpkts,
                     swin, dwin, stcpb, dtcpb, smeansz, dmeansz, trans_depth, res_bdy_len,
                     sjit, djit, sinpkt, dinpkt, tcprtt, synack, ackdat,
                     is_sm_ips_ports, ct_state_ttl, ct_flw_http_mthd, is_ftp_login, ct_ftp_cmd,
                     ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm,
                     ct_src_dport_ltm, ct_dst_sport_ltm, ct_dst_src_ltm,
                     attack_cat, label)
                    VALUES %s""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO network_flows
                (srcip, sport, dstip, dsport, proto, state, dur, sbytes, dbytes,
                 sttl, dttl, sloss, dloss, service, sload, dload, spkts, dpkts,
                 swin, dwin, stcpb, dtcpb, smeansz, dmeansz, trans_depth, res_bdy_len,
                 sjit, djit, sinpkt, dinpkt, tcprtt, synack, ackdat,
                 is_sm_ips_ports, ct_state_ttl, ct_flw_http_mthd, is_ftp_login, ct_ftp_cmd,
                 ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm,
                 ct_src_dport_ltm, ct_dst_sport_ltm, ct_dst_src_ltm,
                 attack_cat, label)
                VALUES %s""", batch)
            progress.update(len(batch))
        progress.finish()

        # Generate security alerts from attack flows
        log.info("  Generating security alerts from attack flows...")
        cur.execute("""
            INSERT INTO security_alerts (flow_id, alert_type, severity, attack_cat, srcip, dstip, description, status)
            SELECT flow_id, 'intrusion',
                   CASE WHEN attack_cat IN ('Backdoor', 'Shellcode', 'Worms') THEN 'critical'
                        WHEN attack_cat IN ('Exploits', 'DoS') THEN 'high'
                        WHEN attack_cat IN ('Fuzzers', 'Reconnaissance') THEN 'medium'
                        ELSE 'low' END,
                   attack_cat, srcip, dstip,
                   'Detected ' || attack_cat || ' attack from ' || srcip || ' to ' || dstip,
                   'open'
            FROM network_flows WHERE label = 1 LIMIT 50000
        """)

        conn.close()
        log.info("  Network security data loaded")

    # ── Postgres #2: SaaS Billing (synthetic) ────────────────────

    def _load_saas_billing(self, silo: DatabaseSilo, limits: dict):
        import psycopg2
        from psycopg2.extras import execute_values

        log.info("=" * 60)
        log.info("Generating SaaS billing data into pg-saas-billing...")
        log.info("=" * 60)

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM organizations")
        if cur.fetchone()[0] > 0:
            log.info("Already populated, skipping")
            conn.close()
            return

        rng = random.Random(42)
        num_orgs = limits["orgs"]
        batch_size = 5000
        base_date = datetime(2024, 1, 1)

        # Plans
        log.info("  Loading plans...")
        for i, name in enumerate(PLAN_NAMES, 1):
            price = PLAN_PRICES[name]
            cur.execute("""INSERT INTO plans (plan_id, plan_name, monthly_price, annual_price, api_limit, seat_limit, storage_gb)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                (i, name, price, price * 10, [1000, 10000, 100000, 500000, 0][i-1],
                 [1, 5, 25, 100, 0][i-1], [1, 10, 100, 500, 0][i-1]))

        # Organizations
        log.info(f"  Loading {num_orgs:,} organizations...")
        progress = ProgressTracker("Organizations", num_orgs)
        rows = []
        for oid in range(1, num_orgs + 1):
            mrr = round(rng.lognormvariate(5, 2), 2)
            mrr = max(0, min(mrr, 50000))
            rows.append((
                oid, f"Org-{oid:06d}", rng.choice(INDUSTRIES),
                rng.choices(SIZE_TIERS, weights=[40, 30, 20, 10])[0],
                rng.choice(["US", "GB", "DE", "CA", "AU", "FR", "JP", "BR"]),
                rng.choice(["North America", "Europe", "Asia Pacific"]),
                rand_date(base_date, 365, rng), mrr, round(mrr * 12, 2),
                rng.randint(10, 100),
            ))
            if len(rows) >= batch_size:
                execute_values(cur, """INSERT INTO organizations
                    (org_id, org_name, industry, size_tier, country, region, created_at, mrr, arr, health_score)
                    VALUES %s ON CONFLICT DO NOTHING""", rows)
                progress.update(len(rows))
                rows = []
        if rows:
            execute_values(cur, """INSERT INTO organizations
                (org_id, org_name, industry, size_tier, country, region, created_at, mrr, arr, health_score)
                VALUES %s ON CONFLICT DO NOTHING""", rows)
            progress.update(len(rows))
        progress.finish()

        # Users (5-20 per org)
        num_users = num_orgs * 8  # avg 8 users per org
        log.info(f"  Loading ~{num_users:,} users...")
        progress = ProgressTracker("Users", num_users)
        rows = []
        uid = 0
        for oid in range(1, num_orgs + 1):
            n_users = rng.randint(1, 20)
            for _ in range(n_users):
                uid += 1
                first = rng.choice(FIRST_NAMES)
                last = rng.choice(LAST_NAMES)
                rows.append((
                    uid, oid, rand_email(first, last, uid, rng),
                    rng.choice(USER_ROLES),
                    rng.choices(["active", "active", "active", "suspended", "deactivated"], weights=[70, 10, 5, 10, 5])[0],
                    rand_date(base_date, 365, rng), rand_date(base_date, 365, rng),
                ))
                if len(rows) >= batch_size:
                    execute_values(cur, """INSERT INTO users
                        (user_id, org_id, email, role, status, last_login, created_at)
                        VALUES %s ON CONFLICT DO NOTHING""", rows)
                    progress.update(len(rows))
                    rows = []
        if rows:
            execute_values(cur, """INSERT INTO users
                (user_id, org_id, email, role, status, last_login, created_at)
                VALUES %s ON CONFLICT DO NOTHING""", rows)
            progress.update(len(rows))
        progress.finish()

        # Subscriptions (1 per org)
        log.info(f"  Loading {num_orgs:,} subscriptions...")
        rows = []
        for oid in range(1, num_orgs + 1):
            plan = rng.choices([1, 2, 3, 4, 5], weights=[20, 30, 25, 15, 10])[0]
            start = rand_date(base_date, 365, rng)
            rows.append((
                oid, oid, plan, rng.choices(SUB_STATUSES, weights=[60, 10, 10, 15, 5])[0],
                rng.choice(["monthly", "annual"]),
                start, start + timedelta(days=30), None, None, start,
            ))
            if len(rows) >= batch_size:
                execute_values(cur, """INSERT INTO subscriptions
                    (subscription_id, org_id, plan_id, status, billing_cycle,
                     current_period_start, current_period_end, trial_end, cancelled_at, created_at)
                    VALUES %s ON CONFLICT DO NOTHING""", rows)
                rows = []
        if rows:
            execute_values(cur, """INSERT INTO subscriptions
                (subscription_id, org_id, plan_id, status, billing_cycle,
                 current_period_start, current_period_end, trial_end, cancelled_at, created_at)
                VALUES %s ON CONFLICT DO NOTHING""", rows)

        # Invoices & Payments (3-12 per org)
        num_invoices = num_orgs * 6
        log.info(f"  Loading ~{num_invoices:,} invoices & payments...")
        inv_rows = []
        pay_rows = []
        inv_id = 0
        pay_id = 0
        for oid in range(1, num_orgs + 1):
            n_inv = rng.randint(3, 12)
            for _ in range(n_inv):
                inv_id += 1
                amount = round(rng.lognormvariate(5, 1.5), 2)
                amount = max(29, min(amount, 10000))
                tax = round(amount * 0.08, 2)
                total = round(amount + tax, 2)
                status = rng.choices(INVOICE_STATUSES, weights=[5, 10, 70, 5, 10])[0]
                created = rand_date(base_date, 365, rng)
                inv_rows.append((
                    inv_id, oid, oid, amount, tax, total, "USD",
                    status, (created + timedelta(days=30)).date(),
                    created if status == "paid" else None, created,
                ))

                if status == "paid":
                    pay_id += 1
                    pay_rows.append((
                        pay_id, inv_id, oid, total,
                        rng.choice(PAYMENT_METHODS), "succeeded", None, created,
                    ))

                if len(inv_rows) >= batch_size:
                    execute_values(cur, """INSERT INTO invoices
                        (invoice_id, org_id, subscription_id, amount, tax, total, currency,
                         status, due_date, paid_at, created_at)
                        VALUES %s ON CONFLICT DO NOTHING""", inv_rows)
                    inv_rows = []
                if len(pay_rows) >= batch_size:
                    execute_values(cur, """INSERT INTO payments
                        (payment_id, invoice_id, org_id, amount, method, status, failure_reason, created_at)
                        VALUES %s ON CONFLICT DO NOTHING""", pay_rows)
                    pay_rows = []
        if inv_rows:
            execute_values(cur, """INSERT INTO invoices
                (invoice_id, org_id, subscription_id, amount, tax, total, currency,
                 status, due_date, paid_at, created_at) VALUES %s ON CONFLICT DO NOTHING""", inv_rows)
        if pay_rows:
            execute_values(cur, """INSERT INTO payments
                (payment_id, invoice_id, org_id, amount, method, status, failure_reason, created_at)
                VALUES %s ON CONFLICT DO NOTHING""", pay_rows)

        # API Keys & Usage
        log.info(f"  Loading API keys & usage...")
        key_rows = []
        usage_rows = []
        key_id = 0
        for oid in range(1, min(num_orgs + 1, 100001)):
            n_keys = rng.randint(1, 5)
            for _ in range(n_keys):
                key_id += 1
                prefix = f"sk_{key_id:08x}"[:16]
                created = rand_date(base_date, 365, rng)
                key_rows.append((
                    key_id, oid, None, prefix, f"API Key {key_id}",
                    rng.choice(API_SCOPES), rng.choice([100, 500, 1000, 5000]),
                    "active", rand_date(base_date, 365, rng), created, None,
                ))

                # Daily usage for last 30 days
                for day_offset in range(30):
                    reqs = rng.randint(0, 5000)
                    usage_rows.append((
                        oid, key_id, (base_date + timedelta(days=335 + day_offset)).date(),
                        reqs, int(reqs * 0.95), int(reqs * 0.05),
                        round(rng.uniform(10, 500), 1), round(rng.uniform(100, 2000), 1),
                        round(rng.uniform(0.1, 50), 2),
                    ))

                if len(key_rows) >= batch_size:
                    execute_values(cur, """INSERT INTO api_keys
                        (key_id, org_id, user_id, key_prefix, name, scopes, rate_limit,
                         status, last_used, created_at, expires_at)
                        VALUES %s ON CONFLICT DO NOTHING""", key_rows)
                    key_rows = []
                if len(usage_rows) >= batch_size:
                    execute_values(cur, """INSERT INTO api_usage_daily
                        (org_id, key_id, usage_date, total_requests, successful, failed,
                         avg_latency_ms, p99_latency_ms, bandwidth_mb)
                        VALUES %s""", usage_rows)
                    usage_rows = []
        if key_rows:
            execute_values(cur, """INSERT INTO api_keys
                (key_id, org_id, user_id, key_prefix, name, scopes, rate_limit,
                 status, last_used, created_at, expires_at) VALUES %s ON CONFLICT DO NOTHING""", key_rows)
        if usage_rows:
            execute_values(cur, """INSERT INTO api_usage_daily
                (org_id, key_id, usage_date, total_requests, successful, failed,
                 avg_latency_ms, p99_latency_ms, bandwidth_mb) VALUES %s""", usage_rows)

        conn.close()
        log.info("  SaaS billing data loaded")

    # ── ClickHouse: User Behavior Events ─────────────────────────

    def _load_user_events(self, silo: DatabaseSilo, limits: dict):
        import clickhouse_connect

        log.info("=" * 60)
        log.info("Loading user behavior events into ClickHouse...")
        log.info("=" * 60)

        ch = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse-events"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "eden"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "eden"),
        )

        count = ch.query("SELECT count() FROM analytics.user_events").result_rows[0][0]
        if count > 0:
            log.info("Already populated, skipping")
            ch.close()
            return

        limit = limits["events"]
        rows, total = load_local_or_stream("tech", "user_events.parquet", silo.hf_dataset, limit=limit)

        progress = ProgressTracker("User events", total or limit)
        columns = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "event_day", "org_id"]
        # Map ecommerce user_ids to org_ids for cross-DB correlation with SaaS billing
        num_orgs = limits.get("orgs", 5000)
        batch = []
        batch_size = 50000
        count = 0
        for row in rows:
            if count >= limit:
                break
            et = row.get("event_time", "")
            try:
                dt = datetime.fromisoformat(str(et).replace(" UTC", "").replace("Z", ""))
            except Exception:
                dt = datetime(2019, 10, 1)
            event_day = (dt - datetime(2019, 1, 1)).days
            uid = int(row.get("user_id", 0))
            org_id = (uid % num_orgs) + 1  # deterministic mapping to org_id

            batch.append([
                dt, str(row.get("event_type", "")),
                int(row.get("product_id", 0)), int(row.get("category_id", 0)),
                str(row.get("category_code", "")), str(row.get("brand", "")),
                float(row.get("price", 0)), uid,
                str(row.get("user_session", "")), event_day, org_id,
            ])
            count += 1
            if len(batch) >= batch_size:
                ch.insert("analytics.user_events", batch, column_names=columns)
                progress.update(len(batch))
                batch = []
        if batch:
            ch.insert("analytics.user_events", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()

        # Populate funnel summary
        log.info("  Building funnel summary...")
        ch.command("""
            INSERT INTO analytics.funnel_daily
            SELECT event_day, category_code,
                   countIf(event_type = 'view') AS views,
                   countIf(event_type = 'cart') AS carts,
                   countIf(event_type = 'purchase') AS purchases,
                   sumIf(price, event_type = 'purchase') AS revenue,
                   uniqIf(user_id, event_type = 'view') AS unique_viewers,
                   uniqIf(user_id, event_type = 'purchase') AS unique_buyers
            FROM analytics.user_events
            GROUP BY event_day, category_code
        """)

        ch.close()
        log.info("  User behavior events loaded")

    # ── MongoDB: CVE Vulnerabilities ─────────────────────────────

    def _load_cve(self, silo: DatabaseSilo, limits: dict):
        from pymongo import MongoClient

        log.info("=" * 60)
        log.info("Loading CVE vulnerabilities into MongoDB...")
        log.info("=" * 60)

        client = MongoClient(os.environ["MONGO_URL"])
        db = client[os.environ.get("MONGO_DB", "vulnerabilities")]

        if db["cves"].count_documents({}) > 0:
            log.info("Already populated, skipping")
            client.close()
            return

        limit = limits["cves"]
        rows, total = load_local_or_stream("tech", "cve_vulnerabilities.parquet", silo.hf_dataset, limit=limit)

        col = db["cves"]
        batch = []
        batch_size = 2000
        count = 0
        progress = ProgressTracker("CVE records", total or limit)
        for row in rows:
            if count >= limit:
                break
            # Parse CVSS scores, falling back to None
            cvss_v3 = None
            try:
                cvss_v3 = float(row.get("CVSS-V3", "None"))
            except (ValueError, TypeError):
                pass
            cvss_v2 = None
            try:
                cvss_v2 = float(row.get("CVSS-V2", "None"))
            except (ValueError, TypeError):
                pass

            doc = {
                "cve_id": str(row.get("CVE-ID", "")),
                "cwe_id": str(row.get("CWE-ID", "")),
                "severity": str(row.get("SEVERITY", "UNKNOWN")),
                "cvss_v3": cvss_v3,
                "cvss_v2": cvss_v2,
                "description": str(row.get("DESCRIPTION", ""))[:10000],
                "year": int(str(row.get("CVE-ID", "CVE-0000-0000")).split("-")[1]) if "-" in str(row.get("CVE-ID", "")) else 0,
            }
            batch.append(doc)
            count += 1
            if len(batch) >= batch_size:
                col.insert_many(batch)
                progress.update(len(batch))
                batch = []
        if batch:
            col.insert_many(batch)
            progress.update(len(batch))
        progress.finish()

        log.info("  Creating indexes...")
        col.create_index("cve_id", unique=True)
        col.create_index("cwe_id")
        col.create_index("severity")
        col.create_index("cvss_v3")
        col.create_index("year")

        client.close()
        log.info("  CVE vulnerabilities loaded")

    # ── Redis: Sessions & Rate Limits ────────────────────────────

    def _load_redis(self, silo: DatabaseSilo, limits: dict):
        import redis as r

        log.info("=" * 60)
        log.info("Loading Redis sessions, rate limits, feature flags...")
        log.info("=" * 60)

        client = r.from_url(os.environ["REDIS_URL"], decode_responses=True)
        if client.dbsize() > 0:
            log.info("Already populated, skipping")
            client.close()
            return

        pipe = client.pipeline()
        rng = random.Random(42)
        num_orgs = limits["orgs"]

        # Active sessions + IP→org reverse index for cross-DB correlation
        log.info(f"  Generating {min(num_orgs, 50000)} sessions...")
        for uid in range(1, min(num_orgs * 5, 50001)):
            org_id = str(rng.randint(1, num_orgs))
            ip = f"{rng.randint(1,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
            pipe.hset(f"session:{uid}", mapping={
                "user_id": str(uid),
                "org_id": org_id,
                "last_active": datetime.now().isoformat(),
                "ip": ip,
                "user_agent": rng.choice(["Chrome/120", "Firefox/121", "Safari/17", "Edge/120"]),
            })
            # Reverse indexes for cross-DB correlation:
            # IP → org (so attack IPs can be traced to affected orgs)
            pipe.sadd(f"ip_orgs:{ip}", org_id)
            # Org → active session count
            pipe.incr(f"org_sessions:{org_id}")

        # Rate limit counters
        log.info(f"  Generating rate limit counters...")
        for oid in range(1, min(num_orgs + 1, 100001)):
            pipe.set(f"rate_limit:{oid}", str(rng.randint(0, 1000)))

        # Feature flags
        log.info("  Setting feature flags...")
        flags = {
            "feature:new_dashboard": "true",
            "feature:dark_mode": "true",
            "feature:api_v2": "false",
            "feature:ai_assistant": "true",
            "feature:advanced_analytics": "false",
            "feature:sso_enforcement": "true",
            "feature:custom_webhooks": "true",
            "feature:data_export_v2": "false",
        }
        for key, val in flags.items():
            pipe.set(key, val)

        # Aggregate stats
        pipe.set("stats:total_orgs", str(num_orgs))
        pipe.set("stats:active_sessions", str(min(num_orgs * 3, 50000)))
        pipe.set("stats:total_api_calls_today", str(rng.randint(100000, 5000000)))

        # Org leaderboard by MRR
        for oid in range(1, min(num_orgs + 1, 50001)):
            mrr = round(rng.lognormvariate(5, 2), 2)
            pipe.zadd("leaderboard:org_mrr", {f"org:{oid}": mrr})

        pipe.execute()
        log.info(f"  {client.dbsize():,} keys loaded into Redis")
        client.close()

    # ── Weaviate: CVE Embeddings ─────────────────────────────────

    def _load_weaviate(self, silo: DatabaseSilo, limits: dict):
        import weaviate
        from weaviate.classes.config import Configure, Property, DataType

        log.info("=" * 60)
        log.info("Loading CVE embeddings into Weaviate...")
        log.info("=" * 60)

        weaviate_url = os.environ["WEAVIATE_URL"]
        client = weaviate.connect_to_custom(
            http_host=weaviate_url.replace("http://", "").split(":")[0],
            http_port=int(weaviate_url.split(":")[-1]),
            http_secure=False,
            grpc_host=weaviate_url.replace("http://", "").split(":")[0],
            grpc_port=50051,
            grpc_secure=False,
        )

        if client.collections.exists("Vulnerability"):
            collection = client.collections.get("Vulnerability")
            resp = collection.aggregate.over_all(total_count=True)
            if resp.total_count > 0:
                log.info("Already populated, skipping")
                client.close()
                return

        if not client.collections.exists("Vulnerability"):
            client.collections.create(
                name="Vulnerability",
                vectorizer_config=Configure.Vectorizer.none(),
                properties=[
                    Property(name="cve_id", data_type=DataType.TEXT),
                    Property(name="cwe_id", data_type=DataType.TEXT),
                    Property(name="severity", data_type=DataType.TEXT),
                    Property(name="description", data_type=DataType.TEXT),
                ],
            )

        log.info("  Loading sentence-transformers model...")
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")

        # Sample CVEs from MongoDB for embedding
        from pymongo import MongoClient
        mongo_client = MongoClient(os.environ["MONGO_URL"])
        db = mongo_client[os.environ.get("MONGO_DB", "vulnerabilities")]

        weaviate_limit = int(os.environ.get("WEAVIATE_LIMIT", "10000"))
        log.info(f"  Embedding {weaviate_limit:,} CVEs...")

        collection = client.collections.get("Vulnerability")
        batch_size = 100
        progress = ProgressTracker("CVE embeddings", weaviate_limit)
        count = 0

        # Batch encode + insert to avoid per-item HTTP round trips
        docs = list(db["cves"].find({}, {"cve_id": 1, "cwe_id": 1, "severity": 1, "description": 1}).limit(weaviate_limit))

        with collection.batch.fixed_size(batch_size=batch_size) as batch:
            for i in range(0, len(docs), batch_size):
                chunk = docs[i:i + batch_size]
                texts = [str(d.get("description", ""))[:512] for d in chunk]
                embeddings = model.encode(texts, show_progress_bar=False)

                for doc, emb in zip(chunk, embeddings):
                    batch.add_object(
                        properties={
                            "cve_id": str(doc.get("cve_id", "")),
                            "cwe_id": str(doc.get("cwe_id", "")),
                            "severity": str(doc.get("severity", "")),
                            "description": str(doc.get("description", ""))[:200],
                        },
                        vector=emb.tolist(),
                    )
                count += len(chunk)
                progress.update(len(chunk))

        progress.finish()

        mongo_client.close()
        client.close()
