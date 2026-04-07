"""
Finance / Banking vertical.

Silos (6 databases):
  pg_core_banking      (Postgres)   — CiferAI fraud detection (21M txns)
  pg_credit_scoring    (Postgres)   — Sparkov credit card transactions (1.85M)
  ch_trading           (ClickHouse) — S&P 500 1-minute stock bars
  mongo_compliance     (MongoDB)    — SEC 10-K annual filings (245K reports)
  redis_fraud          (Redis)      — Real-time fraud scores, account balances
  weaviate_risk        (Weaviate)   — Semantic search on filings & transactions
"""

import os
import random
import logging
from datetime import datetime

from verticals.base import VerticalBase, DatabaseSilo, ProgressTracker, load_local_or_stream

log = logging.getLogger("adam-init")

SCALE_LIMITS = {
    "demo":    {"core_txns": 100_000,    "credit_txns": 50_000,   "stock_bars": 100_000,   "filings": 5_000},
    "small":   {"core_txns": 500_000,    "credit_txns": 100_000,  "stock_bars": 500_000,   "filings": 10_000},
    "medium":  {"core_txns": 5_000_000, "credit_txns": 500_000,  "stock_bars": 2_000_000, "filings": 50_000},
    "large":   {"core_txns": 15_000_000, "credit_txns": 1_850_000, "stock_bars": 5_000_000, "filings": 100_000},
    "massive": {"core_txns": 21_000_000, "credit_txns": 1_850_000, "stock_bars": 10_000_000, "filings": 245_000},
}


class FinanceVertical(VerticalBase):
    name = "finance"
    description = "Banking & Financial Services"

    def silos(self) -> list[DatabaseSilo]:
        return [
            DatabaseSilo(name="pg_core_banking", db_type="postgres",
                         description="Core Banking — Fraud detection transactions (CiferAI, 21M txns)",
                         url_env_var="PG_CORE_BANKING_URL", eden_url_env_var="EDEN_PG_CORE_BANKING_URL",
                         schema_file="finance/postgres_core_banking.sql",
                         hf_dataset="CiferAI/Cifer-Fraud-Detection-Dataset-AF", team="Core Banking"),
            DatabaseSilo(name="pg_credit_scoring", db_type="postgres",
                         description="Credit Dept — Credit card transactions (Sparkov, 1.85M txns)",
                         url_env_var="PG_CREDIT_SCORING_URL", eden_url_env_var="EDEN_PG_CREDIT_SCORING_URL",
                         schema_file="finance/postgres_credit_scoring.sql",
                         hf_dataset="pointe77/credit-card-transaction", team="Credit"),
            DatabaseSilo(name="ch_trading", db_type="clickhouse",
                         description="Trading Desk — S&P 500 1-minute stock bars",
                         url_env_var="CLICKHOUSE_HOST", eden_url_env_var="EDEN_CLICKHOUSE_URL",
                         schema_file="finance/clickhouse_trading.sql",
                         hf_dataset="Traders-Lab/TroveLedger", team="Trading"),
            DatabaseSilo(name="mongo_compliance", db_type="mongo",
                         description="Compliance — SEC 10-K annual filings (245K reports)",
                         url_env_var="MONGO_URL", eden_url_env_var="EDEN_MONGO_URL",
                         hf_dataset="PleIAs/SEC", team="Compliance"),
            DatabaseSilo(name="redis_fraud", db_type="redis",
                         description="Real-time — Fraud scores, account balances, rate limits",
                         url_env_var="REDIS_URL", eden_url_env_var="EDEN_REDIS_URL", team="Fraud Ops"),
            DatabaseSilo(name="weaviate_risk", db_type="weaviate",
                         description="Risk — Semantic search on SEC filings and transactions",
                         url_env_var="WEAVIATE_URL", eden_url_env_var="EDEN_WEAVIATE_URL", team="Risk"),
        ]

    def load_silo(self, silo: DatabaseSilo, scale: str):
        limits = SCALE_LIMITS.get(scale, SCALE_LIMITS["small"])
        if silo.name == "pg_core_banking":
            self._load_core_banking(silo, limits)
        elif silo.name == "pg_credit_scoring":
            self._load_credit_scoring(silo, limits)
        elif silo.name == "ch_trading":
            self._load_trading(silo, limits)
        elif silo.name == "mongo_compliance":
            self._load_compliance(silo, limits)
        elif silo.name == "redis_fraud":
            self._load_redis(silo, limits)
        elif silo.name == "weaviate_risk":
            self._load_weaviate(silo, limits)

    def _load_core_banking(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM transactions")
        if cur.fetchone()[0] > 0:
            log.info("Core banking already populated, skipping")
            conn.close()
            return

        limit = limits["core_txns"]
        rows, total = load_local_or_stream("finance", "fraud_transactions.parquet", silo.hf_dataset, limit=limit)

        progress = ProgressTracker("Core banking txns", total or limit)
        batch = []
        batch_size = 5000
        count = 0
        for row in rows:
            if count >= limit:
                break
            batch.append((
                row.get("step", 0), row.get("type", ""),
                row.get("amount", 0), row.get("nameOrig", ""),
                row.get("oldbalanceOrg", 0), row.get("newbalanceOrig", 0),
                row.get("nameDest", ""),
                row.get("oldbalanceDest", 0), row.get("newbalanceDest", 0),
                row.get("isFraud", 0), row.get("isFlaggedFraud", 0),
            ))
            count += 1
            if len(batch) >= batch_size:
                execute_values(cur, """INSERT INTO transactions
                    (step, type, amount, name_orig, oldbalance_org, newbalance_org,
                     name_dest, oldbalance_dest, newbalance_dest, is_fraud, is_flagged_fraud)
                    VALUES %s""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO transactions
                (step, type, amount, name_orig, oldbalance_org, newbalance_org,
                 name_dest, oldbalance_dest, newbalance_dest, is_fraud, is_flagged_fraud)
                VALUES %s""", batch)
            progress.update(len(batch))
        progress.finish()
        conn.close()

    def _load_credit_scoring(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM credit_transactions")
        if cur.fetchone()[0] > 0:
            log.info("Credit scoring already populated, skipping")
            conn.close()
            return

        limit = limits["credit_txns"]
        rows, total = load_local_or_stream("finance", "credit_card_transactions.parquet", silo.hf_dataset, limit=limit)

        progress = ProgressTracker("Credit card txns", total or limit)
        batch = []
        batch_size = 5000
        count = 0
        for row in rows:
            if count >= limit:
                break
            batch.append((
                row.get("trans_date_trans_time", None),
                row.get("cc_num", 0), row.get("merchant", ""),
                row.get("category", ""), row.get("amt", 0),
                row.get("first", ""), row.get("last", ""),
                row.get("gender", ""), row.get("street", ""),
                row.get("city", ""), row.get("state", ""),
                row.get("zip", ""), row.get("lat", 0), row.get("long", 0),
                row.get("city_pop", 0), row.get("job", ""),
                row.get("dob", None), row.get("trans_num", ""),
                row.get("unix_time", 0),
                row.get("merch_lat", 0), row.get("merch_long", 0),
                row.get("is_fraud", 0),
            ))
            count += 1
            if len(batch) >= batch_size:
                execute_values(cur, """INSERT INTO credit_transactions
                    (trans_date_time, cc_num, merchant, category, amt,
                     first_name, last_name, gender, street, city, state, zip,
                     lat, long, city_pop, job, dob, trans_num, unix_time,
                     merch_lat, merch_long, is_fraud)
                    VALUES %s""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO credit_transactions
                (trans_date_time, cc_num, merchant, category, amt,
                 first_name, last_name, gender, street, city, state, zip,
                 lat, long, city_pop, job, dob, trans_num, unix_time,
                 merch_lat, merch_long, is_fraud)
                VALUES %s""", batch)
            progress.update(len(batch))
        progress.finish()
        conn.close()

    def _load_trading(self, silo, limits):
        import clickhouse_connect

        ch = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse-trading"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "eden"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "eden"),
        )

        count = ch.query("SELECT count() FROM analytics.stock_bars").result_rows[0][0]
        if count > 0:
            log.info("Trading data already populated, skipping")
            ch.close()
            return

        limit = limits["stock_bars"]
        rows, total = load_local_or_stream("finance", "stock_bars.parquet", silo.hf_dataset,
                                           hf_split="validation", limit=limit)

        progress = ProgressTracker("Stock bars", total or limit)
        columns = ["symbol", "trade_time", "open", "high", "low", "close", "volume"]
        batch = []
        batch_size = 50000
        count = 0
        for row in rows:
            if count >= limit:
                break
            # TroveLedger 'time' is Unix epoch seconds
            try:
                ts = int(row.get("time", 0))
                trade_time = datetime.fromtimestamp(ts, tz=None) if ts > 0 else datetime(2020, 1, 1)
            except Exception:
                trade_time = datetime(2020, 1, 1)
            batch.append([
                str(row.get("symbol", "")), trade_time,
                float(row.get("open", 0)), float(row.get("high", 0)),
                float(row.get("low", 0)), float(row.get("close", 0)),
                int(row.get("volume", 0)),
            ])
            count += 1
            if len(batch) >= batch_size:
                ch.insert("analytics.stock_bars", batch, column_names=columns)
                progress.update(len(batch))
                batch = []
        if batch:
            ch.insert("analytics.stock_bars", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()
        ch.close()

    def _load_compliance(self, silo, limits):
        from pymongo import MongoClient

        client = MongoClient(os.environ["MONGO_URL"])
        db = client[os.environ.get("MONGO_DB", "compliance")]

        if db["sec_filings"].count_documents({}) > 0:
            log.info("Compliance data already populated, skipping")
            client.close()
            return

        limit = limits["filings"]
        rows, total = load_local_or_stream("finance", "sec_filings.parquet", silo.hf_dataset, limit=limit)

        col = db["sec_filings"]
        batch = []
        batch_size = 500
        count = 0
        progress = ProgressTracker("SEC filings", total or limit)
        for row in rows:
            if count >= limit:
                break
            text = str(row.get("text", ""))
            doc = {
                "company_name": row.get("company_name", ""),
                "cik": row.get("cik", ""),
                "filing_year": row.get("filing_year", 0),
                "word_count": int(row.get("word_count", 0) or 0),
                "character_count": int(row.get("character_count", len(text)) or len(text)),
                "text": text[:50000],  # cap at 50K chars
            }
            batch.append(doc)
            count += 1
            if len(batch) >= batch_size:
                col.insert_many(batch, ordered=False)
                progress.update(len(batch))
                batch = []
        if batch:
            col.insert_many(batch, ordered=False)
            progress.update(len(batch))
        progress.finish()

        col.create_index("company_name")
        col.create_index("cik")
        col.create_index("filing_year")
        client.close()

    def _load_redis(self, silo, limits):
        import redis as r

        client = r.from_url(os.environ["REDIS_URL"], decode_responses=True)
        if client.dbsize() > 0:
            log.info("Redis already populated, skipping")
            client.close()
            return

        pipe = client.pipeline()
        rng = random.Random(42)

        # Fraud scores for accounts
        log.info("  Generating fraud scores and account balances...")
        for i in range(1, min(limits["core_txns"] // 100, 100001)):
            pipe.set(f"fraud_score:{i}", str(round(rng.betavariate(1, 15) * 100, 2)))
            pipe.set(f"balance:{i}", str(round(rng.lognormvariate(8, 2.5), 2)))

        # Customer linkage: connect core_banking accounts to credit_scoring cards
        log.info("  Generating customer linkage keys...")
        num_customers = min(limits["core_txns"] // 100, 100000)
        cc_rng = random.Random(99)
        for i in range(1, num_customers + 1):
            fraud_score = round(rng.betavariate(1, 15) * 100, 2)  # reuse same distribution
            cc_num = f"{cc_rng.randint(4000000000000000, 4999999999999999)}"
            bank_account = f"C{i}"

            if fraud_score < 15:
                risk_tier = "low"
            elif fraud_score < 40:
                risk_tier = "medium"
            elif fraud_score < 70:
                risk_tier = "high"
            else:
                risk_tier = "critical"

            pipe.hset(f"customer:{i}", mapping={
                "bank_account": bank_account,
                "cc_num": cc_num,
                "fraud_score": str(fraud_score),
                "risk_tier": risk_tier,
            })
            # Reverse indexes for cross-DB lookups
            pipe.set(f"account_customer:{bank_account}", str(i))
            pipe.set(f"cc_customer:{cc_num}", str(i))

        # Aggregate stats
        pipe.set("stats:total_transactions", str(limits["core_txns"]))
        pipe.set("stats:total_credit_txns", str(limits["credit_txns"]))
        pipe.set("stats:fraud_rate", "0.012")
        pipe.set("stats:total_customers", str(num_customers))

        # Leaderboard: top transactors
        for i in range(1, 50001):
            pipe.zadd("leaderboard:top_transactors", {f"account:{i}": round(rng.lognormvariate(6, 2), 2)})

        pipe.execute()
        log.info(f"  {client.dbsize():,} keys loaded into Redis")
        client.close()

    def _load_weaviate(self, silo, limits):
        import weaviate
        from weaviate.classes.config import Configure, Property, DataType

        weaviate_url = os.environ["WEAVIATE_URL"]
        client = weaviate.connect_to_custom(
            http_host=weaviate_url.replace("http://", "").split(":")[0],
            http_port=int(weaviate_url.split(":")[-1]),
            http_secure=False,
            grpc_host=weaviate_url.replace("http://", "").split(":")[0],
            grpc_port=50051, grpc_secure=False,
        )

        if client.collections.exists("ComplianceDocument"):
            col = client.collections.get("ComplianceDocument")
            resp = col.aggregate.over_all(total_count=True)
            if resp.total_count > 0:
                log.info("Weaviate already populated, skipping")
                client.close()
                return

        if not client.collections.exists("ComplianceDocument"):
            client.collections.create(
                name="ComplianceDocument",
                vectorizer_config=Configure.Vectorizer.none(),
                properties=[
                    Property(name="company_name", data_type=DataType.TEXT),
                    Property(name="filing_year", data_type=DataType.INT),
                    Property(name="text_snippet", data_type=DataType.TEXT),
                ],
            )

        log.info("  Loading sentence-transformers model...")
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")

        # Sample filings from MongoDB
        from pymongo import MongoClient
        mongo_client = MongoClient(os.environ["MONGO_URL"])
        db = mongo_client[os.environ.get("MONGO_DB", "compliance")]

        weaviate_limit = int(os.environ.get("WEAVIATE_LIMIT", "10000"))
        log.info(f"  Embedding {weaviate_limit:,} SEC filing excerpts...")

        col = client.collections.get("ComplianceDocument")
        progress = ProgressTracker("Filing embeddings", weaviate_limit)
        count = 0
        batch_size = 100

        docs = list(db["sec_filings"].find({}, {"company_name": 1, "filing_year": 1, "text": 1}).limit(weaviate_limit))
        docs = [d for d in docs if str(d.get("text", "")).strip()]

        with col.batch.fixed_size(batch_size=batch_size) as batch:
            for i in range(0, len(docs), batch_size):
                chunk = docs[i:i + batch_size]
                texts = [str(d.get("text", ""))[:512] for d in chunk]
                embeddings = model.encode(texts, show_progress_bar=False)
                for doc, emb in zip(chunk, embeddings):
                    batch.add_object(
                        properties={
                            "company_name": str(doc.get("company_name", "")),
                            "filing_year": int(doc.get("filing_year", 0)),
                            "text_snippet": str(doc.get("text", ""))[:200],
                        },
                        vector=emb.tolist(),
                    )
                count += len(chunk)
                progress.update(len(chunk))
        progress.finish()

        mongo_client.close()
        client.close()
