"""
Insurance / Risk vertical.

Silos (6 databases):
  pg_policy_admin      (Postgres)   — freMTPL2 policies (678K policies + 26K claims)
  pg_risk_scoring      (Postgres)   — Porto Seguro driver prediction (595K records)
  ch_claims_analytics  (ClickHouse) — US Accidents analytics (2.85M)
  mongo_incidents      (MongoDB)    — US Accident reports with geo/weather (2.85M)
  redis_claims         (Redis)      — Claim status tracker, active policies cache
  weaviate_claims      (Weaviate)   — Accident description embeddings
"""

import os
import random
import logging
from datetime import datetime

from verticals.base import VerticalBase, DatabaseSilo, ProgressTracker, load_local_or_stream

log = logging.getLogger("adam-init")

# Map French region codes (freMTPL2) to US states for geographic correlation
# with the US Accidents dataset.  Each French region maps to a pool of US
# states; a deterministic hash of the policy ID picks one.
REGION_TO_STATES = {
    "R11": ["CA", "WA", "OR"],        # Île-de-France        → West
    "R21": ["NY", "NJ", "CT"],        # Champagne-Ardenne    → Northeast
    "R22": ["PA", "MA", "NH"],        # Picardie             → Northeast
    "R23": ["TX", "FL", "GA"],        # Haute-Normandie      → South
    "R24": ["IL", "OH", "MI"],        # Centre               → Midwest
    "R25": ["CO", "AZ", "NV"],        # Basse-Normandie      → Mountain
    "R26": ["NC", "VA", "TN"],        # Bourgogne            → Southeast
    "R31": ["MN", "WI", "IA"],        # Nord-Pas-de-Calais   → Upper Midwest
    "R41": ["MD", "DE", "DC"],        # Lorraine             → Mid-Atlantic
    "R42": ["IN", "KY", "MO"],        # Alsace               → Central
    "R43": ["SC", "AL", "MS"],        # Franche-Comté        → Deep South
    "R52": ["WA", "OR", "ID"],        # Pays de la Loire     → Pacific NW
    "R53": ["UT", "NM", "MT"],        # Bretagne             → Mountain
    "R54": ["LA", "AR", "OK"],        # Poitou-Charentes     → South Central
    "R72": ["OH", "PA", "WV"],        # Aquitaine            → Appalachian
    "R73": ["NE", "KS", "SD"],        # Midi-Pyrénées        → Plains
    "R74": ["VT", "ME", "RI"],        # Limousin             → New England
    "R82": ["AZ", "NV", "NM"],        # Rhône-Alpes          → Southwest
    "R83": ["FL", "GA", "SC"],        # Auvergne             → Southeast
    "R91": ["TX", "CA", "NY"],        # Languedoc-Roussillon → Major metros
    "R93": ["IL", "MI", "WI"],        # Provence-Alpes-C.A.  → Great Lakes
    "R94": ["HI", "AK", "WY"],        # Corse                → Remote/Island
}
# Fallback for any unexpected region code
_DEFAULT_STATES = ["CA", "TX", "NY"]


def _region_to_us_state(region: str, id_pol: int) -> str:
    """Deterministically assign a US state based on region code and policy ID."""
    states = REGION_TO_STATES.get(str(region), _DEFAULT_STATES)
    return states[id_pol % len(states)]


SCALE_LIMITS = {
    "demo":    {"policies": 20_000,  "risk_records": 20_000,  "accidents": 50_000},
    "small":   {"policies": 50_000,  "risk_records": 50_000,  "accidents": 100_000},
    "medium":  {"policies": 200_000, "risk_records": 200_000, "accidents": 500_000},
    "large":   {"policies": 500_000, "risk_records": 595_000, "accidents": 1_500_000},
    "massive": {"policies": 678_000, "risk_records": 595_000, "accidents": 2_850_000},
}


class InsuranceVertical(VerticalBase):
    name = "insurance"
    description = "Insurance / Risk"

    def silos(self) -> list[DatabaseSilo]:
        return [
            DatabaseSilo(name="pg_policy_admin", db_type="postgres",
                         description="Policy Admin — French motor liability policies & claims (freMTPL2, 678K)",
                         url_env_var="PG_POLICY_ADMIN_URL", eden_url_env_var="EDEN_PG_POLICY_ADMIN_URL",
                         schema_file="insurance/postgres_policy_admin.sql",
                         hf_dataset="mabilton/fremtpl2", team="Policy Admin"),
            DatabaseSilo(name="pg_risk_scoring", db_type="postgres",
                         description="Underwriting — Driver risk prediction (Porto Seguro, 595K records)",
                         url_env_var="PG_RISK_SCORING_URL", eden_url_env_var="EDEN_PG_RISK_SCORING_URL",
                         schema_file="insurance/postgres_risk_scoring.sql",
                         hf_dataset="TheFinAI/en-forecasting-portoseguro", team="Underwriting"),
            DatabaseSilo(name="ch_claims_analytics", db_type="clickhouse",
                         description="Claims Analytics — US accident severity, geographic risk (2.85M)",
                         url_env_var="CLICKHOUSE_HOST", eden_url_env_var="EDEN_CLICKHOUSE_URL",
                         schema_file="insurance/clickhouse_claims.sql",
                         hf_dataset="nateraw/us-accidents", team="Actuarial"),
            DatabaseSilo(name="mongo_incidents", db_type="mongo",
                         description="Incident Reports — US traffic accidents with geo & weather (2.85M)",
                         url_env_var="MONGO_URL", eden_url_env_var="EDEN_MONGO_URL",
                         hf_dataset="nateraw/us-accidents", team="Claims"),
            DatabaseSilo(name="redis_claims", db_type="redis",
                         description="Real-time — Claim status tracker, active policies cache, agent queues",
                         url_env_var="REDIS_URL", eden_url_env_var="EDEN_REDIS_URL", team="Claims Ops"),
            DatabaseSilo(name="weaviate_claims", db_type="weaviate",
                         description="Claims Search — Accident description & similarity search",
                         url_env_var="WEAVIATE_URL", eden_url_env_var="EDEN_WEAVIATE_URL", team="Investigation"),
        ]

    def load_silo(self, silo: DatabaseSilo, scale: str):
        limits = SCALE_LIMITS.get(scale, SCALE_LIMITS["small"])
        if silo.name == "pg_policy_admin":
            self._load_policies(silo, limits)
        elif silo.name == "pg_risk_scoring":
            self._load_risk_scoring(silo, limits)
        elif silo.name == "ch_claims_analytics":
            self._load_ch_accidents(silo, limits)
        elif silo.name == "mongo_incidents":
            self._load_mongo_accidents(silo, limits)
        elif silo.name == "redis_claims":
            self._load_redis(limits)
        elif silo.name == "weaviate_claims":
            self._load_weaviate(limits)

    def _load_policies(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()
        # Older volumes may still have the original VARCHAR(8) region column.
        cur.execute("ALTER TABLE policies ALTER COLUMN region TYPE VARCHAR(32)")

        cur.execute("SELECT COUNT(*) FROM policies")
        if cur.fetchone()[0] > 0:
            log.info("Policies already populated, skipping")
            conn.close()
            return

        limit = limits["policies"]

        # Load frequency table (policies)
        freq_rows, freq_total = load_local_or_stream("insurance", "fremtpl2_freq.parquet", silo.hf_dataset, hf_config="freMTPL2freq", limit=limit)
        progress = ProgressTracker("Policies", freq_total or limit)
        batch = []
        count = 0
        state_counts: dict[str, int] = {}
        for row in freq_rows:
            if count >= limit:
                break
            id_pol = row.get("IDpol", 0)
            region = row.get("Region", "")
            us_state = _region_to_us_state(region, id_pol)
            state_counts[us_state] = state_counts.get(us_state, 0) + 1
            batch.append((
                id_pol, row.get("ClaimNb", 0), row.get("Exposure", 0),
                row.get("Area", ""), row.get("VehPower", 0), row.get("VehAge", 0),
                row.get("DrivAge", 0), row.get("BonusMalus", 0),
                row.get("VehBrand", ""), row.get("VehGas", ""),
                row.get("Density", 0), region, us_state,
            ))
            count += 1
            if len(batch) >= 5000:
                execute_values(cur, """INSERT INTO policies
                    (id_pol, claim_nb, exposure, area, veh_power, veh_age, driv_age,
                     bonus_malus, veh_brand, veh_gas, density, region, us_state)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO policies
                (id_pol, claim_nb, exposure, area, veh_power, veh_age, driv_age,
                 bonus_malus, veh_brand, veh_gas, density, region, us_state)
                VALUES %s ON CONFLICT DO NOTHING""", batch)
            progress.update(len(batch))
        # Store state_counts so _load_redis can publish them
        self._policy_state_counts = state_counts
        progress.finish()

        # Load severity table (claims)
        log.info("  Downloading freMTPL2 sev (claims)...")
        try:
            sev_rows, _ = load_local_or_stream("insurance", "fremtpl2_sev.parquet", silo.hf_dataset, hf_config="freMTPL2sev")
            batch = []
            count = 0
            for row in sev_rows:
                batch.append((row.get("IDpol", 0), row.get("ClaimAmount", 0)))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, "INSERT INTO claims (id_pol, claim_amount) VALUES %s", batch)
                    batch = []
            if batch:
                execute_values(cur, "INSERT INTO claims (id_pol, claim_amount) VALUES %s", batch)
            log.info(f"  {count:,} claims loaded")
        except Exception as e:
            log.warning(f"  Could not load claims severity: {e}")

        conn.close()

    def _load_risk_scoring(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM driver_risk")
        if cur.fetchone()[0] > 0:
            log.info("Risk scoring already populated, skipping")
            conn.close()
            return

        limit = limits["risk_records"]

        # Try loading Porto Seguro data; fall back to synthetic generation
        try:
            risk_rows, risk_total = load_local_or_stream("insurance", "portoseguro.parquet", silo.hf_dataset, limit=limit)
            progress = ProgressTracker("Risk records", risk_total or limit)
            batch = []
            count = 0
            for row in risk_rows:
                if count >= limit:
                    break
                vals = [row.get("target", 0)]
                for prefix in ["ps_ind", "ps_reg", "ps_car", "ps_calc"]:
                    for key in sorted(k for k in row.keys() if k.startswith(prefix)):
                        vals.append(row.get(key))
                batch.append(tuple(vals))
                count += 1
                if len(batch) >= 5000:
                    placeholders = ",".join(["%s"] * len(batch[0]))
                    cur.executemany(f"INSERT INTO driver_risk VALUES (DEFAULT, {placeholders})", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                placeholders = ",".join(["%s"] * len(batch[0]))
                cur.executemany(f"INSERT INTO driver_risk VALUES (DEFAULT, {placeholders})", batch)
                progress.update(len(batch))
            progress.finish()
        except Exception as e:
            log.warning(f"  Porto Seguro dataset unavailable ({e}), generating synthetic risk data...")
            self._generate_synthetic_risk(cur, limit)

        conn.close()

    def _generate_synthetic_risk(self, cur, limit):
        """Generate synthetic driver risk data matching Porto Seguro schema."""
        from psycopg2.extras import execute_values
        rng = random.Random(42)

        # Column names matching schema (57 features + target)
        ind_cols = [f"ps_ind_{i:02d}" if i not in (2,4,5) else f"ps_ind_{i:02d}_cat"
                    for i in range(1, 19)]
        # Fix: bins
        for i in (6,7,8,9,10,11,12,13,16,17,18):
            idx = i - 1
            ind_cols[idx] = f"ps_ind_{i:02d}_bin"
        ind_cols[13] = "ps_ind_14"
        ind_cols[14] = "ps_ind_15"

        progress = ProgressTracker("Synthetic risk records", limit)
        batch = []
        for i in range(limit):
            target = 1 if rng.random() < 0.036 else 0  # ~3.6% claim rate
            vals = [target]
            # 18 individual features (mix of int/binary/categorical)
            for _ in range(18):
                vals.append(rng.randint(0, 7))
            # 3 registration features (float)
            for _ in range(3):
                vals.append(round(rng.uniform(0, 2), 2))
            # 16 car features (mix)
            for _ in range(16):
                vals.append(rng.randint(0, 10) if rng.random() < 0.5 else round(rng.uniform(0, 3), 4))
            # 20 calculated features
            for _ in range(20):
                vals.append(rng.randint(0, 10) if rng.random() < 0.7 else round(rng.uniform(0, 1), 4))
            batch.append(tuple(vals))
            if len(batch) >= 5000:
                placeholders = ",".join(["%s"] * len(batch[0]))
                cur.executemany(f"INSERT INTO driver_risk VALUES (DEFAULT, {placeholders})", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            placeholders = ",".join(["%s"] * len(batch[0]))
            cur.executemany(f"INSERT INTO driver_risk VALUES (DEFAULT, {placeholders})", batch)
            progress.update(len(batch))
        progress.finish()
        log.info(f"  Generated {limit:,} synthetic risk records")

    def _load_ch_accidents(self, silo, limits):
        import clickhouse_connect

        ch = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse-claims"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "eden"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "eden"),
        )

        count = ch.query("SELECT count() FROM analytics.accidents").result_rows[0][0]
        if count > 0:
            log.info("ClickHouse accidents already populated, skipping")
            ch.close()
            return

        limit = limits["accidents"]
        acc_rows, acc_total = load_local_or_stream("insurance", "us_accidents.parquet", silo.hf_dataset, limit=limit)
        log.info(f"  Loading US Accidents into ClickHouse (limit: {limit:,})...")

        progress = ProgressTracker("CH accidents", acc_total or limit)
        columns = ["accident_id", "severity", "start_time", "end_time",
                    "start_lat", "start_lng", "distance_mi", "description",
                    "street", "city", "county", "state", "zipcode", "country", "timezone",
                    "temperature_f", "humidity_pct", "pressure_in", "visibility_mi",
                    "wind_direction", "wind_speed_mph", "precipitation_in", "weather_condition",
                    "amenity", "bump", "crossing", "give_way", "junction", "no_exit",
                    "railway", "roundabout", "station", "stop", "traffic_calming",
                    "traffic_signal", "turning_loop", "sunrise_sunset", "event_day"]
        batch = []
        batch_size = 50000
        cnt = 0
        for row in acc_rows:
            if cnt >= limit:
                break
            start = row.get("Start_Time", "")
            try:
                dt = datetime.fromisoformat(str(start).replace("Z", ""))
                day = (dt - datetime(2016, 1, 1)).days
            except Exception:
                dt = datetime(2020, 1, 1)
                day = 0
            end = row.get("End_Time", None)
            try:
                end_dt = datetime.fromisoformat(str(end).replace("Z", "")) if end else None
            except Exception:
                end_dt = None

            def b(v):
                return 1 if v else 0

            batch.append([
                str(row.get("ID", "")), int(row.get("Severity", 1)),
                dt, end_dt,
                float(row.get("Start_Lat", 0)), float(row.get("Start_Lng", 0)),
                float(row.get("Distance(mi)", 0)), str(row.get("Description", ""))[:500],
                str(row.get("Street", "")), str(row.get("City", "")),
                str(row.get("County", "")), str(row.get("State", "")),
                str(row.get("Zipcode", "")), str(row.get("Country", "")),
                str(row.get("Timezone", "")),
                row.get("Temperature(F)"), row.get("Humidity(%)"),
                row.get("Pressure(in)"), row.get("Visibility(mi)"),
                str(row.get("Wind_Direction", "")), row.get("Wind_Speed(mph)"),
                row.get("Precipitation(in)"), str(row.get("Weather_Condition", "")),
                b(row.get("Amenity")), b(row.get("Bump")), b(row.get("Crossing")),
                b(row.get("Give_Way")), b(row.get("Junction")), b(row.get("No_Exit")),
                b(row.get("Railway")), b(row.get("Roundabout")), b(row.get("Station")),
                b(row.get("Stop")), b(row.get("Traffic_Calming")),
                b(row.get("Traffic_Signal")), b(row.get("Turning_Loop")),
                str(row.get("Sunrise_Sunset", "")), max(0, day),
            ])
            cnt += 1
            if len(batch) >= batch_size:
                ch.insert("analytics.accidents", batch, column_names=columns)
                progress.update(len(batch))
                batch = []
        if batch:
            ch.insert("analytics.accidents", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()

        # Build daily summary
        ch.command("""
            INSERT INTO analytics.daily_severity
            SELECT event_day, state, severity,
                   count() AS incident_count, avg(distance_mi) AS avg_distance,
                   avg(temperature_f) AS avg_temperature
            FROM analytics.accidents
            GROUP BY event_day, state, severity
        """)
        ch.close()

    def _load_mongo_accidents(self, silo, limits):
        from pymongo import MongoClient

        client = MongoClient(os.environ["MONGO_URL"])
        db = client[os.environ.get("MONGO_DB", "incidents")]

        if db["accidents"].count_documents({}) > 0:
            log.info("Mongo accidents already populated, skipping")
            client.close()
            return

        limit = limits["accidents"]
        mongo_rows, mongo_total = load_local_or_stream("insurance", "us_accidents.parquet", silo.hf_dataset, limit=limit)

        col = db["accidents"]
        batch = []
        count = 0
        progress = ProgressTracker("Mongo accidents", mongo_total or limit)
        for row in mongo_rows:
            if count >= limit:
                break
            doc = {
                "ID": row.get("ID", ""),
                "Severity": row.get("Severity", 1),
                "Start_Time": row.get("Start_Time", ""),
                "End_Time": row.get("End_Time", ""),
                "Start_Lat": row.get("Start_Lat", 0),
                "Start_Lng": row.get("Start_Lng", 0),
                "Distance": row.get("Distance(mi)", 0),
                "Description": str(row.get("Description", ""))[:1000],
                "Street": row.get("Street", ""),
                "City": row.get("City", ""),
                "County": row.get("County", ""),
                "State": row.get("State", ""),
                "Zipcode": row.get("Zipcode", ""),
                "Country": row.get("Country", ""),
                "Weather_Condition": row.get("Weather_Condition", ""),
                "Temperature": row.get("Temperature(F)"),
                "Humidity": row.get("Humidity(%)"),
                "Visibility": row.get("Visibility(mi)"),
                "Wind_Speed": row.get("Wind_Speed(mph)"),
                "Sunrise_Sunset": row.get("Sunrise_Sunset", ""),
                "Traffic_Signal": bool(row.get("Traffic_Signal")),
                "Crossing": bool(row.get("Crossing")),
                "Junction": bool(row.get("Junction")),
            }
            batch.append(doc)
            count += 1
            if len(batch) >= 2000:
                col.insert_many(batch)
                progress.update(len(batch))
                batch = []
        if batch:
            col.insert_many(batch)
            progress.update(len(batch))
        progress.finish()

        col.create_index("State")
        col.create_index("Severity")
        col.create_index("City")
        col.create_index("Weather_Condition")
        col.create_index([("Start_Lat", 1), ("Start_Lng", 1)])
        client.close()

    def _load_redis(self, limits):
        import redis as r

        client = r.from_url(os.environ["REDIS_URL"], decode_responses=True)
        if client.dbsize() > 0:
            log.info("Redis already populated, skipping")
            client.close()
            return

        pipe = client.pipeline()
        rng = random.Random(42)

        # Active policies cache
        num_policies = limits["policies"]
        for i in range(1, min(num_policies, 50001)):
            pipe.hset(f"policy:{i}", mapping={
                "status": rng.choice(["active", "active", "active", "lapsed", "cancelled"]),
                "premium": str(round(rng.lognormvariate(6, 0.8), 2)),
                "claims": str(rng.choices([0, 0, 0, 1, 2, 3], weights=[60, 10, 5, 15, 7, 3])[0]),
            })

        # Claim status tracker
        for i in range(1, min(limits["accidents"] // 100, 10001)):
            pipe.hset(f"claim:{i}", mapping={
                "status": rng.choice(["open", "investigating", "approved", "denied", "paid"]),
                "severity": str(rng.randint(1, 4)),
                "assigned_to": f"agent_{rng.randint(1, 50)}",
            })

        # Cross-DB linkage: policy → claim mapping (synthetic)
        # Links policy IDs to claim IDs so cross-DB queries can correlate
        log.info("  Creating policy ↔ claim linkage for cross-DB correlation...")
        for i in range(1, min(limits["accidents"] // 100, 10001)):
            policy_id = rng.randint(1, min(num_policies, 50000))
            pipe.sadd(f"policy_claims:{policy_id}", str(i))
            pipe.set(f"claim_policy:{i}", str(policy_id))

        # Per-state policy counts (for cross-DB geographic correlation)
        state_counts = getattr(self, "_policy_state_counts", {})
        for state, cnt in state_counts.items():
            pipe.set(f"state_policies:{state}", str(cnt))
        log.info(f"  Writing state_policies keys for {len(state_counts)} states")

        # Stats
        pipe.set("stats:active_policies", str(num_policies))
        pipe.set("stats:open_claims", str(limits["accidents"] // 100))
        pipe.set("stats:avg_claim_amount", str(round(rng.uniform(2000, 15000), 2)))
        pipe.set("stats:loss_ratio", str(round(rng.uniform(0.55, 0.85), 3)))

        # Agent leaderboard
        for i in range(1, 51):
            pipe.zadd("leaderboard:agent_resolutions", {f"agent_{i}": rng.randint(10, 500)})

        pipe.execute()
        log.info(f"  {client.dbsize():,} keys loaded into Redis")
        client.close()

    def _load_weaviate(self, limits):
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

        if client.collections.exists("AccidentReport"):
            col = client.collections.get("AccidentReport")
            resp = col.aggregate.over_all(total_count=True)
            if resp.total_count > 0:
                log.info("Weaviate already populated, skipping")
                client.close()
                return

        if not client.collections.exists("AccidentReport"):
            client.collections.create(
                name="AccidentReport",
                vectorizer_config=Configure.Vectorizer.none(),
                properties=[
                    Property(name="state", data_type=DataType.TEXT),
                    Property(name="severity", data_type=DataType.INT),
                    Property(name="description", data_type=DataType.TEXT),
                    Property(name="weather", data_type=DataType.TEXT),
                ],
            )

        log.info("  Loading sentence-transformers model...")
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")

        from pymongo import MongoClient
        mongo_client = MongoClient(os.environ["MONGO_URL"])
        db = mongo_client[os.environ.get("MONGO_DB", "incidents")]

        weaviate_limit = int(os.environ.get("WEAVIATE_LIMIT", "10000"))
        log.info(f"  Embedding {weaviate_limit:,} accident descriptions...")

        col = client.collections.get("AccidentReport")
        progress = ProgressTracker("Accident embeddings", weaviate_limit)
        count = 0
        batch_size = 100

        docs = list(db["accidents"].find({}, {"State": 1, "Severity": 1, "Description": 1, "Weather_Condition": 1}).limit(weaviate_limit))
        docs = [d for d in docs if str(d.get("Description", "")).strip()]

        with col.batch.fixed_size(batch_size=batch_size) as batch:
            for i in range(0, len(docs), batch_size):
                chunk = docs[i:i + batch_size]
                texts = [str(d.get("Description", ""))[:512] for d in chunk]
                embeddings = model.encode(texts, show_progress_bar=False)
                for doc, emb in zip(chunk, embeddings):
                    batch.add_object(
                        properties={
                            "state": str(doc.get("State", "")),
                            "severity": int(doc.get("Severity", 1)),
                            "description": str(doc.get("Description", ""))[:200],
                            "weather": str(doc.get("Weather_Condition", "")),
                        },
                        vector=emb.tolist(),
                    )
                count += len(chunk)
                progress.update(len(chunk))
        progress.finish()

        mongo_client.close()
        client.close()
