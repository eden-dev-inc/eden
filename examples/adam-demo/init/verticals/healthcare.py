"""
Healthcare / Clinical vertical.

Silos (8 databases):
  pg_patient_records   (Postgres)   — Synthea patients & encounters (575K patients)
  pg_billing           (Postgres)   — Claims, payer coverage, costs
  pg_cms_claims        (Postgres)   — CMS DE-SynPUF Medicare claims (legacy schema)
  mongo_clinical_docs  (MongoDB)    — Conditions, procedures, medications
  mongo_lab_results    (MongoDB)    — Observations, vitals, lab results
  ch_billing_analytics (ClickHouse) — Billing analytics, encounter trends
  redis_alerts         (Redis)      — Bed availability, patient alerts
  weaviate_clinical    (Weaviate)   — Clinical condition embeddings

HuggingFace: richardyoung/synthea-575k-patients (multi-table EHR)
CMS DE-SynPUF: https://data.nber.org/synpuf/ (Medicare claims, legacy schema)
"""

import os
import math
import time
import random
import logging
from datetime import datetime

from verticals.base import VerticalBase, DatabaseSilo, ProgressTracker, load_local_or_stream, DATA_DIR

log = logging.getLogger("adam-init")

HF_DATASET = "richardyoung/synthea-575k-patients"

SCALE_LIMITS = {
    "demo":    {"patients": 5_000,   "encounters": 20_000,  "conditions": 10_000,  "observations": 20_000,
                "cms_bene": 5_000,   "cms_ip": 2_000,   "cms_op": 10_000,  "cms_pde": 20_000, "cms_car": 20_000},
    "small":   {"patients": 10_000,  "encounters": 50_000,  "conditions": 30_000,  "observations": 50_000,
                "cms_bene": 10_000,  "cms_ip": 5_000,   "cms_op": 30_000,  "cms_pde": 50_000, "cms_car": 50_000},
    "medium":  {"patients": 100_000, "encounters": 500_000, "conditions": 300_000, "observations": 500_000,
                "cms_bene": 50_000,  "cms_ip": 30_000,  "cms_op": 200_000, "cms_pde": 500_000, "cms_car": 500_000},
    "large":   {"patients": 300_000, "encounters": 2_000_000, "conditions": 1_000_000, "observations": 2_000_000,
                "cms_bene": 100_000, "cms_ip": 60_000,  "cms_op": 700_000, "cms_pde": 2_000_000, "cms_car": 2_000_000},
    "massive": {"patients": 575_000, "encounters": 5_000_000, "conditions": 3_000_000, "observations": 5_000_000,
                "cms_bene": 116_000, "cms_ip": 67_000,  "cms_op": 790_000, "cms_pde": 2_500_000, "cms_car": 5_500_000},
}


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "nat", "none", "null"}
    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        return False


def _clean_text(value, default=""):
    return default if _is_missing(value) else str(value)


def _clean_nullable(value):
    return None if _is_missing(value) else value


def _clean_date(value):
    if _is_missing(value):
        return None
    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except TypeError:
            pass
    return str(value)


def _clean_timestamp(value):
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    return text[:-1] if text.endswith("Z") else text


def _clean_float(value, default=0.0):
    if _is_missing(value):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(number) or math.isinf(number) else number


def _get_clickhouse_client(host_env_var: str, default_host: str):
    import clickhouse_connect

    host = os.environ.get(host_env_var, default_host)
    port = int(os.environ.get("CLICKHOUSE_PORT", 8123))
    username = os.environ.get("CLICKHOUSE_USER", "eden")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "eden")

    last_error = None
    for attempt in range(1, 7):
        try:
            return clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
            )
        except Exception as exc:
            last_error = exc
            if attempt == 6:
                break
            log.warning(f"ClickHouse not ready at {host}:{port} (attempt {attempt}/6): {exc}")
            time.sleep(2)
    raise last_error


class HealthcareVertical(VerticalBase):
    name = "healthcare"
    description = "Healthcare / Clinical"

    def __init__(self):
        self._patient_ids: set[str] | None = None       # Synthea patient IDs (captured in _load_patients)
        self._cms_desynpuf_ids: set[str] | None = None   # CMS beneficiary IDs (captured in _load_cms_claims)

    def silos(self) -> list[DatabaseSilo]:
        return [
            DatabaseSilo(name="pg_patient_records", db_type="postgres",
                         description="EHR — Patient demographics & encounters (Synthea 575K patients)",
                         url_env_var="PG_PATIENT_RECORDS_URL", eden_url_env_var="EDEN_PG_PATIENT_RECORDS_URL",
                         schema_file="healthcare/postgres_patient_records.sql",
                         hf_dataset=HF_DATASET, team="Clinical"),
            DatabaseSilo(name="pg_billing", db_type="postgres",
                         description="Billing — Insurance claims, payer coverage, costs",
                         url_env_var="PG_BILLING_URL", eden_url_env_var="EDEN_PG_BILLING_URL",
                         schema_file="healthcare/postgres_billing.sql",
                         hf_dataset=HF_DATASET, team="Revenue Cycle"),
            DatabaseSilo(name="pg_cms_claims", db_type="postgres",
                         description="CMS Medicare Claims — Legacy claims warehouse",
                         url_env_var="PG_CMS_CLAIMS_URL", eden_url_env_var="EDEN_PG_CMS_CLAIMS_URL",
                         schema_file="healthcare/postgres_cms_claims.sql",
                         team="Claims Processing"),
            DatabaseSilo(name="mongo_clinical_docs", db_type="mongo",
                         description="Clinical Docs — Conditions, procedures, medications as documents",
                         url_env_var="MONGO_CLINICAL_URL", eden_url_env_var="EDEN_MONGO_CLINICAL_URL",
                         hf_dataset=HF_DATASET, team="Clinical IT"),
            DatabaseSilo(name="mongo_lab_results", db_type="mongo",
                         description="Lab System — Observations, vitals, lab results",
                         url_env_var="MONGO_LAB_URL", eden_url_env_var="EDEN_MONGO_LAB_URL",
                         hf_dataset=HF_DATASET, team="Laboratory"),
            DatabaseSilo(name="ch_billing_analytics", db_type="clickhouse",
                         description="Billing Analytics — Claims aggregates, cost trends",
                         url_env_var="CLICKHOUSE_HOST", eden_url_env_var="EDEN_CLICKHOUSE_URL",
                         schema_file="healthcare/clickhouse_billing.sql",
                         hf_dataset=HF_DATASET, team="Analytics"),
            DatabaseSilo(name="redis_alerts", db_type="redis",
                         description="Real-time — Bed availability, patient alerts, appointment slots",
                         url_env_var="REDIS_URL", eden_url_env_var="EDEN_REDIS_URL", team="Operations"),
            DatabaseSilo(name="weaviate_clinical", db_type="weaviate",
                         description="Clinical Search — Condition & procedure description embeddings",
                         url_env_var="WEAVIATE_URL", eden_url_env_var="EDEN_WEAVIATE_URL", team="Clinical Research"),
        ]

    def load_silo(self, silo: DatabaseSilo, scale: str):
        limits = SCALE_LIMITS.get(scale, SCALE_LIMITS["small"])
        if silo.name == "pg_patient_records":
            self._load_patients(silo, limits)
        elif silo.name == "pg_billing":
            self._load_billing(silo, limits)
        elif silo.name == "pg_cms_claims":
            self._load_cms_claims(silo, limits)
        elif silo.name == "mongo_clinical_docs":
            self._load_clinical_docs(silo, limits)
        elif silo.name == "mongo_lab_results":
            self._load_lab_results(silo, limits)
        elif silo.name == "ch_billing_analytics":
            self._load_ch_analytics(silo, limits)
        elif silo.name == "redis_alerts":
            self._load_redis(limits)
        elif silo.name == "weaviate_clinical":
            self._load_weaviate(limits)

    def _load_patients(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM patients")
        if cur.fetchone()[0] > 0:
            log.info("Patients already populated, skipping")
            # Still need patient IDs for downstream silo filtering
            cur.execute("SELECT id FROM patients")
            self._patient_ids = {row[0] for row in cur.fetchall()}
            log.info(f"  Captured {len(self._patient_ids):,} existing patient IDs for cross-silo filtering")
            conn.close()
            return

        limit_patients = limits["patients"]
        limit_encounters = limits["encounters"]

        # Load patients
        rows, total = load_local_or_stream("healthcare", "synthea_patients.parquet", HF_DATASET, hf_parquet_path="data/patients.parquet", limit=limit_patients)

        progress = ProgressTracker("Patients", total or limit_patients)
        batch = []
        count = 0
        patient_ids = set()
        for row in rows:
            if count >= limit_patients:
                break
            pid = _clean_text(row.get("Id", ""))
            patient_ids.add(pid)
            batch.append((
                pid,
                _clean_date(row.get("BIRTHDATE", None)),
                _clean_date(row.get("DEATHDATE", None)),
                _clean_text(row.get("SSN", "")),
                _clean_text(row.get("PREFIX", "")),
                _clean_text(row.get("FIRST", "")),
                _clean_text(row.get("LAST", "")),
                _clean_text(row.get("SUFFIX", "")),
                _clean_text(row.get("MAIDEN", "")),
                _clean_text(row.get("MARITAL", "")),
                _clean_text(row.get("RACE", "")),
                _clean_text(row.get("ETHNICITY", "")),
                _clean_text(row.get("GENDER", "")),
                _clean_text(row.get("BIRTHPLACE", "")),
                _clean_text(row.get("ADDRESS", "")),
                _clean_text(row.get("CITY", "")),
                _clean_text(row.get("STATE", "")),
                _clean_text(row.get("COUNTY", "")),
                _clean_text(row.get("ZIP", "")),
                _clean_float(row.get("LAT", 0)),
                _clean_float(row.get("LON", 0)),
                _clean_float(row.get("HEALTHCARE_EXPENSES", 0)),
                _clean_float(row.get("HEALTHCARE_COVERAGE", 0)),
            ))
            count += 1
            if len(batch) >= 5000:
                execute_values(cur, """INSERT INTO patients
                    (id, birthdate, deathdate, ssn, prefix, first_name, last_name, suffix,
                     maiden, marital, race, ethnicity, gender, birthplace, address,
                     city, state, county, zip, lat, lon,
                     healthcare_expenses, healthcare_coverage)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO patients
                (id, birthdate, deathdate, ssn, prefix, first_name, last_name, suffix,
                 maiden, marital, race, ethnicity, gender, birthplace, address,
                 city, state, county, zip, lat, lon,
                 healthcare_expenses, healthcare_coverage)
                VALUES %s ON CONFLICT DO NOTHING""", batch)
            progress.update(len(batch))
        progress.finish()
        self._patient_ids = patient_ids
        log.info(f"  Captured {len(self._patient_ids):,} patient IDs for cross-silo filtering")

        # Load encounters (filtered to only loaded patients)
        enc_rows, _ = load_local_or_stream("healthcare", "synthea_encounters.parquet", HF_DATASET, hf_parquet_path="data/encounters.parquet")

        progress = ProgressTracker("Encounters", limit_encounters)
        batch = []
        count = 0
        for row in enc_rows:
            if count >= limit_encounters:
                break
            patient = _clean_text(row.get("PATIENT", ""))
            if patient not in self._patient_ids:
                continue
            batch.append((
                _clean_text(row.get("Id", "")),
                _clean_timestamp(row.get("START", None)),
                _clean_timestamp(row.get("STOP", None)),
                patient,
                _clean_text(row.get("ORGANIZATION", "")),
                _clean_text(row.get("PROVIDER", "")),
                _clean_text(row.get("PAYER", "")),
                _clean_text(row.get("ENCOUNTERCLASS", "")),
                _clean_text(row.get("CODE", "")),
                _clean_text(row.get("DESCRIPTION", "")),
                _clean_float(row.get("BASE_ENCOUNTER_COST", 0)),
                _clean_float(row.get("TOTAL_CLAIM_COST", 0)),
                _clean_float(row.get("PAYER_COVERAGE", 0)),
                _clean_text(row.get("REASONCODE", "")),
                _clean_text(row.get("REASONDESCRIPTION", "")),
            ))
            count += 1
            if len(batch) >= 5000:
                execute_values(cur, """INSERT INTO encounters
                    (id, start_ts, stop_ts, patient, organization, provider, payer,
                     encounterclass, code, description, base_encounter_cost,
                     total_claim_cost, payer_coverage, reasoncode, reasondescription)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO encounters
                (id, start_ts, stop_ts, patient, organization, provider, payer,
                 encounterclass, code, description, base_encounter_cost,
                 total_claim_cost, payer_coverage, reasoncode, reasondescription)
                VALUES %s ON CONFLICT DO NOTHING""", batch)
            progress.update(len(batch))
        progress.finish()
        conn.close()

    def _load_billing(self, silo, limits):
        import psycopg2
        from psycopg2.extras import execute_values

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM claims")
        if cur.fetchone()[0] > 0:
            log.info("Billing already populated, skipping")
            conn.close()
            return

        # Load payers
        log.info("  Downloading Synthea payers...")
        try:
            payer_rows, _ = load_local_or_stream("healthcare", "synthea_payers.parquet", HF_DATASET, hf_parquet_path="data/payers.parquet")
            batch = []
            for row in payer_rows:
                batch.append((
                    row.get("Id", ""), row.get("NAME", ""), row.get("OWNERSHIP", ""),
                    row.get("STATE_HEADQUARTERED", ""), row.get("REVENUE", 0),
                    row.get("COVERED_ENCOUNTERS", 0), row.get("UNCOVERED_ENCOUNTERS", 0),
                    row.get("COVERED_MEDICATIONS", 0), row.get("UNCOVERED_MEDICATIONS", 0),
                ))
                if len(batch) >= 5000:
                    execute_values(cur, """INSERT INTO payers
                        (id, name, ownership, state_headquartered, revenue,
                         covered_encounters, uncovered_encounters,
                         covered_medications, uncovered_medications)
                        VALUES %s ON CONFLICT DO NOTHING""", batch)
                    batch = []
            if batch:
                execute_values(cur, """INSERT INTO payers
                    (id, name, ownership, state_headquartered, revenue,
                     covered_encounters, uncovered_encounters,
                     covered_medications, uncovered_medications)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
            log.info("  Payers loaded")
        except Exception as e:
            log.warning(f"  Could not load payers: {e}")

        # Generate claims from encounters (synthetic billing records, filtered to loaded patients)
        log.info("  Generating claims from encounter data...")
        rng = random.Random(42)
        limit = limits["encounters"]
        claim_rows, _ = load_local_or_stream("healthcare", "synthea_encounters.parquet", HF_DATASET, hf_parquet_path="data/encounters.parquet")
        batch = []
        count = 0
        progress = ProgressTracker("Claims", limit)
        for row in claim_rows:
            if count >= limit:
                break
            patient = _clean_text(row.get("PATIENT", ""))
            if self._patient_ids and patient not in self._patient_ids:
                continue
            total_cost = _clean_float(row.get("TOTAL_CLAIM_COST", 0))
            payer_cov = _clean_float(row.get("PAYER_COVERAGE", 0))
            batch.append((
                _clean_text(row.get("Id", "")),
                patient,
                _clean_text(row.get("PROVIDER", "")),
                _clean_text(row.get("Id", "")),
                _clean_text(row.get("CODE", "")),
                _clean_text(row.get("REASONCODE", "")),
                total_cost, payer_cov, round(total_cost - payer_cov, 2),
                rng.choice(["submitted", "approved", "denied", "paid"]),
                _clean_timestamp(row.get("START", None)),
                None,
            ))
            count += 1
            if len(batch) >= 5000:
                execute_values(cur, """INSERT INTO claims
                    (id, patient_id, provider_id, encounter_id, diagnosis_code,
                     procedure_code, total_cost, payer_coverage, patient_responsibility,
                     status, submitted_at, processed_at)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO claims
                (id, patient_id, provider_id, encounter_id, diagnosis_code,
                 procedure_code, total_cost, payer_coverage, patient_responsibility,
                 status, submitted_at, processed_at)
                VALUES %s ON CONFLICT DO NOTHING""", batch)
            progress.update(len(batch))
        progress.finish()
        conn.close()

    def _load_cms_claims(self, silo, limits):
        """Load CMS DE-SynPUF Medicare claims data (legacy schema with cryptic column names)."""
        import psycopg2
        from psycopg2.extras import execute_values
        import csv
        import io

        url = os.environ[silo.url_env_var]
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM bene_summary")
        if cur.fetchone()[0] > 0:
            log.info("CMS claims already populated, skipping")
            # Still need CMS IDs for the mapping table
            cur.execute("SELECT DESYNPUF_ID FROM bene_summary")
            self._cms_desynpuf_ids = {row[0] for row in cur.fetchall()}
            log.info(f"  Captured {len(self._cms_desynpuf_ids):,} existing CMS IDs for patient mapping")
            self._build_patient_mapping()
            conn.close()
            return

        cms_dir = DATA_DIR / "healthcare"

        # ── Beneficiary Summary ──
        bene_file = cms_dir / "cms_beneficiary.parquet"
        if bene_file.exists():
            limit_bene = limits.get("cms_bene", 5000)
            rows, total = load_local_or_stream("healthcare", "cms_beneficiary.parquet", "", limit=limit_bene)
            progress = ProgressTracker("CMS Beneficiaries", total or limit_bene)
            batch = []
            count = 0
            cms_ids = set()
            for row in rows:
                if count >= limit_bene:
                    break
                desynpuf_id = str(row.get("DESYNPUF_ID", ""))
                cms_ids.add(desynpuf_id)
                batch.append((
                    desynpuf_id,
                    _clean_nullable(row.get("BENE_BIRTH_DT", None)),
                    _clean_nullable(row.get("BENE_DEATH_DT", None)),
                    _clean_nullable(row.get("BENE_SEX_IDENT_CD", None)),
                    _clean_nullable(row.get("BENE_RACE_CD", None)),
                    _clean_text(row.get("BENE_ESRD_IND", ""), default="")[:1],
                    _clean_nullable(row.get("SP_STATE_CODE", None)),
                    _clean_nullable(row.get("BENE_COUNTY_CD", row.get("BENE_COUNTY_CODE", None))),
                    _clean_nullable(row.get("BENE_HI_CVRAGE_TOT_MONS", None)),
                    _clean_nullable(row.get("BENE_SMI_CVRAGE_TOT_MONS", None)),
                    _clean_nullable(row.get("BENE_HMO_CVRAGE_TOT_MONS", None)),
                    _clean_nullable(row.get("PLAN_CVRG_MOS_NUM", None)),
                    _clean_nullable(row.get("SP_ALZHDMTA", None)),
                    _clean_nullable(row.get("SP_CHF", None)),
                    _clean_nullable(row.get("SP_CHRNKIDN", None)),
                    _clean_nullable(row.get("SP_CNCR", None)),
                    _clean_nullable(row.get("SP_COPD", None)),
                    _clean_nullable(row.get("SP_DEPRESSN", None)),
                    _clean_nullable(row.get("SP_DIABETES", None)),
                    _clean_nullable(row.get("SP_ISCHMCHT", None)),
                    _clean_nullable(row.get("SP_OSTEOPRS", None)),
                    _clean_nullable(row.get("SP_RA_OA", None)),
                    _clean_nullable(row.get("SP_STRKETIA", None)),
                    _clean_float(row.get("MEDREIMB_IP", 0)),
                    _clean_float(row.get("BENRES_IP", 0)),
                    _clean_float(row.get("PPPYMT_IP", 0)),
                    _clean_float(row.get("MEDREIMB_OP", 0)),
                    _clean_float(row.get("BENRES_OP", 0)),
                    _clean_float(row.get("PPPYMT_OP", 0)),
                    _clean_float(row.get("MEDREIMB_CAR", 0)),
                    _clean_float(row.get("BENRES_CAR", 0)),
                    _clean_float(row.get("PPPYMT_CAR", 0)),
                ))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, """INSERT INTO bene_summary
                        (DESYNPUF_ID, BENE_BIRTH_DT, BENE_DEATH_DT,
                         BENE_SEX_IDENT_CD, BENE_RACE_CD, BENE_ESRD_IND,
                         SP_STATE_CODE, BENE_COUNTY_CODE,
                         BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS,
                         BENE_HMO_CVRAGE_TOT_MONS, PLAN_CVRG_MOS_NUM,
                         SP_ALZHDMTA, SP_CHF, SP_CHRNKIDN, SP_CNCR,
                         SP_COPD, SP_DEPRESSN, SP_DIABETES, SP_ISCHMCHT,
                         SP_OSTEOPRS, SP_RA_OA, SP_STRKETIA,
                         MEDREIMB_IP, BENRES_IP, PPPYMT_IP,
                         MEDREIMB_OP, BENRES_OP, PPPYMT_OP,
                         MEDREIMB_CAR, BENRES_CAR, PPPYMT_CAR)
                        VALUES %s ON CONFLICT DO NOTHING""", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                execute_values(cur, """INSERT INTO bene_summary
                    (DESYNPUF_ID, BENE_BIRTH_DT, BENE_DEATH_DT,
                     BENE_SEX_IDENT_CD, BENE_RACE_CD, BENE_ESRD_IND,
                     SP_STATE_CODE, BENE_COUNTY_CODE,
                     BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS,
                     BENE_HMO_CVRAGE_TOT_MONS, PLAN_CVRG_MOS_NUM,
                     SP_ALZHDMTA, SP_CHF, SP_CHRNKIDN, SP_CNCR,
                     SP_COPD, SP_DEPRESSN, SP_DIABETES, SP_ISCHMCHT,
                     SP_OSTEOPRS, SP_RA_OA, SP_STRKETIA,
                     MEDREIMB_IP, BENRES_IP, PPPYMT_IP,
                     MEDREIMB_OP, BENRES_OP, PPPYMT_OP,
                     MEDREIMB_CAR, BENRES_CAR, PPPYMT_CAR)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
            progress.finish()
            self._cms_desynpuf_ids = cms_ids
            log.info(f"  Captured {len(self._cms_desynpuf_ids):,} CMS beneficiary IDs for patient mapping")
        else:
            log.warning("  CMS beneficiary data not found — run download_datasets.py healthcare first")

        # ── Inpatient Claims ──
        ip_file = cms_dir / "cms_inpatient.parquet"
        if ip_file.exists():
            limit_ip = limits.get("cms_ip", 2000)
            rows, total = load_local_or_stream("healthcare", "cms_inpatient.parquet", "", limit=limit_ip)
            progress = ProgressTracker("CMS Inpatient Claims", total or limit_ip)
            batch = []
            count = 0

            # Column list matches ip_claims table
            ip_cols = ["CLM_ID", "DESYNPUF_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT",
                        "PRVDR_NUM", "CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT",
                        "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI",
                        "CLM_ADMSN_DT", "ADMTNG_ICD9_DGNS_CD",
                        "CLM_PASS_THRU_PER_DIEM_AMT", "NCH_BENE_IP_DDCTBL_AMT",
                        "NCH_BENE_PTA_COINSRNC_LBLTY_AM", "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM",
                        "CLM_UTLZTN_DAY_CNT", "NCH_BENE_DSCHRG_DT", "CLM_DRG_CD"] + \
                       [f"ICD9_DGNS_CD_{i}" for i in range(1, 11)] + \
                       [f"ICD9_PRCDR_CD_{i}" for i in range(1, 7)] + \
                       [f"HCPCS_CD_{i}" for i in range(1, 46)]

            placeholders = ", ".join(["%s"] * len(ip_cols))
            col_list = ", ".join(ip_cols)

            for row in rows:
                if count >= limit_ip:
                    break
                vals = []
                for c in ip_cols:
                    vals.append(_clean_nullable(row.get(c, None)))
                batch.append(tuple(vals))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, f"INSERT INTO ip_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                execute_values(cur, f"INSERT INTO ip_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                progress.update(len(batch))
            progress.finish()
        else:
            log.warning("  CMS inpatient data not found — run download_datasets.py healthcare first")

        # ── Outpatient Claims ──
        op_file = cms_dir / "cms_outpatient.parquet"
        if op_file.exists():
            limit_op = limits.get("cms_op", 10000)
            rows, total = load_local_or_stream("healthcare", "cms_outpatient.parquet", "", limit=limit_op)
            progress = ProgressTracker("CMS Outpatient Claims", total or limit_op)
            batch = []
            count = 0

            op_cols = ["CLM_ID", "DESYNPUF_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT",
                        "PRVDR_NUM", "CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT",
                        "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI",
                        "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM"] + \
                       [f"ICD9_DGNS_CD_{i}" for i in range(1, 11)] + \
                       [f"ICD9_PRCDR_CD_{i}" for i in range(1, 7)] + \
                       ["NCH_BENE_PTB_DDCTBL_AMT", "NCH_BENE_PTB_COINSRNC_AMT",
                        "ADMTNG_ICD9_DGNS_CD"] + \
                       [f"HCPCS_CD_{i}" for i in range(1, 46)]

            col_list = ", ".join(op_cols)

            for row in rows:
                if count >= limit_op:
                    break
                vals = []
                for c in op_cols:
                    vals.append(_clean_nullable(row.get(c, None)))
                batch.append(tuple(vals))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, f"INSERT INTO op_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                execute_values(cur, f"INSERT INTO op_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                progress.update(len(batch))
            progress.finish()
        else:
            log.warning("  CMS outpatient data not found — run download_datasets.py healthcare first")

        # ── Prescription Drug Events ──
        pde_file = cms_dir / "cms_pde.parquet"
        if pde_file.exists():
            limit_pde = limits.get("cms_pde", 20000)
            rows, total = load_local_or_stream("healthcare", "cms_pde.parquet", "", limit=limit_pde)
            progress = ProgressTracker("CMS Prescription Drug Events", total or limit_pde)
            batch = []
            count = 0
            for row in rows:
                if count >= limit_pde:
                    break
                batch.append((
                    str(row.get("PDE_ID", "")), str(row.get("DESYNPUF_ID", "")),
                    _clean_nullable(row.get("SRVC_DT", None)),
                    _clean_text(row.get("PROD_SRVC_ID", ""), default=""),
                    _clean_float(row.get("QTY_DSPNSD_NUM", 0)),
                    _clean_nullable(row.get("DAYS_SUPLY_NUM", 0)),
                    _clean_float(row.get("PTNT_PAY_AMT", 0)),
                    _clean_float(row.get("TOT_RX_CST_AMT", 0)),
                ))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, """INSERT INTO pde
                        (PDE_ID, DESYNPUF_ID, SRVC_DT, PROD_SRVC_ID,
                         QTY_DSPNSD_NUM, DAYS_SUPLY_NUM, PTNT_PAY_AMT, TOT_RX_CST_AMT)
                        VALUES %s ON CONFLICT DO NOTHING""", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                execute_values(cur, """INSERT INTO pde
                    (PDE_ID, DESYNPUF_ID, SRVC_DT, PROD_SRVC_ID,
                     QTY_DSPNSD_NUM, DAYS_SUPLY_NUM, PTNT_PAY_AMT, TOT_RX_CST_AMT)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
            progress.finish()
        else:
            log.warning("  CMS PDE data not found — run download_datasets.py healthcare first")

        # ── Carrier Claims ──
        car_file = cms_dir / "cms_carrier.parquet"
        if car_file.exists():
            limit_car = limits.get("cms_car", 20000)
            rows, total = load_local_or_stream("healthcare", "cms_carrier.parquet", "", limit=limit_car)
            progress = ProgressTracker("CMS Carrier Claims", total or limit_car)
            batch = []
            count = 0

            car_cols = ["CLM_ID", "DESYNPUF_ID", "CLM_FROM_DT", "CLM_THRU_DT"] + \
                       [f"ICD9_DGNS_CD_{i}" for i in range(1, 9)] + \
                       [f"PRF_PHYSN_NPI_{i}" for i in range(1, 14)] + \
                       [f"TAX_NUM_{i}" for i in range(1, 14)] + \
                       [f"HCPCS_CD_{i}" for i in range(1, 14)] + \
                       [f"LINE_NCH_PMT_AMT_{i}" for i in range(1, 14)] + \
                       [f"LINE_BENE_PTB_DDCTBL_AMT_{i}" for i in range(1, 14)] + \
                       [f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}" for i in range(1, 14)] + \
                       [f"LINE_COINSRNC_AMT_{i}" for i in range(1, 14)] + \
                       [f"LINE_ALOWD_CHRG_AMT_{i}" for i in range(1, 14)] + \
                       [f"LINE_PRCSG_IND_CD_{i}" for i in range(1, 14)] + \
                       [f"LINE_ICD9_DGNS_CD_{i}" for i in range(1, 14)]

            col_list = ", ".join(car_cols)

            for row in rows:
                if count >= limit_car:
                    break
                vals = []
                for c in car_cols:
                    vals.append(_clean_nullable(row.get(c, None)))
                batch.append(tuple(vals))
                count += 1
                if len(batch) >= 5000:
                    execute_values(cur, f"INSERT INTO car_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                    progress.update(len(batch))
                    batch = []
            if batch:
                execute_values(cur, f"INSERT INTO car_claims ({col_list}) VALUES %s ON CONFLICT DO NOTHING", batch)
                progress.update(len(batch))
            progress.finish()
        else:
            log.warning("  CMS carrier data not found — run download_datasets.py healthcare first")

        conn.close()

        # Build mapping table between Synthea and CMS patient IDs
        self._build_patient_mapping()

    def _build_patient_mapping(self):
        """Create synthetic 1:1 mapping between Synthea patient IDs and CMS DESYNPUF_IDs.

        This enables cross-silo queries between the Synthea EHR (pg_patient_records)
        and CMS Medicare claims (pg_cms_claims) which otherwise use incompatible ID spaces.
        """
        import psycopg2
        from psycopg2.extras import execute_values

        if not self._patient_ids or not self._cms_desynpuf_ids:
            log.warning("Cannot build patient mapping: missing patient ID sets")
            return

        url = os.environ.get("PG_PATIENT_RECORDS_URL")
        if not url:
            log.warning("Cannot build patient mapping: PG_PATIENT_RECORDS_URL not set")
            return

        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM patient_id_mapping")
        if cur.fetchone()[0] > 0:
            log.info("Patient ID mapping already populated, skipping")
            conn.close()
            return

        # Deterministic 1:1 mapping (sorted + seeded shuffle for reproducibility)
        rng = random.Random(42)
        synthea_list = sorted(self._patient_ids)
        cms_list = sorted(self._cms_desynpuf_ids)
        rng.shuffle(cms_list)

        # Map up to min(len(synthea), len(cms)) patients — realistic: not all EHR patients are Medicare
        pairs = list(zip(synthea_list, cms_list))
        log.info(f"  Building patient ID mapping: {len(pairs):,} pairs "
                 f"({len(self._patient_ids):,} Synthea, {len(self._cms_desynpuf_ids):,} CMS)")

        progress = ProgressTracker("Patient ID Mapping", len(pairs))
        batch = []
        for synthea_id, cms_id in pairs:
            batch.append((synthea_id, cms_id))
            if len(batch) >= 5000:
                execute_values(cur, """INSERT INTO patient_id_mapping
                    (synthea_patient_id, cms_desynpuf_id)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                progress.update(len(batch))
                batch = []
        if batch:
            execute_values(cur, """INSERT INTO patient_id_mapping
                (synthea_patient_id, cms_desynpuf_id)
                VALUES %s ON CONFLICT DO NOTHING""", batch)
            progress.update(len(batch))
        progress.finish()
        conn.close()

    def _load_clinical_docs(self, silo, limits):
        from pymongo import MongoClient

        client = MongoClient(os.environ[silo.url_env_var])
        db = client[os.environ.get("MONGO_CLINICAL_DB", "clinical")]

        if db["conditions"].count_documents({}) > 0:
            log.info("Clinical docs already populated, skipping")
            client.close()
            return

        limit = limits["conditions"]

        # Conditions (filtered to loaded patients)
        cond_rows, _ = load_local_or_stream("healthcare", "synthea_conditions.parquet", HF_DATASET, hf_parquet_path="data/conditions.parquet")
        col = db["conditions"]
        batch = []
        count = 0
        progress = ProgressTracker("Conditions", limit)
        for row in cond_rows:
            if count >= limit:
                break
            patient = row.get("PATIENT", "")
            if self._patient_ids and patient not in self._patient_ids:
                continue
            batch.append({
                "start": row.get("START", ""),
                "stop": row.get("STOP", ""),
                "patient": row.get("PATIENT", ""),
                "encounter": row.get("ENCOUNTER", ""),
                "code": row.get("CODE", ""),
                "description": row.get("DESCRIPTION", ""),
            })
            count += 1
            if len(batch) >= 2000:
                col.insert_many(batch)
                progress.update(len(batch))
                batch = []
        if batch:
            col.insert_many(batch)
            progress.update(len(batch))
        progress.finish()
        col.create_index("patient")
        col.create_index("code")
        col.create_index("description")

        # Medications (filtered to loaded patients)
        log.info("  Downloading Synthea medications...")
        try:
            med_rows, _ = load_local_or_stream("healthcare", "synthea_medications.parquet", HF_DATASET, hf_parquet_path="data/medications.parquet")
            col = db["medications"]
            batch = []
            count = 0
            for row in med_rows:
                if count >= limit:
                    break
                patient = row.get("PATIENT", "")
                if self._patient_ids and patient not in self._patient_ids:
                    continue
                batch.append({
                    "start": row.get("START", ""),
                    "stop": row.get("STOP", ""),
                    "patient": row.get("PATIENT", ""),
                    "encounter": row.get("ENCOUNTER", ""),
                    "code": row.get("CODE", ""),
                    "description": row.get("DESCRIPTION", ""),
                    "base_cost": row.get("BASE_COST", 0),
                    "payer_coverage": row.get("PAYER_COVERAGE", 0),
                    "reason_code": row.get("REASONCODE", ""),
                    "reason_description": row.get("REASONDESCRIPTION", ""),
                })
                count += 1
                if len(batch) >= 2000:
                    col.insert_many(batch)
                    batch = []
            if batch:
                col.insert_many(batch)
            col.create_index("patient")
            col.create_index("code")
            log.info(f"  {col.count_documents({}):,} medications loaded")
        except Exception as e:
            log.warning(f"  Could not load medications: {e}")

        client.close()

    def _load_lab_results(self, silo, limits):
        from pymongo import MongoClient

        client = MongoClient(os.environ[silo.url_env_var])
        db = client[os.environ.get("MONGO_LAB_DB", "laboratory")]

        if db["observations"].count_documents({}) > 0:
            log.info("Lab results already populated, skipping")
            client.close()
            return

        limit = limits["observations"]

        # Observations (filtered to loaded patients)
        obs_rows, _ = load_local_or_stream("healthcare", "synthea_observations.parquet", HF_DATASET, hf_parquet_path="data/observations.parquet")
        col = db["observations"]
        batch = []
        count = 0
        progress = ProgressTracker("Observations", limit)
        for row in obs_rows:
            if count >= limit:
                break
            patient = row.get("PATIENT", "")
            if self._patient_ids and patient not in self._patient_ids:
                continue
            batch.append({
                "date": row.get("DATE", ""),
                "patient": row.get("PATIENT", ""),
                "encounter": row.get("ENCOUNTER", ""),
                "category": row.get("CATEGORY", ""),
                "code": row.get("CODE", ""),
                "description": row.get("DESCRIPTION", ""),
                "value": row.get("VALUE", ""),
                "units": row.get("UNITS", ""),
                "type": row.get("TYPE", ""),
            })
            count += 1
            if len(batch) >= 2000:
                col.insert_many(batch)
                progress.update(len(batch))
                batch = []
        if batch:
            col.insert_many(batch)
            progress.update(len(batch))
        progress.finish()
        col.create_index("patient")
        col.create_index("code")
        col.create_index("category")
        col.create_index("description")
        client.close()

    def _load_ch_analytics(self, silo, limits):
        ch = _get_clickhouse_client(silo.url_env_var, "clickhouse-analytics")

        count = ch.query("SELECT count() FROM analytics.encounter_events").result_rows[0][0]
        if count > 0:
            log.info("ClickHouse analytics already populated, skipping")
            ch.close()
            return

        limit = limits["encounters"]
        ch_rows, _ = load_local_or_stream("healthcare", "synthea_encounters.parquet", HF_DATASET, hf_parquet_path="data/encounters.parquet")
        log.info(f"  Loading encounter events into ClickHouse (limit: {limit:,}, filtered to loaded patients)...")

        progress = ProgressTracker("CH encounters", limit)
        columns = ["encounter_id", "patient_id", "encounterclass", "code", "description",
                    "payer", "total_claim_cost", "payer_coverage", "patient_cost",
                    "event_day", "state", "gender", "race"]
        batch = []
        batch_size = 50000
        cnt = 0
        for row in ch_rows:
            if cnt >= limit:
                break
            patient = _clean_text(row.get("PATIENT", ""))
            if self._patient_ids and patient not in self._patient_ids:
                continue
            total = _clean_float(row.get("TOTAL_CLAIM_COST", 0))
            coverage = _clean_float(row.get("PAYER_COVERAGE", 0))
            start = row.get("START", "")
            try:
                dt = datetime.fromisoformat(str(start).replace("Z", ""))
                day = (dt - datetime(2020, 1, 1)).days
            except Exception:
                day = 0
            batch.append([
                _clean_text(row.get("Id", "")),
                patient,
                _clean_text(row.get("ENCOUNTERCLASS", "")),
                _clean_text(row.get("CODE", "")),
                _clean_text(row.get("DESCRIPTION", "")),
                _clean_text(row.get("PAYER", "")),
                total, coverage, round(total - coverage, 2),
                max(0, day), "", "", "",
            ])
            cnt += 1
            if len(batch) >= batch_size:
                ch.insert("analytics.encounter_events", batch, column_names=columns)
                progress.update(len(batch))
                batch = []
        if batch:
            ch.insert("analytics.encounter_events", batch, column_names=columns)
            progress.update(len(batch))
        progress.finish()

        # Build daily summary
        ch.command("""
            INSERT INTO analytics.daily_cost_summary
            SELECT event_day, encounterclass, payer,
                   count() AS encounter_count, sum(total_claim_cost) AS total_cost,
                   sum(payer_coverage) AS total_coverage, uniq(patient_id) AS unique_patients
            FROM analytics.encounter_events
            GROUP BY event_day, encounterclass, payer
        """)
        ch.close()

    def _load_redis(self, limits):
        import redis as r

        client = r.from_url(os.environ["REDIS_URL"], decode_responses=True)
        if client.dbsize() > 0:
            log.info("Redis already populated, skipping")
            client.close()
            return

        pipe = client.pipeline()
        rng = random.Random(42)

        # Bed availability per department
        departments = ["Emergency", "ICU", "Pediatrics", "Surgery", "General", "Maternity", "Cardiac"]
        for dept in departments:
            total = rng.randint(20, 100)
            occupied = rng.randint(int(total * 0.5), total)
            pipe.hset(f"beds:{dept}", mapping={
                "total": str(total), "occupied": str(occupied),
                "available": str(total - occupied),
            })

        # Patient alert queue
        for i in range(1, 1001):
            pipe.zadd("alerts:patient_priority", {f"patient:{i}": rng.randint(1, 10)})

        # Stats
        pipe.set("stats:total_patients", str(limits["patients"]))
        pipe.set("stats:total_encounters", str(limits["encounters"]))
        pipe.set("stats:avg_claim_cost", str(round(rng.uniform(500, 5000), 2)))

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

        if client.collections.exists("ClinicalCondition"):
            col = client.collections.get("ClinicalCondition")
            resp = col.aggregate.over_all(total_count=True)
            if resp.total_count > 0:
                log.info("Weaviate already populated, skipping")
                client.close()
                return

        if not client.collections.exists("ClinicalCondition"):
            client.collections.create(
                name="ClinicalCondition",
                vectorizer_config=Configure.Vectorizer.none(),
                properties=[
                    Property(name="patient_id", data_type=DataType.TEXT),
                    Property(name="code", data_type=DataType.TEXT),
                    Property(name="description", data_type=DataType.TEXT),
                ],
            )

        log.info("  Loading sentence-transformers model...")
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")

        from pymongo import MongoClient
        mongo_client = MongoClient(os.environ.get("MONGO_CLINICAL_URL", os.environ.get("MONGO_URL", "")))
        db = mongo_client[os.environ.get("MONGO_CLINICAL_DB", "clinical")]

        weaviate_limit = int(os.environ.get("WEAVIATE_LIMIT", "10000"))
        log.info(f"  Embedding {weaviate_limit:,} clinical conditions...")

        col = client.collections.get("ClinicalCondition")
        progress = ProgressTracker("Condition embeddings", weaviate_limit)
        count = 0
        batch_size = 100

        docs = list(db["conditions"].find({}, {"patient": 1, "code": 1, "description": 1}).limit(weaviate_limit))
        docs = [d for d in docs if str(d.get("description", "")).strip()]

        with col.batch.fixed_size(batch_size=batch_size) as batch:
            for i in range(0, len(docs), batch_size):
                chunk = docs[i:i + batch_size]
                texts = [str(d.get("description", ""))[:512] for d in chunk]
                embeddings = model.encode(texts, show_progress_bar=False)
                for doc, emb in zip(chunk, embeddings):
                    batch.add_object(
                        properties={
                            "patient_id": str(doc.get("patient", "")),
                            "code": str(doc.get("code", "")),
                            "description": str(doc.get("description", ""))[:200],
                        },
                        vector=emb.tolist(),
                    )
                count += len(chunk)
                progress.update(len(chunk))
        progress.finish()

        mongo_client.close()
        client.close()
