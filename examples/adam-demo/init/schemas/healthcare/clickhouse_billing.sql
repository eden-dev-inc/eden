-- Healthcare vertical: Billing Analytics silo
-- Encounter-level cost analytics from Synthea data

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.encounter_events (
    encounter_id   String,
    patient_id     String,
    encounterclass String,
    code           String,
    description    String,
    payer          String,
    total_claim_cost Float64,
    payer_coverage   Float64,
    patient_cost     Float64,
    event_day      UInt32,
    state          String,
    gender         String,
    race           String
) ENGINE = MergeTree()
ORDER BY (event_day, encounterclass, patient_id)
PARTITION BY intDiv(event_day, 30);

CREATE TABLE IF NOT EXISTS analytics.daily_cost_summary (
    event_day      UInt32,
    encounterclass String,
    payer          String,
    encounter_count UInt64,
    total_cost     Float64,
    total_coverage Float64,
    unique_patients UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, encounterclass, payer);
