-- Healthcare vertical: Billing silo
-- Source: richardyoung/synthea-575k-patients (claims/payer data)

CREATE TABLE IF NOT EXISTS claims (
    id             VARCHAR(64) PRIMARY KEY,
    patient_id     VARCHAR(64) NOT NULL,
    provider_id    VARCHAR(64),
    encounter_id   VARCHAR(64),
    diagnosis_code VARCHAR(32),
    procedure_code VARCHAR(32),
    total_cost     DOUBLE PRECISION,
    payer_coverage DOUBLE PRECISION,
    patient_responsibility DOUBLE PRECISION,
    status         VARCHAR(16) NOT NULL DEFAULT 'submitted',
    submitted_at   TIMESTAMP,
    processed_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payers (
    id             VARCHAR(64) PRIMARY KEY,
    name           VARCHAR(128) NOT NULL,
    ownership      VARCHAR(32),
    state_headquartered VARCHAR(8),
    revenue        DOUBLE PRECISION,
    covered_encounters BIGINT,
    uncovered_encounters BIGINT,
    covered_medications BIGINT,
    uncovered_medications BIGINT
);

CREATE INDEX IF NOT EXISTS idx_claims_patient   ON claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_claims_encounter ON claims(encounter_id);
CREATE INDEX IF NOT EXISTS idx_claims_status    ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_diag      ON claims(diagnosis_code);
CREATE INDEX IF NOT EXISTS idx_claims_cost      ON claims(total_cost);
