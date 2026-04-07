-- Healthcare vertical: Patient Records silo (EHR)
-- Source: richardyoung/synthea-575k-patients

CREATE TABLE IF NOT EXISTS patients (
    id             VARCHAR(64) PRIMARY KEY,
    birthdate      DATE,
    deathdate      DATE,
    ssn            VARCHAR(16),
    prefix         VARCHAR(8),
    first_name     VARCHAR(64),
    last_name      VARCHAR(64),
    suffix         VARCHAR(8),
    maiden         VARCHAR(64),
    marital        VARCHAR(8),
    race           VARCHAR(32),
    ethnicity      VARCHAR(32),
    gender         VARCHAR(2),
    birthplace     VARCHAR(128),
    address        VARCHAR(255),
    city           VARCHAR(64),
    state          VARCHAR(32),
    county         VARCHAR(64),
    zip            VARCHAR(16),
    lat            DOUBLE PRECISION,
    lon            DOUBLE PRECISION,
    healthcare_expenses DOUBLE PRECISION,
    healthcare_coverage DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS encounters (
    id             VARCHAR(64) PRIMARY KEY,
    start_ts       TIMESTAMP,
    stop_ts        TIMESTAMP,
    patient        VARCHAR(64) NOT NULL,
    organization   VARCHAR(64),
    provider       VARCHAR(64),
    payer          VARCHAR(64),
    encounterclass VARCHAR(32),
    code           VARCHAR(32),
    description    TEXT,
    base_encounter_cost DOUBLE PRECISION,
    total_claim_cost    DOUBLE PRECISION,
    payer_coverage      DOUBLE PRECISION,
    reasoncode     VARCHAR(32),
    reasondescription TEXT
);

CREATE INDEX IF NOT EXISTS idx_patients_gender    ON patients(gender);
CREATE INDEX IF NOT EXISTS idx_patients_race      ON patients(race);
CREATE INDEX IF NOT EXISTS idx_patients_state     ON patients(state);
CREATE INDEX IF NOT EXISTS idx_patients_city      ON patients(city);

CREATE INDEX IF NOT EXISTS idx_encounters_patient ON encounters(patient);
CREATE INDEX IF NOT EXISTS idx_encounters_class   ON encounters(encounterclass);
CREATE INDEX IF NOT EXISTS idx_encounters_code    ON encounters(code);
CREATE INDEX IF NOT EXISTS idx_encounters_payer   ON encounters(payer);
CREATE INDEX IF NOT EXISTS idx_encounters_start   ON encounters(start_ts);

-- Mapping table: links Synthea patient IDs to CMS DE-SynPUF beneficiary IDs
-- Enables cross-silo queries between pg_patient_records (Synthea EHR) and pg_cms_claims (CMS Medicare)
CREATE TABLE IF NOT EXISTS patient_id_mapping (
    synthea_patient_id   VARCHAR(64) NOT NULL,
    cms_desynpuf_id      VARCHAR(16) NOT NULL,
    PRIMARY KEY (synthea_patient_id, cms_desynpuf_id)
);

CREATE INDEX IF NOT EXISTS idx_mapping_synthea ON patient_id_mapping(synthea_patient_id);
CREATE INDEX IF NOT EXISTS idx_mapping_cms ON patient_id_mapping(cms_desynpuf_id);
