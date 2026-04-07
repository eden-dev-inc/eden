-- Insurance vertical: Policy Admin silo
-- Source: mabilton/fremtpl2 (French Motor Third-Party Liability)

-- Policy/frequency table (678K policies)
CREATE TABLE IF NOT EXISTS policies (
    id_pol         BIGINT PRIMARY KEY,
    claim_nb       INTEGER NOT NULL DEFAULT 0,
    exposure       DOUBLE PRECISION,
    area           VARCHAR(8),
    veh_power      INTEGER,
    veh_age        INTEGER,
    driv_age       INTEGER,
    bonus_malus    INTEGER,
    veh_brand      VARCHAR(16),
    veh_gas        VARCHAR(16),
    density        INTEGER,
    region         VARCHAR(32),
    us_state       VARCHAR(2)
);

-- Claims/severity table (26K claims)
CREATE TABLE IF NOT EXISTS claims (
    claim_id       BIGSERIAL PRIMARY KEY,
    id_pol         BIGINT NOT NULL,
    claim_amount   DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pol_area      ON policies(area);
CREATE INDEX IF NOT EXISTS idx_pol_brand     ON policies(veh_brand);
CREATE INDEX IF NOT EXISTS idx_pol_region    ON policies(region);
CREATE INDEX IF NOT EXISTS idx_pol_usstate   ON policies(us_state);
CREATE INDEX IF NOT EXISTS idx_pol_claimnb   ON policies(claim_nb);
CREATE INDEX IF NOT EXISTS idx_pol_bonmal    ON policies(bonus_malus);
CREATE INDEX IF NOT EXISTS idx_pol_drivage   ON policies(driv_age);

CREATE INDEX IF NOT EXISTS idx_claims_pol    ON claims(id_pol);
CREATE INDEX IF NOT EXISTS idx_claims_amt    ON claims(claim_amount);
