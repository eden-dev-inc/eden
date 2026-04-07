-- Insurance vertical: Risk Scoring / Underwriting silo
-- Source: TheFinAI/en-forecasting-portoseguro (595K driver risk records)
-- 57 anonymized features: binary, categorical, continuous, ordinal

CREATE TABLE IF NOT EXISTS driver_risk (
    id             BIGSERIAL PRIMARY KEY,
    target         INTEGER NOT NULL,      -- 0/1: whether driver filed a claim
    -- Individual features
    ps_ind_01      INTEGER, ps_ind_02_cat INTEGER, ps_ind_03      INTEGER,
    ps_ind_04_cat  INTEGER, ps_ind_05_cat INTEGER, ps_ind_06_bin  INTEGER,
    ps_ind_07_bin  INTEGER, ps_ind_08_bin INTEGER, ps_ind_09_bin  INTEGER,
    ps_ind_10_bin  INTEGER, ps_ind_11_bin INTEGER, ps_ind_12_bin  INTEGER,
    ps_ind_13_bin  INTEGER, ps_ind_14    INTEGER, ps_ind_15      INTEGER,
    ps_ind_16_bin  INTEGER, ps_ind_17_bin INTEGER, ps_ind_18_bin  INTEGER,
    -- Registration features
    ps_reg_01      DOUBLE PRECISION, ps_reg_02 DOUBLE PRECISION,
    ps_reg_03      DOUBLE PRECISION,
    -- Car features
    ps_car_01_cat  INTEGER, ps_car_02_cat INTEGER, ps_car_03_cat  INTEGER,
    ps_car_04_cat  INTEGER, ps_car_05_cat INTEGER, ps_car_06_cat  INTEGER,
    ps_car_07_cat  INTEGER, ps_car_08_cat INTEGER, ps_car_09_cat  INTEGER,
    ps_car_10_cat  INTEGER, ps_car_11_cat INTEGER, ps_car_11     DOUBLE PRECISION,
    ps_car_12      DOUBLE PRECISION, ps_car_13 DOUBLE PRECISION,
    ps_car_14      DOUBLE PRECISION, ps_car_15 DOUBLE PRECISION,
    -- Calculated features
    ps_calc_01     DOUBLE PRECISION, ps_calc_02 DOUBLE PRECISION,
    ps_calc_03     DOUBLE PRECISION, ps_calc_04 INTEGER,
    ps_calc_05     INTEGER, ps_calc_06 INTEGER, ps_calc_07 INTEGER,
    ps_calc_08     INTEGER, ps_calc_09 INTEGER, ps_calc_10 INTEGER,
    ps_calc_11     INTEGER, ps_calc_12 INTEGER, ps_calc_13 INTEGER,
    ps_calc_14     INTEGER, ps_calc_15_bin INTEGER, ps_calc_16_bin INTEGER,
    ps_calc_17_bin INTEGER, ps_calc_18_bin INTEGER, ps_calc_19_bin INTEGER,
    ps_calc_20_bin INTEGER
);

CREATE INDEX IF NOT EXISTS idx_risk_target ON driver_risk(target);
