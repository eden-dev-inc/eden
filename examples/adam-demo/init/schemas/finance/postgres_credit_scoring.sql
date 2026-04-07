-- Finance vertical: Credit Scoring silo
-- Source: pointe77/credit-card-transaction (1.85M Sparkov credit card txns)

CREATE TABLE IF NOT EXISTS credit_transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    trans_date_time     TIMESTAMP,
    cc_num              BIGINT,
    merchant            VARCHAR(128),
    category            VARCHAR(64),
    amt                 DOUBLE PRECISION NOT NULL,
    first_name          VARCHAR(64),
    last_name           VARCHAR(64),
    gender              VARCHAR(1),
    street              VARCHAR(255),
    city                VARCHAR(128),
    state               VARCHAR(64),
    zip                 VARCHAR(16),
    lat                 DOUBLE PRECISION,
    long                DOUBLE PRECISION,
    city_pop            INTEGER,
    job                 VARCHAR(128),
    dob                 DATE,
    trans_num           VARCHAR(64),
    unix_time           BIGINT,
    merch_lat           DOUBLE PRECISION,
    merch_long          DOUBLE PRECISION,
    is_fraud            INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cc_merchant   ON credit_transactions(merchant);
CREATE INDEX IF NOT EXISTS idx_cc_category   ON credit_transactions(category);
CREATE INDEX IF NOT EXISTS idx_cc_fraud      ON credit_transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_cc_amt        ON credit_transactions(amt);
CREATE INDEX IF NOT EXISTS idx_cc_state      ON credit_transactions(state);
CREATE INDEX IF NOT EXISTS idx_cc_city       ON credit_transactions(city);
CREATE INDEX IF NOT EXISTS idx_cc_date       ON credit_transactions(trans_date_time);
CREATE INDEX IF NOT EXISTS idx_cc_ccnum      ON credit_transactions(cc_num);
