-- PostgreSQL schema for ADAM Demo -- Finance vertical
-- Banking/financial services: accounts, transactions, loans, compliance

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ═══════════════════════════════════════════════════════════════
-- Core Reference Tables
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS customers (
    customer_id    BIGINT PRIMARY KEY,
    first_name     VARCHAR(64),
    last_name      VARCHAR(64),
    email          VARCHAR(255),
    phone          VARCHAR(32),
    risk_tier      VARCHAR(16),       -- low, medium, high, critical
    kyc_status     VARCHAR(16),       -- pending, verified, rejected, expired
    segment        VARCHAR(32),       -- retail, premium, private, corporate
    region         VARCHAR(64),
    country        VARCHAR(8),
    created_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS branches (
    branch_id      SERIAL PRIMARY KEY,
    branch_name    VARCHAR(128) NOT NULL,
    city           VARCHAR(64),
    country        VARCHAR(8),
    branch_type    VARCHAR(32)        -- retail, corporate, digital, atm
);

-- ═══════════════════════════════════════════════════════════════
-- Accounts & Transactions
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS accounts (
    account_id     BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    account_type   VARCHAR(32) NOT NULL,  -- checking, savings, investment, credit_card, mortgage
    currency       VARCHAR(8) NOT NULL DEFAULT 'USD',
    balance        NUMERIC(14,2) NOT NULL DEFAULT 0,
    credit_limit   NUMERIC(14,2),
    interest_rate  NUMERIC(6,4),
    status         VARCHAR(16) NOT NULL DEFAULT 'active',  -- active, frozen, closed, dormant
    opened_at      TIMESTAMP NOT NULL,
    branch_id      INTEGER
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id         BIGINT PRIMARY KEY,
    account_id     BIGINT NOT NULL,
    customer_id    BIGINT NOT NULL,
    amount         NUMERIC(14,2) NOT NULL,
    txn_type       VARCHAR(32) NOT NULL,   -- deposit, withdrawal, transfer, payment, fee, interest
    category       VARCHAR(64),            -- groceries, utilities, salary, rent, entertainment, etc.
    merchant       VARCHAR(128),
    status         VARCHAR(16) NOT NULL,   -- completed, pending, failed, reversed
    channel        VARCHAR(16),            -- mobile, web, branch, atm, pos
    fraud_score    NUMERIC(5,2),           -- 0.00 to 100.00
    created_at     TIMESTAMP NOT NULL,
    country        VARCHAR(8)
);

-- ═══════════════════════════════════════════════════════════════
-- Loans & Credit
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS loans (
    loan_id        BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    loan_type      VARCHAR(32) NOT NULL,   -- personal, mortgage, auto, business, student
    principal      NUMERIC(14,2) NOT NULL,
    interest_rate  NUMERIC(6,4) NOT NULL,
    term_months    INTEGER NOT NULL,
    monthly_payment NUMERIC(12,2),
    remaining_balance NUMERIC(14,2),
    status         VARCHAR(16) NOT NULL,   -- active, paid_off, defaulted, delinquent
    originated_at  TIMESTAMP NOT NULL,
    collateral_type VARCHAR(32)            -- property, vehicle, securities, unsecured
);

CREATE TABLE IF NOT EXISTS credit_applications (
    app_id         BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    product_type   VARCHAR(32) NOT NULL,   -- credit_card, personal_loan, mortgage, auto_loan
    requested_amount NUMERIC(14,2) NOT NULL,
    credit_score   INTEGER,
    annual_income  NUMERIC(14,2),
    debt_to_income NUMERIC(5,2),
    decision       VARCHAR(16) NOT NULL,   -- approved, denied, pending, manual_review
    decision_reason VARCHAR(128),
    applied_at     TIMESTAMP NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Wire Transfers & Compliance
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS wire_transfers (
    transfer_id    BIGINT PRIMARY KEY,
    from_account   BIGINT NOT NULL,
    to_account     BIGINT,
    customer_id    BIGINT NOT NULL,
    amount         NUMERIC(14,2) NOT NULL,
    currency       VARCHAR(8) NOT NULL,
    dest_country   VARCHAR(8),
    swift_code     VARCHAR(16),
    status         VARCHAR(16) NOT NULL,   -- completed, pending, flagged, rejected
    initiated_at   TIMESTAMP NOT NULL,
    completed_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id       BIGINT PRIMARY KEY,
    txn_id         BIGINT,
    customer_id    BIGINT NOT NULL,
    alert_type     VARCHAR(32) NOT NULL,   -- suspicious_amount, velocity, geo_anomaly, pattern
    severity       VARCHAR(16) NOT NULL,   -- low, medium, high, critical
    status         VARCHAR(16) NOT NULL,   -- open, investigating, resolved, false_positive
    details        TEXT,
    created_at     TIMESTAMP NOT NULL,
    resolved_at    TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════
-- Indexes
-- ═══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_customers_risk      ON customers(risk_tier);
CREATE INDEX IF NOT EXISTS idx_customers_segment   ON customers(segment);
CREATE INDEX IF NOT EXISTS idx_customers_kyc       ON customers(kyc_status);
CREATE INDEX IF NOT EXISTS idx_customers_region    ON customers(region);

CREATE INDEX IF NOT EXISTS idx_accounts_customer   ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_accounts_type       ON accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_accounts_status     ON accounts(status);

CREATE INDEX IF NOT EXISTS idx_txn_account         ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_txn_customer        ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_type            ON transactions(txn_type);
CREATE INDEX IF NOT EXISTS idx_txn_status          ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_txn_created         ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_txn_category        ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_txn_fraud           ON transactions(fraud_score);

CREATE INDEX IF NOT EXISTS idx_loans_customer      ON loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_loans_status        ON loans(status);
CREATE INDEX IF NOT EXISTS idx_loans_type          ON loans(loan_type);

CREATE INDEX IF NOT EXISTS idx_credit_app_customer ON credit_applications(customer_id);
CREATE INDEX IF NOT EXISTS idx_credit_app_decision ON credit_applications(decision);

CREATE INDEX IF NOT EXISTS idx_wire_customer       ON wire_transfers(customer_id);
CREATE INDEX IF NOT EXISTS idx_wire_status         ON wire_transfers(status);
CREATE INDEX IF NOT EXISTS idx_wire_dest           ON wire_transfers(dest_country);

CREATE INDEX IF NOT EXISTS idx_fraud_customer      ON fraud_alerts(customer_id);
CREATE INDEX IF NOT EXISTS idx_fraud_severity      ON fraud_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_fraud_status        ON fraud_alerts(status);
