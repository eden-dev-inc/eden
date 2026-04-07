-- PostgreSQL schema for ADAM Demo — Tech vertical: SaaS Billing silo
-- Simulates: Finance/billing team's subscription management system

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ═══════════════════════════════════════════════════════════════
-- Organizations & Users
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS organizations (
    org_id         BIGINT PRIMARY KEY,
    org_name       VARCHAR(255) NOT NULL,
    industry       VARCHAR(64),
    size_tier      VARCHAR(16),        -- startup, smb, mid_market, enterprise
    country        VARCHAR(8),
    region         VARCHAR(64),
    created_at     TIMESTAMP NOT NULL,
    mrr            NUMERIC(12,2) DEFAULT 0,     -- monthly recurring revenue
    arr            NUMERIC(14,2) DEFAULT 0,     -- annual recurring revenue
    health_score   INTEGER DEFAULT 50           -- 0-100 customer health
);

CREATE TABLE IF NOT EXISTS users (
    user_id        BIGINT PRIMARY KEY,
    org_id         BIGINT NOT NULL,
    email          VARCHAR(255) NOT NULL,
    role           VARCHAR(32),        -- admin, developer, viewer, billing
    status         VARCHAR(16) NOT NULL DEFAULT 'active',  -- active, suspended, deactivated
    last_login     TIMESTAMP,
    created_at     TIMESTAMP NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Subscriptions & Plans
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS plans (
    plan_id        SERIAL PRIMARY KEY,
    plan_name      VARCHAR(64) NOT NULL,       -- free, starter, pro, enterprise
    monthly_price  NUMERIC(10,2) NOT NULL,
    annual_price   NUMERIC(12,2),
    api_limit      INTEGER,                    -- monthly API call limit
    seat_limit     INTEGER,                    -- max users
    storage_gb     INTEGER,
    features       TEXT[]
);

CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id BIGINT PRIMARY KEY,
    org_id          BIGINT NOT NULL,
    plan_id         INTEGER NOT NULL,
    status          VARCHAR(16) NOT NULL,      -- active, trial, past_due, cancelled, paused
    billing_cycle   VARCHAR(16) NOT NULL,      -- monthly, annual
    current_period_start TIMESTAMP,
    current_period_end   TIMESTAMP,
    trial_end      TIMESTAMP,
    cancelled_at   TIMESTAMP,
    created_at     TIMESTAMP NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Invoices & Payments
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id     BIGINT PRIMARY KEY,
    org_id         BIGINT NOT NULL,
    subscription_id BIGINT,
    amount         NUMERIC(12,2) NOT NULL,
    tax            NUMERIC(10,2) DEFAULT 0,
    total          NUMERIC(12,2) NOT NULL,
    currency       VARCHAR(8) NOT NULL DEFAULT 'USD',
    status         VARCHAR(16) NOT NULL,       -- draft, open, paid, void, uncollectible
    due_date       DATE,
    paid_at        TIMESTAMP,
    created_at     TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id     BIGINT PRIMARY KEY,
    invoice_id     BIGINT NOT NULL,
    org_id         BIGINT NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    method         VARCHAR(32) NOT NULL,       -- credit_card, wire, ach, paypal
    status         VARCHAR(16) NOT NULL,       -- succeeded, failed, refunded, pending
    failure_reason VARCHAR(128),
    created_at     TIMESTAMP NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- API Keys & Usage
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS api_keys (
    key_id         BIGINT PRIMARY KEY,
    org_id         BIGINT NOT NULL,
    user_id        BIGINT,
    key_prefix     VARCHAR(16) NOT NULL,       -- first 8 chars for identification
    name           VARCHAR(128),
    scopes         TEXT[],                     -- read, write, admin
    rate_limit     INTEGER DEFAULT 1000,       -- requests per minute
    status         VARCHAR(16) NOT NULL DEFAULT 'active',
    last_used      TIMESTAMP,
    created_at     TIMESTAMP NOT NULL,
    expires_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_usage_daily (
    usage_id       BIGSERIAL PRIMARY KEY,
    org_id         BIGINT NOT NULL,
    key_id         BIGINT,
    usage_date     DATE NOT NULL,
    total_requests BIGINT NOT NULL DEFAULT 0,
    successful     BIGINT NOT NULL DEFAULT 0,
    failed         BIGINT NOT NULL DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION,
    p99_latency_ms DOUBLE PRECISION,
    bandwidth_mb   DOUBLE PRECISION
);

-- ═══════════════════════════════════════════════════════════════
-- Indexes
-- ═══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_orgs_industry     ON organizations(industry);
CREATE INDEX IF NOT EXISTS idx_orgs_size         ON organizations(size_tier);
CREATE INDEX IF NOT EXISTS idx_orgs_country      ON organizations(country);

CREATE INDEX IF NOT EXISTS idx_users_org         ON users(org_id);
CREATE INDEX IF NOT EXISTS idx_users_role        ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_status      ON users(status);

CREATE INDEX IF NOT EXISTS idx_subs_org          ON subscriptions(org_id);
CREATE INDEX IF NOT EXISTS idx_subs_status       ON subscriptions(status);
CREATE INDEX IF NOT EXISTS idx_subs_plan         ON subscriptions(plan_id);

CREATE INDEX IF NOT EXISTS idx_invoices_org      ON invoices(org_id);
CREATE INDEX IF NOT EXISTS idx_invoices_status   ON invoices(status);
CREATE INDEX IF NOT EXISTS idx_invoices_created  ON invoices(created_at);

CREATE INDEX IF NOT EXISTS idx_payments_invoice  ON payments(invoice_id);
CREATE INDEX IF NOT EXISTS idx_payments_org      ON payments(org_id);
CREATE INDEX IF NOT EXISTS idx_payments_status   ON payments(status);

CREATE INDEX IF NOT EXISTS idx_apikeys_org       ON api_keys(org_id);
CREATE INDEX IF NOT EXISTS idx_apikeys_status    ON api_keys(status);

CREATE INDEX IF NOT EXISTS idx_usage_org         ON api_usage_daily(org_id);
CREATE INDEX IF NOT EXISTS idx_usage_date        ON api_usage_daily(usage_date);
CREATE INDEX IF NOT EXISTS idx_usage_key         ON api_usage_daily(key_id);
