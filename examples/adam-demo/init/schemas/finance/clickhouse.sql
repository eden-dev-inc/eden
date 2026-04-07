-- ClickHouse schema for ADAM Demo -- Finance vertical
-- Transaction analytics, fraud detection, risk metrics

CREATE DATABASE IF NOT EXISTS analytics;

-- Transaction events (denormalized for analytics)
CREATE TABLE IF NOT EXISTS analytics.transaction_events (
    txn_id        UInt64,
    account_id    UInt64,
    customer_id   UInt64,
    amount        Float64,
    txn_type      String,
    category      String,
    merchant      String,
    channel       String,
    fraud_score   Float32,
    country       String,
    event_day     UInt32,
    event_hour    UInt8
) ENGINE = MergeTree()
ORDER BY (event_day, txn_type, customer_id)
PARTITION BY intDiv(event_day, 30);

-- Fraud detection events
CREATE TABLE IF NOT EXISTS analytics.fraud_events (
    alert_id      UInt64,
    txn_id        UInt64,
    customer_id   UInt64,
    alert_type    String,
    severity      String,
    fraud_score   Float32,
    amount        Float64,
    event_day     UInt32
) ENGINE = MergeTree()
ORDER BY (event_day, severity, customer_id)
PARTITION BY intDiv(event_day, 30);

-- Daily transaction summary by type and channel
CREATE TABLE IF NOT EXISTS analytics.daily_txn_summary (
    event_day     UInt32,
    txn_type      String,
    channel       String,
    txn_count     UInt64,
    total_amount  Float64,
    avg_amount    Float64,
    unique_customers UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, txn_type, channel);

-- Daily risk metrics
CREATE TABLE IF NOT EXISTS analytics.risk_metrics_daily (
    event_day     UInt32,
    risk_tier     String,
    segment       String,
    total_exposure Float64,
    avg_fraud_score Float32,
    alert_count   UInt64,
    flagged_amount Float64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, risk_tier, segment);

-- Loan performance analytics
CREATE TABLE IF NOT EXISTS analytics.loan_events (
    loan_id       UInt64,
    customer_id   UInt64,
    loan_type     String,
    principal     Float64,
    interest_rate Float32,
    status        String,
    event_day     UInt32,
    days_delinquent UInt32
) ENGINE = MergeTree()
ORDER BY (event_day, loan_type, status)
PARTITION BY intDiv(event_day, 30);
