-- ClickHouse schema for ADAM Demo analytics
-- Combines T-ECD marketplace analytics with synthetic e-commerce OLAP tables
-- All tables share user_id for cross-database queries

CREATE DATABASE IF NOT EXISTS analytics;

-- ═══════════════════════════════════════════════════════════════
-- T-ECD Marketplace Analytics
-- ═══════════════════════════════════════════════════════════════

-- Marketplace events (replicated from the same T-ECD marketplace data for analytics)
CREATE TABLE IF NOT EXISTS analytics.marketplace_events (
    user_id       UInt64,
    item_id       String,
    action_type   String,
    subdomain     String,
    os            String,
    event_day     UInt32
) ENGINE = MergeTree()
ORDER BY (event_day, action_type, user_id)
PARTITION BY intDiv(event_day, 10);

-- Pre-aggregated daily summary by action type
CREATE TABLE IF NOT EXISTS analytics.daily_action_summary (
    event_day     UInt32,
    action_type   String,
    subdomain     String,
    event_count   UInt64,
    unique_users  UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, action_type, subdomain);

-- ═══════════════════════════════════════════════════════════════
-- Synthetic E-Commerce Analytics
-- ═══════════════════════════════════════════════════════════════

-- Clickstream events (page views, product views, add-to-cart, etc.)
CREATE TABLE IF NOT EXISTS analytics.clickstream_events (
    event_id      UInt64,
    user_id       UInt64,
    product_id    UInt64,
    event_type    String,
    device_type   String,
    browser       String,
    os            String,
    country       String,
    region        String,
    session_id    String,
    referrer      String,
    event_day     UInt32,
    event_hour    UInt8
) ENGINE = MergeTree()
ORDER BY (event_day, event_type, user_id)
PARTITION BY intDiv(event_day, 30);

-- Purchase events (denormalized order data for analytics)
CREATE TABLE IF NOT EXISTS analytics.purchase_events (
    order_id      UInt64,
    user_id       UInt64,
    product_id    UInt64,
    quantity      UInt32,
    unit_price    Float64,
    total         Float64,
    payment_method String,
    country       String,
    region        String,
    brand_id      UInt64,
    category      String,
    event_day     UInt32
) ENGINE = MergeTree()
ORDER BY (event_day, category, user_id)
PARTITION BY intDiv(event_day, 30);

-- Pre-aggregated daily revenue by category and country
CREATE TABLE IF NOT EXISTS analytics.revenue_daily (
    event_day     UInt32,
    category      String,
    country       String,
    order_count   UInt64,
    revenue       Float64,
    unique_buyers UInt64,
    units_sold    UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, category, country);

-- Conversion funnel aggregates
CREATE TABLE IF NOT EXISTS analytics.funnel_events (
    event_day     UInt32,
    step          String,
    device_type   String,
    country       String,
    event_count   UInt64,
    unique_users  UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, step, device_type);
