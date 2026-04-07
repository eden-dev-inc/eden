-- ClickHouse schema for ADAM Demo — Tech vertical: User Behavior Analytics
-- Source: ecommerce-behavior-data (~285M events)
-- Simulates: Product analytics team's event pipeline

CREATE DATABASE IF NOT EXISTS analytics;

-- Raw user behavior events (view, cart, purchase)
-- org_id is derived from user_id to enable cross-DB joins with SaaS billing
CREATE TABLE IF NOT EXISTS analytics.user_events (
    event_time     DateTime,
    event_type     String,             -- view, cart, remove_from_cart, purchase
    product_id     UInt64,
    category_id    UInt64,
    category_code  String,
    brand          String,
    price          Float64,
    user_id        UInt64,
    user_session   String,
    event_day      UInt32,
    org_id         UInt32 DEFAULT 0    -- links to pg_saas_billing.organizations
) ENGINE = MergeTree()
ORDER BY (event_day, event_type, user_id)
PARTITION BY intDiv(event_day, 30);

-- Session aggregates
CREATE TABLE IF NOT EXISTS analytics.session_summary (
    user_session   String,
    user_id        UInt64,
    event_day      UInt32,
    session_start  DateTime,
    session_end    DateTime,
    page_views     UInt32,
    cart_adds      UInt32,
    purchases      UInt32,
    total_revenue  Float64,
    unique_products UInt32,
    unique_categories UInt32
) ENGINE = SummingMergeTree()
ORDER BY (event_day, user_id, user_session);

-- Conversion funnel daily
CREATE TABLE IF NOT EXISTS analytics.funnel_daily (
    event_day      UInt32,
    category_code  String,
    views          UInt64,
    carts          UInt64,
    purchases      UInt64,
    revenue        Float64,
    unique_viewers UInt64,
    unique_buyers  UInt64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, category_code);

-- Product performance
CREATE TABLE IF NOT EXISTS analytics.product_metrics (
    product_id     UInt64,
    brand          String,
    category_code  String,
    event_day      UInt32,
    views          UInt64,
    cart_adds      UInt64,
    purchases      UInt64,
    revenue        Float64
) ENGINE = SummingMergeTree()
ORDER BY (event_day, product_id);
