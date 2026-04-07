-- PostgreSQL schema for ADAM Demo e-commerce data
-- Combines T-ECD dataset tables with synthetic e-commerce tables
-- All tables share user_id / brand_id for cross-database queries

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ═══════════════════════════════════════════════════════════════
-- Core Reference Tables
-- ═══════════════════════════════════════════════════════════════

-- Shared user catalog (extended with profile fields for synthetic data)
CREATE TABLE IF NOT EXISTS users (
    user_id        BIGINT PRIMARY KEY,
    first_name     VARCHAR(64),
    last_name      VARCHAR(64),
    email          VARCHAR(255),
    loyalty_tier   VARCHAR(16),
    region         INTEGER,
    socdem_cluster INTEGER,
    created_at     TIMESTAMP
);

-- Shared brand catalog (extended with brand name)
CREATE TABLE IF NOT EXISTS brands (
    brand_id       BIGINT PRIMARY KEY,
    brand_name     VARCHAR(128)
);

-- Product categories
CREATE TABLE IF NOT EXISTS categories (
    category_id      SERIAL PRIMARY KEY,
    category_name    VARCHAR(128) NOT NULL,
    subcategory_name VARCHAR(128) NOT NULL,
    UNIQUE(category_name, subcategory_name)
);

-- ═══════════════════════════════════════════════════════════════
-- T-ECD Marketplace Tables (from HuggingFace dataset)
-- ═══════════════════════════════════════════════════════════════

-- Marketplace item catalog (from marketplace/items.pq)
CREATE TABLE IF NOT EXISTS marketplace_items (
    item_id       VARCHAR(32) PRIMARY KEY,
    brand_id      BIGINT,
    category      VARCHAR(255),
    subcategory   VARCHAR(255),
    price         DOUBLE PRECISION
);

-- Marketplace events (from marketplace/events — OLTP transactional data)
CREATE TABLE IF NOT EXISTS marketplace_events (
    event_id      BIGSERIAL PRIMARY KEY,
    user_id       BIGINT,
    item_id       VARCHAR(32),
    action_type   VARCHAR(32) NOT NULL,
    subdomain     VARCHAR(64),
    os            VARCHAR(32),
    event_day     INTEGER NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Synthetic E-Commerce Tables (generated data)
-- ═══════════════════════════════════════════════════════════════

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    order_id       BIGINT PRIMARY KEY,
    user_id        BIGINT NOT NULL,
    status         VARCHAR(32) NOT NULL,
    subtotal       NUMERIC(12,2) NOT NULL,
    tax            NUMERIC(12,2) NOT NULL,
    shipping_cost  NUMERIC(12,2) NOT NULL DEFAULT 0,
    total          NUMERIC(12,2) NOT NULL,
    coupon_id      INTEGER,
    created_at     TIMESTAMP NOT NULL,
    country        VARCHAR(8),
    region         VARCHAR(64)
);

-- Order line items
CREATE TABLE IF NOT EXISTS order_items (
    item_id        BIGINT PRIMARY KEY,
    order_id       BIGINT NOT NULL,
    product_id     BIGINT NOT NULL,
    quantity       INTEGER NOT NULL,
    unit_price     NUMERIC(12,2) NOT NULL,
    line_total     NUMERIC(12,2) NOT NULL
);

-- Invoices (1:1 with orders)
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id     BIGINT PRIMARY KEY,
    order_id       BIGINT NOT NULL,
    user_id        BIGINT NOT NULL,
    total          NUMERIC(12,2) NOT NULL,
    tax            NUMERIC(12,2) NOT NULL,
    status         VARCHAR(32) NOT NULL,
    issued_at      TIMESTAMP NOT NULL,
    due_at         TIMESTAMP NOT NULL
);

-- Payments
CREATE TABLE IF NOT EXISTS payments (
    payment_id     BIGINT PRIMARY KEY,
    order_id       BIGINT NOT NULL,
    user_id        BIGINT NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    method         VARCHAR(32) NOT NULL,
    status         VARCHAR(32) NOT NULL,
    transaction_ref VARCHAR(64),
    paid_at        TIMESTAMP
);

-- Coupons / Promotions
CREATE TABLE IF NOT EXISTS coupons (
    coupon_id      SERIAL PRIMARY KEY,
    code           VARCHAR(16) UNIQUE NOT NULL,
    coupon_type    VARCHAR(32) NOT NULL,
    discount_value NUMERIC(10,2) NOT NULL DEFAULT 0,
    min_purchase   NUMERIC(10,2) NOT NULL DEFAULT 0,
    max_uses       INTEGER NOT NULL DEFAULT 0,
    used_count     INTEGER NOT NULL DEFAULT 0,
    start_date     DATE,
    end_date       DATE,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

-- ═══════════════════════════════════════════════════════════════
-- Indexes
-- ═══════════════════════════════════════════════════════════════

-- Marketplace indexes
CREATE INDEX IF NOT EXISTS idx_events_user       ON marketplace_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_item       ON marketplace_events(item_id);
CREATE INDEX IF NOT EXISTS idx_events_action     ON marketplace_events(action_type);
CREATE INDEX IF NOT EXISTS idx_events_day        ON marketplace_events(event_day);
CREATE INDEX IF NOT EXISTS idx_events_subdomain  ON marketplace_events(subdomain);
CREATE INDEX IF NOT EXISTS idx_items_brand       ON marketplace_items(brand_id);
CREATE INDEX IF NOT EXISTS idx_items_category    ON marketplace_items(category);

-- User indexes
CREATE INDEX IF NOT EXISTS idx_users_region      ON users(region);
CREATE INDEX IF NOT EXISTS idx_users_email       ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_loyalty     ON users(loyalty_tier);

-- Order indexes
CREATE INDEX IF NOT EXISTS idx_orders_user       ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created    ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_country    ON orders(country);
CREATE INDEX IF NOT EXISTS idx_order_items_order  ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);

-- Invoice & payment indexes
CREATE INDEX IF NOT EXISTS idx_invoices_order    ON invoices(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_user     ON invoices(user_id);
CREATE INDEX IF NOT EXISTS idx_invoices_status   ON invoices(status);
CREATE INDEX IF NOT EXISTS idx_payments_order    ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_user     ON payments(user_id);
CREATE INDEX IF NOT EXISTS idx_payments_method   ON payments(method);
CREATE INDEX IF NOT EXISTS idx_payments_status   ON payments(status);
