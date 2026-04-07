-- PostgreSQL initialization script for analytics demo
-- This runs when the container starts for the first time

-- Enable basic extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Set basic performance configurations (removed pg_stat_statements for now)
ALTER SYSTEM SET log_statement = 'none';
ALTER SYSTEM SET log_min_duration_statement = 1000;

-- Basic connection and memory settings for better performance
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';

-- Reload configuration
SELECT pg_reload_conf();

-- ============================================================================
-- ENUM types
-- These are defined here so they exist before the Rust app's setup_schema()
-- creates the tables that reference them. Each is wrapped in a DO block with
-- exception handling so the script remains idempotent.
-- ============================================================================

DO $$ BEGIN
    CREATE TYPE device_type AS ENUM ('desktop', 'mobile', 'tablet', 'bot', 'unknown');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE campaign_status AS ENUM ('draft', 'active', 'paused', 'completed', 'archived');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE experiment_status AS ENUM ('draft', 'running', 'paused', 'concluded');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE goal_type AS ENUM ('page_view', 'event', 'duration', 'pages_per_session', 'revenue');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE order_status AS ENUM ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE cart_status AS ENUM ('active', 'converted', 'abandoned', 'expired');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE discount_type AS ENUM ('percentage', 'fixed_amount', 'buy_x_get_y', 'free_shipping');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE payment_status AS ENUM ('pending', 'processing', 'completed', 'failed', 'refunded', 'disputed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE payment_method AS ENUM ('credit_card', 'debit_card', 'bank_transfer', 'paypal', 'stripe', 'crypto', 'invoice');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE subscription_status AS ENUM ('trialing', 'active', 'past_due', 'cancelled', 'expired', 'paused');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE invoice_status AS ENUM ('draft', 'sent', 'paid', 'overdue', 'void', 'disputed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE ledger_entry_type AS ENUM ('debit', 'credit');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE refund_status AS ENUM ('requested', 'processing', 'approved', 'completed', 'rejected');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create a simple monitoring view for later use
CREATE OR REPLACE VIEW pg_stat_activity_summary AS
SELECT
    state,
    COUNT(*) as connection_count,
    AVG(EXTRACT(EPOCH FROM (now() - state_change))) as avg_state_duration
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
GROUP BY state;
