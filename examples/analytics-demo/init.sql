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

-- Create a simple monitoring view for later use
CREATE OR REPLACE VIEW pg_stat_activity_summary AS
SELECT
    state,
    COUNT(*) as connection_count,
    AVG(EXTRACT(EPOCH FROM (now() - state_change))) as avg_state_duration
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
GROUP BY state;