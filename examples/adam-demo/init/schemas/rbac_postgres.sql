-- RBAC roles for ADAM Demo (Postgres)
-- Reader: SELECT-only access
-- Writer: SELECT + INSERT + UPDATE + DELETE
-- Admin: eden user (already superuser)
--
-- This script is database-agnostic and runs after schema creation.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'reader') THEN
        CREATE ROLE reader WITH LOGIN PASSWORD 'reader_pass';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'writer') THEN
        CREATE ROLE writer WITH LOGIN PASSWORD 'writer_pass';
    END IF;
END
$$;

-- Grant CONNECT on the current database
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO reader', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO writer', current_database());
END
$$;

-- Reader: SELECT only
GRANT USAGE ON SCHEMA public TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;

-- Writer: SELECT + DML
GRANT USAGE ON SCHEMA public TO writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO writer;
