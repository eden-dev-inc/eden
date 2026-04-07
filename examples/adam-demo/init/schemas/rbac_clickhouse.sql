-- RBAC roles for ADAM Demo (ClickHouse)
-- Reader: SELECT-only access
-- Writer: SELECT + INSERT access
-- Admin: eden user (already has full access)

CREATE USER IF NOT EXISTS reader IDENTIFIED BY 'reader_pass' SETTINGS PROFILE 'default';
CREATE USER IF NOT EXISTS writer IDENTIFIED BY 'writer_pass' SETTINGS PROFILE 'default';

-- Reader role
CREATE ROLE IF NOT EXISTS reader_role;
GRANT SELECT ON analytics.* TO reader_role;
GRANT reader_role TO reader;

-- Writer role
CREATE ROLE IF NOT EXISTS writer_role;
GRANT SELECT, INSERT ON analytics.* TO writer_role;
GRANT writer_role TO writer;

-- Admin (eden user already has full access)
