-- RETENTION: purged automatically by the RBAC PG sync worker based on
-- the `rbac_pg_sync.tombstone_retention_days` setting (default 90 days).
-- Set to 0 to disable. See RbacPgSyncService::maybe_purge_tombstones.
CREATE TABLE IF NOT EXISTS rbac_subject_tombstones (
    org_uuid UUID NOT NULL REFERENCES organizations(uuid),
    subject_kind VARCHAR(32) NOT NULL,
    subject_uuid UUID NOT NULL,
    version_ms BIGINT NOT NULL,
    version_seq BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_uuid, subject_kind, subject_uuid)
);
