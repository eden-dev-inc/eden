-- Replay guard: stores the highest delete version seen for this endpoint scope
-- so older replayed grants cannot resurrect access.
-- RETENTION: purged automatically by the RBAC PG sync worker based on
-- the `rbac_pg_sync.tombstone_retention_days` setting (default 90 days).
-- Set to 0 to disable. See RbacPgSyncService::maybe_purge_tombstones.
CREATE TABLE IF NOT EXISTS rbac_data_entity_tombstones (
    org_uuid UUID NOT NULL REFERENCES organizations(uuid),
    endpoint_uuid UUID NOT NULL,
    version_ms BIGINT NOT NULL,
    version_seq BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_uuid, endpoint_uuid)
);
