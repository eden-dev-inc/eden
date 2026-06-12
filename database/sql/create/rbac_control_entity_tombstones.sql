-- Replay guard: stores the highest delete version seen for this entity scope so
-- older replayed grants cannot resurrect access.
-- RETENTION: purged automatically by the RBAC PG sync worker based on
-- the `rbac_pg_sync.tombstone_retention_days` setting (default 90 days).
-- Set to 0 to disable. See RbacPgSyncService::maybe_purge_tombstones.
CREATE TABLE IF NOT EXISTS rbac_control_entity_tombstones (
    org_uuid UUID NOT NULL REFERENCES organizations(uuid),
    entity_kind rbac_entity_kind NOT NULL,
    entity_uuid UUID NOT NULL,
    version_ms BIGINT NOT NULL,
    version_seq BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_uuid, entity_kind, entity_uuid)
);
