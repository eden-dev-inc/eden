-- RETENTION: purged automatically by the RBAC PG sync worker based on
-- the `rbac_pg_sync.tombstone_retention_days` setting (default 90 days).
-- Set to 0 to disable. See RbacPgSyncService::maybe_purge_tombstones.
CREATE TABLE IF NOT EXISTS rbac_control_row_tombstones (
    org_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    entity_kind TEXT NOT NULL,
    entity_uuid TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_uuid TEXT NOT NULL,
    version_ms INTEGER NOT NULL,
    version_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid)
);
