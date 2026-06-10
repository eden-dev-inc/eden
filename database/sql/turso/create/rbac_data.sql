CREATE TABLE IF NOT EXISTS rbac_data (
    org_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    endpoint_uuid TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_uuid TEXT NOT NULL,
    perms TEXT NOT NULL DEFAULT '',
    is_active INTEGER NOT NULL DEFAULT 1,
    version_ms INTEGER NOT NULL,
    version_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (org_uuid, endpoint_uuid, subject_kind, subject_uuid)
);
CREATE INDEX IF NOT EXISTS idx_rbac_data_subject
    ON rbac_data (org_uuid, subject_kind, subject_uuid);
