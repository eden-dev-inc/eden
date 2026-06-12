CREATE TABLE IF NOT EXISTS els_policy_versions (
    policy_uuid TEXT NOT NULL REFERENCES els_policies(uuid) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'draft',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (policy_uuid, version),
    CONSTRAINT els_version_status_check CHECK (status IN ('draft', 'active', 'superseded', 'rejected'))
);
CREATE INDEX IF NOT EXISTS idx_els_policy_versions_active
    ON els_policy_versions (policy_uuid);
