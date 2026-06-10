CREATE TABLE IF NOT EXISTS els_policy_assignments (
    org_uuid TEXT NOT NULL,
    endpoint_uuid TEXT NOT NULL,
    user_uuid TEXT NOT NULL,
    policy_uuid TEXT NOT NULL REFERENCES els_policies(uuid) ON DELETE CASCADE,
    mode TEXT NOT NULL DEFAULT 'sync',
    strategy_snapshot TEXT,
    config_snapshot TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (endpoint_uuid, user_uuid),
    CONSTRAINT els_assignment_mode_check CHECK (mode IN ('sync', 'copy'))
);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_org ON els_policy_assignments (org_uuid);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_policy ON els_policy_assignments (policy_uuid);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_sync_policy ON els_policy_assignments (policy_uuid) WHERE mode = 'sync';
