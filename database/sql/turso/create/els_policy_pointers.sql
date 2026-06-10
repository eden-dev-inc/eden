CREATE TABLE IF NOT EXISTS els_policy_pointers (
    policy_uuid TEXT PRIMARY KEY REFERENCES els_policies(uuid) ON DELETE CASCADE,
    active_version INTEGER,
    activated_by TEXT,
    activated_at TEXT
);
