CREATE TABLE IF NOT EXISTS els_policy_pointers (
    policy_uuid UUID PRIMARY KEY REFERENCES els_policies(uuid) ON DELETE CASCADE,
    active_version INTEGER,
    activated_by UUID,
    activated_at TIMESTAMP WITH TIME ZONE
);
