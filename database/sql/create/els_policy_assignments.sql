CREATE TABLE IF NOT EXISTS els_policy_assignments (
    org_uuid UUID NOT NULL,
    endpoint_uuid UUID NOT NULL,
    user_uuid UUID NOT NULL,
    policy_uuid UUID NOT NULL REFERENCES els_policies(uuid) ON DELETE CASCADE,
    mode VARCHAR(10) NOT NULL DEFAULT 'sync',
    strategy_snapshot VARCHAR(50),
    config_snapshot JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (endpoint_uuid, user_uuid),
    CONSTRAINT els_assignment_mode_check CHECK (mode IN ('sync', 'copy'))
);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_org ON els_policy_assignments (org_uuid);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_policy ON els_policy_assignments (policy_uuid);
CREATE INDEX IF NOT EXISTS idx_els_policy_assignments_sync_policy ON els_policy_assignments (policy_uuid) WHERE mode = 'sync';
