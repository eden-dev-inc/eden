CREATE TABLE IF NOT EXISTS els_policy_versions (
    policy_uuid UUID NOT NULL REFERENCES els_policies(uuid) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(16) NOT NULL DEFAULT 'draft',
    created_by UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_uuid, version),
    CONSTRAINT els_version_status_check CHECK (status IN ('draft', 'active', 'superseded', 'rejected'))
);
CREATE INDEX IF NOT EXISTS idx_els_policy_versions_active
    ON els_policy_versions (policy_uuid) WHERE status = 'active';
