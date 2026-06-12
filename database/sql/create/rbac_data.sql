CREATE TABLE IF NOT EXISTS rbac_data (
    org_uuid UUID NOT NULL REFERENCES organizations(uuid),
    endpoint_uuid UUID NOT NULL,
    subject_kind rbac_subject_kind NOT NULL,
    subject_uuid UUID NOT NULL,
    perms VARCHAR(4) NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    version_ms BIGINT NOT NULL,
    version_seq BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_uuid, endpoint_uuid, subject_kind, subject_uuid)
);
CREATE INDEX IF NOT EXISTS idx_rbac_data_subject
    ON rbac_data (org_uuid, subject_kind, subject_uuid)
    WHERE is_active = TRUE;
