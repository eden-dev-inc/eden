CREATE TABLE IF NOT EXISTS encryption_keys (
    key_uuid UUID PRIMARY KEY,
    org_uuid UUID NOT NULL REFERENCES organizations(uuid),
    endpoint_uuid UUID NOT NULL,
    wrapped_key BYTEA NOT NULL,
    wrapping_org UUID NOT NULL REFERENCES org_key_refs(org_uuid),
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    rotated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (org_uuid, endpoint_uuid, version)
);
