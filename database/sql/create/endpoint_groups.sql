CREATE TABLE IF NOT EXISTS endpoint_groups (
    id VARCHAR(255) UNIQUE NOT NULL,
    uuid UUID PRIMARY KEY,
    description TEXT,
    ep_kind TEXT NOT NULL,
    default_endpoint UUID,
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
