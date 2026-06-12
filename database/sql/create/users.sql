CREATE TABLE IF NOT EXISTS users (
    uuid UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    organization_uuid UUID REFERENCES organizations(uuid) NOT NULL,
    password JSONB,
    description TEXT,
    email VARCHAR(255),
    display_name VARCHAR(255),
    bio TEXT DEFAULT NULL,
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (username, organization_uuid)
);
