CREATE TABLE IF NOT EXISTS auths (
    id VARCHAR(255) UNIQUE NOT NULL,
    uuid UUID PRIMARY KEY,
    auth VARCHAR(255),
    endpoint_uuid UUID UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid) ON DELETE CASCADE
);
