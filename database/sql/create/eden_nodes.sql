CREATE TABLE IF NOT EXISTS eden_nodes (
    id VARCHAR(255) UNIQUE NOT NULL,
    uuid UUID PRIMARY KEY,
    info JSONB,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
