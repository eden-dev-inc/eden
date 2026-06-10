CREATE TABLE IF NOT EXISTS organizations (
    id VARCHAR(255) UNIQUE NOT NULL,
    uuid UUID PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
