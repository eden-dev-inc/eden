CREATE TABLE IF NOT EXISTS templates (
    id VARCHAR(255) UNIQUE NOT NULL,
    uuid UUID PRIMARY KEY,
    template JSONB,
    description TEXT,
    llm_recommendation TEXT,
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
