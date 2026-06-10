CREATE TABLE IF NOT EXISTS apis
(
    id             VARCHAR(255) UNIQUE NOT NULL,
    uuid           UUID PRIMARY KEY,
    description    TEXT,
    fields         JSONB,
    bindings       JSONB,
    response_logic JSONB,
    created_by     UUID NOT NULL,
    updated_by     UUID NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE,
    updated_at     TIMESTAMP WITH TIME ZONE
);
