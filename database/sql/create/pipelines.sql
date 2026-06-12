CREATE TABLE IF NOT EXISTS pipelines
(
    id                    VARCHAR(255) UNIQUE NOT NULL,
    uuid                  UUID PRIMARY KEY NOT NULL,
    description           TEXT,
    status                VARCHAR(255) DEFAULT 'Pending',
    source_endpoint       UUID NOT NULL,
    target_endpoint       UUID NOT NULL,
    filter                TEXT,
    cdc_config            JSONB NOT NULL,
    last_lsn              VARCHAR(255),
    write_template_uuid   UUID,
    read_template_uuid    UUID,
    created_by            UUID NOT NULL,
    updated_by            UUID NOT NULL,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
