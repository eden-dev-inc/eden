CREATE TABLE IF NOT EXISTS interlays
(
    id             VARCHAR(255) UNIQUE NOT NULL,
    uuid           UUID PRIMARY KEY,
    description    TEXT,
    endpoint       UUID,
    port           INTEGER, -- smallint is signed, can't fit 32768-65535,
    listeners      JSONB,
    advertise_host TEXT,
    tls            JSONB,
    settings       JSONB,
    created_by     UUID NOT NULL,
    updated_by     UUID NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE,
    updated_at     TIMESTAMP WITH TIME ZONE
);

ALTER TABLE interlays
    ADD COLUMN IF NOT EXISTS listeners JSONB;

ALTER TABLE interlays
    ADD COLUMN IF NOT EXISTS advertise_host TEXT;

-- TODO: Track owning eden_node_uuid in interlays table for multi-node port conflict detection
