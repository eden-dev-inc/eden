CREATE TABLE IF NOT EXISTS interlays
(
    id             TEXT UNIQUE NOT NULL,
    uuid           TEXT PRIMARY KEY,
    description    TEXT,
    endpoint       TEXT,
    port           INTEGER,
    listeners      TEXT,
    advertise_host TEXT,
    tls            TEXT,
    settings       TEXT,
    created_by     TEXT NOT NULL,
    updated_by     TEXT NOT NULL,
    created_at     TEXT,
    updated_at     TEXT
);

ALTER TABLE interlays
ADD COLUMN IF NOT EXISTS listeners TEXT;

ALTER TABLE interlays
ADD COLUMN IF NOT EXISTS advertise_host TEXT;

-- TODO: Track owning eden_node_uuid in interlays table for multi-node port conflict detection
