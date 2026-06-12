CREATE TABLE IF NOT EXISTS pipelines
(
    id                    TEXT UNIQUE NOT NULL,
    uuid                  TEXT PRIMARY KEY NOT NULL,
    description           TEXT,
    status                TEXT DEFAULT 'Pending',
    source_endpoint       TEXT NOT NULL,
    target_endpoint       TEXT NOT NULL,
    filter                TEXT,
    cdc_config            TEXT NOT NULL,
    last_lsn              TEXT,
    write_template_uuid   TEXT,
    read_template_uuid    TEXT,
    created_by            TEXT NOT NULL,
    updated_by            TEXT NOT NULL,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
);
