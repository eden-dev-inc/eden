CREATE TABLE IF NOT EXISTS apis
(
    id             TEXT UNIQUE NOT NULL,
    uuid           TEXT PRIMARY KEY,
    description    TEXT,
    fields         TEXT,
    bindings       TEXT,
    response_logic TEXT,
    created_by     TEXT NOT NULL,
    updated_by     TEXT NOT NULL,
    created_at     TEXT,
    updated_at     TEXT
);
