CREATE TABLE IF NOT EXISTS jwt_blacklist
(
    blacklist_key TEXT PRIMARY KEY,
    expires_at_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
