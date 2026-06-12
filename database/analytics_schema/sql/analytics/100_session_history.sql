-- Session history table
-- Tracks user login sessions including start time, end time, device info, and IP
-- Used for security auditing and user activity monitoring

CREATE TABLE IF NOT EXISTS analytics.session_history
(
    -- Session identity
    session_uuid String CODEC(ZSTD(3)),

    -- Time tracking
    started_at DateTime64(6, 'UTC'),
    ended_at Nullable(DateTime64(6, 'UTC')),
    last_active_at DateTime64(6, 'UTC'),

    -- User identity
    organization_uuid String CODEC(ZSTD(3)),
    user_uuid String CODEC(ZSTD(3)),
    user_id String CODEC(ZSTD(3)),

    -- Session metadata
    device String CODEC(ZSTD(3)),
    user_agent String CODEC(ZSTD(3)),
    ip_address String CODEC(ZSTD(3)),

    -- Authentication method
    auth_method LowCardinality(String),  -- 'basic', 'bearer', 'api_key'

    -- Session status
    status LowCardinality(String),  -- 'active', 'expired', 'revoked', 'logged_out'

    -- Request stats during session
    request_count UInt64 DEFAULT 0,
    error_count UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (organization_uuid, user_uuid, started_at, session_uuid)
TTL toDateTime(started_at) + INTERVAL 365 DAY;
