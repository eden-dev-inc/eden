CREATE INDEX IF NOT EXISTS idx_run_events_run_id
    ON run_events (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_run_events_type
    ON run_events (event_type, created_at DESC);
