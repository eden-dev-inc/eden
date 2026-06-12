CREATE INDEX IF NOT EXISTS trigger_events_source_received_idx
    ON trigger_events (source_id, received_at DESC);
