CREATE UNIQUE INDEX IF NOT EXISTS trigger_events_source_idempotency_idx
    ON trigger_events (source_id, idempotency_key);
