CREATE TABLE IF NOT EXISTS trigger_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES trigger_sources(id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    idempotency_key TEXT,
    correlation_id UUID,
    matched_agent_id UUID,
    matched_run_id UUID,
    state TEXT NOT NULL DEFAULT 'received',
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);
