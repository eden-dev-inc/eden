CREATE TABLE IF NOT EXISTS run_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    tokens_used INTEGER,
    cost_usd NUMERIC(12, 6),
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
