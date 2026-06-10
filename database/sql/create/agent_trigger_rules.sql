CREATE TABLE IF NOT EXISTS agent_trigger_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES llm_agents(id),
    source_id UUID NOT NULL REFERENCES trigger_sources(id),
    event_type_filter TEXT,
    payload_filter JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
