CREATE TABLE IF NOT EXISTS llm_agent_runs (
    id UUID PRIMARY KEY,
    agent_id UUID NOT NULL REFERENCES llm_agents(id) ON DELETE CASCADE,
    run_status TEXT NOT NULL DEFAULT 'running',
    workflow_id UUID,
    conversation_id UUID,
    response_text TEXT,
    error TEXT,
    duration_ms BIGINT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE
);
