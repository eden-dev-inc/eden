CREATE TABLE IF NOT EXISTS llm_notifications (
    id UUID PRIMARY KEY,
    user_uuid UUID NOT NULL,
    organization_uuid UUID NOT NULL,
    agent_id UUID REFERENCES llm_agents(id) ON DELETE SET NULL,
    run_id UUID REFERENCES llm_agent_runs(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
