CREATE TABLE IF NOT EXISTS llm_agent_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES llm_agents(id),
    version INTEGER NOT NULL,
    prompt TEXT NOT NULL,
    cron_expression TEXT NOT NULL,
    scope JSONB NOT NULL,
    skill_ids TEXT[] DEFAULT '{}',
    tool_endpoint_uuids TEXT[] DEFAULT '{}',
    orchestrate BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL
);
