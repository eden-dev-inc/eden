CREATE INDEX IF NOT EXISTS agent_tasks_to_agent_status_idx
ON agent_tasks (to_agent, status);
