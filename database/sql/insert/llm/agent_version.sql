INSERT INTO llm_agent_versions (
    id,
    agent_id,
    version,
    prompt,
    cron_expression,
    scope,
    skill_ids,
    tool_endpoint_uuids,
    orchestrate,
    created_by
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
