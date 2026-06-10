SELECT id, user_uuid, organization_uuid, agent_id, run_id, title, body, read, created_at
FROM llm_notifications
WHERE user_uuid = $1
  AND organization_uuid = $2
ORDER BY created_at DESC
LIMIT $3
