SELECT id, organization_uuid, principal_type, principal_id, endpoint_uuid,
       trigger_kind, trigger_metadata, conversation_id, agent_id,
       request_payload, state, plan, checkpoint, response_text, error,
       duration_ms, created_at, updated_at, completed_at
FROM execution_runs
WHERE conversation_id = $1
  AND state IN ('executing', 'interrupted')
  AND updated_at > NOW() - INTERVAL '30 minutes'
ORDER BY created_at DESC
LIMIT 1
