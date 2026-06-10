SELECT id, organization_uuid, principal_type, principal_id, endpoint_uuid,
       trigger_kind, trigger_metadata, conversation_id, agent_id,
       request_payload, state, plan, checkpoint, response_text, error,
       duration_ms, created_at, updated_at, completed_at
FROM execution_runs
WHERE id = $1
LIMIT 1
