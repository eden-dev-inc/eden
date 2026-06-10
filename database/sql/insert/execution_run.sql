INSERT INTO execution_runs (
    id, organization_uuid, principal_type, principal_id, endpoint_uuid,
    trigger_kind, trigger_metadata, conversation_id, agent_id,
    request_payload, state
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
