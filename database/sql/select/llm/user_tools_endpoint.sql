SELECT
    id,
    organization_uuid,
    created_by,
    name,
    description,
    client_key,
    tools_url,
    bearer_token,
    tool_snapshot,
    validated_at,
    last_error,
    created_at,
    updated_at
FROM llm_user_tools_endpoints
WHERE id = $1;
