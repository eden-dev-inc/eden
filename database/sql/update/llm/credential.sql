UPDATE llm_credentials
SET
    label = $3,
    description = $4,
    base_url = $5,
    api_key = $6,
    updated_at = NOW(),
    deleted_at = NULL
WHERE organization_uuid = $1
  AND id = $2
RETURNING
    id,
    organization_uuid,
    provider,
    label,
    description,
    base_url,
    api_key,
    deleted_at,
    created_at,
    updated_at;
