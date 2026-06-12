SELECT
    id,
    organization_uuid,
    provider,
    label,
    description,
    base_url,
    api_key,
    deleted_at,
    created_at,
    updated_at
FROM llm_credentials
WHERE organization_uuid = $1
  AND id = $2
  AND deleted_at IS NULL;
