INSERT INTO llm_credentials (
    id,
    organization_uuid,
    provider,
    label,
    description,
    base_url,
    api_key
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
)
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
