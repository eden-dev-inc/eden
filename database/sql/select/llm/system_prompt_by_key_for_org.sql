SELECT
    id,
    prompt_key,
    display_name,
    description,
    prompt,
    is_active,
    is_default,
    created_at,
    updated_at
FROM llm_system_prompts
WHERE prompt_key = $2
  AND (organization_uuid = $1 OR organization_uuid IS NULL)
ORDER BY (organization_uuid IS NULL)
LIMIT 1
