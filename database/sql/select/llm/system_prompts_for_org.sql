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
WHERE organization_uuid = $1
   OR organization_uuid IS NULL
ORDER BY (organization_uuid IS NULL), display_name
