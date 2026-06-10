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
WHERE prompt_key = $1;
