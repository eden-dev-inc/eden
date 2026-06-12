INSERT INTO llm_system_prompts (
    id,
    prompt_key,
    display_name,
    description,
    prompt,
    is_active,
    is_default,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
ON CONFLICT (prompt_key) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    prompt = EXCLUDED.prompt,
    is_active = EXCLUDED.is_active,
    is_default = EXCLUDED.is_default,
    updated_at = NOW()
RETURNING
    id,
    prompt_key,
    display_name,
    description,
    prompt,
    is_active,
    is_default,
    created_at,
    updated_at;
