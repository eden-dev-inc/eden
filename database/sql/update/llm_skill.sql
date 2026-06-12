UPDATE llm_skills
SET display_name = $2,
    description = $3,
    body_markdown = $4,
    tags = $5,
    estimated_tokens = $6,
    source_format = $7,
    is_active = $8,
    source_provider = $9,
    source_repo_url = $10,
    source_path = $11,
    source_ref = $12,
    source_url = $13,
    skill_tier = $14,
    endpoint_kind = $15,
    updated_at = NOW()
WHERE id = $1
  AND organization_uuid IS NOT DISTINCT FROM $16
RETURNING id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format, is_active, source_provider, source_repo_url, source_path, source_ref, source_url, skill_tier, endpoint_kind, organization_uuid, created_at, updated_at;
