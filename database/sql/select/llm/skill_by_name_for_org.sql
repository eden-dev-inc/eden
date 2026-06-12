SELECT id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format, is_active,
       source_provider, source_repo_url, source_path, source_ref, source_url, skill_tier, endpoint_kind,
       organization_uuid, created_at, updated_at
FROM llm_skills
WHERE name = $2
  AND (organization_uuid = $1 OR organization_uuid IS NULL)
ORDER BY (organization_uuid IS NULL)
LIMIT 1
