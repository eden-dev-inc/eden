-- `name` is no longer globally unique once `organization_uuid` distinguishes
-- tenant-private rows from globals. Keep the old `query_opt` ergonomics
-- safe by explicitly preferring the global row (matching the historical
-- behaviour of this helper when only global rows existed) and capping the
-- result to one row. Callers that need a tenant-scoped lookup must use
-- the `_for_org` variant.
SELECT id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format, is_active, source_provider, source_repo_url, source_path, source_ref, source_url, skill_tier, endpoint_kind, organization_uuid, created_at, updated_at
FROM llm_skills
WHERE name = $1
ORDER BY (organization_uuid IS NOT NULL), name
LIMIT 1;
