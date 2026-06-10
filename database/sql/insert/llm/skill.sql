-- Upsert a global skill (organization_uuid IS NULL).
--
-- Pairs with the `llm_skills_global_name_unique` partial index which scopes
-- the uniqueness constraint to rows where organization_uuid IS NULL. The
-- `$17` parameter is accepted for positional parity with the tenant variant
-- but the VALUES clause forces NULL so a stray non-NULL binding cannot
-- escalate a global write into an unintended tenant write.
INSERT INTO llm_skills (
    id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format,
    is_active, source_provider, source_repo_url, source_path, source_ref, source_url,
    skill_tier, endpoint_kind, organization_uuid
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NULL)
ON CONFLICT (name) WHERE organization_uuid IS NULL DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    body_markdown = EXCLUDED.body_markdown,
    tags = EXCLUDED.tags,
    estimated_tokens = EXCLUDED.estimated_tokens,
    source_format = EXCLUDED.source_format,
    is_active = EXCLUDED.is_active,
    source_provider = EXCLUDED.source_provider,
    source_repo_url = EXCLUDED.source_repo_url,
    source_path = EXCLUDED.source_path,
    source_ref = EXCLUDED.source_ref,
    source_url = EXCLUDED.source_url,
    skill_tier = EXCLUDED.skill_tier,
    endpoint_kind = EXCLUDED.endpoint_kind,
    updated_at = NOW()
RETURNING id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format,
          is_active, source_provider, source_repo_url, source_path, source_ref, source_url,
          skill_tier, endpoint_kind, organization_uuid, created_at, updated_at;
