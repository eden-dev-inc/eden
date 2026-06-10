-- Upsert a tenant-scoped skill (organization_uuid IS NOT NULL).
--
-- Pairs with the `llm_skills_org_name_unique` partial index on
-- `(organization_uuid, name) WHERE organization_uuid IS NOT NULL`. The
-- `$17` parameter must be a non-null UUID; the calling Rust code routes to
-- this file only when `NewSkill.organization_uuid` is `Some(_)`.
INSERT INTO llm_skills (
    id, name, display_name, description, body_markdown, tags, estimated_tokens, source_format,
    is_active, source_provider, source_repo_url, source_path, source_ref, source_url,
    skill_tier, endpoint_kind, organization_uuid
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (organization_uuid, name) WHERE organization_uuid IS NOT NULL DO UPDATE
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
