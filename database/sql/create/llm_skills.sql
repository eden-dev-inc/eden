CREATE TABLE IF NOT EXISTS llm_skills (
    id UUID PRIMARY KEY,
    -- `name` is unique-per-scope (global vs. per-organization) rather than
    -- globally unique. Uniqueness is enforced by the partial indexes below.
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    body_markdown TEXT NOT NULL,
    tags TEXT[] NOT NULL DEFAULT '{}',
    estimated_tokens INT NOT NULL DEFAULT 0,
    source_format TEXT NOT NULL DEFAULT 'markdown',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source_provider TEXT NOT NULL DEFAULT 'manual',
    source_repo_url TEXT,
    source_path TEXT,
    source_ref TEXT,
    source_url TEXT,
    skill_tier TEXT NOT NULL DEFAULT 'core',
    endpoint_kind TEXT,
    organization_uuid UUID NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Global skills (organization_uuid IS NULL): names must be globally unique.
CREATE UNIQUE INDEX IF NOT EXISTS llm_skills_global_name_unique
    ON llm_skills (name)
    WHERE organization_uuid IS NULL;

-- Tenant-scoped skills: names must be unique within a given organization.
CREATE UNIQUE INDEX IF NOT EXISTS llm_skills_org_name_unique
    ON llm_skills (organization_uuid, name)
    WHERE organization_uuid IS NOT NULL;

-- Support `_for_org` lookups with a conventional b-tree.
CREATE INDEX IF NOT EXISTS llm_skills_organization_uuid_idx
    ON llm_skills (organization_uuid);
