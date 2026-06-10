-- Embedded (single-tenant) llm_skills schema. Every local row has
-- `organization_uuid IS NULL`, so the uniqueness indexes below are plain
-- (non-partial) — libsql cannot resolve an `ON CONFLICT` arbiter against a
-- partial index, and the shared upsert SQL's partial `WHERE` predicate is
-- stripped by `rewrite_pg_sql`/`strip_on_conflict_predicate` before execution,
-- leaving the column list to select these indexes.
CREATE TABLE IF NOT EXISTS llm_skills (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    body_markdown TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    estimated_tokens INT NOT NULL DEFAULT 0,
    source_format TEXT NOT NULL DEFAULT 'markdown',
    is_active INTEGER NOT NULL DEFAULT 1,
    source_provider TEXT NOT NULL DEFAULT 'manual',
    source_repo_url TEXT,
    source_path TEXT,
    source_ref TEXT,
    source_url TEXT,
    skill_tier TEXT NOT NULL DEFAULT 'core',
    endpoint_kind TEXT,
    organization_uuid TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS llm_skills_global_name_unique
    ON llm_skills (name);

CREATE UNIQUE INDEX IF NOT EXISTS llm_skills_org_name_unique
    ON llm_skills (organization_uuid, name);

CREATE INDEX IF NOT EXISTS llm_skills_organization_uuid_idx
    ON llm_skills (organization_uuid);
