-- Add per-tenant scoping to llm_skills.
--
-- Prior to this schema update the in-tree schema had `name TEXT NOT NULL UNIQUE`
-- and no organization_uuid column, which meant every row was effectively a
-- global skill and the `_for_org` queries that already ship (shipped
-- referencing `organization_uuid`) only worked because out-of-tree operator
-- schema updates had added the column by hand. This update brings the tree
-- into agreement with the code.
--
-- Semantics:
--   organization_uuid IS NULL     -> global skill, visible to every org
--   organization_uuid = <tenant>  -> tenant-private skill
--
-- A tenant may shadow a global skill by creating a row with the same name
-- and a non-null organization_uuid. The existing `_for_org` queries already
-- ORDER BY `(organization_uuid IS NULL)` so tenant rows win.
--
-- Uniqueness is enforced with two partial indexes rather than a single
-- COALESCE expression so the intent is readable and the ON CONFLICT target
-- in the upsert can quote an actual constraint name. The legacy
-- `name TEXT NOT NULL UNIQUE` constraint is dropped (IF EXISTS) so fresh
-- installs and out-of-tree migrated databases converge on the same shape.

ALTER TABLE llm_skills ADD COLUMN IF NOT EXISTS organization_uuid UUID NULL;

-- Legacy unique-on-name constraint. `IF EXISTS` so this is idempotent for
-- databases that either never had it or already dropped it.
ALTER TABLE llm_skills DROP CONSTRAINT IF EXISTS llm_skills_name_key;

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
