CREATE INDEX IF NOT EXISTS idx_workspace_views_org_user
ON workspace_views (organization_uuid, user_uuid);
