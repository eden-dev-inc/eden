WITH deleted_organization_workflows AS (
    DELETE FROM organization_workflows
    WHERE workflow_uuid = $1
    RETURNING organization_uuid
),
deleted_workflow_templates AS (
    DELETE FROM workflow_templates
    WHERE workflow_uuid = $1
    RETURNING template_uuid
),
deleted_workflow AS (
    DELETE FROM workflows
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_workflows), '[]'::json) as organization_uuids;
