WITH deleted_organization_templates AS (
    DELETE FROM organization_templates
    WHERE template_uuid = $1
    RETURNING organization_uuid
),
deleted_workflow_templates AS (
    DELETE FROM workflow_templates
    WHERE template_uuid = $1
    RETURNING workflow_uuid
),
deleted_template AS (
    DELETE FROM templates
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(workflow_uuid) FROM deleted_workflow_templates), '[]'::json) as workflow_uuids,
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_templates), '[]'::json) as organization_uuids;
