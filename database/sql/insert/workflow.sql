WITH templates_exist AS (
    SELECT $2::uuid workflow_uuid, array_agg(t.uuid) template_uuid
    FROM templates t
    INNER JOIN organization_templates ot
        ON ot.template_uuid = t.uuid
        WHERE t.uuid = ANY($10) AND ot.organization_uuid = $9
    GROUP BY $2::uuid
),
workflow_created AS (
    -- Create the new workflow
    INSERT INTO workflows (id, uuid, dag, description, created_by, updated_by, created_at, updated_at)
        SELECT $1, workflow_uuid, $3, $4, $5, $6, $7, $8 FROM templates_exist
),
org_workflows AS (
    -- Link workflow to organization using looked-up uuid
    INSERT INTO organization_workflows(organization_uuid, workflow_uuid)
        SELECT $9, workflow_uuid FROM templates_exist
)
-- Link workflow to existing templates
INSERT INTO workflow_templates (workflow_uuid, template_uuid, created_at, updated_at)
    SELECT templates_exist.workflow_uuid, unnest(templates_exist.template_uuid), $7, $8 FROM templates_exist;
