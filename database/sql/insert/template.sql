WITH
-- Create template
template_insert AS (
    INSERT INTO templates (
        id,
        uuid,
        template,
        description,
        llm_recommendation,
        created_by,
        updated_by,
        created_at,
        updated_at
    )
    VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9)
    RETURNING uuid
),
-- Link template to organization using returned uuid
org_link AS (
    INSERT INTO organization_templates (organization_uuid, template_uuid)
    SELECT $10, template_insert.uuid
    FROM template_insert
    ON CONFLICT (organization_uuid, template_uuid) DO NOTHING
)
-- Return the actual template UUID from the upsert
SELECT uuid FROM template_insert;