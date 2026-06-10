SELECT t.id, t.uuid, t.description, t.template, t.llm_recommendation, t.created_by, t.updated_by
FROM templates t
         JOIN organization_templates ot ON t.uuid = ot.template_uuid
WHERE ot.organization_uuid = $1;
