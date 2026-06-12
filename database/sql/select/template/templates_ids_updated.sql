SELECT t.id, t.uuid
FROM templates t
         JOIN organization_templates ot ON t.uuid = ot.template_uuid
WHERE ot.organization_uuid = $1
  AND t.updated_at >= $2;