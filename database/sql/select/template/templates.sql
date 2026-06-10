SELECT t.*
FROM templates t
         JOIN organization_templates ot ON t.uuid = ot.template_uuid
WHERE ot.organization_uuid = $1;