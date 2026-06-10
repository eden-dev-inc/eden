SELECT a.id, a.uuid, a.description, a.fields, a.bindings, a.created_by, a.updated_by, a.created_at, a.updated_at
FROM apis a
         INNER JOIN organization_apis oa ON a.uuid = oa.api_uuid
WHERE oa.organization_uuid = $1;
