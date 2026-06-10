SELECT a.*
FROM apis a
         JOIN organization_apis oa ON a.uuid = oa.api_uuid
WHERE oa.organization_uuid = $1
  AND a.updated_at >= $2;