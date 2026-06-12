SELECT a.id,
       a.uuid,
       a.description,
       a.endpoint,
       a.created_by,
       a.updated_by,
       a.created_at,
       a.updated_at
FROM interlays a
         INNER JOIN organization_interlays oa ON a.uuid = oa.interlay_uuid
WHERE oa.organization_uuid = $1;
