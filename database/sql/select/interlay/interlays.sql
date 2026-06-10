SELECT a.*
FROM interlays a
         JOIN organization_interlays oa ON a.uuid = oa.interlay_uuid
WHERE oa.organization_uuid = $1;