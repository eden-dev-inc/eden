SELECT e.*
FROM endpoints e
         JOIN organization_endpoints oe ON e.uuid = oe.endpoint_uuid
WHERE oe.organization_uuid = $1;