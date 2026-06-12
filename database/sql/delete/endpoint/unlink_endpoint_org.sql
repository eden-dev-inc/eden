DELETE FROM organization_endpoints WHERE endpoint_uuid = $1 RETURNING organization_uuid;
