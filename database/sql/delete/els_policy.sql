DELETE FROM els_policies
WHERE uuid = $1 AND endpoint_uuid = $2 AND org_uuid = $3;
