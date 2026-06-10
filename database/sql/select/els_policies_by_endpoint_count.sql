SELECT COUNT(*)::BIGINT AS total
FROM els_policies
WHERE endpoint_uuid = $1 AND org_uuid = $2;
