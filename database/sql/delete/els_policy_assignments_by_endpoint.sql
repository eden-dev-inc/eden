DELETE FROM els_policy_assignments
WHERE endpoint_uuid = $1 AND org_uuid = $2;
