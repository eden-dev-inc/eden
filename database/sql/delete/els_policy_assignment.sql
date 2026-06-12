DELETE FROM els_policy_assignments
WHERE endpoint_uuid = $1 AND user_uuid = $2 AND org_uuid = $3;
