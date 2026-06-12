SELECT policy_uuid, active_version, activated_by, activated_at
FROM els_policy_pointers
WHERE policy_uuid = $1;
