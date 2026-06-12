INSERT INTO els_policy_pointers (policy_uuid, active_version, activated_by, activated_at)
VALUES ($1, NULL, NULL, NULL)
ON CONFLICT (policy_uuid) DO NOTHING;
