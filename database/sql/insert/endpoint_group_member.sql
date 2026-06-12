INSERT INTO endpoint_group_members (endpoint_group_uuid, endpoint_uuid)
VALUES ($1, $2)
ON CONFLICT (endpoint_group_uuid, endpoint_uuid) DO NOTHING;
