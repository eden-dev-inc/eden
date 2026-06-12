DELETE FROM endpoint_group_members
WHERE endpoint_group_uuid = $1 AND endpoint_uuid = $2;
