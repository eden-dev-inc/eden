-- TODO(key-rotation): is_active is always TRUE here. When key rotation is
-- implemented, add an UPDATE query to SET is_active = FALSE on the previous
-- DEK row before inserting the new version.
INSERT INTO encryption_keys (key_uuid, org_uuid, endpoint_uuid, wrapped_key, wrapping_org, version, is_active, created_at, rotated_at)
VALUES ($1, $2, $3, $4, $5, $6, TRUE, NOW(), NOW());
