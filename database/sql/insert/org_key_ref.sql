-- TODO(key-rotation): ON CONFLICT DO NOTHING means a rotated org key won't
-- update key_ref or bump key_version. Add an UPDATE query (or change to
-- DO UPDATE SET key_ref = EXCLUDED.key_ref, key_version = key_version + 1,
-- rotated_at = NOW()) when key rotation is implemented.
INSERT INTO org_key_refs (org_uuid, provider, key_ref, key_version, created_at, rotated_at)
VALUES ($1, $2, $3, 1, NOW(), NOW())
ON CONFLICT (org_uuid) DO NOTHING;
