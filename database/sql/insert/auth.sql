BEGIN;

-- Verify endpoint exists first
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM endpoints WHERE uuid = $4) THEN
    RAISE EXCEPTION 'Endpoint with uuid % does not exist', $4;
  END IF;
END $$;

-- Insert the auth
INSERT INTO auths (id, uuid, auth, endpoint_uuid, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6);

COMMIT;