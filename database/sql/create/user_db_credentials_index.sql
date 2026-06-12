CREATE INDEX IF NOT EXISTS user_db_credentials_user_endpoint_idx
    ON user_db_credentials (user_uuid, endpoint_uuid);
