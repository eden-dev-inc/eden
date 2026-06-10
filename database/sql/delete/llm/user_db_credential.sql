DELETE FROM user_db_credentials
WHERE user_uuid = $1 AND endpoint_uuid = $2;
