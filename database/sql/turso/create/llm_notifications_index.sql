CREATE INDEX IF NOT EXISTS idx_llm_notifications_user
    ON llm_notifications (user_uuid, read, created_at DESC);
