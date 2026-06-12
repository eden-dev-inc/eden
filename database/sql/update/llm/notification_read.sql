UPDATE llm_notifications
SET read = true
WHERE id = $1
  AND user_uuid = $2
  AND organization_uuid = $3
