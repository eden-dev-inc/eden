UPDATE user_notifications
SET read = true
WHERE user_uuid = $1
  AND read = false
