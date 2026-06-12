DELETE FROM llm_user_tools_endpoints
WHERE id = $1
  AND organization_uuid = $2
  AND created_by = $3;
