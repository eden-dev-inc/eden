UPDATE llm_credentials
SET deleted_at = NOW(), updated_at = NOW()
WHERE organization_uuid = $1
  AND id = $2
  AND deleted_at IS NULL;
