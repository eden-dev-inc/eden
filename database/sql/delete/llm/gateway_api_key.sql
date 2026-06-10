DELETE FROM llm_gateway_api_keys
WHERE organization_uuid = $1
  AND id = $2
RETURNING id;
