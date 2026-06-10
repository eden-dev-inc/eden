
WITH org_insert AS (
  INSERT INTO organizations (id, uuid, description, created_at, updated_at)
  VALUES ($1, $2, $3, $4, $5)
  RETURNING uuid
)
INSERT INTO organization_eden_nodes (organization_uuid, eden_node_uuid)
SELECT $2, unnest($6::uuid[]);
