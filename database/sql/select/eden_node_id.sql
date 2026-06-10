SELECT
    eden_nodes.*,
    array_remove(array_agg(DISTINCT ene.endpoint_uuid), NULL) as endpoint_uuids
FROM eden_nodes
LEFT JOIN eden_node_endpoints ene ON ene.eden_node_uuid = eden_nodes.uuid
WHERE id = $1
GROUP BY eden_nodes.id, eden_nodes.uuid, eden_nodes.info, eden_nodes.description, eden_nodes.created_at, eden_nodes.updated_at
LIMIT 1;
