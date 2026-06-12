DELETE FROM eden_node_endpoints WHERE endpoint_uuid = $1 RETURNING eden_node_uuid;
