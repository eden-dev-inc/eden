CREATE TABLE IF NOT EXISTS eden_node_endpoints (
    eden_node_uuid UUID,
    endpoint_uuid UUID,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (eden_node_uuid, endpoint_uuid),
    FOREIGN KEY (eden_node_uuid) REFERENCES eden_nodes(uuid),
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid)
);