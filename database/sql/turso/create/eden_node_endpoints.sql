CREATE TABLE IF NOT EXISTS eden_node_endpoints (
    eden_node_uuid TEXT,
    endpoint_uuid TEXT,
    created_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (eden_node_uuid, endpoint_uuid),
    FOREIGN KEY (eden_node_uuid) REFERENCES eden_nodes(uuid),
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid)
);
