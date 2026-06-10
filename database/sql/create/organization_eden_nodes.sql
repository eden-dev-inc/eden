CREATE TABLE IF NOT EXISTS organization_eden_nodes (
    organization_uuid UUID,
    eden_node_uuid UUID,
    PRIMARY KEY (organization_uuid, eden_node_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (eden_node_uuid) REFERENCES eden_nodes(uuid)
);