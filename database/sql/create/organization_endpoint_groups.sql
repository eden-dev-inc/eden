CREATE TABLE IF NOT EXISTS organization_endpoint_groups (
    organization_uuid UUID,
    endpoint_group_uuid UUID,
    PRIMARY KEY (organization_uuid, endpoint_group_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (endpoint_group_uuid) REFERENCES endpoint_groups(uuid)
);
