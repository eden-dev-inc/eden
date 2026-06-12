CREATE TABLE IF NOT EXISTS organization_endpoints (
    organization_uuid UUID,
    endpoint_uuid UUID,
    PRIMARY KEY (organization_uuid, endpoint_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid)
);