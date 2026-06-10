CREATE TABLE IF NOT EXISTS organization_endpoints (
    organization_uuid TEXT,
    endpoint_uuid TEXT,
    PRIMARY KEY (organization_uuid, endpoint_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid)
);
