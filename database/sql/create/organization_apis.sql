CREATE TABLE IF NOT EXISTS organization_apis (
    organization_uuid UUID,
    api_uuid UUID,
    PRIMARY KEY (organization_uuid, api_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (api_uuid) REFERENCES apis(uuid)
);