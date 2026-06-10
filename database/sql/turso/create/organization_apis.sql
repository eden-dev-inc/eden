CREATE TABLE IF NOT EXISTS organization_apis (
    organization_uuid TEXT,
    api_uuid TEXT,
    PRIMARY KEY (organization_uuid, api_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (api_uuid) REFERENCES apis(uuid)
);
