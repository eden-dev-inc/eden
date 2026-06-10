CREATE TABLE IF NOT EXISTS organization_pipelines (
    organization_uuid TEXT,
    pipeline_uuid TEXT,
    PRIMARY KEY (organization_uuid, pipeline_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (pipeline_uuid) REFERENCES pipelines(uuid)
);
