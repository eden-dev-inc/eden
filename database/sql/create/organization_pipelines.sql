CREATE TABLE IF NOT EXISTS organization_pipelines (
    organization_uuid UUID,
    pipeline_uuid UUID,
    PRIMARY KEY (organization_uuid, pipeline_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (pipeline_uuid) REFERENCES pipelines(uuid)
);
