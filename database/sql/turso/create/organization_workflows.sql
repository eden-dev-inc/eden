CREATE TABLE IF NOT EXISTS organization_workflows (
    organization_uuid TEXT,
    workflow_uuid TEXT,
    PRIMARY KEY (organization_uuid, workflow_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (workflow_uuid) REFERENCES workflows(uuid)
);
