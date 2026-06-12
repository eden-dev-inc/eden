CREATE TABLE IF NOT EXISTS organization_templates (
    organization_uuid TEXT,
    template_uuid TEXT,
    PRIMARY KEY (organization_uuid, template_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (template_uuid) REFERENCES templates(uuid)
);
