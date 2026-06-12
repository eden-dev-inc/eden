CREATE TABLE IF NOT EXISTS organization_templates (
    organization_uuid UUID,
    template_uuid UUID,
    PRIMARY KEY (organization_uuid, template_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (template_uuid) REFERENCES templates(uuid)
);