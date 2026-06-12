CREATE TABLE IF NOT EXISTS workflow_templates (
    workflow_uuid TEXT,
    template_uuid TEXT,
    created_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (workflow_uuid, template_uuid),
    FOREIGN KEY (workflow_uuid) REFERENCES workflows(uuid),
    FOREIGN KEY (template_uuid) REFERENCES templates(uuid)
);
