CREATE TABLE IF NOT EXISTS workflow_templates (
    workflow_uuid UUID,
    template_uuid UUID,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (workflow_uuid, template_uuid),
    FOREIGN KEY (workflow_uuid) REFERENCES workflows(uuid),
    FOREIGN KEY (template_uuid) REFERENCES templates(uuid)
);