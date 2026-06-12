CREATE TABLE IF NOT EXISTS endpoint_group_members (
    endpoint_group_uuid TEXT,
    endpoint_uuid TEXT,
    PRIMARY KEY (endpoint_group_uuid, endpoint_uuid),
    FOREIGN KEY (endpoint_group_uuid) REFERENCES endpoint_groups(uuid),
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid)
);
