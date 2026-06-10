CREATE TABLE IF NOT EXISTS organization_users (
    organization_uuid TEXT,
    user_uuid TEXT,
    PRIMARY KEY (organization_uuid, user_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (user_uuid) REFERENCES users(uuid)
);
