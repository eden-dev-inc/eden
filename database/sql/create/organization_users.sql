CREATE TABLE IF NOT EXISTS organization_users (
    organization_uuid UUID,
    user_uuid UUID,
    PRIMARY KEY (organization_uuid, user_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (user_uuid) REFERENCES users(uuid)
);