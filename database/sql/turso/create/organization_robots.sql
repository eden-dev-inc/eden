CREATE TABLE IF NOT EXISTS organization_robots (
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    robot_uuid TEXT NOT NULL REFERENCES robots(uuid),
    PRIMARY KEY (organization_uuid, robot_uuid)
);
