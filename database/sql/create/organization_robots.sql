CREATE TABLE IF NOT EXISTS organization_robots (
    organization_uuid UUID REFERENCES organizations(uuid) NOT NULL,
    robot_uuid UUID REFERENCES robots(uuid) NOT NULL,
    PRIMARY KEY (organization_uuid, robot_uuid)
);
