-- Associate the user with the organization
INSERT INTO organization_users (organization_uuid, user_uuid)
VALUES ($1, $2);
