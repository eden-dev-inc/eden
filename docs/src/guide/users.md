# User Management

This guide covers creating and managing users within your Eden-MDBS organization.

## Overview

Users in Eden are organization-scoped accounts with hierarchical access levels. User management is handled through the IAM (Identity and Access Management) API.

## Access Levels

Eden uses a four-tier access level system:

| Level          | Capabilities                                                    |
| -------------- | --------------------------------------------------------------- |
| **Read**       | View resources and execute read-only operations                 |
| **Write**      | All Read permissions + execute write operations                 |
| **Admin**      | All Write permissions + manage users, endpoints, templates      |
| **SuperAdmin** | All Admin permissions + manage other admins, organization settings |

### Permission Rules

- Users can only manage users with **lower** access levels
- Only **SuperAdmins** can create Admin and SuperAdmin users
- Only **SuperAdmins** can change passwords for other users
- Users can always modify their own profile information

## Creating Users

### Create a Standard User

Requires Admin or SuperAdmin access:

```bash
curl http://{host}:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "username": "developer@company.com",
    "password": "SecurePassword123!",
    "description": "Development team member",
    "access_level": "Write"
  }'
```

**Response:**

```json
{
  "status": "success",
  "message": "success"
}
```

### Create an Admin User

Requires SuperAdmin access:

```bash
curl http://{host}:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "username": "admin@company.com",
    "password": "AdminPassword456!",
    "description": "System Administrator",
    "access_level": "Admin"
  }'
```

### User Input Fields

| Field          | Required | Description                              |
| -------------- | -------- | ---------------------------------------- |
| `username`     | Yes      | Unique identifier (email recommended)    |
| `password`     | Yes      | User password                            |
| `description`  | No       | User description or role                 |
| `access_level` | No       | Access level (defaults to "Read")        |

## Retrieving User Information

### Get User Details

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "developer@company.com",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Development team member",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

> **Note**: Passwords are never returned in API responses.

## Updating Users

### Update Your Own Profile

Users can update their own username, password, and description:

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "description": "Senior Development team member",
    "password": "NewSecurePassword789!"
  }'
```

### Update Another User (Admin)

Admins can update users with lower access levels:

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "access_level": "Admin"
  }'
```

### Update Fields

All fields are optional in PATCH requests:

| Field          | Description                    |
| -------------- | ------------------------------ |
| `username`     | Change the user's username     |
| `password`     | Change the user's password     |
| `description`  | Change the user's description  |
| `access_level` | Change the user's access level |

### Update Permission Matrix

| Requester       | Target User     | Can Update                              |
| --------------- | --------------- | --------------------------------------- |
| User (Self)     | Self            | username, password, description         |
| Admin           | Read/Write      | username, description, access_level     |
| Admin           | Admin           | Cannot modify                           |
| SuperAdmin      | Any             | All fields including password           |

## Deleting Users

Remove a user from the organization:

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

**Response:**

```json
{
  "status": "success",
  "message": "success"
}
```

> **Note**: Deleting a user removes all their RBAC permissions and access to resources.

## Error Handling

### User Already Exists

```json
{
  "error": "Bad Request",
  "message": "user developer@company.com exists"
}
```

### User Not Found

```json
{
  "error": "Bad Request",
  "message": "user developer@company.com doesn't exist"
}
```

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "Insufficient access level to perform this operation"
}
```

### Cannot Modify Higher-Level User

```json
{
  "error": "Bad Request",
  "message": "Cannot modify user with equal or higher access level"
}
```

## Best Practices

### User Lifecycle

1. **Onboarding**: Create users with minimal privileges
2. **Role Evolution**: Update access levels as responsibilities change
3. **Offboarding**: Promptly delete users when they leave

### Username Conventions

- Use email addresses for clarity and uniqueness
- Establish consistent naming conventions across your organization
- Be aware of case sensitivity

### Security

- Enforce strong password policies
- Grant minimum necessary access levels
- Regularly review user access
- Remove inactive accounts promptly

### Access Level Assignment

| Role Type               | Recommended Level |
| ----------------------- | ----------------- |
| Read-only analysts      | Read              |
| Application developers  | Write             |
| Team leads/managers     | Admin             |
| System administrators   | SuperAdmin        |

## API Reference

| Operation     | Endpoint                      | Method | Required Access |
| ------------- | ----------------------------- | ------ | --------------- |
| Create user   | `/api/v1/iam/users`           | POST   | Admin           |
| Get user      | `/api/v1/iam/users/{username}`| GET    | Admin           |
| Update user   | `/api/v1/iam/users/{username}`| PATCH  | Admin/Self      |
| Delete user   | `/api/v1/iam/users/{username}`| DELETE | Admin           |

## Related

- [Authentication](./authentication.md) - Login and tokens
- [RBAC](./rbac.md) - Fine-grained access control
- [Organizations](./organizations.md) - Organization management
