# Users API

This reference covers the APIs for managing users in Eden-MDBS.

## Overview

Users are members of an organization with specific access permissions. Each user has credentials for authentication and can be assigned various access levels to resources.

## Create User

Create a new user in your organization.

### Request

```http
POST /api/v1/iam/users
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field      | Type   | Required | Description                     |
| ---------- | ------ | -------- | ------------------------------- |
| `id`       | string | Yes      | User identifier (email or ID)   |
| `password` | string | Yes      | User password                   |

### Example

```bash
curl http://{host}:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "developer@company.com",
    "password": "secure_password_123"
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "message": "User created successfully"
  }
}
```

## Get User

Retrieve details for a specific user.

### Request

```http
GET /api/v1/iam/users/{user}
Authorization: Bearer <token>
```

### Path Parameters

| Parameter | Type   | Description          |
| --------- | ------ | -------------------- |
| `user`    | string | User identifier      |

### Example

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "developer@company.com",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Update User

Update an existing user's information.

### Request

```http
PATCH /api/v1/iam/users/{user}
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field      | Type   | Required | Description        |
| ---------- | ------ | -------- | ------------------ |
| `password` | string | No       | New password       |

### Example

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "password": "new_secure_password_456"
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "message": "User updated successfully"
  }
}
```

## Delete User

Remove a user from your organization.

### Request

```http
DELETE /api/v1/iam/users/{user}
Authorization: Bearer <token>
```

### Path Parameters

| Parameter | Type   | Description          |
| --------- | ------ | -------------------- |
| `user`    | string | User identifier      |

### Example

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

Returns `204 No Content` on success.

## User Identification

Eden supports two formats for user identification:

### By User ID (String)

Use email addresses or custom user identifiers:

```json
{
  "id": "john.doe@company.com"
}
```

### By UUID

Use UUID format for user identification:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

The system automatically detects the format:
- If parseable as UUID → treated as User UUID
- Otherwise → treated as User ID string

## Access Control

| Operation      | Required Access |
| -------------- | --------------- |
| Create user    | Admin           |
| Get user       | Admin           |
| Update user    | Admin           |
| Delete user    | Admin           |

## Error Responses

### User Already Exists

```json
{
  "error": "Conflict",
  "message": "User already exists"
}
```

### User Not Found

```json
{
  "error": "Not Found",
  "message": "User not found"
}
```

### Invalid Password

```json
{
  "error": "Bad Request",
  "message": "Password does not meet requirements"
}
```

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "Admin access required to manage users"
}
```

## Related

- [Authentication](./authentication.md) - Login and tokens
- [RBAC APIs](./rbac.md) - Managing permissions
- [Users Guide](../guide/users.md) - User management concepts
