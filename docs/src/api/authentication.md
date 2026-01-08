# Authentication API

Eden-MDBS uses JWT (JSON Web Token) based authentication with support for username/password login and token refresh mechanisms.

## Authentication Flow

1. **Login**: Provide credentials via Basic Auth to get a JWT token
2. **API Access**: Use the JWT token in the Authorization header for API requests
3. **Token Refresh**: Refresh tokens before expiration to maintain session

## Login

Authenticate a user and receive a JWT token.

### Request

```http
POST /api/v1/auth/login
Authorization: Basic <base64(username:password)>
X-Org-Id: <organization_id>
```

### Required Headers

| Header          | Description                              |
| --------------- | ---------------------------------------- |
| `Authorization` | Basic auth with base64-encoded credentials |
| `X-Org-Id`      | Organization identifier                  |

### Basic Auth Header

The Authorization header uses Base64-encoded credentials:

```bash
# Encode credentials
echo -n "admin:password" | base64
# Result: YWRtaW46cGFzc3dvcmQ=

# Use in header
Authorization: Basic YWRtaW46cGFzc3dvcmQ=
```

### Example

```bash
curl http://{host}:8000/api/v1/auth/login \
  -u admin:password \
  -H "X-Org-Id: TestOrg" \
  -X POST
```

Or with explicit Basic auth header:

```bash
curl http://{host}:8000/api/v1/auth/login \
  -H "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
  -H "X-Org-Id: TestOrg" \
  -X POST
```

### Response

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiYWRtaW4iLCJ1c2VyX3V1aWQiOiI1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDAiLCJvcmdfaWQiOiJteV9jb21wYW55Iiwib3JnX3V1aWQiOiI1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDEiLCJleHAiOjE3MDU0OTI4MDAsImlhdCI6MTcwNTQwNjQwMH0.signature"
}
```

## Using JWT Tokens

Include the JWT token in the Authorization header for all authenticated API requests:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json"
```

## Token Refresh

Refresh your JWT token before it expires to maintain session continuity.

### Request

```http
POST /api/v1/auth/refresh
Authorization: Bearer <current_jwt_token>
```

### Example

```bash
curl http://{host}:8000/api/v1/auth/refresh \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

### Response

```json
{
  "status": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...new_token..."
  }
}
```

### Alternative: GET Method

Token refresh also supports GET requests:

```bash
curl http://{host}:8000/api/v1/auth/refresh \
  -H "Authorization: Bearer $TOKEN"
```

## JWT Token Structure

Eden JWT tokens contain these claims:

```json
{
  "user_id": "admin",
  "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "org_id": "my_company",
  "org_uuid": "550e8400-e29b-41d4-a716-446655440001",
  "exp": 1705492800,
  "iat": 1705406400
}
```

| Claim       | Description                             |
| ----------- | --------------------------------------- |
| `user_id`   | String identifier for the user          |
| `user_uuid` | UUID identifier for the user            |
| `org_id`    | String identifier for the organization  |
| `org_uuid`  | UUID identifier for the organization    |
| `exp`       | Token expiration timestamp (Unix epoch) |
| `iat`       | Token issued at timestamp (Unix epoch)  |

## User Identification Methods

Eden supports two methods for user identification:

### By User ID (String)

Use email addresses or custom user identifiers:

```
Username: john.doe@company.com
Password: mypassword123
```

### By User UUID

Use UUID format for user identification:

```
Username: 550e8400-e29b-41d4-a716-446655440000
Password: mypassword123
```

The system automatically detects the format:

- If parseable as UUID → treated as User UUID
- Otherwise → treated as User ID string

## Error Responses

### Missing Password

```json
{
  "error": "Bad Request",
  "message": "password not provided"
}
```

### Invalid Credentials

```json
{
  "error": "Unauthorized",
  "message": "Invalid credentials"
}
```

### User Not Found

```json
{
  "error": "Not Found",
  "message": "User not found in organization"
}
```

### Expired Token

```json
{
  "error": "Unauthorized",
  "message": "Token has expired"
}
```

### Invalid Token Format

```json
{
  "error": "Unauthorized",
  "message": "Invalid token format"
}
```

## Client-Side Token Validation

You can validate tokens client-side before making API calls:

```javascript
function isTokenValid(token) {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    const now = Math.floor(Date.now() / 1000);
    return payload.exp > now;
  } catch {
    return false;
  }
}

function getTokenExpiry(token) {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return new Date(payload.exp * 1000);
  } catch {
    return null;
  }
}
```

## Best Practices

### Token Storage

- **Browser apps**: Use `sessionStorage` or secure httpOnly cookies
- **Server apps**: Store in memory or secure environment variables
- **Never**: Store tokens in `localStorage` for sensitive applications

### Token Refresh Strategy

Refresh tokens proactively before expiration:

```javascript
// Refresh 5 minutes before expiry
const expiry = getTokenExpiry(token);
const refreshTime = expiry.getTime() - Date.now() - 5 * 60 * 1000;

if (refreshTime > 0) {
  setTimeout(async () => {
    const newToken = await refreshToken();
  }, refreshTime);
}
```

### Error Handling

Handle authentication errors gracefully:

```javascript
async function apiCall(endpoint) {
  const response = await fetch(endpoint, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (response.status === 401) {
    // Try to refresh token
    try {
      await refreshToken();
      return apiCall(endpoint); // Retry
    } catch {
      // Refresh failed, redirect to login
      logout();
    }
  }

  return response;
}
```

## Related

- [API Overview](./overview.md)
- [User Management](./users.md)
- [RBAC](./rbac.md)
