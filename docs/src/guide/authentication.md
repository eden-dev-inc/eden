# Authentication Guide

This guide covers authentication in Eden-MDBS, including login, token management, and best practices for securing your API access.

## Authentication Overview

Eden uses JWT (JSON Web Token) based authentication:

1. **Login** with credentials to receive a JWT token
2. **Use the token** in API requests via the Authorization header
3. **Refresh the token** before it expires to maintain your session

## Logging In

### Basic Authentication

Use HTTP Basic Auth to login and receive a JWT token:

```bash
curl http://{host}:8000/api/v1/auth/login \
  -u username:password \
  -X POST
```

Or with an explicit Authorization header:

```bash
# Encode credentials
echo -n "username:password" | base64
# Result: dXNlcm5hbWU6cGFzc3dvcmQ=

curl http://{host}:8000/api/v1/auth/login \
  -H "Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=" \
  -X POST
```

### Response

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiYWRtaW4iLCJvcmdfaWQiOiJteV9jb21wYW55IiwiZXhwIjoxNzA1NDkyODAwfQ.signature"
}
```

## Using Your Token

Include the JWT token in the Authorization header for all API requests:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json"
```

### Environment Variable

For convenience, store your token in an environment variable:

```bash
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN"
```

## Token Refresh

Refresh your token before it expires to maintain uninterrupted access:

```bash
curl http://{host}:8000/api/v1/auth/refresh \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...new_token..."
  }
}
```

The refresh endpoint also supports GET requests:

```bash
curl http://{host}:8000/api/v1/auth/refresh \
  -H "Authorization: Bearer $TOKEN"
```

## JWT Token Structure

Eden JWT tokens contain these claims:

| Claim       | Description                             |
| ----------- | --------------------------------------- |
| `user_id`   | String identifier for the user          |
| `user_uuid` | UUID identifier for the user            |
| `org_id`    | String identifier for the organization  |
| `org_uuid`  | UUID identifier for the organization    |
| `exp`       | Token expiration timestamp (Unix epoch) |
| `iat`       | Token issued at timestamp (Unix epoch)  |

### Decoding a Token

You can decode the payload (middle part) of a JWT to see its contents:

```bash
echo "eyJ1c2VyX2lkIjoiYWRtaW4iLCJvcmdfaWQiOiJteV9jb21wYW55IiwiZXhwIjoxNzA1NDkyODAwfQ" | base64 -d
```

## User Identification

Eden supports two methods for user identification during login:

### By Username (String)

Use email addresses or custom identifiers:

```
Username: john.doe@company.com
Password: mypassword123
```

### By User UUID

Use UUID format:

```
Username: 550e8400-e29b-41d4-a716-446655440000
Password: mypassword123
```

The system automatically detects the format and handles accordingly.

## Client-Side Token Validation

Validate tokens client-side before making API calls:

### JavaScript

```javascript
function isTokenExpired(token) {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    const now = Math.floor(Date.now() / 1000);
    return payload.exp <= now;
  } catch {
    return true;
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

### Python

```python
import base64
import json
from datetime import datetime

def is_token_expired(token):
    try:
        payload = token.split('.')[1]
        # Add padding if needed
        payload += '=' * (4 - len(payload) % 4)
        decoded = json.loads(base64.b64decode(payload))
        return decoded['exp'] <= datetime.now().timestamp()
    except:
        return True

def get_token_expiry(token):
    try:
        payload = token.split('.')[1]
        payload += '=' * (4 - len(payload) % 4)
        decoded = json.loads(base64.b64decode(payload))
        return datetime.fromtimestamp(decoded['exp'])
    except:
        return None
```

## Automatic Token Refresh

Implement automatic token refresh to maintain sessions:

### JavaScript Example

```javascript
class EdenClient {
  constructor(baseUrl) {
    this.baseUrl = baseUrl;
    this.token = null;
    this.refreshTimer = null;
  }

  async login(username, password) {
    const credentials = btoa(`${username}:${password}`);
    const response = await fetch(`${this.baseUrl}/api/v1/auth/login`, {
      method: "POST",
      headers: { Authorization: `Basic ${credentials}` },
    });

    const data = await response.json();
    this.token = data.token;
    this.scheduleRefresh();
    return this.token;
  }

  scheduleRefresh() {
    if (this.refreshTimer) clearTimeout(this.refreshTimer);

    const expiry = this.getTokenExpiry();
    if (!expiry) return;

    // Refresh 5 minutes before expiry
    const refreshTime = expiry.getTime() - Date.now() - 5 * 60 * 1000;

    if (refreshTime > 0) {
      this.refreshTimer = setTimeout(() => this.refresh(), refreshTime);
    }
  }

  async refresh() {
    const response = await fetch(`${this.baseUrl}/api/v1/auth/refresh`, {
      method: "POST",
      headers: { Authorization: `Bearer ${this.token}` },
    });

    const data = await response.json();
    this.token = data.data.token;
    this.scheduleRefresh();
    return this.token;
  }

  getTokenExpiry() {
    try {
      const payload = JSON.parse(atob(this.token.split(".")[1]));
      return new Date(payload.exp * 1000);
    } catch {
      return null;
    }
  }

  async request(endpoint, options = {}) {
    if (this.isTokenExpired()) {
      await this.refresh();
    }

    return fetch(`${this.baseUrl}${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        Authorization: `Bearer ${this.token}`,
      },
    });
  }

  isTokenExpired() {
    const expiry = this.getTokenExpiry();
    return !expiry || expiry <= new Date();
  }
}
```

## Error Handling

### Missing or Invalid Credentials

```json
{
  "error": "Unauthorized",
  "message": "Invalid credentials"
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

### User Not Found

```json
{
  "error": "Not Found",
  "message": "User not found in organization"
}
```

## Best Practices

### Token Storage

| Environment     | Recommended Storage                    |
| --------------- | -------------------------------------- |
| Browser apps    | `sessionStorage` or httpOnly cookies   |
| Server apps     | Memory or secure environment variables |
| Mobile apps     | Secure keychain/keystore               |

**Never** store tokens in `localStorage` for sensitive applications.

### Security Recommendations

1. **Always use HTTPS** in production
2. **Set appropriate token expiration** times
3. **Implement token refresh** before expiration
4. **Handle 401 errors** gracefully by refreshing or re-authenticating
5. **Clear tokens** on logout

### Error Recovery

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

- [API Overview](../api/overview.md) - Full API reference
- [Users](./users.md) - User management
- [RBAC](./rbac.md) - Access control
