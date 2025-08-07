# Authentication Implementation Guide

Eden uses JWT (JSON Web Token) based authentication with support for username/password login and token refresh mechanisms. This guide provides detailed implementation instructions for integrating authentication into your applications and managing user sessions securely.

## Authentication Overview

Eden's authentication system provides:
- **Basic Authentication**: Username/password login with organization context
- **JWT Token Management**: Secure token generation and validation
- **Token Refresh**: Seamless token renewal without re-authentication
- **Organization Scoping**: All authentication is scoped to specific organizations
- **Flexible User Identification**: Support for both User ID and User UUID
- **Comprehensive Logging**: Full telemetry and audit trail

## Authentication Flow

1. **Initial Login**: User provides credentials via Basic Auth
2. **Credential Verification**: System validates username/password against database
3. **JWT Generation**: Upon successful auth, system generates signed JWT token
4. **API Access**: Client uses JWT token for subsequent API requests
5. **Token Refresh**: Before expiration, client can refresh token without re-authentication

## User Login Implementation

### Step 1: Basic Authentication Login

```http
POST /api/v1/auth/login
Content-Type: application/json
Authorization: Basic base64(username:password)

{
  "id": "my_organization"
}
```

**Authentication Header Format:**
```
Authorization: Basic <base64-encoded-credentials>
```

Where `<base64-encoded-credentials>` is the Base64 encoding of `username:password`.

**Example with curl:**
```bash
# Encode credentials
echo -n "john.doe@company.com:mypassword123" | base64
# Result: am9obi5kb2VAY29tcGFueS5jb206bXlwYXNzd29yZDEyMw==

# Make login request
curl -X POST "https://api.eden.com/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic am9obi5kb2VAY29tcGFueS5jb206bXlwYXNzd29yZDEyMw==" \
  -d '{"id": "my_organization"}'
```

### Step 2: Successful Login Response

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiam9obi5kb2VAY29tcGFueS5jb20iLCJ1c2VyX3V1aWQiOiI1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDAiLCJvcmdfaWQiOiJteV9vcmdhbml6YXRpb24iLCJvcmdfdXVpZCI6IjU1MGU4NDAwLWUyOWItNDFkNC1hNzE2LTQ0NjY1NTQ0MDAwMSIsImV4cCI6MTcwNTQ5MjgwMCwiaWF0IjoxNzA1NDA2NDAwfQ.signature"
}
```

### Step 3: Using JWT Token for API Requests

Include the JWT token in the Authorization header for all subsequent API requests:

```http
GET /api/v1/endpoints
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiam9obi5kb2VAY29tcGFueS5jb20iLCJ1c2VyX3V1aWQiOiI1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDAiLCJvcmdfaWQiOiJteV9vcmdhbml6YXRpb24iLCJvcmdfdXVpZCI6IjU1MGU4NDAwLWUyOWItNDFkNC1hNzE2LTQ0NjY1NTQ0MDAwMSIsImV4cCI6MTcwNTQ5MjgwMCwiaWF0IjoxNzA1NDA2NDAwfQ.signature
```

## Token Refresh Implementation

### Refresh JWT Token

Before your token expires, refresh it to maintain session continuity:

```http
POST /api/v1/auth/refresh
Authorization: Bearer <current-jwt-token>
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiam9obi5kb2VAY29tcGFueS5jb20iLCJ1c2VyX3V1aWQiOiI1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDAiLCJvcmdfaWQiOiJteV9vcmdhbml6YXRpb24iLCJvcmdfdXVpZCI6IjU1MGU4NDAwLWUyOWItNDFkNC1hNzE2LTQ0NjY1NTQ0MDAwMSIsImV4cCI6MTcwNTU3OTIwMCwiaWF0IjoxNzA1NDkyODAwfQ.new-signature"
  }
}
```

### Alternative GET Method for Refresh

You can also use GET for token refresh:

```http
GET /api/v1/auth/refresh
Authorization: Bearer <current-jwt-token>
```

This returns the same response format as the POST method.

## User Identification Methods

Eden supports two methods for user identification:

### 1. User ID (String-based)
Use email addresses or custom user identifiers:
```
Username: john.doe@company.com
Password: mypassword123
```

### 2. User UUID (UUID-based)
Use UUID format for user identification:
```
Username: 550e8400-e29b-41d4-a716-446655440000
Password: mypassword123
```

**System Behavior:**
- If the username can be parsed as a UUID, the system treats it as a User UUID
- If parsing fails, the system treats it as a User ID string
- Both methods work seamlessly with the same authentication endpoints

## JWT Token Structure

Eden JWT tokens contain the following claims:

```json
{
  "user_id": "john.doe@company.com",
  "user_uuid": "550e8400-e29b-41d4-a716-446655440000", 
  "org_id": "my_organization",
  "org_uuid": "550e8400-e29b-41d4-a716-446655440001",
  "exp": 1705492800,
  "iat": 1705406400
}
```

**Token Claims Explained:**
- `user_id`: String identifier for the user (email, username, etc.)
- `user_uuid`: UUID identifier for the user 
- `org_id`: String identifier for the organization
- `org_uuid`: UUID identifier for the organization
- `exp`: Token expiration timestamp (Unix epoch)
- `iat`: Token issued at timestamp (Unix epoch)

## Client Implementation Examples

### JavaScript/TypeScript Implementation

```typescript
class EdenAuth {
  private baseUrl: string;
  private token: string | null = null;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  async login(username: string, password: string, orgId: string): Promise<string> {
    const credentials = btoa(`${username}:${password}`);
    
    const response = await fetch(`${this.baseUrl}/api/v1/auth/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${credentials}`
      },
      body: JSON.stringify({ id: orgId })
    });

    if (!response.ok) {
      throw new Error('Login failed');
    }

    const data = await response.json();
    this.token = data.token;
    return this.token;
  }

  async refreshToken(): Promise<string> {
    if (!this.token) {
      throw new Error('No token to refresh');
    }

    const response = await fetch(`${this.baseUrl}/api/v1/auth/refresh`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error('Token refresh failed');
    }

    const data = await response.json();
    this.token = data.data.token;
    return this.token;
  }

  async apiCall(endpoint: string, options: RequestInit = {}): Promise<Response> {
    if (!this.token) {
      throw new Error('Not authenticated');
    }

    return fetch(`${this.baseUrl}${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        'Authorization': `Bearer ${this.token}`
      }
    });
  }

  getToken(): string | null {
    return this.token;
  }

  logout(): void {
    this.token = null;
  }
}

// Usage example
const auth = new EdenAuth('https://api.eden.com');

try {
  const token = await auth.login('john.doe@company.com', 'password123', 'my_org');
  console.log('Logged in successfully:', token);
  
  // Make authenticated API calls
  const response = await auth.apiCall('/api/v1/endpoints');
  const endpoints = await response.json();
  
  // Refresh token when needed
  const newToken = await auth.refreshToken();
  console.log('Token refreshed:', newToken);
} catch (error) {
  console.error('Authentication error:', error);
}
```

### Python Implementation

```python
import base64
import requests
import json
from typing import Optional

class EdenAuth:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.token: Optional[str] = None

    def login(self, username: str, password: str, org_id: str) -> str:
        """Login and get JWT token"""
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        
        response = requests.post(
            f"{self.base_url}/api/v1/auth/login",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {credentials}"
            },
            json={"id": org_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            self.token = data["token"]
            return self.token
        else:
            raise Exception(f"Login failed: {response.status_code} {response.text}")

    def refresh_token(self) -> str:
        """Refresh the current JWT token"""
        if not self.token:
            raise Exception("No token to refresh")
            
        response = requests.post(
            f"{self.base_url}/api/v1/auth/refresh",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        
        if response.status_code == 200:
            data = response.json()
            self.token = data["data"]["token"]
            return self.token
        else:
            raise Exception(f"Token refresh failed: {response.status_code}")

    def api_call(self, endpoint: str, method: str = "GET", **kwargs) -> requests.Response:
        """Make authenticated API call"""
        if not self.token:
            raise Exception("Not authenticated")
            
        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {self.token}"
        kwargs["headers"] = headers
        
        return requests.request(method, f"{self.base_url}{endpoint}", **kwargs)

    def logout(self):
        """Clear the token"""
        self.token = None

# Usage example
auth = EdenAuth("https://api.eden.com")

try:
    # Login
    token = auth.login("john.doe@company.com", "password123", "my_org")
    print(f"Logged in successfully: {token}")
    
    # Make authenticated API calls
    response = auth.api_call("/api/v1/endpoints")
    endpoints = response.json()
    print(f"Endpoints: {endpoints}")
    
    # Refresh token
    new_token = auth.refresh_token()
    print(f"Token refreshed: {new_token}")
    
except Exception as e:
    print(f"Authentication error: {e}")
```

## Error Handling

### Common Authentication Errors

1. **Missing Password**
```json
{
  "error": "Bad Request",
  "message": "password not provided"
}
```

2. **Invalid Credentials**
```json
{
  "error": "Unauthorized", 
  "message": "Invalid credentials"
}
```

3. **Organization Not Found**
```json
{
  "error": "Not Found",
  "message": "Organization not found"
}
```

4. **User Not Found**
```json
{
  "error": "Not Found",
  "message": "User not found in organization"
}
```

5. **Expired Token**
```json
{
  "error": "Unauthorized",
  "message": "Token has expired"
}
```

6. **Invalid Token Format**
```json
{
  "error": "Unauthorized", 
  "message": "Invalid token format"
}
```

### Error Handling Best Practices

```typescript
async function handleAuthenticatedRequest(auth: EdenAuth, endpoint: string) {
  try {
    const response = await auth.apiCall(endpoint);
    
    if (response.status === 401) {
      // Token might be expired, try to refresh
      try {
        await auth.refreshToken();
        // Retry the original request
        return await auth.apiCall(endpoint);
      } catch (refreshError) {
        // Refresh failed, need to re-login
        auth.logout();
        throw new Error('Session expired, please login again');
      }
    }
    
    return response;
  } catch (error) {
    console.error('API request failed:', error);
    throw error;
  }
}
```

## Security Considerations

### Token Storage

**Browser Applications:**
- **sessionStorage**: Good for single-session storage
- **localStorage**: Persistent storage but vulnerable to XSS
- **Secure Cookies**: Most secure option with httpOnly flag
- **Memory Only**: Most secure but lost on page refresh

```typescript
// Secure token storage example
class SecureTokenStorage {
  private static readonly TOKEN_KEY = 'eden_auth_token';

  static setToken(token: string): void {
    // Use secure, httpOnly cookie in production
    if (this.isProduction()) {
      document.cookie = `${this.TOKEN_KEY}=${token}; secure; httpOnly; samesite=strict`;
    } else {
      sessionStorage.setItem(this.TOKEN_KEY, token);
    }
  }

  static getToken(): string | null {
    if (this.isProduction()) {
      // Extract from cookie
      const match = document.cookie.match(new RegExp(`${this.TOKEN_KEY}=([^;]+)`));
      return match ? match[1] : null;
    } else {
      return sessionStorage.getItem(this.TOKEN_KEY);
    }
  }

  static clearToken(): void {
    if (this.isProduction()) {
      document.cookie = `${this.TOKEN_KEY}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
    } else {
      sessionStorage.removeItem(this.TOKEN_KEY);
    }
  }

  private static isProduction(): boolean {
    return process.env.NODE_ENV === 'production';
  }
}
```

### Token Validation

**Client-Side Validation:**
```typescript
function isTokenValid(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    const now = Math.floor(Date.now() / 1000);
    return payload.exp > now;
  } catch {
    return false;
  }
}

function getTokenExpiry(token: string): Date | null {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    return new Date(payload.exp * 1000);
  } catch {
    return null;
  }
}
```

### Automatic Token Refresh

```typescript
class AutoRefreshAuth extends EdenAuth {
  private refreshTimer: NodeJS.Timeout | null = null;

  async login(username: string, password: string, orgId: string): Promise<string> {
    const token = await super.login(username, password, orgId);
    this.scheduleTokenRefresh(token);
    return token;
  }

  private scheduleTokenRefresh(token: string): void {
    const expiry = getTokenExpiry(token);
    if (!expiry) return;

    // Refresh 5 minutes before expiry
    const refreshTime = expiry.getTime() - Date.now() - (5 * 60 * 1000);
    
    if (refreshTime > 0) {
      this.refreshTimer = setTimeout(async () => {
        try {
          const newToken = await this.refreshToken();
          this.scheduleTokenRefresh(newToken);
        } catch (error) {
          console.error('Auto-refresh failed:', error);
          this.logout();
        }
      }, refreshTime);
    }
  }

  logout(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
    super.logout();
  }
}
```

## Integration with RBAC

Authentication tokens contain organization context that integrates with Eden's RBAC system:

```typescript
// Extract user context from token for RBAC decisions
function getUserContext(token: string) {
  const payload = JSON.parse(atob(token.split('.')[1]));
  return {
    userId: payload.user_id,
    userUuid: payload.user_uuid,
    orgId: payload.org_id,
    orgUuid: payload.org_uuid
  };
}

// Check if user has access to specific resource
async function checkAccess(auth: EdenAuth, resourceType: string, resourceId: string) {
  const response = await auth.apiCall(`/api/v1/iam/rbac/${resourceType}/${resourceId}/subjects`);
  return response.ok;
}
```

## Best Practices for Implementation

### 1. Session Management
- **Token Expiry**: Implement proper token expiry handling
- **Automatic Refresh**: Set up automatic token refresh before expiry
- **Graceful Logout**: Clear tokens and session data on logout
- **Session Validation**: Validate tokens before making API calls

### 2. Error Handling
- **Retry Logic**: Implement retry logic for token refresh failures
- **Fallback Authentication**: Handle cases where refresh fails
- **User Feedback**: Provide clear error messages to users
- **Logging**: Log authentication events for debugging and security

### 3. Security
- **HTTPS Only**: Always use HTTPS in production
- **Secure Storage**: Use secure storage mechanisms for tokens
- **Token Validation**: Validate token format and expiry client-side
- **CSP Headers**: Implement Content Security Policy headers

### 4. User Experience
- **Persistent Sessions**: Implement remember me functionality
- **Background Refresh**: Refresh tokens in background without user interaction
- **Loading States**: Show appropriate loading states during authentication
- **Error Recovery**: Provide clear paths for error recovery

## Testing Authentication

### Unit Test Example

```typescript
describe('EdenAuth', () => {
  let auth: EdenAuth;
  
  beforeEach(() => {
    auth = new EdenAuth('https://api.test.com');
  });

  test('successful login returns token', async () => {
    // Mock successful login response
    jest.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ token: 'mock-jwt-token' })
    } as Response);

    const token = await auth.login('test@example.com', 'password', 'test-org');
    
    expect(token).toBe('mock-jwt-token');
    expect(auth.getToken()).toBe('mock-jwt-token');
  });

  test('failed login throws error', async () => {
    jest.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: false,
      status: 401
    } as Response);

    await expect(auth.login('test@example.com', 'wrong-password', 'test-org'))
      .rejects.toThrow('Login failed');
  });
});
```

This comprehensive authentication implementation guide provides all the necessary information to successfully integrate Eden's JWT-based authentication system into your applications, ensuring secure and reliable user session management.