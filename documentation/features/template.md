# Templates Implementation Guide

Templates in Eden are reusable, parameterized components that define database operations, API calls, and data transformations. They serve as the building blocks for APIs, workflows, and migrations, providing a unified interface for executing various types of operations across different endpoints.

## What Are Templates?

Templates are structured definitions that:
- Define reusable operations that can be executed across different endpoints
- Support parameterization through Handlebars templating for dynamic content
- Provide type safety and validation for inputs and outputs
- Enable composition into higher-level constructs like APIs and workflows
- Support different operation types: Read, Write, Transaction, and TwoPhaseTransaction
- Integrate with RBAC for fine-grained access control

## Template Types and Operations

### Template Kinds

Eden supports four types of template operations:

1. **Read**: Query operations that retrieve data without modification
2. **Write**: Operations that modify data (INSERT, UPDATE, DELETE)
3. **Transaction**: Multi-operation transactions with rollback capability
4. **TwoPhaseTransaction**: Distributed transactions across multiple endpoints

### Template Structure

```json
{
  "id": "user_query_template",
  "description": "Retrieve user information with optional filtering",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT * FROM users WHERE status = '{{status}}' AND created_at > '{{since_date}}' LIMIT {{limit}}",
      "params": ["{{status}}", "{{since_date}}", "{{limit}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

## Creating Templates

### Step 1: Define Template Configuration

#### PostgreSQL Read Template
```json
{
  "id": "get_user_orders",
  "description": "Retrieve all orders for a specific user",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT o.*, p.name as product_name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = {{user_id}} ORDER BY o.created_at DESC",
      "params": ["{{user_id}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

#### PostgreSQL Write Template
```json
{
  "id": "create_user_order",
  "description": "Create a new order for a user", 
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Write",
    "template": {
      "query": "INSERT INTO orders (user_id, product_id, quantity, total_amount, status) VALUES ({{user_id}}, {{product_id}}, {{quantity}}, {{total_amount}}, '{{status}}') RETURNING id, created_at",
      "params": ["{{user_id}}", "{{product_id}}", "{{quantity}}", "{{total_amount}}", "{{status}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

#### Redis Cache Template
```json
{
  "id": "cache_user_session",
  "description": "Store user session data in cache",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440001",
    "kind": "Write",
    "template": {
      "operation": "set",
      "key": "user_session_{{user_id}}",
      "value": "{{session_data}}",
      "ttl": "{{session_ttl}}"
    },
    "endpoint_kind": "Redis"
  }
}
```

#### MongoDB Document Template
```json
{
  "id": "find_user_documents",
  "description": "Find documents by user criteria",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440002",
    "kind": "Read", 
    "template": {
      "collection": "user_documents",
      "operation": "find",
      "filter": {
        "user_id": "{{user_id}}",
        "document_type": "{{doc_type}}",
        "created_at": {
          "$gte": "{{start_date}}"
        }
      },
      "options": {
        "limit": "{{limit}}",
        "sort": {"created_at": -1}
      }
    },
    "endpoint_kind": "Mongo"
  }
}
```

#### HTTP API Template
```json
{
  "id": "external_user_sync",
  "description": "Sync user data with external service",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440003", 
    "kind": "Write",
    "template": {
      "method": "POST",
      "path": "/api/users/{{user_id}}/sync",
      "headers": {
        "Content-Type": "application/json",
        "X-API-Key": "{{api_key}}"
      },
      "body": {
        "user_data": "{{user_data}}",
        "sync_timestamp": "{{timestamp}}"
      }
    },
    "endpoint_kind": "Http"
  }
}
```

#### Transaction Template
```json
{
  "id": "transfer_funds_transaction",
  "description": "Transfer funds between accounts atomically",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Transaction",
    "template": {
      "operations": [
        {
          "query": "UPDATE accounts SET balance = balance - {{amount}} WHERE id = {{from_account}} AND balance >= {{amount}}",
          "params": ["{{amount}}", "{{from_account}}", "{{amount}}"]
        },
        {
          "query": "UPDATE accounts SET balance = balance + {{amount}} WHERE id = {{to_account}}",
          "params": ["{{amount}}", "{{to_account}}"]
        },
        {
          "query": "INSERT INTO transactions (from_account, to_account, amount, transaction_type, status) VALUES ({{from_account}}, {{to_account}}, {{amount}}, 'transfer', 'completed')",
          "params": ["{{from_account}}", "{{to_account}}", "{{amount}}"]
        }
      ]
    },
    "endpoint_kind": "Postgres"
  }
}
```

### Step 2: HTTP Request to Create Template

```http
POST /api/v1/templates
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "id": "get_user_orders",
  "description": "Retrieve all orders for a specific user",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT o.*, p.name as product_name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = {{user_id}} ORDER BY o.created_at DESC",
      "params": ["{{user_id}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

**Response:**
```json
{
  "status": "success", 
  "message": "success"
}
```

**What Happens During Creation:**
1. **RBAC Verification**: Admin access level required
2. **Template Parsing**: Handlebars template is parsed and validated
3. **Endpoint Validation**: System verifies the endpoint exists and is accessible
4. **Schema Storage**: Template configuration is stored in database
5. **Registry Update**: Template is added to the in-memory template registry for fast access
6. **Cache Initialization**: Template is cached for efficient execution

## Retrieving Templates

### Get Template Details

```http
GET /api/v1/templates/get_user_orders
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "get_user_orders",
    "uuid": "550e8400-e29b-41d4-a716-446655440004",
    "description": "Retrieve all orders for a specific user",
    "template": {
      "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
      "kind": "Read",
      "template": {
        "query": "SELECT o.*, p.name as product_name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = {{user_id}} ORDER BY o.created_at DESC",
        "params": ["{{user_id}}"]
      },
      "endpoint_kind": "Postgres"
    },
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Executing Templates

### Direct Template Execution

Templates can be executed directly with parameter substitution:

#### Render Template (Parse Only)

```http
POST /api/v1/templates/get_user_orders/render
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "user_id": 12345
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "request": {
      "query": "SELECT o.*, p.name as product_name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = 12345 ORDER BY o.created_at DESC",
      "params": [12345]
    }
  }
}
```

#### Run Template (Execute Operation)

```http
POST /api/v1/templates/get_user_orders
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "user_id": 12345
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rows": [
      {
        "id": 1001,
        "user_id": 12345,
        "product_id": 456,
        "product_name": "Wireless Headphones",
        "quantity": 2,
        "total_amount": 199.98,
        "status": "shipped",
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "id": 1000,
        "user_id": 12345,
        "product_id": 789,
        "product_name": "USB Cable",
        "quantity": 1,
        "total_amount": 15.99,
        "status": "delivered",
        "created_at": "2024-01-14T15:20:00Z"
      }
    ],
    "row_count": 2,
    "execution_time_ms": 45
  }
}
```

### Execution Flow

1. **Parameter Validation**: Input parameters are validated against template requirements
2. **Template Rendering**: Handlebars processes parameters and generates final query/operation
3. **RBAC Enforcement**: Access levels are checked based on operation type:
   - **Read Templates**: Require Read access
   - **Write Templates**: Require Write access
   - **Transaction Templates**: Require Write access
4. **Endpoint Execution**: Rendered template is executed against the target endpoint
5. **Response Processing**: Results are formatted and returned

## Updating Templates

### Modify Template Configuration

```http
PATCH /api/v1/templates/get_user_orders
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "description": "Retrieve all orders for a specific user with enhanced product details",
  "template": {
    "query": "SELECT o.*, p.name as product_name, p.category, p.price FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = {{user_id}} AND o.status != 'cancelled' ORDER BY o.created_at DESC",
    "params": ["{{user_id}}"]
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

**Update Process:**
1. **RBAC Verification**: Admin access required
2. **Template Validation**: New template configuration is validated
3. **Registry Update**: Template registry is updated with new configuration
4. **Database Update**: Changes are persisted to database
5. **Cache Refresh**: Template cache is updated for immediate availability

### Partial Updates

You can update individual fields:

```json
{
  "description": "Updated template description"
}
```

```json
{
  "template": {
    "query": "SELECT * FROM users WHERE id = {{user_id}}"
  }
}
```

## Template Parameters and Handlebars

### Parameter Substitution

Templates use Handlebars for parameter substitution:

```handlebars
SELECT * FROM users 
WHERE status = '{{status}}' 
AND created_at > '{{since_date}}'
{{#if include_deleted}}
AND deleted_at IS NOT NULL
{{else}}
AND deleted_at IS NULL
{{/if}}
LIMIT {{limit}}
```

### Advanced Handlebars Features

#### Conditional Logic
```handlebars
SELECT * FROM orders
WHERE user_id = {{user_id}}
{{#if status}}
AND status = '{{status}}'
{{/if}}
{{#if date_range}}
AND created_at BETWEEN '{{date_range.start}}' AND '{{date_range.end}}'
{{/if}}
```

#### Loops and Arrays
```handlebars
SELECT * FROM products 
WHERE id IN (
{{#each product_ids}}
  {{this}}{{#unless @last}},{{/unless}}
{{/each}}
)
```

#### Helper Functions
```handlebars
UPDATE users 
SET last_login = '{{now}}',
    updated_at = '{{now}}',
    login_count = login_count + 1
WHERE email = '{{escape email}}'
```

### Parameter Types

Templates support various parameter types:

```json
{
  "user_id": 12345,                    // Number
  "status": "active",                  // String
  "include_deleted": false,            // Boolean
  "date_range": {                      // Object
    "start": "2024-01-01",
    "end": "2024-01-31"
  },
  "product_ids": [101, 102, 103],      // Array
  "metadata": {                        // Complex Object
    "source": "api",
    "version": "1.2.3",
    "tags": ["urgent", "customer"]
  }
}
```

## Advanced Template Patterns

### Multi-Step Templates

Create templates that reference results from other templates:

```json
{
  "id": "user_with_order_summary",
  "description": "Get user info with order statistics",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "WITH user_orders AS (SELECT user_id, COUNT(*) as order_count, SUM(total_amount) as total_spent FROM orders WHERE user_id = {{user_id}} GROUP BY user_id) SELECT u.*, COALESCE(uo.order_count, 0) as order_count, COALESCE(uo.total_spent, 0) as total_spent FROM users u LEFT JOIN user_orders uo ON u.id = uo.user_id WHERE u.id = {{user_id}}",
      "params": ["{{user_id}}", "{{user_id}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

### Dynamic Query Construction

Build queries dynamically based on parameters:

```json
{
  "id": "flexible_user_search",
  "description": "Search users with flexible criteria",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT * FROM users WHERE 1=1 {{#if name}}AND name ILIKE '%{{name}}%'{{/if}} {{#if email}}AND email = '{{email}}'{{/if}} {{#if status}}AND status = '{{status}}'{{/if}} {{#if created_after}}AND created_at > '{{created_after}}'{{/if}} ORDER BY {{sort_by}} {{sort_order}} LIMIT {{limit}}",
      "params": []
    },
    "endpoint_kind": "Postgres"
  }
}
```

### Batch Processing Templates

Handle batch operations efficiently:

```json
{
  "id": "batch_update_user_status",
  "description": "Update status for multiple users",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Write",
    "template": {
      "query": "UPDATE users SET status = '{{new_status}}', updated_at = NOW() WHERE id = ANY(ARRAY[{{#each user_ids}}{{this}}{{#unless @last}},{{/unless}}{{/each}}])",
      "params": []
    },
    "endpoint_kind": "Postgres"
  }
}
```

## Error Handling

### Common Template Errors

1. **Template Not Found**
```json
{
  "error": "Template not found",
  "template_id": "nonexistent_template",
  "suggestion": "Verify template ID and ensure it exists in your organization"
}
```

2. **Parameter Missing**
```json
{
  "error": "Required parameter missing",
  "parameter": "user_id",
  "template": "get_user_orders",
  "suggestion": "Provide all required parameters for template execution"
}
```

3. **Invalid Template Syntax**
```json
{
  "error": "Handlebars parsing error",
  "details": "Unclosed handlebars expression at line 2",
  "template": "get_user_orders"
}
```

4. **Endpoint Execution Error**
```json
{
  "error": "Database execution failed",
  "details": "Column 'nonexistent_column' does not exist",
  "template": "get_user_orders",
  "rendered_query": "SELECT nonexistent_column FROM orders..."
}
```

5. **Access Denied**
```json
{
  "error": "Insufficient permissions",
  "required_level": "Write",
  "user_level": "Read", 
  "template": "create_user_order"
}
```

## Template Management Best Practices

### 1. Template Design
- **Single Responsibility**: Each template should have one clear purpose
- **Parameterization**: Use parameters for all variable content
- **SQL Safety**: Always use parameterized queries to prevent injection
- **Error Handling**: Include appropriate error handling in complex queries
- **Documentation**: Provide clear descriptions and parameter documentation

### 2. Parameter Management
- **Type Safety**: Validate parameter types in your application
- **Required vs Optional**: Clearly define which parameters are required
- **Default Values**: Use Handlebars helpers for default values when appropriate
- **Validation**: Implement parameter validation before template execution

### 3. Performance Optimization
- **Query Optimization**: Write efficient SQL with proper indexing
- **Parameter Caching**: Cache frequently used parameter combinations
- **Template Registry**: Leverage the built-in template registry for fast access
- **Connection Pooling**: Use endpoint connection pooling effectively

### 4. Security Considerations
- **RBAC Integration**: Properly configure access levels for templates
- **Parameter Sanitization**: Validate and sanitize all input parameters
- **Audit Logging**: Monitor template usage and modifications
- **Sensitive Data**: Avoid exposing sensitive information in template descriptions

### 5. Development Workflow
- **Version Control**: Track template changes in your version control system
- **Testing**: Test templates thoroughly with various parameter combinations
- **Staging**: Use separate environments for template development and testing
- **Rollback**: Maintain previous template versions for rollback capability

## Deleting Templates

### Remove Template

```http
DELETE /api/v1/templates/get_user_orders
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

**Deletion Process:**
1. **RBAC Verification**: Admin access level required
2. **Dependency Check**: Verify template is not used in active APIs or workflows
3. **Registry Removal**: Template is removed from the in-memory registry
4. **Database Cleanup**: Template configuration is deleted from database
5. **Cache Invalidation**: All cached template data is cleared

**Important Notes:**
- Templates used in APIs or workflows cannot be deleted until dependencies are removed
- Deletion is immediate and cannot be undone
- Consider updating templates instead of deleting when possible

## Template Client Implementation

### TypeScript Template Client

```typescript
interface TemplateConfig {
  id: string;
  description?: string;
  template: {
    endpoint_uuid: string;
    kind: 'Read' | 'Write' | 'Transaction' | 'TwoPhaseTransaction';
    template: any;
    endpoint_kind: string;
  };
}

interface TemplateUpdate {
  description?: string;
  template?: any;
}

class EdenTemplateClient {
  private baseUrl: string;
  private token: string;

  constructor(baseUrl: string, token: string) {
    this.baseUrl = baseUrl;
    this.token = token;
  }

  async createTemplate(config: TemplateConfig): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(config)
    });

    if (!response.ok) {
      throw new Error(`Failed to create template: ${response.statusText}`);
    }
  }

  async getTemplate(templateId: string): Promise<any> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates/${encodeURIComponent(templateId)}`, {
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to get template: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async renderTemplate(templateId: string, parameters: any): Promise<any> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates/${encodeURIComponent(templateId)}/render`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(parameters)
    });

    if (!response.ok) {
      throw new Error(`Failed to render template: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async executeTemplate(templateId: string, parameters: any): Promise<any> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates/${encodeURIComponent(templateId)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(parameters)
    });

    if (!response.ok) {
      throw new Error(`Failed to execute template: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async updateTemplate(templateId: string, updates: TemplateUpdate): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates/${encodeURIComponent(templateId)}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(updates)
    });

    if (!response.ok) {
      throw new Error(`Failed to update template: ${response.statusText}`);
    }
  }

  async deleteTemplate(templateId: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/templates/${encodeURIComponent(templateId)}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to delete template: ${response.statusText}`);
    }
  }
}

// Usage examples
const templates = new EdenTemplateClient('https://api.eden.com', 'your-jwt-token');

// Create a new template
await templates.createTemplate({
  id: 'get_user_profile',
  description: 'Retrieve complete user profile with preferences',
  template: {
    endpoint_uuid: '550e8400-e29b-41d4-a716-446655440000',
    kind: 'Read',
    template: {
      query: 'SELECT u.*, p.* FROM users u LEFT JOIN user_preferences p ON u.id = p.user_id WHERE u.id = {{user_id}}',
      params: ['{{user_id}}']
    },
    endpoint_kind: 'Postgres'
  }
});

// Get template details
const template = await templates.getTemplate('get_user_profile');
console.log('Template:', template);

// Render template without execution
const rendered = await templates.renderTemplate('get_user_profile', { user_id: 123 });
console.log('Rendered template:', rendered);

// Execute template
const result = await templates.executeTemplate('get_user_profile', { user_id: 123 });
console.log('Execution result:', result);

// Update template
await templates.updateTemplate('get_user_profile', {
  description: 'Enhanced user profile query with additional details'
});

// Delete template
await templates.deleteTemplate('get_user_profile');
```

### Python Template Client

```python
import requests
from typing import Dict, Any, Optional
from urllib.parse import quote

class EdenTemplateClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def create_template(self, config: Dict[str, Any]) -> None:
        """Create a new template"""
        response = requests.post(
            f'{self.base_url}/api/v1/templates',
            headers=self.headers,
            json=config
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to create template: {response.text}')

    def get_template(self, template_id: str) -> Dict[str, Any]:
        """Get template details"""
        encoded_id = quote(template_id, safe='')
        response = requests.get(
            f'{self.base_url}/api/v1/templates/{encoded_id}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get template: {response.text}')

    def render_template(self, template_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Render template without execution"""
        encoded_id = quote(template_id, safe='')
        response = requests.post(
            f'{self.base_url}/api/v1/templates/{encoded_id}/render',
            headers=self.headers,
            json=parameters
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to render template: {response.text}')

    def execute_template(self, template_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute template with parameters"""
        encoded_id = quote(template_id, safe='')
        response = requests.post(
            f'{self.base_url}/api/v1/templates/{encoded_id}',
            headers=self.headers,
            json=parameters
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to execute template: {response.text}')

    def update_template(self, template_id: str, updates: Dict[str, Any]) -> None:
        """Update template configuration"""
        encoded_id = quote(template_id, safe='')
        response = requests.patch(
            f'{self.base_url}/api/v1/templates/{encoded_id}',
            headers=self.headers,
            json=updates
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to update template: {response.text}')

    def delete_template(self, template_id: str) -> None:
        """Delete template"""
        encoded_id = quote(template_id, safe='')
        response = requests.delete(
            f'{self.base_url}/api/v1/templates/{encoded_id}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to delete template: {response.text}')

# Usage examples
templates = EdenTemplateClient('https://api.eden.com', 'your-jwt-token')

# Create template
templates.create_template({
    'id': 'user_analytics',
    'description': 'User behavior analytics query',
    'template': {
        'endpoint_uuid': '550e8400-e29b-41d4-a716-446655440000',
        'kind': 'Read',
        'template': {
            'query': '''
                SELECT 
                    u.id,
                    u.name,
                    COUNT(o.id) as order_count,
                    SUM(o.total_amount) as total_spent,
                    AVG(o.total_amount) as avg_order_value
                FROM users u 
                LEFT JOIN orders o ON u.id = o.user_id 
                WHERE u.created_at >= {{start_date}}
                GROUP BY u.id, u.name
                ORDER BY total_spent DESC
                LIMIT {{limit}}
            ''',
            'params': ['{{start_date}}', '{{limit}}']
        },
        'endpoint_kind': 'Postgres'
    }
})

# Execute template
result = templates.execute_template('user_analytics', {
    'start_date': '2024-01-01',
    'limit': 100
})

print(f"Analytics result: {result}")
```

## Integration with APIs and Workflows

Templates serve as the foundation for higher-level constructs:

### API Integration
Templates are bound to APIs through field mappings, enabling complex data flows and array processing.

### Workflow Integration  
Templates can be orchestrated in workflows for multi-step data processing and business logic execution.

### Migration Integration
Templates are used in migrations to perform schema changes, data transformations, and validation operations.

This comprehensive template implementation guide provides all the necessary information to successfully create, manage, and use templates in your Eden environment, enabling powerful and flexible data operations across various endpoint types.