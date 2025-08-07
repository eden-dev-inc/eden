# MCP (Model Context Protocol) Implementation Guide

Eden's MCP (Model Context Protocol) integration provides AI models with secure, validated access to database endpoints through a standardized protocol. This enables AI applications to interact with databases safely while maintaining proper access controls and command validation.

## What is MCP in Eden?

MCP (Model Context Protocol) in Eden provides:
- **Secure Database Access**: AI models can execute database commands through validated, controlled interfaces
- **Multi-Database Support**: Unified interface for PostgreSQL, MongoDB, Redis, and ClickHouse
- **Command Validation**: Built-in safety checks to prevent dangerous operations
- **Tool-Based Interaction**: Structured tools for different database operations
- **Real-Time Communication**: Server-Sent Events (SSE) for live AI model interaction
- **Access Control Integration**: Full RBAC enforcement for AI operations

## Supported Database Types

Eden's MCP implementation supports the following databases with specialized tools:

### PostgreSQL MCP Tools
- **execute_postgres_query**: Execute SQL queries with parameterized support
- **Metadata Discovery**: Schema information and table structure
- **Transaction Support**: ACID-compliant database operations

### MongoDB MCP Tools
- **execute_mongo_command**: Execute MongoDB commands in JSON format
- **get_mongo_metadata**: Discover database and collection schemas
- **Aggregation Support**: Complex data processing pipelines

### Redis MCP Tools
- **execute_redis_command**: Execute Redis commands with full argument support
- **Data Structure Operations**: Support for all Redis data types
- **Caching Operations**: High-performance key-value operations

### ClickHouse MCP Tools
- **execute_clickhouse_query**: Execute analytical queries with parameters
- **get_clickhouse_metadata**: Database and table discovery
- **Analytics Support**: Optimized for large-scale data analysis

## MCP Server Architecture

### Core Components

1. **MCP Router**: Routes requests to appropriate database handlers
2. **Command Validators**: Ensure safe command execution
3. **Tool Definitions**: Structured interfaces for AI model interaction
4. **Transport Layer**: JSON-RPC over SSE for real-time communication
5. **Safety Classifications**: Three-tier safety system for commands

### Safety Classification System

Eden implements a three-tier safety system for database commands:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandSafety {
    Safe,      // Read-only operations, metadata queries
    Moderate,  // Data modification operations
    Dangerous, // Schema changes, user management, system operations
}
```

#### Safety Levels

- **Safe**: Read-only operations that cannot modify data or schema
- **Moderate**: Operations that modify data but are generally safe
- **Dangerous**: Operations requiring manual approval due to potential system impact

## MCP Server Setup and Usage

### Step 1: List Available MCP Servers for an Endpoint

```http
GET /api/v1/endpoints/{endpoint_id}/mcp
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "servers": [
    {
      "name": "postgres",
      "description": "Unified PostgreSQL query execution server"
    }
  ]
}
```

### Step 2: Connect to MCP Server via SSE

```http
GET /api/v1/endpoints/{endpoint_id}/mcp/{server_name}
Authorization: Bearer your_jwt_token
Accept: text/event-stream
Cache-Control: no-cache
```

This establishes a Server-Sent Events connection for real-time MCP communication.

### Step 3: MCP Tool Interaction

Once connected, AI models can interact through the MCP protocol using JSON-RPC messages.

## Database-Specific MCP Tools

### PostgreSQL MCP Tools

#### Execute PostgreSQL Query Tool

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "method": "tools/call",
  "params": {
    "name": "execute_postgres_query",
    "arguments": {
      "query": "SELECT * FROM users WHERE status = $1 LIMIT $2",
      "parameters": ["active", 10]
    }
  }
}
```

**Tool Schema:**
```json
{
  "name": "execute_postgres_query",
  "description": "Execute PostgreSQL queries with parameterized support",
  "input_schema": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "The SQL query to execute"
      },
      "parameters": {
        "type": "array",
        "description": "Optional parameters for parameterized queries",
        "items": {}
      }
    },
    "required": ["query"]
  }
}
```

### MongoDB MCP Tools

#### Execute MongoDB Command Tool

```json
{
  "jsonrpc": "2.0",
  "id": "2",
  "method": "tools/call",
  "params": {
    "name": "execute_mongo_command",
    "arguments": {
      "command": "{\"find\": \"users\", \"filter\": {\"status\": \"active\"}, \"limit\": 10}",
      "database": "myapp"
    }
  }
}
```

#### Get MongoDB Metadata Tool

```json
{
  "jsonrpc": "2.0",
  "id": "3",
  "method": "tools/call",
  "params": {
    "name": "get_mongo_metadata",
    "arguments": {
      "metadata_type": "schema"
    }
  }
}
```

**Example Response:**
```
MongoDB Schema Discovery
========================

Databases:
-----------

Database: myapp
  Collections:
    - users
    - orders
    - products

Database: analytics
  Collections:
    - events
    - metrics
```

### Redis MCP Tools

#### Execute Redis Command Tool

```json
{
  "jsonrpc": "2.0",
  "id": "4",
  "method": "tools/call",
  "params": {
    "name": "execute_redis_command",
    "arguments": {
      "command": "GET user:12345"
    }
  }
}
```

**Tool Schema:**
```json
{
  "name": "execute_redis_command",
  "description": "Execute Redis commands with full argument support",
  "input_schema": {
    "type": "object",
    "properties": {
      "command": {
        "type": "string",
        "description": "The full Redis command to execute, including all arguments"
      }
    },
    "required": ["command"]
  }
}
```

### ClickHouse MCP Tools

#### Execute ClickHouse Query Tool

```json
{
  "jsonrpc": "2.0",
  "id": "5",
  "method": "tools/call",
  "params": {
    "name": "execute_clickhouse_query",
    "arguments": {
      "query": "SELECT COUNT(*) FROM events WHERE date >= {start_date:Date} AND user_id = {user_id:UInt32}",
      "parameters": ["2024-01-01", 12345]
    }
  }
}
```

#### Get ClickHouse Metadata Tool

```json
{
  "jsonrpc": "2.0",
  "id": "6",
  "method": "tools/call",
  "params": {
    "name": "get_clickhouse_metadata",
    "arguments": {
      "object_type": "databases"
    }
  }
}
```

## Command Validation Examples

### Safe Commands (Automatically Approved)

#### PostgreSQL Safe Commands
```sql
SELECT * FROM users WHERE id = $1
EXPLAIN SELECT * FROM orders
SHOW TABLES
DESCRIBE users
```

#### MongoDB Safe Commands
```json
{"find": "users", "limit": 10}
{"listCollections": 1}
{"count": "orders"}
{"aggregate": [...]}
```

#### Redis Safe Commands
```
GET user:123
KEYS user:*
INFO
PING
```

### Moderate Commands (Data Modification)

#### PostgreSQL Moderate Commands
```sql
INSERT INTO users (name, email) VALUES ($1, $2)
UPDATE users SET last_login = NOW() WHERE id = $1
DELETE FROM sessions WHERE expired_at < NOW()
```

#### MongoDB Moderate Commands
```json
{"insert": "users", "documents": [...]}
{"update": "users", "updates": [...]}
{"delete": "logs", "deletes": [...]}
```

#### Redis Moderate Commands
```
SET user:123 "John Doe"
LPUSH queue:tasks "new_task"
DEL temp:*
```

### Dangerous Commands (Require Manual Approval)

#### PostgreSQL Dangerous Commands
```sql
DROP TABLE users
CREATE USER newuser
GRANT ALL PRIVILEGES ON database TO user
ALTER SYSTEM SET parameter = value
```

#### MongoDB Dangerous Commands
```json
{"drop": "users"}
{"dropDatabase": 1}
{"createUser": {...}}
{"shutdown": 1}
```

#### Redis Dangerous Commands
```
FLUSHALL
SHUTDOWN
CONFIG SET parameter value
CLIENT KILL
```

## Error Handling and Safety

### Command Validation Errors

When dangerous commands are attempted:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "error": {
    "code": -32603,
    "message": "Command 'DROP' is classified as dangerous and requires manual approval. This is a potentially dangerous PostgreSQL operation that requires careful consideration"
  }
}
```

### Command Parsing Errors

```json
{
  "jsonrpc": "2.0",
  "id": "2",
  "error": {
    "code": -32603,
    "message": "Invalid JSON command: Expected property name or '}' at line 1 column 2"
  }
}
```

### Database Execution Errors

```json
{
  "jsonrpc": "2.0",
  "id": "3",
  "error": {
    "code": -32603,
    "message": "PostgreSQL error: column 'nonexistent' does not exist"
  }
}
```

## Client Implementation Examples

### Python MCP Client

```python
import asyncio
import json
import httpx
from typing import Dict, Any

class EdenMCPClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache'
        }

    async def list_mcp_servers(self, endpoint_id: str) -> Dict[str, Any]:
        """List available MCP servers for an endpoint"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f'{self.base_url}/api/v1/endpoints/{endpoint_id}/mcp',
                headers={'Authorization': f'Bearer {self.token}'}
            )
            return response.json()

    async def connect_mcp_server(self, endpoint_id: str, server_name: str):
        """Connect to MCP server via SSE"""
        url = f'{self.base_url}/api/v1/endpoints/{endpoint_id}/mcp/{server_name}'
        
        async with httpx.AsyncClient() as client:
            async with client.stream('GET', url, headers=self.headers) as response:
                async for line in response.aiter_lines():
                    if line.startswith('data: '):
                        data = line[6:]  # Remove 'data: ' prefix
                        if data.strip():
                            yield json.loads(data)

    async def execute_tool(self, endpoint_id: str, server_name: str, 
                          tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool through MCP"""
        message = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        # Send message through SSE connection
        async for response in self.connect_mcp_server(endpoint_id, server_name):
            if response.get('id') == '1':
                return response
        
        raise Exception("No response received")

# Usage examples
async def main():
    client = EdenMCPClient('https://api.eden.com', 'your-jwt-token')
    
    # List available MCP servers
    servers = await client.list_mcp_servers('endpoint-uuid')
    print("Available servers:", servers)
    
    # Execute PostgreSQL query
    result = await client.execute_tool(
        'endpoint-uuid',
        'postgres',
        'execute_postgres_query',
        {
            'query': 'SELECT * FROM users LIMIT 5',
            'parameters': []
        }
    )
    print("Query result:", result)
    
    # Execute MongoDB command
    result = await client.execute_tool(
        'mongo-endpoint-uuid',
        'mongo',
        'execute_mongo_command',
        {
            'command': '{"find": "users", "limit": 5}',
            'database': 'myapp'
        }
    )
    print("MongoDB result:", result)

asyncio.run(main())
```

### JavaScript MCP Client

```javascript
class EdenMCPClient {
    constructor(baseUrl, token) {
        this.baseUrl = baseUrl;
        this.token = token;
    }

    async listMCPServers(endpointId) {
        const response = await fetch(`${this.baseUrl}/api/v1/endpoints/${endpointId}/mcp`, {
            headers: {
                'Authorization': `Bearer ${this.token}`
            }
        });
        return await response.json();
    }

    async connectMCPServer(endpointId, serverName) {
        const url = `${this.baseUrl}/api/v1/endpoints/${endpointId}/mcp/${serverName}`;
        
        const eventSource = new EventSource(url, {
            headers: {
                'Authorization': `Bearer ${this.token}`
            }
        });

        return new Promise((resolve, reject) => {
            eventSource.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    resolve(data);
                } catch (error) {
                    reject(error);
                }
            };

            eventSource.onerror = (error) => {
                reject(error);
            };
        });
    }

    async executeTool(endpointId, serverName, toolName, arguments) {
        const message = {
            jsonrpc: "2.0",
            id: "1",
            method: "tools/call",
            params: {
                name: toolName,
                arguments: arguments
            }
        };

        // For this example, we'll use a simplified approach
        // In practice, you'd send this through the SSE connection
        return await this.connectMCPServer(endpointId, serverName);
    }
}

// Usage examples
const client = new EdenMCPClient('https://api.eden.com', 'your-jwt-token');

// List MCP servers
client.listMCPServers('endpoint-uuid').then(servers => {
    console.log('Available servers:', servers);
});

// Execute Redis command
client.executeTool(
    'redis-endpoint-uuid',
    'redis',
    'execute_redis_command',
    { command: 'GET user:123' }
).then(result => {
    console.log('Redis result:', result);
});

// Execute ClickHouse query
client.executeTool(
    'clickhouse-endpoint-uuid',
    'clickhouse',
    'execute_clickhouse_query',
    {
        query: 'SELECT COUNT(*) FROM events WHERE date >= {start_date:Date}',
        parameters: ['2024-01-01']
    }
).then(result => {
    console.log('ClickHouse result:', result);
});
```

## Advanced MCP Usage Patterns

### AI-Driven Database Analysis

```python
async def ai_database_analysis(mcp_client: EdenMCPClient, endpoint_id: str):
    """Example of AI-driven database analysis using MCP"""
    
    # Get schema information
    schema_result = await mcp_client.execute_tool(
        endpoint_id,
        'postgres',
        'execute_postgres_query',
        {
            'query': '''
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
            '''
        }
    )
    
    # Analyze table sizes
    size_result = await mcp_client.execute_tool(
        endpoint_id,
        'postgres',
        'execute_postgres_query',
        {
            'query': '''
                SELECT 
                    schemaname,
                    tablename,
                    attname,
                    n_distinct,
                    correlation
                FROM pg_stats 
                WHERE schemaname = 'public'
                ORDER BY tablename, attname
            '''
        }
    )
    
    # Generate recommendations based on analysis
    return {
        'schema': schema_result,
        'statistics': size_result,
        'recommendations': analyze_schema_and_stats(schema_result, size_result)
    }

def analyze_schema_and_stats(schema_data, stats_data):
    """AI logic to analyze database schema and statistics"""
    recommendations = []
    
    # Example analysis logic
    for table_stat in stats_data.get('result', {}).get('rows', []):
        if table_stat.get('n_distinct', 0) < 10:
            recommendations.append({
                'type': 'index_suggestion',
                'table': table_stat['tablename'],
                'column': table_stat['attname'],
                'reason': 'Low cardinality column might benefit from indexing'
            })
    
    return recommendations
```

### Multi-Database Cross-Analysis

```python
async def cross_database_analysis(mcp_client: EdenMCPClient, 
                                postgres_endpoint: str, 
                                mongo_endpoint: str):
    """Analyze data across multiple database types"""
    
    # Get PostgreSQL user data
    pg_users = await mcp_client.execute_tool(
        postgres_endpoint,
        'postgres',
        'execute_postgres_query',
        {
            'query': 'SELECT id, email, created_at FROM users',
            'parameters': []
        }
    )
    
    # Get MongoDB activity data
    mongo_activities = await mcp_client.execute_tool(
        mongo_endpoint,
        'mongo',
        'execute_mongo_command',
        {
            'command': '{"find": "user_activities", "projection": {"user_id": 1, "activity_type": 1, "timestamp": 1}}',
            'database': 'analytics'
        }
    )
    
    # Cross-reference and analyze
    return correlate_user_activity(pg_users, mongo_activities)

def correlate_user_activity(users_data, activities_data):
    """Correlate user registration with activity patterns"""
    # Implementation would analyze the correlation between
    # user registration dates and activity patterns
    pass
```

## Security Considerations

### Command Validation
- All commands are validated before execution
- Dangerous operations require manual approval
- SQL injection protection through parameterized queries
- Command parsing and safety classification

### Access Control
- Full RBAC integration for MCP operations
- JWT token authentication required
- Endpoint-level permissions enforced
- Audit logging for all MCP operations

### Safe Defaults
- Unknown commands classified as "Moderate" safety level
- Read-only operations prioritized in tool design
- Automatic rejection of dangerous operations
- Comprehensive error handling and logging

## Best Practices for MCP Implementation

### 1. Tool Design
- **Single Responsibility**: Each tool should have a clear, focused purpose
- **Input Validation**: Validate all tool arguments before execution
- **Error Handling**: Provide clear, actionable error messages
- **Documentation**: Include comprehensive tool descriptions

### 2. Safety Management
- **Command Classification**: Properly classify commands by safety level
- **Approval Workflows**: Implement approval processes for dangerous operations
- **Audit Trails**: Log all MCP operations for security and debugging
- **Rate Limiting**: Implement appropriate rate limits for AI operations

### 3. AI Model Integration
- **Context Awareness**: Provide rich context about database schemas
- **Progressive Disclosure**: Start with safe operations, escalate as needed
- **Feedback Loops**: Use operation results to improve AI decision-making
- **Error Recovery**: Implement strategies for handling failed operations

### 4. Performance Optimization
- **Connection Pooling**: Leverage existing endpoint connection pools
- **Result Caching**: Cache frequently accessed metadata and results
- **Batch Operations**: Support batch processing where appropriate
- **Streaming**: Use streaming for large result sets

This comprehensive MCP implementation guide provides the foundation for secure, efficient AI-database interactions in your Eden environment, enabling sophisticated AI applications while maintaining proper security controls and operational safety.