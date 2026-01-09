# MCP Tooling API

This reference covers Eden's MCP (Model Context Protocol) endpoints. MCP servers expose tool APIs that can be used by LLM clients and automation.

## Overview

Each database endpoint can expose one or more MCP servers. Clients discover the available servers, then connect using the streamable HTTP transport. Streamable HTTP uses:

- POST for client-to-server JSON-RPC messages
- GET for server-to-client Server-Sent Events (SSE)
- DELETE to close the session

## List MCP servers

Get the MCP servers available for an endpoint.

### Request

```http
GET /api/v1/endpoints/{id}/mcp
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/{id}/mcp \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "servers": [
    {
      "name": "mongodb",
      "description": "MongoDB MCP server"
    }
  ]
}
```

## Streamable HTTP transport

Use the MCP server name to connect over streamable HTTP.

### Base URL

```
/api/v1/endpoints/{id}/mcp/{mcp_server}
```

### Required headers

POST requests:

- `Authorization: Bearer <token>`
- `Accept: application/json, text/event-stream`
- `Content-Type: application/json`

GET requests:

- `Authorization: Bearer <token>`
- `Accept: text/event-stream`

Session headers:

- `Mcp-Session-Id` is returned by the server and must be sent on subsequent requests
- `Last-Event-Id` is optional and used to resume SSE streams

### Session flow (high level)

1. Send an MCP initialize request with POST and no `Mcp-Session-Id`.
2. The response opens an SSE stream and returns `Mcp-Session-Id`.
3. Include `Mcp-Session-Id` on subsequent POST and GET requests.
4. Use GET to open an SSE stream for server notifications and progress.
5. Send DELETE with `Mcp-Session-Id` when you are done.

### Notes

- POST responses may be a single JSON response or an SSE stream, depending on the request.
- Use an MCP client library when possible to manage SSE streaming and reconnects.

## Built-in MCP servers

### Migrations MCP server

Base URL:

```
/api/v1/mcp/migrations
```

This server exposes migration management tools over MCP. Use the same streamable HTTP headers and session flow described above.

Available tools:

- `list_migrations` (optional `verbose`, `updated_since`, `status`, `limit`, `cursor`)
- `get_migration` (`migration_id`, optional `verbose`)
- `define_migration` (`id`, optional `description`, `strategy`, `data`, `failure_handling`, optional `apis`, optional `interlays`)
- `run_migration` (`migration_id`, `mode` = `test|migrate`, optional `wait`)
