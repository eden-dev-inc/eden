# Troubleshooting

This guide helps you diagnose and resolve common issues when using Eden-MDBS.

## Authentication Issues

### "Unauthorized" Error

**Symptoms:** API returns 401 status code with "Unauthorized" error.

**Possible Causes:**

1. **Expired token**
   - Check if your JWT token has expired
   - Solution: Refresh your token or re-authenticate

2. **Missing Authorization header**
   - Ensure you're including the header: `Authorization: Bearer <token>`

3. **Invalid credentials during login**
   - Verify username and password are correct
   - Check for typos or extra whitespace

**Diagnosis:**

```bash
# Check token expiry (decode the payload)
echo "<your_token_payload>" | base64 -d | jq '.exp'

# Compare with current timestamp
date +%s
```

### "Forbidden" Error

**Symptoms:** API returns 403 status code.

**Possible Causes:**

1. **Insufficient access level**
   - You have a valid token but lack permission for the operation
   - Solution: Request higher access level from an Admin

2. **Resource-specific restriction**
   - You may have organization access but not endpoint-specific access
   - Solution: Ask Admin to grant access to the specific resource

**Diagnosis:**

```bash
# Check your access level for an endpoint
curl http://{host}:8000/api/v1/iam/rbac/endpoints/{endpoint_id}/subjects \
  -H "Authorization: Bearer $TOKEN"
```

## Connection Issues

### "Connection Timeout"

**Symptoms:** Queries fail with connection timeout errors.

**Possible Causes:**

1. **Database server unreachable**
   - Network connectivity issues
   - Firewall blocking connections
   - Database server is down

2. **Incorrect connection URL**
   - Wrong host, port, or database name in endpoint config

**Solutions:**

- Verify the database server is running and accessible
- Check network connectivity between Eden and the database
- Verify the endpoint configuration

### "Connection Pool Exhausted"

**Symptoms:** Requests fail with "connection pool exhausted" error.

**Possible Causes:**

1. **Too many concurrent requests**
2. **Long-running queries holding connections**
3. **Pool size too small for workload**

**Solutions:**

- Contact your administrator to increase pool size
- Optimize long-running queries
- Implement connection retry logic with backoff

### "Connection Refused"

**Symptoms:** Endpoint operations fail with connection refused.

**Possible Causes:**

1. **Database not running**
2. **Wrong port number**
3. **Database not accepting connections from Eden's IP**

**Solutions:**

- Verify database server is running
- Check port configuration
- Ensure firewall rules allow connections

## Query Errors

### "SQL Syntax Error"

**Symptoms:** Query fails with syntax error message.

**Possible Causes:**

1. **Invalid SQL syntax**
2. **Wrong parameter placeholder format**
3. **Database-specific syntax issues**

**Solutions:**

- Verify SQL syntax is correct
- Use `$1, $2, $3` placeholders for PostgreSQL
- Use `?` placeholders for MySQL
- Test query directly against database first

**Example Fix:**

```bash
# Wrong - string concatenation
{"query": "SELECT * FROM users WHERE id = 123"}

# Correct - parameterized
{"query": "SELECT * FROM users WHERE id = $1", "params": [123]}
```

### "Parameter Count Mismatch"

**Symptoms:** Query fails with "wrong number of parameters" error.

**Possible Causes:**

- Number of `params` doesn't match placeholders in query

**Solution:**

```bash
# Query has 3 placeholders, provide 3 params
{
  "query": "SELECT * FROM users WHERE status = $1 AND role = $2 LIMIT $3",
  "params": ["active", "admin", 10]
}
```

### "Column Not Found"

**Symptoms:** Query fails with "column does not exist" error.

**Possible Causes:**

1. **Typo in column name**
2. **Wrong table schema**
3. **Case sensitivity issues**

**Solutions:**

- Verify column names match database schema
- Check table structure
- Use double quotes for case-sensitive identifiers in PostgreSQL

## Transaction Errors

### "Transaction Rolled Back"

**Symptoms:** Transaction fails and all operations are rolled back.

**Possible Causes:**

1. **Constraint violation** in one of the operations
2. **Deadlock detected**
3. **Timeout exceeded**

**Solutions:**

- Check constraint definitions
- Ensure operations are ordered consistently to prevent deadlocks
- Break large transactions into smaller ones

### "Deadlock Detected"

**Symptoms:** Transaction fails with deadlock error.

**Possible Causes:**

- Multiple transactions competing for the same resources in different orders

**Solutions:**

- Order operations consistently across transactions
- Use shorter transactions
- Implement retry logic with backoff

## Template Errors

### "Template Not Found"

**Symptoms:** Template execution fails with "not found" error.

**Possible Causes:**

1. **Typo in template ID**
2. **Template doesn't exist in your organization**
3. **Template was deleted**

**Solution:**

```bash
# List available templates
curl http://{host}:8000/api/v1/templates \
  -H "Authorization: Bearer $TOKEN"
```

### "Missing Required Parameter"

**Symptoms:** Template execution fails with missing parameter error.

**Possible Causes:**

- Not providing all required parameters

**Solution:**

```bash
# Check template to see required parameters
curl http://{host}:8000/api/v1/templates/{template_id} \
  -H "Authorization: Bearer $TOKEN"
```

### "Handlebars Parsing Error"

**Symptoms:** Template fails with parsing error.

**Possible Causes:**

1. **Unclosed Handlebars expression**
2. **Invalid Handlebars syntax**

**Solution:**

- Check template syntax for unclosed `{{` or `}}`
- Verify conditional logic (`{{#if}}...{{/if}}`)

## Performance Issues

### Slow Queries

**Symptoms:** Queries take longer than expected.

**Possible Causes:**

1. **Missing indexes**
2. **Large result sets**
3. **Complex joins**
4. **Database server under load**

**Solutions:**

- Add LIMIT clauses to queries
- Use parameterized queries for query plan caching
- Work with DBA to add appropriate indexes
- Use templates for frequently executed queries

### Rate Limiting

**Symptoms:** Receiving 429 "Too Many Requests" responses.

**Possible Causes:**

- Exceeding configured rate limits

**Solutions:**

- Implement backoff and retry logic
- Batch operations where possible
- Contact administrator for rate limit adjustments

## Endpoint-Specific Issues

### PostgreSQL

**"SSL Required" Error:**
- Database requires SSL but endpoint not configured for it
- Update endpoint config to include SSL parameters

**"Role Does Not Exist" Error:**
- Connection user doesn't exist in PostgreSQL
- Verify username in connection URL

### MongoDB

**"Authentication Failed" Error:**
- Wrong credentials or auth database
- Verify connection string includes correct authSource

### Redis

**"NOAUTH Authentication Required" Error:**
- Redis requires password but none provided
- Update endpoint config with password

## Getting Help

If you can't resolve an issue:

1. **Gather information:**
   - Error message and status code
   - Request body (without sensitive data)
   - Timestamp of the issue
   - Your access level

2. **Check API documentation:**
   - Swagger UI: `http://{host}:8000/swagger-ui/`

3. **Contact your administrator** with the gathered information

## Common Error Reference

| Error | Status | Common Cause |
|-------|--------|--------------|
| Unauthorized | 401 | Invalid/expired token |
| Forbidden | 403 | Insufficient permissions |
| Not Found | 404 | Resource doesn't exist |
| Bad Request | 400 | Invalid request format |
| Conflict | 409 | Resource already exists |
| Too Many Requests | 429 | Rate limit exceeded |
| Internal Server Error | 500 | Server-side issue |

## Related

- [Authentication](../guide/authentication.md) - Token management
- [RBAC](../guide/rbac.md) - Access control
- [Error Responses](../api/errors.md) - Error codes
- [FAQ](../appendix/faq.md) - Common questions
