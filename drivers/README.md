# Eden Platform Drivers

Official client libraries and SDKs for the Eden platform across multiple programming languages. These drivers provide native, idiomatic interfaces to Eden's unified data infrastructure platform, enabling developers to seamlessly integrate databases, APIs, and AI services into their applications.

## What is Eden?

Eden is a comprehensive data infrastructure platform that provides unified access to multiple databases, AI services, and external APIs through a powerful orchestration layer. It offers enterprise-grade security, scalability, and observability while simplifying complex data operations.

**Platform Capabilities:**
- **Multi-Database Support**: PostgreSQL, MongoDB, Redis, ClickHouse, MySQL, SQL Server, Cassandra, Oracle, Pinecone
- **AI Integration**: Model Context Protocol (MCP) for safe AI-database interactions
- **Enterprise Security**: Role-based access control (RBAC) with fine-grained permissions
- **Template System**: Reusable, parameterized operations with Handlebars templating
- **API Orchestration**: Compose multiple templates into complex workflows
- **Migration Management**: Database schema evolution with atomic rollback capabilities

## Repository Structure

This repository contains official Eden drivers organized by programming language within the Eden monorepo:

```
eden/
├── drivers/
│   ├── README.md (this file)
│   ├── python/
│   │   ├── README.md
│   │   ├── eden/
│   │   ├── setup.py
│   │   ├── requirements.txt
│   │   ├── tests/
│   │   └── docs/
│   ├── javascript/
│   │   ├── README.md
│   │   ├── src/
│   │   ├── package.json
│   │   ├── tests/
│   │   └── docs/
│   ├── typescript/
│   │   ├── README.md
│   │   ├── src/
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── tests/
│   │   └── docs/
│   ├── go/
│   │   ├── README.md
│   │   ├── go.mod
│   │   ├── eden/
│   │   ├── tests/
│   │   └── docs/
│   ├── java/
│   │   ├── README.md
│   │   ├── pom.xml
│   │   ├── src/
│   │   ├── tests/
│   │   └── docs/
│   ├── csharp/
│   │   ├── README.md
│   │   ├── Eden.NET/
│   │   ├── Eden.NET.sln
│   │   ├── tests/
│   │   └── docs/
│   ├── php/
│   │   ├── README.md
│   │   ├── composer.json
│   │   ├── src/
│   │   ├── tests/
│   │   └── docs/
│   ├── ruby/
│   │   ├── README.md
│   │   ├── eden.gemspec
│   │   ├── lib/
│   │   ├── tests/
│   │   └── docs/
│   ├── rust/
│   │   ├── README.md
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   ├── tests/
│   │   └── docs/
│   └── shared/
│       ├── api-specs/
│       ├── test-fixtures/
│       └── documentation/
└── [other eden platform components...]
```

## Available Drivers

### [Python Driver](./python/) 🐍
**Package**: `eden-python`  
**Installation**: `pip install eden-python`

**Features:**
- Full async/await support with asyncio integration
- Type hints and mypy compatibility
- Pandas integration for data analysis workflows
- Django and Flask integration helpers
- Comprehensive error handling and retry logic
- Built-in connection pooling and health monitoring

```python
from eden import EdenClient

client = EdenClient(api_url="https://api.eden.com", org_id="my_org")
await client.authenticate("user@example.com", "password")
result = await client.templates.execute("user_query", {"user_id": 123})
```

### [JavaScript/Node.js Driver](./javascript/) ⚡
**Package**: `@eden/client`  
**Installation**: `npm install @eden/client`

**Features:**
- Native Promise and async/await support
- Browser and Node.js compatibility
- Streaming support for large datasets
- Express.js middleware integration
- Built-in request/response interceptors
- Automatic token refresh and session management

```javascript
import { EdenClient } from '@eden/client';

const client = new EdenClient({
  apiUrl: 'https://api.eden.com',
  orgId: 'my_org'
});

await client.auth.login('user@example.com', 'password');
const result = await client.templates.execute('user_query', { userId: 123 });
```

### [TypeScript Driver](./typescript/) 📘
**Package**: `@eden/client-ts`  
**Installation**: `npm install @eden/client-ts`

**Features:**
- Full TypeScript support with comprehensive type definitions
- Generic types for template parameters and responses
- Strict type checking for API operations
- IntelliSense support in IDEs
- Compile-time validation of API calls
- Integration with popular TypeScript frameworks

```typescript
import { EdenClient, TemplateResponse } from '@eden/client-ts';

interface UserQueryParams {
  userId: number;
  includeOrders?: boolean;
}

const client = new EdenClient({ apiUrl: 'https://api.eden.com', orgId: 'my_org' });
const result: TemplateResponse<User[]> = await client.templates.execute<UserQueryParams, User[]>(
  'user_query', 
  { userId: 123, includeOrders: true }
);
```

### [Go Driver](./go/) 🚀
**Package**: `github.com/eden-dev-inc/eden/drivers/go`  
**Installation**: `go get github.com/eden-dev-inc/eden/drivers/go`

**Features:**
- Idiomatic Go patterns and interfaces
- Context-based request handling
- Built-in connection pooling and health checks
- Structured logging with popular Go logging libraries
- Comprehensive error handling with wrapped errors
- High-performance concurrent operations

```go
import "github.com/eden-dev-inc/eden/drivers/go"

client := eden.NewClient(&eden.Config{
    APIUrl: "https://api.eden.com",
    OrgID:  "my_org",
})

ctx := context.Background()
err := client.Auth.Login(ctx, "user@example.com", "password")
result, err := client.Templates.Execute(ctx, "user_query", map[string]interface{}{
    "user_id": 123,
})
```

### [Java Driver](./java/) ☕
**Package**: `com.eden:eden-java`  
**Installation**: Maven/Gradle dependency

**Features:**
- Spring Boot auto-configuration support
- Reactive programming with Project Reactor
- Jackson integration for JSON serialization
- Comprehensive builder patterns
- Connection pool management
- Enterprise-grade logging and monitoring integration

```java
import com.eden.EdenClient;
import com.eden.models.TemplateRequest;

EdenClient client = EdenClient.builder()
    .apiUrl("https://api.eden.com")
    .orgId("my_org")
    .build();

client.auth().login("user@example.com", "password").block();
Map<String, Object> params = Map.of("user_id", 123);
TemplateResponse result = client.templates().execute("user_query", params).block();
```

### [C#/.NET Driver](./csharp/) 💙
**Package**: `Eden.NET`  
**Installation**: `dotnet add package Eden.NET`

**Features:**
- Full .NET Standard 2.0 compatibility
- Async/await patterns throughout
- Dependency injection integration
- Configuration providers support
- Comprehensive logging with ILogger
- Entity Framework Core integration helpers

```csharp
using Eden.NET;

var client = new EdenClient(new EdenClientOptions
{
    ApiUrl = "https://api.eden.com",
    OrgId = "my_org"
});

await client.Auth.LoginAsync("user@example.com", "password");
var result = await client.Templates.ExecuteAsync("user_query", new { user_id = 123 });
```

### [PHP Driver](./php/) 🐘
**Package**: `eden/eden-php`  
**Installation**: `composer require eden/eden-php`

**Features:**
- PSR-4 autoloading and PSR-7 HTTP message interfaces
- Laravel service provider and Symfony bundle
- Guzzle HTTP client integration
- Comprehensive exception hierarchy
- Built-in caching and connection pooling
- PHP 8+ features and type declarations

```php
use Eden\EdenClient;

$client = new EdenClient([
    'api_url' => 'https://api.eden.com',
    'org_id' => 'my_org'
]);

$client->auth()->login('user@example.com', 'password');
$result = $client->templates()->execute('user_query', ['user_id' => 123]);
```

### [Ruby Driver](./ruby/) 💎
**Package**: `eden-ruby`  
**Installation**: `gem install eden-ruby`

**Features:**
- Idiomatic Ruby patterns and conventions
- Rails integration with generators and middleware
- Built-in connection pooling with connection_pool gem
- Comprehensive logging and instrumentation
- Support for Ruby's fiber-based concurrency
- ActiveRecord-style query building

```ruby
require 'eden'

client = Eden::Client.new(
  api_url: 'https://api.eden.com',
  org_id: 'my_org'
)

client.auth.login('user@example.com', 'password')
result = client.templates.execute('user_query', user_id: 123)
```

### [Rust Driver](./rust/) 🦀
**Package**: `eden-rs`  
**Installation**: `cargo add eden-rs`

**Features:**
- Memory-safe, zero-cost abstractions
- Tokio async runtime integration
- Serde integration for JSON serialization
- Comprehensive error handling with thiserror
- Built-in connection pooling and health monitoring
- High-performance concurrent operations

```rust
use eden_rs::{EdenClient, EdenConfig};

let client = EdenClient::new(EdenConfig {
    api_url: "https://api.eden.com".to_string(),
    org_id: "my_org".to_string(),
}).await?;

client.auth().login("user@example.com", "password").await?;
let result = client.templates().execute("user_query", 
    serde_json::json!({"user_id": 123})
).await?;
```

## Driver Architecture

### Core Components

All Eden drivers implement a consistent architecture across languages:

#### 1. **EdenClient**
- Main entry point for all Eden operations
- Handles authentication and session management
- Manages connection pooling and health monitoring
- Provides access to all Eden services

#### 2. **Authentication Module**
- JWT token management with automatic refresh
- Login/logout operations
- Session persistence and restoration
- Multi-organization support

#### 3. **Service Clients**
- **Organizations**: Organization management operations
- **Endpoints**: Database and service connection management
- **Templates**: Template creation, execution, and management
- **APIs**: API orchestration and execution
- **IAM**: User and permission management
- **RBAC**: Role-based access control operations
- **MCP**: AI model integration and execution

#### 4. **Connection Management**
- Automatic connection pooling
- Health monitoring and failover
- Retry logic with exponential backoff
- Circuit breaker patterns

#### 5. **Error Handling**
- Structured exception hierarchy
- Comprehensive error codes and messages
- Retry strategies for transient failures
- Detailed logging and diagnostics

### Shared Features

#### Authentication & Security
- JWT token management with automatic refresh
- Secure credential storage and transmission
- RBAC integration with fine-grained permissions
- Audit logging for all operations

#### Connection Management
- Automatic connection pooling and health monitoring
- Configurable retry strategies and timeouts
- Circuit breaker patterns for fault tolerance
- Load balancing across multiple endpoints

#### Performance Optimization
- Request/response caching where appropriate
- Batch operation support for bulk operations
- Streaming support for large datasets
- Compression and efficient serialization

#### Observability
- Comprehensive logging with structured formats
- Metrics collection and reporting
- Distributed tracing support
- Health check endpoints and monitoring

## Installation & Quick Start

### General Installation Pattern

Each driver follows language-specific conventions for installation:

```bash
# Python
pip install eden-python

# JavaScript/Node.js
npm install @eden/client

# Go
go get github.com/eden-dev-inc/eden/drivers/go

# Java (Maven)
<dependency>
    <groupId>com.eden</groupId>
    <artifactId>eden-java</artifactId>
    <version>1.0.0</version>
</dependency>

# C#/.NET
dotnet add package Eden.NET

# PHP
composer require eden/eden-php

# Ruby
gem install eden-ruby

# Rust
cargo add eden-rs
```

### Universal Quick Start Pattern

1. **Install the driver** for your language
2. **Configure credentials** (environment variables or configuration file)
3. **Initialize client** with your organization details
4. **Authenticate** with your Eden credentials
5. **Start using Eden services**

### Environment Configuration

All drivers support standard environment variables:

```bash
EDEN_API_URL=https://api.eden.com
EDEN_ORG_ID=your-organization-id
EDEN_JWT_TOKEN=your-jwt-token  # Optional: for pre-authenticated scenarios
EDEN_LOG_LEVEL=info
EDEN_TIMEOUT=30
EDEN_RETRY_ATTEMPTS=3
```

## Testing & Quality Assurance

### Comprehensive Test Suites

Each driver includes:

#### Unit Tests
- Individual component testing
- Mock-based testing for isolation
- Edge case and error condition testing
- Performance benchmarking

#### Integration Tests
- End-to-end workflow testing
- Real Eden API integration testing
- Multi-database operation testing
- AI integration testing with MCP

#### Performance Tests
- Load testing and benchmarking
- Memory usage and leak detection
- Connection pool performance
- Concurrent operation testing

### Quality Standards

- **Code Coverage**: Minimum 90% test coverage
- **Documentation**: Comprehensive API documentation with examples
- **Type Safety**: Strong typing where supported by language
- **Security**: Regular security audits and dependency updates
- **Performance**: Regular performance regression testing

## Documentation & Support

### Driver Documentation

Each driver includes:
- **README**: Getting started guide and basic usage
- **API Documentation**: Comprehensive API reference
- **Examples**: Common usage patterns and examples
- **Migration Guides**: Upgrading between versions
- **Best Practices**: Production deployment guidance

### Community & Support

#### Community Resources
- **[GitHub Repository](https://github.com/eden-dev-inc/eden)**: Source code, issues, and contributions
- **[Documentation Portal](https://docs.eden.com)**: Complete platform documentation
- **[Discord Community](https://discord.gg/eden)**: Real-time community support
- **[Stack Overflow](https://stackoverflow.com/questions/tagged/eden-platform)**: Technical Q&A

#### Enterprise Support
- **Dedicated Support**: Priority support channels for enterprise customers
- **Professional Services**: Implementation consulting and optimization
- **Training Programs**: Developer training and certification
- **Custom Development**: Tailored driver features and integrations

### Contributing

We welcome contributions to Eden drivers! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on:

#### Development Process
- **Code Standards**: Language-specific coding conventions and style guides
- **Testing Requirements**: Required test coverage and testing patterns
- **Documentation Standards**: API documentation and example requirements
- **Review Process**: Code review and approval workflows

#### Contribution Types
- **Bug Fixes**: Issues and bug reports with fixes
- **Feature Additions**: New functionality and enhancements
- **Documentation**: Improvements to documentation and examples
- **Performance**: Performance optimizations and improvements
- **Security**: Security enhancements and vulnerability fixes

### Driver Versioning & Releases

#### Semantic Versioning
All drivers follow semantic versioning (semver):
- **Major**: Breaking changes requiring code updates
- **Minor**: New features maintaining backward compatibility
- **Patch**: Bug fixes and security updates

#### Release Cadence
- **Regular Releases**: Monthly minor releases with new features
- **Patch Releases**: As needed for critical fixes
- **Security Updates**: Immediate releases for security vulnerabilities
- **Breaking Changes**: Coordinated major releases with migration guides

#### Compatibility Matrix

| Driver Version | Eden API Version | Minimum Language Version |
|----------------|------------------|--------------------------|
| 1.0.x          | 1.0+             | Varies by language       |
| 1.1.x          | 1.1+             | Varies by language       |
| 2.0.x          | 2.0+             | Varies by language       |

## Roadmap

### Short-term Goals (Next 3 months)
- **Performance Optimizations**: Improved connection pooling and caching
- **Enhanced Error Handling**: Better error messages and recovery strategies
- **Documentation Improvements**: More examples and use case documentation
- **Testing Enhancements**: Improved test coverage and integration testing

### Medium-term Goals (Next 6 months)
- **Additional Language Support**: Kotlin, Swift, Scala drivers
- **Advanced Features**: Distributed tracing, advanced monitoring
- **Framework Integrations**: More framework-specific helpers and middleware
- **Performance Tooling**: Built-in performance monitoring and optimization tools

### Long-term Goals (Next 12 months)
- **Code Generation**: Automatic client generation from API specifications
- **Advanced AI Features**: Enhanced MCP integration and AI-powered optimizations
- **Edge Computing**: Support for edge deployment scenarios
- **Advanced Security**: Enhanced security features and compliance tools

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See individual driver CHANGELOG.md files for detailed version history and breaking changes.

---

**Ready to get started?** Choose your programming language and start building with Eden! Each driver directory contains detailed setup instructions, comprehensive documentation, and examples to help you integrate Eden into your applications quickly and efficiently.

For questions, issues, or contributions, please reach out through our community channels or submit a GitHub issue.