# Public Documentation Standards

This document defines the standards and best practices for writing public-facing documentation for Eden-MDBS.

## Table of Contents

- [Audience](#audience)
- [Writing Principles](#writing-principles)
- [Structure Standards](#structure-standards)
- [Page Templates](#page-templates)
- [Code Examples](#code-examples)
- [Style Guide](#style-guide)
- [Review Checklist](#review-checklist)

## Audience

Public documentation targets **end users** of Eden-MDBS, not contributors to the codebase.

## Writing Principles

### 1. **User-Focused**

Write from the user's perspective, not the system's.

**Good:**

> "To connect a database, send a POST request to `/api/v1/endpoints`"

**Bad:**

> "The PostgreSQL connection handler processes incoming requests at the connect endpoint"

### 2. **Task-Oriented**

Organize around what users want to accomplish.

**Good:**

> "Connecting Your First Database"

**Bad:**

> "The Endpoint System Architecture"

### 3. **Progressive Disclosure**

Start simple, add complexity gradually.

**Structure:**

1. Quick example (minimal)
2. Explanation
3. Advanced options
4. Edge cases

### 4. **Show, Then Tell**

Code first, explanation second.

**Good:**

````markdown
## Creating an Organization

```bash
curl http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{"id": "myorg", "super_admins": [{"username": "admin", "password": "secure_password"}]}'
```

This creates a new organization named "myorg" with a super admin user...
````

**Bad:**

> Organizations in Eden-MDBS are multi-tenant constructs that encapsulate users, endpoints, and permissions. To create one, you must...

### 5. **Practical Examples**

Every major concept needs a working example.

**Requirements:**

- Examples must be copy-pasteable
- Examples must work with Quick Start setup
- Examples must include expected output
- Complex examples need prerequisites listed

### 6. **Consistent Tone**

- **Professional but friendly**
- **Clear and concise**
- **Active voice** - "Create an organization" not "An organization is created"
- **Present tense** - "Eden-MDBS connects to PostgreSQL" not "Eden-MDBS will connect"
- **Avoid jargon** - Explain technical terms when first used

## Structure Standards

### Page Structure

Every documentation page should follow this structure:

```markdown
# Page Title

Brief one-line description (what this page covers).

## Overview [Optional]

1-2 paragraphs explaining what this page covers and why it matters.

## Prerequisites [If applicable]

- Requirement 1
- Requirement 2

## Main Content

### Clear Section Headings

Content with examples...

## Common Issues [If applicable]

### Issue 1

How to identify and resolve...

## Next Steps

- [Related Topic 1](./related-1.md)
- [Related Topic 2](./related-2.md)

## See Also [Optional]

- [Reference](../reference/config.md)
- [API Docs](../api/endpoints.md)
```

### Section Ordering

1. **What** - What is this feature/concept?
2. **Why** - Why would I use it?
3. **How** - How do I use it? (with examples)
4. **Advanced** - Advanced usage, edge cases
5. **Troubleshooting** - Common problems
6. **Next Steps** - What to learn next

### Heading Levels

- **H1 (`#`)** - Page title only (one per page)
- **H2 (`##`)** - Main sections
- **H3 (`###`)** - Subsections
- **H4 (`####`)** - Rarely needed, avoid if possible

## Page Templates

### Tutorial Page Template

````markdown
# [Task Name] Tutorial

Learn how to [accomplish specific goal] with Eden-MDBS.

## What You'll Learn

- Skill 1
- Skill 2
- Skill 3

## Prerequisites

- [Prerequisite 1](./link.md)
- Prerequisite 2

## Step 1: [Action]

Description of what we'll do in this step.

```bash
# Code example
curl ...
```

**Expected output:**

```json
{ "status": "success" }
```

**What happened:** Explanation...

## Step 2: [Next Action]

...

## What You've Accomplished

Summary of what was learned.

## Next Steps

- [Advance to next topic](./next.md)
- [Learn about related feature](./related.md)
````

### Reference Page Template

````markdown
# [Feature] Reference

Complete reference for [feature].

## Overview

Brief description.

## Syntax

```bash
# General syntax
command [OPTIONS] <REQUIRED> [OPTIONAL]
```

## Options

| Option      | Type    | Default     | Description  |
| ----------- | ------- | ----------- | ------------ |
| `--option1` | string  | `"default"` | What it does |
| `--option2` | integer | `0`         | What it does |

## Examples

### Example 1: Common Use Case

```bash
# Example code
```

Description...

### Example 2: Advanced Use Case

```bash
# Example code
```

Description...

## Error Codes

| Code | Meaning      | Solution           |
| ---- | ------------ | ------------------ |
| 400  | Bad request  | Check JSON syntax  |
| 401  | Unauthorized | Verify credentials |
````

### Concept Page Template

````markdown
# Understanding [Concept]

Explanation of [concept] and how it works in Eden-MDBS.

## What Is [Concept]?

Clear definition and explanation.

## Why Use [Concept]?

Benefits and use cases.

## How It Works

High-level explanation with diagrams if helpful.

```
[Diagram or flow]
┌──────┐    ┌──────┐    ┌──────┐
│Step 1│───▶│Step 2│───▶│Step 3│
└──────┘    └──────┘    └──────┘
```

## Example

Practical example showing the concept in action.

## Best Practices

- Practice 1
- Practice 2

## Common Patterns

### Pattern 1: [Name]

When to use and example...

### Pattern 2: [Name]

When to use and example...

## See Also

- [Related Concept](./related.md)
- [API Reference](../api/reference.md)
````

## Code Examples

### General Rules

1. **Always include expected output**
2. **Use realistic values** (not foo/bar)
3. **Keep examples minimal** - only what's needed
4. **Test all examples** - they must work
5. **Comment complex parts**

### Bash/curl Examples

**Format:**

```bash
# Comment explaining what this does
curl http://localhost:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "key": "value"
  }'
```

**Best practices:**

- Use `\` for line continuation
- Indent JSON for readability
- Include authentication
- Show full URLs
- Add comments for clarity

### JSON Examples

**Format:**

```json
{
  "field1": "value",
  "field2": 123,
  "nested": {
    "field3": true
  }
}
```

**Best practices:**

- Use 2-space indentation
- Include realistic values
- Show complete structure
- Comment with text before/after if needed

### Multi-Language Examples

When showing integration code:

**Format:**

````markdown
Choose your language:

**Python:**

```python
import requests
response = requests.post('http://localhost:8000/api/v1/endpoints',
                        headers={'Authorization': f'Bearer {token}'},
                        json={'key': 'value'})
```

**JavaScript:**

```javascript
const response = await fetch("http://localhost:8000/api/v1/endpoints", {
  method: "POST",
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({ key: "value" }),
});
```

**Go:**

```go
// Go example...
```
````

### Expected Output

Always show what users should see:

````markdown
```bash
curl http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{"id": "myorg", "super_admins": [{"username": "admin", "password": "secure_password"}]}'
```

**Response:**

```json
{ "id": "myorg", "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" }
```

**HTTP Status:** `200 OK`
````

## Style Guide

### Formatting

#### Command-Line Tools

- Use backticks: `curl`, `docker`, `kubectl`

#### File Paths

- Use backticks: `public-docs/src/guide.md`
- Or markdown links: [book.toml](../book.toml)

#### Configuration Values

- Use backticks: `EDEN_PORT=8000`

#### Code/API Elements

- Endpoints: Use backticks or bold: `POST /api/v1/endpoints`
- JSON keys: Use backticks: `"organization"`
- Environment variables: Use backticks: `RUST_LOG`

#### UI Elements

- Bold for clickable items: **Settings → Pages**

#### Emphasis

- **Bold** for important callouts
- _Italic_ for emphasis (use sparingly)

### Lists

#### Unordered Lists

Use `-` (not `*` or `+`):

```markdown
- Item 1
- Item 2
  - Nested item
```

#### Ordered Lists

Use `1.` for all items (auto-numbered):

```markdown
1. First step
1. Second step
1. Third step
```

### Links

#### Internal Links

```markdown
[Link text](./relative/path.md)
[Specific section](./page.md#section-name)
```

#### External Links

```markdown
[Docker Documentation](https://docs.docker.com)
```

**Best practices:**

- Use descriptive link text (not "click here")
- Verify links before committing
- Use relative paths for internal links

### Admonitions

Use blockquotes with emoji for callouts:

#### Note

```markdown
> **Note:** This is additional information that's helpful to know.
```

#### Warning

```markdown
> **Warning:** This could cause data loss or system issues.
```

#### Tip

```markdown
> **Tip:** Here's a helpful best practice or shortcut.
```

#### Important

```markdown
> **Important:** Critical information you must read.
```

### Tables

Use tables for structured data:

```markdown
| Column 1 | Column 2 | Column 3 |
| -------- | -------- | -------- |
| Value 1  | Value 2  | Value 3  |
| Value 4  | Value 5  | Value 6  |
```

**Best practices:**

- Keep tables simple (max 5 columns)
- Align pipes for readability (in source)
- Use header row
- Consider lists if table has only 2 columns

### Diagrams

Use ASCII art for simple diagrams:

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Eden-MDBS  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Database   │
└─────────────┘
```

For complex diagrams, use:

- Mermaid diagrams (if mdBook plugin added)
- External images (SVG preferred)

## Review Checklist

Before submitting documentation, verify:

### Content Quality

- [ ] **Audience-appropriate** - Written for end users, not developers
- [ ] **Accurate** - All information is correct
- [ ] **Complete** - Covers all important aspects
- [ ] **Up-to-date** - Reflects current version

### Structure

- [ ] **Clear title** - Describes page content
- [ ] **Logical flow** - Information progresses naturally
- [ ] **Proper headings** - One H1, hierarchical H2/H3
- [ ] **SUMMARY.md updated** - Page appears in navigation

### Examples

- [ ] **Tested** - All code examples work
- [ ] **Complete** - Examples are copy-pasteable
- [ ] **Explained** - Examples have context
- [ ] **Output shown** - Expected results included

### Formatting

- [ ] **Consistent style** - Follows style guide
- [ ] **Links work** - All links verified
- [ ] **Code blocks** - Proper syntax highlighting
- [ ] **Tables formatted** - Easy to read

### Writing Quality

- [ ] **Clear language** - No unnecessary jargon
- [ ] **Active voice** - "Create X" not "X is created"
- [ ] **Concise** - No unnecessary words
- [ ] **Spelling/grammar** - Run spell checker

### Navigation

- [ ] **Next steps** - Links to related pages
- [ ] **Breadcrumbs clear** - User knows where they are
- [ ] **Cross-references** - Related topics linked

## Common Mistakes to Avoid

### Don't: Use Rust-Specific Language

**Bad:**

> "The `PostgresCore` struct implements the `EndpointTrait`..."

**Good:**

> "Eden-MDBS provides PostgreSQL support through its endpoint system..."

### Don't: Explain Implementation Details

**Bad:**

> "Internally, Eden-MDBS uses a connection pool with a HashMap of Arc<Mutex<Connection>>..."

**Good:**

> "Eden-MDBS manages database connections efficiently with connection pooling..."

### Don't: Assume Prior Knowledge

**Bad:**

> "Set the JWT secret in your env vars"

**Good:**

> "Set the `EDEN_JWT_SECRET` environment variable to a secure random string (at least 32 characters)"

### Don't: Bury the Important Information

**Bad:**

```markdown
# Organizations

Organizations in Eden-MDBS are implemented as a hierarchical multi-tenant
structure with isolated resource boundaries and RBAC integration at the
data layer...

[300 more words]

To create an organization, use the API...
```

**Good:**

````markdown
# Organizations

Create an organization to get started with Eden-MDBS:

```bash
curl http://localhost:8000/api/v1/new \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{"id": "myorg", "super_admins": [{"username": "admin", "password": "secure_password"}]}'
```

Organizations provide isolated environments for users and databases...
````

### Don't: Use Vague Examples

**Bad:**

```bash
curl http://example.com/api/foo -d '{"bar": "baz"}'
```

**Good:**

```bash
curl http://localhost:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"id": "mydb", "kind": "Postgres", "config": {"write_conn": {"url": "postgresql://localhost/mydb"}}}'
```

## Quality Standards

### Minimum Requirements

Every page must have:

- Clear title and overview
- At least one working example
- Links to related pages
- Proper grammar and spelling

### Excellent Documentation

Strive for:

- Multiple examples (basic and advanced)
- Troubleshooting section
- Diagrams or visual aids
- Common patterns/best practices
- Integration examples

## Maintenance

### Updating Documentation

When code changes:

1. Update affected pages immediately
2. Test all examples on the page
3. Update version numbers if needed
4. Check cross-references

### Deprecation

When deprecating features:

```markdown
> **Deprecated:** This endpoint is deprecated as of v2.0 and will be
> removed in v3.0. Use [new endpoint](./new.md) instead.
```

### Versioning

Consider version callouts for version-specific features:

```markdown
> **Since:** v1.5.0
```

## Tools and Resources

### Spell Checking

Run before committing:

```bash
# Install aspell
brew install aspell  # macOS
sudo apt install aspell  # Linux

# Check file
aspell check public-docs/src/guide/page.md
```

### Link Checking

Use [mdbook-linkcheck](https://github.com/Michael-F-Bryan/mdbook-linkcheck):

```bash
cargo install mdbook-linkcheck
# Add to book.toml:
# [preprocessor.links]
mdbook build
```

### Preview

Always preview before committing:

```bash
make public-docs-serve
# Visit http://localhost:3000
```

## Getting Help

Questions about documentation standards?

- Review existing completed pages for examples
- Ask in GitHub Discussions
- Open an issue for clarification

## Summary

Good public documentation is:

1. **User-focused** - Helps users accomplish tasks
2. **Practical** - Shows working examples
3. **Clear** - Easy to understand
4. **Complete** - Covers common scenarios
5. **Tested** - All examples work

Follow these standards to create documentation that helps users succeed with Eden-MDBS.
