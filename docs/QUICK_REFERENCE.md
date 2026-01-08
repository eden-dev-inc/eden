# Documentation Quick Reference

Quick reference for common documentation tasks.

## Prerequisites

Install mdbook (Rust's documentation tool):

```bash
cargo install mdbook
```

## Common Commands

```bash
# Serve docs locally (live reload)
make public-docs-serve
# Visit http://localhost:3000

# Build docs
make public-docs-build

# Build and open in browser
make public-docs-open

# Clean build artifacts
make public-docs-clean
```

> **Note:** All commands will check for mdbook and prompt you to install it if missing.

## Adding a New Page

### 1. Create the markdown file

```bash
# Create file in appropriate directory
touch public-docs/src/guide/my-feature.md
```

### 2. Add to table of contents

Edit `public-docs/src/SUMMARY.md`:

```markdown
# User Guide

- [My Feature](./guide/my-feature.md)
```

### 3. Write content

Use this template:

````markdown
# My Feature

Brief description of what this page covers.

## Overview

Why this feature exists and when to use it.

## Quick Example

```bash
curl http://localhost:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}'
```

**Response:**

```json
{ "status": "success" }
```

## Detailed Guide

### Step 1: Do This

Explanation...

### Step 2: Do That

Explanation...

## Common Issues

### Issue: Something doesn't work

**Solution:** Fix it like this...

## Next Steps

- [Related Feature](./related.md)
- [Advanced Usage](./advanced.md)
````

### 4. Test locally

```bash
make public-docs-serve
# Check your page at http://localhost:3000
```

### 5. Verify

- [ ] Page appears in navigation
- [ ] All links work
- [ ] Code examples are tested
- [ ] No spelling errors

## Page Templates

### Tutorial Page

Use for: Step-by-step guides

**Template:** See [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md#tutorial-page-template)

**Example:** [Quick Start](src/getting-started/quick-start.md)

### Reference Page

Use for: Complete option/configuration reference

**Template:** See [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md#reference-page-template)

**When:** Documenting all options for a command/endpoint/config

### Concept Page

Use for: Explaining how something works

**Template:** See [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md#concept-page-template)

**Example:** [What is Eden-MDBS](src/getting-started/what-is-eden.md)

## Formatting Cheat Sheet

### Code Blocks

**Bash/curl:**

````markdown
```bash
curl http://localhost:8000/api/v1/endpoints
```
````

**JSON:**

````markdown
```json
{
  "key": "value"
}
```
````

**Multi-language:**

````markdown
**Python:**

```python
import requests
```

**JavaScript:**

```javascript
const response = await fetch(...)
```
````

### Links

**Internal link:**

```markdown
[Link Text](./relative/path.md)
[Section Link](./page.md#section-name)
```

**External link:**

```markdown
[Docker Docs](https://docs.docker.com)
```

### Callouts

**Note:**

```markdown
> **Note:** Additional helpful information.
```

**Warning:**

```markdown
> ⚠️ **Warning:** This could cause issues.
```

**Tip:**

```markdown
> 💡 **Tip:** Here's a helpful shortcut.
```

**Important:**

```markdown
> ⚡ **Important:** Critical information.
```

### Lists

**Unordered:**

```markdown
- Item 1
- Item 2
  - Nested item
```

**Ordered:**

```markdown
1. Step 1
1. Step 2
1. Step 3
```

**Checklist:**

```markdown
- [ ] Incomplete task
- [x] Completed task
```

### Tables

```markdown
| Column 1 | Column 2 | Column 3 |
| -------- | -------- | -------- |
| Value    | Value    | Value    |
```

### Emphasis

- **Bold:** `**text**`
- _Italic:_ `*text*`
- `Code:` `` `text` ``

## Common Patterns

### API Endpoint Example

````markdown
## Creating an Organization

```bash
curl http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{
    "id": "myorg",
    "super_admins": [{"username": "admin", "password": "password123"}]
  }'
```

**Response:**

```json
{ "id": "myorg", "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" }
```

**HTTP Status:** `200 OK`

### Parameters

| Parameter      | Type   | Required | Description                            |
| -------------- | ------ | -------- | -------------------------------------- |
| `id`           | string | Yes      | Organization ID                        |
| `super_admins` | array  | Yes      | List of `{username, password}` objects |
````

### Configuration Option

````markdown
## EDEN_PORT

**Type:** Integer
**Default:** `8000`
**Example:** `EDEN_PORT=9000`

Sets the port Eden-MDBS listens on for HTTP requests.

```bash
# In .env file
EDEN_PORT=9000

# Start service
docker compose up
```
````

### Error Documentation

````markdown
## Error: Connection Refused

**Symptom:** `curl: (7) Failed to connect to localhost port 8080`

**Cause:** Eden-MDBS is not running or listening on a different port.

**Solution:**

1. Check if service is running:

   ```bash
   docker compose ps
   ```

2. Check configured port:

   ```bash
   grep EDEN_PORT .env
   ```

3. Restart service:
   ```bash
   docker compose restart eden
   ```
````

## Style Guidelines Summary

### Do ✅

- Write for end users (not Rust developers)
- Use REST API examples (curl, HTTP)
- Show code first, explain after
- Include expected output
- Test all examples
- Use active voice ("Create X")
- Link to related pages
- Keep it concise

### Don't ❌

- Use Rust-specific terminology
- Document internal implementation
- Assume prior knowledge
- Use vague examples (foo/bar)
- Skip expected output
- Use passive voice
- Write walls of text

## Before Submitting

Run through this checklist:

```markdown
- [ ] Page added to SUMMARY.md
- [ ] All code examples tested
- [ ] All links verified
- [ ] Spell check passed
- [ ] Follows template structure
- [ ] Includes "Next Steps" section
- [ ] Local preview looks good
- [ ] No Rust-specific jargon
```

## Resources

- **Full Standards:** [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md)
- **mdBook Docs:** https://rust-lang.github.io/mdBook/
- **Example Pages:**
  - [introduction.md](src/introduction.md)
  - [what-is-eden.md](src/getting-started/what-is-eden.md)
  - [quick-start.md](src/getting-started/quick-start.md)

## Need Help?

1. Check [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md)
2. Look at existing pages for examples
3. Ask in GitHub Discussions
4. Open an issue

---

**Remember:** Good docs help users succeed! 🎯
