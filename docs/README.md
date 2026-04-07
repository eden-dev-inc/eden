# Eden-MDBS Public Documentation

This directory contains the public-facing documentation for Eden-MDBS, built with [mdBook](https://rust-lang.github.io/mdBook/).

## Quick Start

### Build Documentation

```bash
# From project root
make public-docs-build

# Or directly
cd public-docs && mdbook build
```

### View Documentation Locally

```bash
# Build and open in browser
make public-docs-open

# Or serve with live reload
make public-docs-serve
# Visit http://localhost:3000
```

## Documentation Structure

```
src/
├── introduction.md              # Landing page
├── getting-started/            # Getting started guides
│   ├── what-is-eden.md
│   ├── quick-start.md
│   ├── first-steps.md
│   └── concepts.md
├── guide/                      # User guides
│   ├── organizations.md
│   ├── endpoints.md
│   ├── authentication.md
│   ├── rbac.md
│   ├── workflows.md
│   └── transactions.md
├── api/                        # API reference
│   ├── overview.md
│   ├── authentication.md
│   ├── organizations.md
│   └── ...
├── architecture/               # Architecture docs
│   ├── overview.md
│   ├── components.md
│   └── ...
├── operations/                 # Operations guides
│   ├── configuration.md
│   ├── monitoring.md
│   └── ...
├── examples/                   # Code examples
├── advanced/                   # Advanced topics
├── reference/                  # Reference materials
└── appendix/                   # Additional resources
```

## Adding New Pages

1. Create a new markdown file in the appropriate directory under `src/`
2. Add an entry to `src/SUMMARY.md` to include it in the navigation
3. Write your content using standard markdown
4. Build to verify: `make public-docs-build`

## Markdown Features

mdBook supports:

- Standard Markdown syntax
- Syntax highlighting for code blocks
- Links between pages: `[text](../path/to/page.md)`
- Anchor links: `[text](./page.md#section)`
- Include files: `{{#include path/to/file.rs}}`
- Runnable code examples (for supported languages)

See the [mdBook documentation](https://rust-lang.github.io/mdBook/) for details.

## Writing Guidelines

**📖 See [PUBLIC_DOCUMENTATION_STANDARDS.md](PUBLIC_DOCUMENTATION_STANDARDS.md) for complete writing guidelines.**

Quick checklist:

- Use clear, concise language (no jargon)
- Include practical, tested examples
- Write for end users (REST API), not Rust developers
- Show code first, explain second
- Link to related pages
- Test all code examples before committing

Key principles:

- **User-focused** - Help users accomplish tasks
- **Task-oriented** - Organize by what users want to do
- **Show, then tell** - Code example first, explanation after
- **Progressive disclosure** - Simple first, complexity later

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch. The workflow is defined in `.github/workflows/docs.yml`.

### Manual Deployment

If needed, you can manually trigger deployment from the GitHub Actions tab.

## Configuration

Documentation configuration is in `book.toml`. Key settings:

- `title` - Book title
- `description` - Book description
- `authors` - Author names
- `output.html.*` - HTML output settings

See the [mdBook configuration docs](https://rust-lang.github.io/mdBook/format/configuration/) for all options.

## Troubleshooting

### mdBook not found

Install mdBook:

```bash
cargo install mdbook
```

### Build errors

Clean and rebuild:

```bash
make public-docs-clean
make public-docs-build
```

### Links not working

- Ensure paths are relative to the current file
- Use `.md` extension in links
- Check that target files exist
