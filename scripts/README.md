# Repo Sync Scripts

## `sync_gitlab_to_github.sh`

Keeps the local GitLab checkout aligned with the GitHub Eden mirror.

Examples:

```bash
# make sure the local github remote exists and points at the expected repo
scripts/sync_gitlab_to_github.sh configure

# compare origin/main against github/main
scripts/sync_gitlab_to_github.sh status main

# push the current local main branch to the github remote
scripts/sync_gitlab_to_github.sh push main

# push the branch and tags
PUSH_TAGS=true scripts/sync_gitlab_to_github.sh push main
```

Defaults:

- source remote: `origin`
- target remote: `github`
- target URL: `https://github.com/eden-dev-inc/eden.git`
