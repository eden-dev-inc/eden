#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SOURCE_REMOTE="${SOURCE_REMOTE:-origin}"
TARGET_REMOTE="${TARGET_REMOTE:-github}"
TARGET_URL="${TARGET_URL:-https://github.com/eden-dev-inc/eden.git}"
BRANCH="${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
PUSH_TAGS="${PUSH_TAGS:-false}"
FETCH_REMOTES="${FETCH_REMOTES:-true}"

usage() {
  cat <<'EOF'
Usage:
  scripts/sync_gitlab_to_github.sh configure
  scripts/sync_gitlab_to_github.sh status [branch]
  scripts/sync_gitlab_to_github.sh push [branch]

Environment overrides:
  SOURCE_REMOTE   Source remote name. Default: origin
  TARGET_REMOTE   Target remote name. Default: github
  TARGET_URL      GitHub mirror URL. Default: https://github.com/eden-dev-inc/eden.git
  BRANCH          Branch to compare or push. Default: current branch
  PUSH_TAGS       When "true", also push tags during the push command
  FETCH_REMOTES   When "false", skip fetching in the status command

Examples:
  scripts/sync_gitlab_to_github.sh configure
  scripts/sync_gitlab_to_github.sh status main
  PUSH_TAGS=true scripts/sync_gitlab_to_github.sh push main
EOF
}

ensure_target_remote() {
  if git remote get-url "$TARGET_REMOTE" >/dev/null 2>&1; then
    local current_url
    current_url="$(git remote get-url "$TARGET_REMOTE")"
    if [[ "$current_url" != "$TARGET_URL" ]]; then
      git remote set-url "$TARGET_REMOTE" "$TARGET_URL"
    fi
  else
    git remote add "$TARGET_REMOTE" "$TARGET_URL"
  fi
}

status() {
  local branch="$1"
  ensure_target_remote

  if [[ "$FETCH_REMOTES" == "true" ]]; then
    git fetch "$SOURCE_REMOTE" "$branch"
    git fetch "$TARGET_REMOTE" "$branch"
  fi

  local source_ref="${SOURCE_REMOTE}/${branch}"
  local target_ref="${TARGET_REMOTE}/${branch}"

  local source_sha
  local target_sha
  source_sha="$(git rev-parse "$source_ref")"
  target_sha="$(git rev-parse "$target_ref")"

  local counts
  counts="$(git rev-list --left-right --count "${source_ref}...${target_ref}")"
  local ahead behind
  ahead="$(awk '{print $1}' <<<"$counts")"
  behind="$(awk '{print $2}' <<<"$counts")"

  echo "source remote: $SOURCE_REMOTE ($source_ref -> $source_sha)"
  echo "target remote: $TARGET_REMOTE ($target_ref -> $target_sha)"
  echo "source ahead by: $ahead commit(s)"
  echo "source behind by: $behind commit(s)"
}

push_branch() {
  local branch="$1"
  ensure_target_remote
  git push "$TARGET_REMOTE" "${branch}:${branch}"
  if [[ "$PUSH_TAGS" == "true" ]]; then
    git push "$TARGET_REMOTE" --tags
  fi
}

main() {
  local command="${1:-status}"
  local branch="${2:-$BRANCH}"

  case "$command" in
    configure)
      ensure_target_remote
      git remote -v | grep "^${TARGET_REMOTE}[[:space:]]"
      ;;
    status)
      status "$branch"
      ;;
    push)
      push_branch "$branch"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      echo "unknown command: $command" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
