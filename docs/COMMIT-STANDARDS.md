# Development Workflow

This document covers commit message style, pull request and code review conventions, branching and releasing, and feature gating.

## Commit Message Style

Prime directive: Use concise, descriptive commit messages that your
team can understand and follow. Imagine an outsider reading your
commit messages and trying to understand the project's history.

> **The Crisis Test:** Imagine a critical bug has taken down
> production at 3 AM. A tired, frustrated on-call engineer is
> frantically bisecting the history to find the cause. Will your
> commit message help them immediately understand what changed and
> why? Or will it hinder them? Write for *that* engineer. (It might
> end up being you.)

We aim to follow the Conventional Commits style:
https://www.conventionalcommits.org/en/v1.0.0/

Conventional Commits describes a standardized format for commit
messages, making it easier to understand the purpose and impact of
each change. This helps maintain a clear and consistent history of the
project, which is crucial for collaboration and long-term maintenance.

TLDR conventional commits:

- Format: `type(scope): description` (scope optional).
- Keep the subject line short (target <= 72 chars), imperative, and without trailing punctuation.
- **Imperative mood**: Use "add" not "adds" or "added". The message
  should complete the sentence "If applied, this commit will...".
- Use `scope` when the change is clearly tied to one crate/module/subsystem (especially in multi-crate changes).
- Example types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`.
- Examples:
  - `feat: add Redis connection pooling`
  - `fix: correct timeout calculation in telemetry`
  - `docs(api): clarify endpoint guide examples`
  - `refactor(parser): simplify tokenization loop`
- Avoid vague messages like `update stuff` or `fix bug`.

### Commit Body and Footer

While the subject line (first line) is mandatory, most changes benefit from a detailed body and footer:

- **Body**: Use the body to explain _what_ and _why_ vs. _how_. The
  code shows _how_. The commit message should explain the intent and
  context.
  - Separate from the subject with a blank line.
  - Wrap lines at 72 characters.
- **Footer**: Use for meta-information.
  - `BREAKING CHANGE: <description>` for breaking changes.
  - Reference related issues where possible (for example, `Refs #1234` or `Closes #1234`).

**Example full commit message:**
```text
feat(auth): implement JWT token rotation

Add a new rotation mechanism that issues a new refresh token
upon use. This prevents replay attacks if a refresh token
is compromised.

BREAKING CHANGE: The `auth.refresh()` API now requires the
old refresh token as an argument.

Closes #452
```

## Pull Requests and Code Review

### Pull Request Philosophy

**Focus on one logical change.**

A pull request should try to address exactly one thing: a single feature, bug
fix, or refactor. If you mix refactoring with feature work, **ensure
it is clear to the reader**:

- **Separate Commits**: Use distinct commits for refactoring vs. feature logic.
- **Explicit Description**: Call out the refactoring in the pull request description so reviewers verify it preserves behavior.

This clarity ensures reviews remain high quality and the history remains understandable.

**Keep it small.**
Aim for pull requests that can be reviewed in under 20 minutes (typically < 400 lines). If your pull request is growing too large, split it into smaller, incremental pull requests.

**Avoid Great Adventures.**
A key mark of high-quality software engineering is the ability to
pursue long-term goals through a series of incremental, safe
steps. "Going on a great adventure", by which I mean disappearing for
days to write a massive, sprawling pull request, imposes high costs:

- **Team Cost**: Reviews become blocked, context is lost, and merge
  conflicts accumulate. Team velocity and morale suffer as a result.
- **Self Cost**: Feedback comes too late (often requiring rewrites),
  and the cognitive load of maintaining a massive diff is exhausting.
- **Project Cost**: Incremental value isn't delivered, and risk
  compounds until the "big bang" merge.

Break the "adventure" down. Ship the refactor first. Ship the
interface next. Ship the implementation last. I know it's hard. It's
imperative. If it means doing a few local-only prototypes first and
then going back and breaking it into pieces and doing the work again,
so be it.

> This aligns with Martin Fowler's own definition of **refactoring**: "to restructure software by applying a series of refactorings without changing its observable behavior." If you change behavior, you are rewriting, not refactoring. If you change everything at once, you are not refactoring. True refactoring is a disciplined, incremental process that keeps the system stable at every step. Part of this discipline is **committing early and often** and **communicating frequently**.

### Commit Granularity within a Pull Request

As an ideal, we should treat the **commit** as the fundamental unit of
work. While it's not _always_ practical, each commit should aim to be:

- **Atomic**: It compiles and runs (ideally passes tests) on its own.
- **Readable**: A reviewer should be able to understand the change by
  looking at the commit in isolation.
- **Revertible**: In an ideal world, we could roll back individual
  commits if needed. While strict revertibility is hard, striving for
  it ensures cleaner history and better production **resilience**.

Avoid "work in progress" commits in the final pull request. `fixup!` commits are
useful during iteration, but they should be autosquashed before merge.

### Rewriting Pull Request History During Review

Rewriting commit history on your own pull request branch is allowed and
encouraged when it improves clarity.

Authors are encouraged to learn interactive rebase (`git rebase -i`),
which is the primary tool for cleaning commit history before merge.

- Prefer rebasing/squashing/amending over adding a long tail of
  "address review comment" commits.
- Consider using `git commit --fixup=<commit>` when making targeted
  follow-up changes to an earlier commit (for example, review feedback).
- It is fine to push `fixup!` commits to your pull request branch while
  review is in progress; autosquash them before final merge.
- Before merge, run `git rebase -i --autosquash <base>` and verify the
  final commit series is readable and logically grouped.
- Use `git push --force-with-lease` to update your branch after
  history rewrites. Avoid plain `--force`.
- Avoid merge commits on pull request branches. Rebase on top of `main`
  instead of merging `main` into your branch.
- Rewrite history only on your own pull request branch (never on shared branches).

### Pull Request Preparation Checklist

Before submitting a pull request, ensure:

- Keep pull requests focused on one logical change.
- Write clear, well-documented code.
- Include tests and docs updates when behavior changes.
- Ensure tests pass locally for all feature-flag configurations.
- Format code with `cargo fmt --all`.
- Fix clippy warnings with `cargo clippy --fix`.
- The CI checks run on every push; monitor them in the pull request status.

### Reviewer Expectations

**For reviewers:**
- Focus on correctness, safety, and maintainability.
- Consider edge cases and error handling.
- Verify tests cover new behavior sufficiently.
- Check for proper error propagation in async contexts.
- Suggest improvements but don't block on nitpicks.

**For authors:**
- Address all reviewer feedback before merging.
- Keep commits clean by squashing related commits when needed.
- Update commit messages to reflect final changes.
- Re-request review after significant updates.

### When to Merge

- At least 1 approval from a code owner or maintainer.
- All CI checks pass (code quality).
- No outstanding blocking comments.
- Breaking changes documented and communicated.
- Prefer squash or rebase merges to keep history linear.
- Avoid merge commits from feature branches into `main`.

## Branching and Releasing

### Branch Roles

- `main` is the active development branch where pull requests land first.
- `1.0.x` is the release branch pattern used for shipping (for example, `1.0.x`).

### Branch Naming Convention

- Use a short, descriptive branch name that includes the type of change.
- Include an issue number when one exists and is practical.
- Preferred: `<type>/<issue>-short-description` (types such as `feature`, `fix`, `chore`).
- If no issue exists, use: `<type>/short-description`.
- Examples: `feature/123-add-metrics-endpoint`,
  `fix/987-timeout-retry`, `chore/cleanup-unused-helpers`.

### Code Quality Checks

You should run the following before putting your code up for review,
and after each change:

- **Format check**: `cargo fmt --all --check` (code must be formatted)
- **Clippy**: `cargo clippy --workspace --all-features --lib --bins` (must pass all checks)

The workspace has clippy configured to deny `unwrap` usage. Use proper
error handling instead of `.unwrap()` in production code.

### Optional Pre-Push Hook

Install the local pre-push hook with:

```bash
bash scripts/setup-hooks.sh
```

The hook is advisory and local-only. It delegates to the affected-package
planner branch workflow:

```bash
cargo run --quiet -p affected-ci-planner -- local --base <base> --head HEAD --run
```

By default, `scripts/pre-push.sh` uses the current branch upstream as the base
ref when one exists, otherwise `origin/main`. Override that base with
`EDEN_PRE_PUSH_BASE` when working on stacked branches.

You can test the hook behavior without installing it by running:

```bash
bash scripts/pre-push.sh
```

For a dry run that only explains the affected package closure and recommended
commands, run the planner directly:

```bash
cargo run --quiet -p affected-ci-planner -- local --base origin/main --head HEAD --explain
```

The hook can be bypassed for work-in-progress pushes with `git push --no-verify`.

### Day-to-Day Workflow

1. Create a feature or fix branch off `main`.
   - Follow the branch naming convention above.
2. Open a pull request into `main`.
3. After review and approval, merge into `main` using a non-merge-commit strategy (squash or rebase).
4. The release owner is responsible for promoting changes from `main`
   into the appropriate release branch (for example, `1.0.x`) as part
   of a release.
   - Use `git cherry-pick -x` when cherry-picking to release branches so source commits remain traceable.

### Handling CI Failures

1. **Format check failures**: Run `cargo fmt --all` locally and commit
2. **Clippy failures**: Run `cargo clippy --fix` locally, address warnings
3. **Test failures**: Ensure tests pass locally first, check for
   environment differences

## Feature Gating

### When to Use Feature Flags

Use feature gates for:

- Breaking changes that need phased rollouts
- High-risk features that need validation
- Features that depend on external deployment or configuration changes
- Experimental behavior that may need quick rollback

### Implementation Patterns

**Compile-time Flags (Cargo Features):**
- Primary mechanism for code inclusion/exclusion.
- Use `#[cfg(feature = "...")]` to conditionally compile code.
- Defined in `Cargo.toml`. Use for experimental modules, heavy
  dependencies, or platform-specific logic.

**Runtime Configuration (Config Structs/Env Vars):**
- Used for behavior tuning (for example, timeouts, buffer sizes, log levels)
  or simple on/off toggles that change without recompilation.
- Runtime toggles are currently managed via standard application
  configuration.

**Caveats with Compile-Time Features:**
- **Hidden Breakage**: It is easy to introduce code that compiles with
  your current features but breaks when features are disabled (or
  enabled).
- **IDE Blindspots**: IDEs often only analyze the currently active
  feature set.
- **Mitigation**: Run tests and clippy with `--all-features` (or
  specific feature combinations) before pushing.

### Naming Conventions

- Use descriptive, snake_case names: `redis_async_pipeline`, `postgres_query_caching`
- Group related features: `endpoint_http_streaming`, `endpoint_mongo_transactions`
- Avoid generic names: don't use `feature_enabled`, be specific

### Documentation Requirements

For every feature flag, document:
- What behavior the flag controls
- What default value is and why
- How to enable it during testing
- Removal steps when deleting the flag
- Whether enabling requires other config or deployment changes
