# AI Agent Guide — chanvoy

Start every session with:

1. `/Users/davethompson/dev/lanytehq/AGENTS.md`
2. **Machine-local config** — read `AGENTS.local.md` in this repo if present (gitignored,
   contains references to private repos and internal briefs)
3. `/Users/davethompson/dev/lanytehq/lanyte-crucible/docs/guides/dev-warmup.md`
4. This repo's `REPOSITORY_SAFETY_PROTOCOLS.md`

## Identity

Derive your identity from the environment variables set before the session:

| Variable | Purpose |
|----------|---------|
| `LANYTE_AGENT_ROLE` | Your role slug (for example `bravo-devlead`) |
| `LANYTE_AGENT_SCOPE` | Org scope (`lanytehq`) |
| `LANYTE_AGENT_TEAM` | Team name (for example `bravo`) |

Check with `echo $LANYTE_AGENT_ROLE $LANYTE_AGENT_SCOPE $LANYTE_AGENT_TEAM` before starting work.

## Overview

`chanvoy` is the Mattermost/chat bridge peer for the Lanyte platform.

## Working rules

- This repo is the Mattermost/chat bridge peer for the Lanyte platform.
- This repo is standalone. Do not add dependencies on crates in the lanyte workspace.
- Use feature branches and PRs; no direct pushes to `main`.
- Keep Rust MSRV at `1.85.0` and avoid nightly features.
- Follow the agentic attribution format from dev-warmup.md §5 (email: `noreply@lanytehq.dev`).

## Attribution

Every agent-generated commit must include:

```
Co-Authored-By: <Model Name> <noreply@lanytehq.dev>
Role: <your-role-slug>
Committer-of-Record: @3leapsdave
```

Every agent-opened PR must include this footer in the body:

```
---
Drafted-By: <Model Name> (<Agentic Tool>)
Role: <your-role-slug>
PR-of-Record: @3leapsdave
```

## Dev Environment

```bash
make check    # fmt + clippy + test
make build    # cargo build
make msrv     # verify MSRV compilation
make pr-final # CI-exact merge gate
```

## Local machine overrides

Create `AGENTS.local.md` for machine-specific notes. This file is gitignored and must not
be committed. Use it to reference private repos, internal briefs, and machine-specific paths.
