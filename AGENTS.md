# AI Agent Guide — chanvoy

Start every session with:

1. `/Users/davethompson/dev/lanytehq/AGENTS.md`
2. **Machine-local config** — read `AGENTS.local.md` in this repo if present (gitignored,
   contains references to private repos and internal briefs)
3. `/Users/davethompson/dev/lanytehq/lanyte-crucible/docs/guides/dev-warmup.md`
4. This repo's `REPOSITORY_SAFETY_PROTOCOLS.md`

## Working rules

- This repo is the Mattermost/chat bridge peer for the Lanyte platform.
- This repo is standalone. Do not add dependencies on crates in the lanyte workspace.
- Use feature branches and PRs; no direct pushes to `main`.
- Keep Rust MSRV at `1.85.0` and avoid nightly features.
- Follow the agentic attribution format from dev-warmup.md §5 (email: `noreply@lanytehq.dev`).

## Local machine overrides

Create `AGENTS.local.md` for machine-specific notes. This file is gitignored and must not
be committed. Use it to reference private repos, internal briefs, and machine-specific paths.
