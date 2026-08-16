# Contributing to chanvoy

Thanks for helping build **chanvoy** — the Mattermost (and eventually Slack)
bridge for AI agents and the operators who run them.

This repo aims to be:

- **agent-first** (the daily user is a long-running autonomous agent; humans are a secondary audience),
- **cross-platform** (Linux + macOS today; Windows when the install convention proves out under the local-daemon model),
- **license-clean** (dual MIT OR Apache-2.0, no copyleft dependencies),
- **public-readable** (every committed file, including this one, is written assuming a public adopter is the reader).

If you are new to chanvoy as an operator or agent, start at
[`docs/getting-started.md`](./docs/getting-started.md) instead. This file
covers the contributor's side.

## Quick start (contributors)

1. Install Rust and the repo toolchain (MSRV is **1.89.0**):

   - `rustup toolchain install 1.89.0`
   - `rustup component add rustfmt clippy`

2. Run the full local quality loop:

   - `cargo fmt --all`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo test --workspace --all-targets`

   Or use the Makefile:

   - `make pr-final` — CI-exact final gate before pushing a PR branch
   - `make check` — fast loop (fmt + clippy + test)
   - `make install` — installs the release binary into `$LOCAL_BIN` (default `~/.local/bin` on Linux/macOS, `$USERPROFILE\bin` on Windows)

> CI is the source of truth; see `.github/workflows/check.yml`.

## Repo layout

```
.
├── crates/
│   ├── chanvoy-core      shared domain types, profile model, JSON-RPC envelopes, MM client
│   ├── chanvoy-daemon    UDS server, identity validation, WebSocket client, attention-state
│   ├── chanvoy-cli       CLI surface, argument parsing, output formatting
│   ├── chanvoy-ipc       JSON-RPC envelope types (factored out for chanvoy-mcp reuse)
│   └── chanvoy-mcp       MCP 2026-07-28 access face (stdio + loopback HTTP)
├── src/main.rs           binary entry, wires chanvoy-cli into the binary
├── tests/                workspace-level integration tests (wiremock-based)
├── scripts/              diagnostic + smoke scripts (per-NNN-diag.sh, per-NNN-smoke.sh)
└── docs/                 user-facing and contributor-facing documentation
```

See [`docs/architecture.md`](./docs/architecture.md) for the runtime model
— daemon lifecycle, profile→bot binding, attention-state cursor isolation,
peer contract relationship.

## Branching model

All feature work happens on branches and lands via pull requests.

### Branch naming

- `feat/<slug>` — new features
- `fix/<slug>` — bug fixes
- `docs/<slug>` — documentation changes
- `chore/<slug>` — build, CI, dependency updates

For automation-driven work (LLM-assisted agents), prefer the
role-prefixed shape `<type>/<slug>-<role>-<YYYYMMDD>` (e.g.,
`docs/per-028-public-readiness-prodmktg-20260514`). This is the
convention used by the supervised-commit chain that drives much of
this repository's work.

### Workflow

1. Create a branch from `main`
2. Develop and commit locally
3. Run `make pr-final`
4. Push the feature branch to origin
5. Open a PR against `main` (`gh pr create`)
6. CI runs automatically on the PR
7. Address review feedback
8. Merge after required review and green CI

## Commit attribution

Chanvoy is built by a mix of human contributors and supervised AI
agents. Both follow the same attribution shape — commits land under
the supervising human's git identity, with the agent identified in
the trailer block:

```
Role: <role-slug>
Committer-of-Record: @<human-supervisor-handle>

Co-authored-by: <Model Name> (<Agentic Tool>) <noreply@lanytehq.dev>
```

For human-only contributions, the `Role:` and `Committer-of-Record:`
lines are optional; the `Co-authored-by:` line is the load-bearing
attribution.

The same convention applies to PR bodies — agent-drafted PRs include a
`Drafted-By: <Model Name> (<Agentic Tool>)` line at the bottom for
provenance. Human-drafted PRs need no special footer.

See [`AGENTS.md`](./AGENTS.md) for the agent-session conventions on
top of this baseline.

## Test discipline

Workspace integration tests live under `tests/` and exercise the
real `chanvoy` binary against a `wiremock` Mattermost. They are
gated behind `#[ignore]` so the fast loop stays fast; `make pr-final`
runs them explicitly.

URL-shape drift between the chanvoy-core HTTP client and Mattermost's
actual endpoints is guarded by a two-tier harness:

- **Tier-A — canonical endpoint manifest + fixture replay** runs on
  every `make pr-final` in CI. `tests/url_shape_replay.rs` walks the
  canonical manifest at
  [`tests/fixtures/mm-v4-shapes/endpoints.json`](./tests/fixtures/mm-v4-shapes/endpoints.json),
  mounts an exact-path wiremock per entry, and asserts every
  call-site issues the manifest's URL. Adding a new MM-endpoint
  call from chanvoy-core requires landing an entry here first
  (schemas-before-code).
- **Tier-B — live-MM safe-subset smoke** runs once per release
  candidate before tag/sign. [`scripts/release-smoke.sh`](./scripts/release-smoke.sh)
  exercises the safe subset of verbs (read/post/pinned/ack/check/
  react/unreact/search/notifications/channel-create/channel-archive,
  plus `whoami` and `channels`) against a disposable test team on
  a real Mattermost server. Daemon-state RPCs (`attention list/show`),
  admin-only verbs (`channel restore`), and peer-principal-dependent
  verbs (`dm`/`dms`) are intentionally excluded from Tier-B; their
  contracts are exercised by Tier-A or other tests.

Tier-A catches URL-shape drift broadly and fails in CI; Tier-B
proves the safe-subset works end-to-end against real Mattermost
before signing.

When adding tests:

- Use synthetic profile names, server URLs (`http://localhost:<port>` against
  `wiremock`), and Mattermost IDs (`chan-id-XXX`, `team-id-XXX`).
- Never commit fixtures that contain real tokens, hostnames, or post IDs.
  See [`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md)
  for the never-commit list.

## Reviewer routing

Reviewer routing for chanvoy work follows the role-shaped review chain
established in the project's release wave conventions:

- **devrev** — correctness, test discipline, CLI surface shape
- **secrev** — security-sensitive changes (credential handling, daemon
  lifecycle, sandbox handling, permission contract, downstream contract
  surfaces enumerated in `REPOSITORY_SAFETY_PROTOCOLS.md`)
- **entarch** — architectural changes (public types in `chanvoy-core`,
  daemon RPC shape, attention-state schema, cursor-advance taxonomy,
  peer contract surfaces)

Smaller changes only need devrev. Anything that touches the
downstream-contract surfaces listed in `REPOSITORY_SAFETY_PROTOCOLS.md`
warrants secrev or entarch in addition. Reviewer pings are by GitHub
handle; the maintainer (`@3leapsdave`) is the final merge gate.

## Code of conduct

We commit to a respectful, welcoming environment for everyone:
contributors, users, agent operators, downstream adopters, and
security researchers.

Be respectful. Critique work, not people. Disagreement is fine;
hostility, harassment, and discrimination are not — and will be
treated as conduct issues regardless of the technical content of the
disagreement.

If you experience or witness behavior that conflicts with this
posture, contact the maintainer (`@3leapsdave` on GitHub, or via the
private channel documented in [`SECURITY.md`](./SECURITY.md) for
serious matters).

A formal `CODE_OF_CONDUCT.md` (likely the Contributor Covenant) may
land if real-world governance need surfaces; until then, the
paragraph above is the operative posture.

## Reporting a security issue

Please report security issues privately. See [`SECURITY.md`](./SECURITY.md)
for the reporting path, response SLA, and disclosure policy.

## Further reading

- [`README.md`](./README.md) — project overview, status, quick start
- [`docs/getting-started.md`](./docs/getting-started.md) — first-time agent / operator onboarding
- [`docs/operator-guide.md`](./docs/operator-guide.md) — per-command reference
- [`docs/architecture.md`](./docs/architecture.md) — runtime model
- [`docs/integration-tests.md`](./docs/integration-tests.md) — test conventions
- [`AGENTS.md`](./AGENTS.md) — agent-session conventions
- [`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md) — never-commit list, permission contract, downstream contract surfaces
- [`SECURITY.md`](./SECURITY.md) — vulnerability reporting + verification
