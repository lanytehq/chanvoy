# Chanvoy

[![CI](https://github.com/lanytehq/chanvoy/actions/workflows/check.yml/badge.svg?branch=main)](https://github.com/lanytehq/chanvoy/actions/workflows/check.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.89.0-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/github/v/tag/lanytehq/chanvoy?label=version&sort=semver)](https://github.com/lanytehq/chanvoy/releases)

A Mattermost (and eventually Slack) bridge for AI agents and the
operators who run them. Chanvoy gives agents a seat at the table in
the chat tools where teams already coordinate — joining channels,
reading context, posting status, replying in threads, reacting,
all under explicit delegation and autonomy gating.

Where mlvoy bridges the inbox, chanvoy bridges the war room.

> **Agents start here:** [`docs/getting-started.md`](./docs/getting-started.md).
> A 30-minute walkthrough from zero to a working session, with a
> sandboxed-agent path tree if you can't reach the local socket.
> The "Agents start here" pointer at the top of that doc is a
> three-line escape hatch if you only need the bootstrap.

---

## Status

Chanvoy v0.3.x. Local-mode (CLI ↔ per-profile daemon ↔ Mattermost)
is the validated operating mode; the remote control plane and
attested transport remain on the roadmap (deferred).

What's shipped:

- **Local daemon model** — per-profile, detached, survives shell
  exit; idempotent restart and stale-socket recovery.
- **Cross-team channel resolution** — channel names resolve across
  every team the bot is a member of. `<team>/<channel>` syntax and
  `--team <slug>` flag for explicit pinning. Cursor isolation per
  qualified channel.
- **Sandbox-aware bootstrap** — `auto-setup` works under sandbox
  restrictions (Codex agents, macOS sandbox-exec, Docker without
  `--network`, OSS sandboxes of similar shape). Identity validation
  runs in the parent CLI; the detached daemon needs no network at
  startup.
- **Session-start ergonomics** — `pinned`, `read --since-bootstrap`,
  `ack`, time-unit suffixes (`30s` / `5m` / `4h` / `2d`) on every
  time-window flag.
- **Conversation primitives** — threaded replies via `post --reply-to`,
  emoji reactions via `react` / `unreact`.
- **Post and thread rehydration** — `show <channel> <post-id>` reopens
  a single cited post; `thread <channel> <post-id>` reads the whole
  conversation and accepts the root's id or any reply's. Both prove
  the post belongs to the named channel before returning any of it.
  Human `read` rows carry `id=` and `root=` crumbs so a citation can
  be handed straight to another verb without re-reading with `--json`.
- **Discovery primitives** — `search <channel> <query>` over MM's
  search endpoint, `channels --sort active` traffic-aware listing.
- **Cross-team channel admin** — `channel create --team <slug>`
  for bots authorized on multiple teams.

Validated platforms: Linux, macOS. Windows is not currently a
supported local-daemon platform.

---

## Quick Start

After installing chanvoy and sourcing your identity profile (so
`LANYTE_AGENT_ROLE`, `LANYTE_AGENT_SCOPE`, `LANYTE_MM_URL`, and a
token reachable via `LANYTE_MM_TOKEN` are set):

```bash
chanvoy auto-setup
chanvoy whoami
chanvoy channels
chanvoy read <channel> --since 1h
```

`chanvoy auto-setup` materializes the canonical `<role>-<scope>`
profile from your sourced env, starts the daemon, and seeds channel
cursors in one step. Subsequent commands work without `--profile` —
the resolver picks the canonical profile automatically.

If you're inside a sandbox (Codex agent, `sandbox-exec`, Docker
without `--network`, or similar), read
[`docs/getting-started.md` §Sandboxed agents](./docs/getting-started.md#sandboxed-agents)
before running `auto-setup`. Most sandboxes work with no extra
configuration; some need `CHANVOY_RUNTIME_DIR` set to a writable
path inside the sandbox.

For the full walkthrough — install, prerequisites, first read, first
post, cross-team example, daily flow — see
[`docs/getting-started.md`](./docs/getting-started.md).

For development against the working copy without installing, replace
`chanvoy` with `cargo run -p chanvoy --` in any of the commands
above.

---

## Install

Build from source:

```bash
make install
```

Default install location:

- Linux / macOS: `~/.local/bin/chanvoy`
- Windows (when supported): `%USERPROFILE%\bin\chanvoy.exe`

Override with `LOCAL_BIN=<path> make install`. Make sure the chosen
directory is on your `PATH`.

MSRV is Rust 1.89.0; older toolchains will fail to build. If you
don't have a Rust toolchain, [install rustup](https://rustup.rs) first.

Verify:

```bash
chanvoy --version
```

---

## Commands at a glance

A categorized index — see [`docs/operator-guide.md`](./docs/operator-guide.md)
for per-command reference, flags, and worked examples.

| Category | Commands | Notes |
|---|---|---|
| **Bootstrap & lifecycle** | `auto-setup`, `daemon {start,serve,stop,status}`, `profile {list,active,create,create-from-env}`, `whoami` | `auto-setup` is the canonical bootstrap. `daemon serve` is the foreground variant for debug or sandbox parent-shell use. |
| **Reading (cursor-neutral)** | `channels`, `pinned <ch>`, `read <ch>` (with `--since` / `--after` / `--since-bootstrap` / `--since-last-mine` / `--limit`), `check <ch>`, `dms`, `notifications --unread`, `search <ch> <query>`, `show <ch> <post-id>`, `thread <ch> <post-id>` (with `--latest`) | Pure reads; do not advance cursors unless `--advance` is passed on `read`. `show` / `thread` refuse a post that is not in the named channel before returning any content. |
| **Cursor-advancing reads** | `read --advance`, `ack <ch>`, full `notifications` (without `--unread`; with or without `--since`) | Channel-cursor advance for `read --advance` / `ack`; mention-cursor advance for full `notifications`. |
| **Writing** | `post <ch> <msg>` (with `--reply-to`), `dm <user> <msg>`, `notify <bot> <msg>`, `react <ch> <post-id> <emoji>`, `unreact ...` | Only `post` advances the channel cursor; `dm`, `notify`, `react`, `unreact` are cursor-neutral. |
| **Channel admin** | `channel {create,archive,restore,add-member}` (with `--team` for cross-team where authorized) | `restore` requires an elevated-capability profile. |
| **Wait / probe** | `wait <ch> --timeout` | Block until new posts arrive or timeout. |
| **Inspect (state, not chat)** | `attention {list,show}` | Strictly read-only on daemon state; never issues Mattermost API calls. |

Time-window flags (`read --since`, `notifications --since`, `wait
--timeout`, `search --since`) accept `s` / `m` / `h` / `d` suffixes.
Bare integer = minutes (today's default). Uppercase `M` and `mo` are
loud-failed to avoid month/minute confusion.

---

## Paths

Config root (default, platform-native under the chanvoy product
namespace):

- Linux: `~/.config/lanytehq/chanvoy/`
- macOS: `~/Library/Application Support/lanytehq/chanvoy/`

Runtime sockets and pid files live separately, under
`$XDG_RUNTIME_DIR/chanvoy/` when available, otherwise the OS temp
dir.

Two env overrides for non-default deployments (sandboxed agents,
parallel test sessions, custom layouts):

- `CHANVOY_CONFIG_DIR` — overrides the config root.
- `CHANVOY_RUNTIME_DIR` — overrides the runtime directory.

Note on the namespace: the `lanytehq` segment in the default config
root is the **product namespace** (chanvoy is a lanytehq-developed
tool), not an org restriction. Operators in any org — lanytehq,
enacthq, fulmenhq, third-party adopters — use the same default
path. Profile data is partitioned per-profile, and profile names
encode the org via the `<role>-<scope>` convention.

---

## Documentation

| Doc | When to read |
|---|---|
| [`docs/getting-started.md`](./docs/getting-started.md) | First time using chanvoy, or onboarding a new agent / operator. |
| [`docs/operator-guide.md`](./docs/operator-guide.md) | Per-command reference; full flag and behavior detail. |
| [`docs/troubleshooting.md`](./docs/troubleshooting.md) | Symptom-keyed recovery for common failure modes. |
| [`docs/architecture.md`](./docs/architecture.md) | Runtime model — daemon, cursors, profiles, peer contract. For contributors and bootstrap-curious agents. |
| [`docs/migration-runbook.md`](./docs/migration-runbook.md) | Replacing `lanyte-chat` with chanvoy across an existing deployment. |
| [`docs/integration-tests.md`](./docs/integration-tests.md) | For contributors adding tests. |
| [`BACKGROUNDER.md`](./BACKGROUNDER.md) | Why chanvoy exists and how it relates to mlvoy. |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | How to contribute — toolchain, branching, commit attribution, reviewer routing, code of conduct. |
| [`SECURITY.md`](./SECURITY.md) | How to report a security issue; signing-key verification posture; supported versions. |
| [`AGENTS.md`](./AGENTS.md) | Agent-session conventions for working in this repo. |
| [`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md) | Repository-level safety + commit-content rules. |

---

## Security note

Chanvoy assumes the local Unix account is the trust boundary.

- Runtime and config files are permission-hardened (`0700`
  directories, `0600` files) to reduce accidental cross-process
  exposure.
- Admin-only operations are gated by an explicit profile capability
  class.
- Chanvoy does not attempt to protect one process from another
  process running as the same Unix user.

The same-user trust boundary is intentional and not a defect. Multi-
user / multi-tenant deployments require the deferred remote control
plane (with attested transport); no local-mode workaround changes
this property.

See [`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md)
for repository-level safety rules (what never to commit, the
permission contract, downstream contract surfaces).

---

## License

Dual-licensed under MIT or Apache-2.0, at your option.
See [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE).
