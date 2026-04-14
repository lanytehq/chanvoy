# Chanvoy

Chanvoy is the Mattermost/chat bridge peer for the Lanyte platform.

`chanvoy` is the local daemon-backed replacement for `lanyte-chat` in agent workflows.

This repository currently contains the M1 control-plane scaffold:

- `chanvoy-core`: shared domain types, profile model, JSON-RPC envelopes, and Mattermost client
- `chanvoy-daemon`: local unix-socket daemon and profile-bound control plane
- `chanvoy-cli`: CLI client surface
- `chanvoy-mcp`: MCP bridge scaffold

## Current Scope

- local per-profile daemon over a Unix socket
- Mattermost operator flows for agent sessions
- local-daemon dogfooding readiness validated in PER-007

Current validated local-daemon support is Unix-only:

- Linux
- macOS

Remote proxy/control-plane serving is not the validated operating mode yet.

## Quick Start

After sourcing the normal Lanyte identity env:

```bash
cargo build
target/debug/chanvoy profile create-from-env --activate
target/debug/chanvoy daemon start
target/debug/chanvoy whoami
target/debug/chanvoy read per-007 --since 60
```

## Paths

Config root is platform-native under the `lanytehq` namespace:

- Linux: `~/.config/lanytehq/chanvoy/`
- macOS: `~/Library/Application Support/lanytehq/chanvoy/`

Windows is not yet a supported local-daemon platform for the current `chanvoy` implementation. If/when Windows support is added under the agreed config-root convention, the expected config root will be `%APPDATA%\\lanytehq\\chanvoy\\`.

Runtime sockets and pid files are stored under `XDG_RUNTIME_DIR` when available, otherwise the OS temp dir.

## Commands

Common commands:

```bash
target/debug/chanvoy whoami
target/debug/chanvoy channels
target/debug/chanvoy read <channel> --since 60
target/debug/chanvoy post <channel> "message"
target/debug/chanvoy dms
target/debug/chanvoy dm <username> "message"
target/debug/chanvoy notifications
target/debug/chanvoy daemon status
```

## Docs

- `docs/operator-guide.md`
- `docs/migration-runbook.md`
- `scripts/per007-smoke.sh`

## Security Note

Chanvoy M1 assumes the local Unix account is the trust boundary.

- Runtime and config files are permission-hardened (`0700` directories, `0600` files) to reduce accidental cross-process exposure.
- Admin-only operations are gated by explicit profile capability class.
- Chanvoy M1 does not attempt to protect one process from another process running as the same Unix user.

That same-user limitation is an acknowledged M1 boundary and should be revisited before adding richer daemon lifecycle ergonomics, auto-start behavior, or deployment models that span different Unix users or service accounts.
