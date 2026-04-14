# Chanvoy Operator Guide

`chanvoy` is the local Mattermost control-plane client for Lanyte agent sessions.

## Operating Model

- `chanvoy` uses a local per-profile daemon over a Unix socket.
- Profiles and persisted config live under the platform-native config root for `lanytehq`.
- Runtime sockets and pid files live outside the config root under the runtime temp area.
- Current PER-007 readiness is for local-daemon replacement of `lanyte-chat`, not a remote proxy/control-plane deployment.

Current validated local-daemon support is:

- Linux
- macOS

## Config Paths

- Linux: `~/.config/lanytehq/chanvoy/`
- macOS: `~/Library/Application Support/lanytehq/chanvoy/`

Windows note:

- Windows is not yet a supported local-daemon platform in the current implementation.
- If/when Windows support is added under the agreed config-root convention, the expected config root will be `%APPDATA%\lanytehq\chanvoy\`.

Profile files are stored under `profiles/` beneath that root.

Examples:

- Linux profile dir: `~/.config/lanytehq/chanvoy/profiles/`
- macOS profile dir: `~/Library/Application Support/lanytehq/chanvoy/profiles/`

Runtime files are separate from config:

- Socket: `XDG_RUNTIME_DIR/chanvoy/<profile>.sock` when available, otherwise the OS temp dir
- Pid: same runtime directory as the socket

Note: the broader `LANYTE_CONFIG_ROOT` override is a cross-app standardization follow-up and is not implemented in `chanvoy` as part of PER-007.

## Bootstrap Flow

Recommended shell setup:

1. Source the normal Lanyte identity env.
2. Ensure `LANYTE_MM_URL`, `LANYTE_MM_TOKEN`, and `LANYTE_MM_TEAM` are present.
3. Create a profile once with `chanvoy profile create-from-env --activate`.
4. Start the daemon with `chanvoy daemon start`.
5. Use plain `chanvoy ...` commands after that.

## Resume And Attention

PER-008 adds cursor-based local-mode workflow primitives:

- `chanvoy read <channel> --after <post-id>`
- `chanvoy read <channel> --since-last-mine`
- `chanvoy check <channel> [--after <post-id>]`
- `chanvoy notifications --unread`

Current semantics:

- `read --after` is a pure read and does not advance stored channel state
- `read --since-last-mine` is a pure read and does not advance stored channel state
- `check` is a pure probe and does not advance stored channel state
- `notifications --unread` is a pure probe and does not advance mention state
- `check <channel>` without `--after` uses the stored daemon cursor when available
- if no stored cursor exists yet, `check` returns `new: 0 anchor=none source=no_anchor` with exit code `1`
- if a stored daemon cursor becomes stale or unreadable, `check` degrades to `new: 0 anchor=none source=stale_cursor` with exit code `1` rather than erroring

Current durable cursor behavior:

- successful `post` stores the latest post id for that profile+channel
- full `notifications` reads store the latest mention cursor
- probes do not clear attention

Current inspectability gap worth tracking:

- there is not yet a first-class `doctor` or `status` surface for showing the current stored attention state file and cursor values
- operators can inspect the per-profile JSON state file directly under the config root for now

Profile resolution precedence is:

1. `--profile`
2. `CHANVOY_PROFILE`
3. unique env-derived match from `LANYTE_AGENT_ROLE` + `LANYTE_AGENT_SCOPE`
4. stored active profile
5. literal `default`

## Daemon Lifecycle

- `chanvoy daemon start`
  - starts the local daemon when absent
  - reports `already running` if an existing daemon is healthy
  - removes a stale socket before binding
- `chanvoy daemon status`
  - reports socket path, profile, and Mattermost health
- `chanvoy daemon stop`
  - stops a running daemon
  - returns `NotRunning` if the daemon is already absent

Observed PER-007 lifecycle behavior:

- stale socket cleanup works on next `daemon start`
- rebuilding the binary requires daemon restart to pick up new RPC surface/output behavior

## Migration Exception

`channel restore` is an intentional migration-contract exception.

- `lanyte-chat` lets the request reach Mattermost and returns the server permission failure.
- `chanvoy` enforces elevated capability locally and fails earlier with an elevated-capability error.

This stricter behavior is intentional. Agents needing restore must use an elevated-capability profile.
