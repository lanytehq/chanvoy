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

### Primary path: `chanvoy auto-setup`

After sourcing the appropriate Lanyte identity env script for your role and scope:

```bash
chanvoy auto-setup
```

This single command:

- Materializes the canonical `<role>-<scope>` profile from your sourced env (per `LANYTE_AGENT_ROLE` + `LANYTE_AGENT_SCOPE`)
- Starts the daemon if it isn't already running
- Seeds channel cursors so subsequent `chanvoy check <channel>` calls return useful state without a fresh time-window probe

Subsequent `chanvoy ...` commands work without `--profile` — the resolver picks the canonical profile from your sourced env automatically. Required env: `LANYTE_AGENT_ROLE`, `LANYTE_AGENT_SCOPE`, `LANYTE_MM_URL`, and a token reachable via `LANYTE_MM_TOKEN` (or the env name configured by `CHANVOY_TOKEN_ENV_NAME`).

### Manual path (debugging or custom scenarios)

For cases where `auto-setup` is the wrong shape — explicit profile naming, custom team-name override, debugging the bootstrap path — the original two-step flow remains available:

```bash
chanvoy profile create-from-env --activate
chanvoy daemon start
```

Use this path only when you have a specific reason to deviate from the canonical flow.

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

## Profile Resolution

When `chanvoy` is invoked without `--profile <name>`, the resolver picks a profile in order:

1. **Explicit `--profile <name>`** — operator's stated intent; always wins.
2. **`CHANVOY_PROFILE` env var** — explicit override. Refuses if the named profile doesn't exist on disk.
3. **Env-derived `<role>-<scope>` exact-name** — when `LANYTE_AGENT_ROLE` and `LANYTE_AGENT_SCOPE` are set, resolves to the profile named exactly `<role>-<scope>`. Refuses with the available-profile list if no exact match exists (does not silently fall through to a different identity).
4. **Single running daemon** — if exactly one chanvoy daemon is currently running on this machine, that profile is used.
5. **`active_profile` marker** — single-tenant convenience. Only consulted when env vars are unset and no daemon is running. Updated by `chanvoy auto-setup` and `chanvoy profile activate <name>`.
6. **Refuse** — print the available-profile list and require explicit `--profile`.

Two carve-outs:

- **Profile-collection management verbs** (`profile list`, `profile create`, `profile create-from-env`) and the **`auto-setup` bootstrap verb** bypass this resolver entirely. They operate on the profile collection or env-derived synthesis, not on a single existing target — and forcing resolution would brick fresh bootstrap on an empty config.
- **Side-effecting daemon-lifecycle verbs** (currently just `daemon stop`) refuse on rules 4 and 5. They require an explicit target — `--profile`, `CHANVOY_PROFILE`, or env-derived `<role>-<scope>` — to avoid acting on another operator's daemon on a shared machine.

The full resolver contract, including policy semantics and per-rule rationale, lives in [`lanyte-crucible/docs/specs/agent-chat-conventions.md`](https://github.com/lanytehq/lanyte-crucible/blob/main/docs/specs/agent-chat-conventions.md) §"Chanvoy Profile Naming".

### Stale `active_profile` recovery

After a profile rename or deletion (e.g., a coordinated migration sweep), the `active_profile` marker may point at a profile that no longer exists. The resolver detects this and refuses with `ActiveProfileNotFound` rather than silently falling through to a different identity:

```
Error: Resolver(ActiveProfileNotFound { name: "old-bare-name", ... })
```

Recovery: rerun `chanvoy auto-setup` to refresh the marker against your current sourced env. The diagnostic error is intentional — it surfaces the stale state instead of letting it propagate as silent mis-attribution.

### `chanvoy profile active`

Reports the current marker contents directly. When no marker is set, prints `(none)` (text mode) or `null` (JSON mode). This replaces a pre-PER-012 fallback that synthesized a name from the resolver — scripts or agents parsing this output to gate behavior may need updating to handle the explicit-empty case.

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
