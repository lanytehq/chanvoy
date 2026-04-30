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

### Product namespace (not org restriction)

The `lanytehq` segment in the default config root (`~/.config/lanytehq/chanvoy/`) is the **product namespace** — chanvoy is a lanytehq-developed tool that namespaces its config the way other vendor-developed tools do (e.g., `~/.config/google/...` housing Google products for non-Google users). It is **not** an org restriction. Operators in any org — lanytehq, enacthq, fulmenhq, third-party adopters — use the same default path. Profile data inside that path is partitioned per-profile, and profile names encode the org via the `<role>-<scope>` convention (see "Profile and Team Naming Convention" below).

### Path overrides

For isolated testing, parallel local sessions, or non-default deployment shapes:

- **`CHANVOY_CONFIG_DIR`** — overrides the default config root (`~/.config/lanytehq/chanvoy/`). Profile files, the `active_profile` marker, and per-profile attention state all live under this path. Operator-set per shell.
- **`CHANVOY_RUNTIME_DIR`** — overrides the default runtime directory (`$XDG_RUNTIME_DIR/chanvoy/` or OS temp). Socket and pid file locations follow this. Operator-set per shell.

Example: run a parallel chanvoy session against an isolated profile set:

```bash
export CHANVOY_CONFIG_DIR="$HOME/chanvoy-test/config"
export CHANVOY_RUNTIME_DIR="$HOME/chanvoy-test/runtime"
chanvoy auto-setup
chanvoy daemon status
```

The two overrides are independent; either can be set without the other. Profile data and runtime state partition by directory.

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

## Profile and Team Naming Convention

Chanvoy profile names and Mattermost team names follow a portable convention that lets `auto-setup` and the resolver work without operator intervention:

- **Profile name:** `<role>-<scope>` (e.g., `cxotech-lanytehq`, `delta-devlead-enacthq`, `bravo-devrev-lanytehq`)
- **Team name:** `org-<scope>` (e.g., `org-lanytehq`, `org-enacthq`)
- **Identity script filename:** `<role>-<scope>.sh` (the source-able shell script that exports `LANYTE_AGENT_ROLE`, `LANYTE_AGENT_SCOPE`, `LANYTE_MM_URL`, etc.)

When you source an identity script with these names, `chanvoy auto-setup` synthesizes the canonical profile name (`<role>-<scope>`) and derives the team name (`org-<scope>`) automatically. Subsequent commands resolve to the canonical profile via your sourced env (no `--profile` flag needed).

The full convention, including rationale and the migration story for legacy bare-name profiles, lives in [`lanyte-crucible/docs/specs/agent-chat-conventions.md`](https://github.com/lanytehq/lanyte-crucible/blob/main/docs/specs/agent-chat-conventions.md) §"Chanvoy Profile Naming".

## Using Chanvoy in Another Org

Chanvoy is org-portable. Operators in any org adopt it the same way:

1. **Source your org's identity script** (e.g., `cxotech-enacthq.sh`, `delta-devlead-fulmenhq.sh`). The script must set `LANYTE_AGENT_ROLE`, `LANYTE_AGENT_SCOPE`, `LANYTE_MM_URL`, and a token reachable via `LANYTE_MM_TOKEN`.
2. **Run `chanvoy auto-setup`.** The canonical profile is synthesized as `<role>-<scope>` (e.g., `cxotech-enacthq`) with team `org-<scope>` (e.g., `org-enacthq`). No `org-lanytehq` is hardcoded anywhere on the creation path.
3. **Use `chanvoy ...` normally.** Default resolution picks the canonical profile from your sourced env automatically.

The `lanytehq` segment in the default config path (`~/.config/lanytehq/chanvoy/`) is the chanvoy product namespace, not an org binding — see "Product namespace (not org restriction)" above. If your environment requires it, override with `CHANVOY_CONFIG_DIR`.

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

## Cross-Team Channel Resolution

As of chanvoy 0.1.3+, channel-name arguments resolve across every
team the bot is a member of. Previously (≤ 0.1.2), every CLI verb
that took a channel name searched only the profile's primary team
and silently 404'd when the channel lived on a different team —
exactly the failure SOP-MM-015 cross-org standing channels expose.

### Resolution chain (γ hybrid)

In order of precedence:

1. **Explicit `<team>/<channel>` syntax** — wins over everything.
   Useful when scripts/agents need to pin a specific team:
   ```bash
   chanvoy post 3-leaps-operations/leadership "..."
   chanvoy read 3-leaps-operations/leadership --since 60
   ```
2. **Explicit `--team <slug>` flag** — same effect as the syntax
   above, but as a flag for verbs that already take other flags:
   ```bash
   chanvoy read leadership --team 3-leaps-operations --since 60
   ```
3. **Profile's primary team** — the `team_name` your profile binds
   to. Tried first; this is the common case and incurs no extra
   API call when the channel lives on the primary team.
4. **Fallback across other member teams** — if the primary team
   doesn't have the channel, chanvoy searches every team the bot
   is a member of (cached for 15 minutes; the cache force-refreshes
   on a no-match before failing so newly-added memberships surface
   without re-running `auto-setup`).

### Diagnostics

When the resolver can't pick a single team, the CLI refuses with one
of three distinct error shapes — never a generic 404:

- **No-match**: `channel "<name>" not found on any team you are a
  member of. Teams searched: [...]`. Suggests either checking the
  spelling or asking dispatch to add the bot to the team that
  actually hosts the channel.
- **Not-a-member** (only via explicit `<team>/<channel>` or
  `--team`): `team "<slug>" requested via <team>/<channel> syntax,
  but you are not a member of it. Teams you are a member of: [...]`.
- **Ambiguous**: `channel "<name>" is ambiguous — found on multiple
  teams: [...]. Use --team <slug> or <team>/<channel> syntax to
  disambiguate.`

### Cursor isolation

Cursors (for `read --since-last-mine`, attention freshness, mention
tracking) are tracked per qualified `<team>/<channel>` pair, so
same-named channels on different teams maintain independent state —
reading `org-lanytehq/general` does not advance cursors for
`3-leaps-operations/general`.

If you upgrade from a pre-0.1.3 daemon and a previously-tracked
channel name now exists on multiple of your member teams, the
migration **quarantines** that record rather than silently binding
it to one team. The next read/post on the channel via `--team` or
`<team>/<channel>` re-establishes a fresh cursor under the correct
qualified key. Quarantined records are preserved verbatim under
`AttentionState.quarantined` for inspection.

### `chanvoy channels` cross-team output

The default `chanvoy channels` listing now groups by team:

```
=== org-lanytehq ===
  org-lanytehq/general
  org-lanytehq/per-019

=== 3-leaps-operations ===
  3-leaps-operations/development
  3-leaps-operations/leadership
```

The qualified `<team>/<channel>` form on each line is directly
copy-pasteable into `chanvoy read` / `post` / `check`.

Flags:
- `--team <slug>` — list only that team's channels.
- `--primary-team` — pre-0.1.3 single-team output for tooling that
  depends on the old shape.
- `--json` — structured per-team output with a `qualified` field
  for each channel.

## Profile Resolution

When `chanvoy` is invoked without `--profile <name>`, the resolver picks a profile in order:

1. **Explicit `--profile <name>`** — operator's stated intent; always wins.
2. **`CHANVOY_PROFILE` env var** — explicit override. Refuses if the named profile doesn't exist on disk.
3. **Env-derived `<role>-<scope>` exact-name** — when `LANYTE_AGENT_ROLE` and `LANYTE_AGENT_SCOPE` are set, resolves to the profile named exactly `<role>-<scope>`. Refuses with the available-profile list if no exact match exists (does not silently fall through to a different identity).
4. **Single running daemon** — if exactly one chanvoy daemon is currently running on this machine, that profile is used.
5. **`active_profile` marker** — single-tenant convenience. Only consulted when env vars are unset and no daemon is running. Updated explicitly by `chanvoy auto-setup` and by `--activate` on `chanvoy profile create` / `chanvoy profile create-from-env`. Also updated implicitly: `profile create` / `profile create-from-env` (without `--activate`) will activate a freshly created profile when no active marker exists yet, so first-profile setup leaves the marker pointing at the new profile.
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

Reports the current marker contents directly. Output shape:

| Marker state | Text mode | JSON mode |
|---|---|---|
| Set to `<name>` | `<name>` | `{"active_profile": "<name>"}` |
| Empty | `(none)` | `{"active_profile": null}` |

This replaces a pre-PER-012 fallback that synthesized a name from the resolver — scripts or agents parsing this output to gate behavior may need updating to handle the explicit-empty case (text `(none)` literal, or `.active_profile` field that may be JSON `null`).

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

## Sandboxed Agent Contexts

Chanvoy is most often invoked from an unsandboxed shell (Terminal, tmux,
direct ssh session). In that case `chanvoy auto-setup` works as
described above — it spawns a detached daemon, the daemon contacts
Mattermost for the identity check, and subsequent CLI invocations talk
to it via Unix socket.

Some agent contexts run with sandbox restrictions that the daemon's
detached child cannot escalate at startup — Codex agents, OSS users
running chanvoy under similar `sandbox-exec`-style policies, Docker
containers without `--network`, etc.

### Native handling (PER-014)

As of chanvoy 0.1.2+, `chanvoy auto-setup` works natively under
sandbox restrictions. The CLI parent process — which already runs in
the operator's interactive shell context where sandbox network-approval
prompts can fire — performs the Mattermost `whoami()` identity check
and hands the validated identity to the detached daemon via a per-profile
bootstrap-state file plus a one-shot env nonce. The daemon child reads
the file, validates it (freshness + profile fingerprint + nonce), then
binds its UDS socket without any network call. WebSocket connections
fail gracefully and retry through the existing reconnect path
(PER-010), so a sandbox-blocked WS does not block daemon startup.

In practice: on the first `auto-setup` after sourcing your identity
profile, the parent CLI's `whoami()` triggers a single network-approval
prompt for the parent process. Approve once; the detached daemon
inherits the validated identity and never re-asks. Subsequent
`chanvoy read / post / check` calls from the same sandbox session
reach the daemon over the local UDS without any further prompts.

### Identity drift surface

If the bot identity diverges from the configured `bot_username`
post-bind (e.g., the token was rotated and now authenticates as a
different bot), `daemon_status.mattermost_identity_drift` reports
`true` and network-backed RPCs (`post`, `read`, `check`,
`notifications`, etc.) refuse with a clear diagnostic. The local
socket stays bound so `daemon_status` remains queryable. To
recover, re-run `chanvoy auto-setup` to re-validate identity end
to end.

### Foreground daemon serve (rare cases)

The original PER-013 workaround — running `chanvoy daemon serve` in
the foreground with explicit network approval at start time — is
retained for environments where the parent-side `whoami()` itself
cannot run interactively (e.g., fully non-interactive batch contexts
where no approval prompt can be answered):

```bash
# In one shell, with network approval granted to this command:
chanvoy --profile <name> daemon serve

# Once the foreground daemon prints "websocket authenticated and healthy",
# subsequent commands from the same sandbox can use it:
chanvoy daemon status
chanvoy read <channel> --since 60
chanvoy post <channel> "..."
```

This path is the rare-case fallback; for typical Codex / sandbox-exec
operator flows, prefer `auto-setup`.

> Sandbox-approval semantics ("approve network access at parent
> `whoami`") vary by sandbox implementation. PER-014's design is
> sandbox-agnostic — it does not detect or branch on sandbox shape;
> it simply moves the network call to where approval can be granted
> (the parent CLI). Originally documented from the 2026-04-25
> `agent-bravo-devrev` Codex transcript; PER-014 ships in chanvoy
> 0.2.0+ with the structural fix.

## Migration Exception

`channel restore` is an intentional migration-contract exception.

- `lanyte-chat` lets the request reach Mattermost and returns the server permission failure.
- `chanvoy` enforces elevated capability locally and fails earlier with an elevated-capability error.

This stricter behavior is intentional. Agents needing restore must use an elevated-capability profile.
