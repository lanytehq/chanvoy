# Chanvoy Architecture

This doc describes the runtime model so contributors can change chanvoy
safely and bootstrap-curious agents can predict why commands behave the
way they do. For day-to-day usage, start with
[`getting-started.md`](./getting-started.md) instead.

> **Agents start here.** This doc is for understanding *why* chanvoy
> commands behave the way they do — particularly cursor isolation across
> teams, daemon detachment, and identity drift. If you only need to use
> chanvoy, you can skip it. If you ever hit "this command did the
> opposite of what I expected," the answer is probably in this doc.

---

## System overview

```
┌──────────────┐     IPC          ┌──────────────────┐    REST + WS    ┌─────────────────┐
│  chanvoy     │ ◄──── UDS ─────► │  chanvoy-daemon  │ ◄────────────► │   Mattermost    │
│  CLI / MCP   │   (JSON-RPC)     │  (per-profile,   │                  │   server        │
│              │                  │   long-running)  │                  │                 │
└──────────────┘                  └──────────────────┘                  └─────────────────┘
                                          ▲
                                          │ optional, future
                                          │ (deferred)
                                          ▼
                                  ┌──────────────────┐
                                  │  Lanyte core     │
                                  │  (channel 260,   │
                                  │   peer contract) │
                                  └──────────────────┘
```

Three layers, each isolated by a different boundary:

1. **CLI** (`chanvoy <verb>`) — short-lived process. Parses arguments,
   resolves the profile, dials the daemon's Unix-domain socket, sends
   one JSON-RPC request, prints the response, exits. Holds no state.
   `chanvoy mcp` is the same client as an MCP 2026-07-28 access face
   (stdio or loopback HTTP). It uses the same `DaemonClient` and does
   not open a second Mattermost connection. Blocking MCP `wait` does
   **not** wake Grok Bot.
2. **Daemon** (`chanvoy-daemon`) — long-running per-profile process.
   Owns the WebSocket connection to Mattermost, holds per-channel
   cursor state on disk, validates identity on bind, and serves CLI
   requests over the UDS socket.
3. **Mattermost** — external chat server. Owns channels, posts,
   memberships, and the bot's identity. Chanvoy never persists chat
   content; it's a pass-through with cursors.

The optional fourth layer — a Lanyte-core peer over IPC channel 260 —
is the contract chanvoy will eventually speak to integrate with the
broader Lanyte runtime. Today's chanvoy ships only the local-mode
subset (CLI + daemon); the channel 260 surface is documented in
[STD-006 peer contract](https://github.com/lanytehq/lanyte-crucible/tree/main/schemas/ipc)
but not wired into the daemon yet.

### Why a daemon at all?

Two reasons:

1. **WebSocket persistence.** Mattermost push events (`inbound_message`,
   `inbound_mention`) arrive over a long-lived WebSocket. A
   request/response CLI process can't hold the WS open across
   invocations; a daemon can.
2. **Cursor authority.** Per-channel cursor state (latest seen post,
   last mention timestamp, staleness verdict) needs a single writer to
   stay coherent. The daemon owns the on-disk state and serializes
   updates; the CLI just asks "where am I?" and "advance to here."

The daemon is intentionally **per-profile**, not per-machine. Each
profile binds to one bot identity on one Mattermost server, so each
gets its own daemon process, socket, and state file. Multiple profiles
on the same machine run multiple daemons in parallel.

---

## Daemon lifecycle

### Bootstrap: `chanvoy auto-setup`

The canonical bootstrap is one command:

```bash
chanvoy auto-setup
```

Five things happen, in order:

1. **Profile synthesis** from sourced env. The CLI reads
   `LANYTE_AGENT_ROLE` and `LANYTE_AGENT_SCOPE` and synthesizes a
   canonical profile name `<role>-<scope>` (e.g., `cxotech-lanytehq`).
   If a profile by that name already exists on disk it's refreshed
   from env; otherwise it's created.
2. **Identity validation** via Mattermost `whoami()`. The CLI parent
   process — *not* the daemon child — calls `whoami()` against the
   Mattermost server using the token in `LANYTE_MM_TOKEN` (or whatever
   env name `CHANVOY_TOKEN_ENV_NAME` points at). The validated bot
   identity is written to a per-profile bootstrap-state file along
   with a one-shot nonce. This step is the only network call in the
   bootstrap path.
3. **Daemon spawn** as a detached process. The CLI forks the
   `chanvoy daemon serve` subprocess and immediately calls
   `libc::setsid()` via `pre_exec`, making the daemon the leader of a
   new session and process group with no controlling terminal.
4. **Daemon binds without a primary-identity network call.** The
   detached daemon child reads the bootstrap-state file, validates it
   (freshness + profile fingerprint + nonce), then binds its UDS
   socket. No `whoami()`-style network call for its **own** identity.
   The WebSocket connection opens after bind; if WS auth fails, the
   daemon stays bound on the socket and reconnects on its existing
   schedule (gateway-friendly, sandbox-friendly).

   **Bounded claim.** This covers the daemon's primary identity only. A
   profile carrying a `[reduce]` policy also builds its family-identity
   writer at startup, and that path performs a family `whoami()` *in the
   detached child* — before the bootstrap handoff is even read. So a
   reduce-configured profile under a network-gated sandbox is not yet
   covered by the parent-validation guarantee; extending it needs a
   handoff that can carry the family identity too. Non-reduce profiles
   (the common case) are fully covered.
5. **Cursor seeding.** The CLI then asks the daemon to enumerate the
   bot's channels and seed an initial "latest post seen" cursor for
   each, so a follow-up `chanvoy check <channel>` returns useful
   state without a fresh time-window probe.

The split between (2) and (4) is **load-bearing for sandboxed
environments**: in sandboxes that gate network access (Codex agents,
macOS sandbox-exec, Docker without `--network`), the parent CLI runs
in the operator's interactive shell where a network-approval prompt
can fire. The detached daemon child inherits sandbox restrictions and
can't escalate; by the time it starts, the network call has already
happened in the parent and only the validated identity travels
forward via the bootstrap-state file.

### Background starts: one shared primitive

There are two ways to start a daemon in the background, and they use
**the same** bootstrap and detachment primitive:

| Command | Purpose | Profile side effects |
|---|---|---|
| `chanvoy auto-setup` | Bootstrap or repair: synthesize/refresh the profile from env, then start the daemon | Creates or refreshes the profile, sets the active marker, seeds cursors |
| `chanvoy --profile <name> daemon start` | Start a daemon for a profile that already exists | None — starts a daemon, nothing else |

Steps (2), (3) and (4) above — parent-side identity validation, the
bootstrap-state handoff, and the `setsid()` spawn — are identical for
both. `daemon start` layers no profile management on top: it will not
create or refresh a profile, will not move the `active_profile` marker,
will not seed cursors, and will not rewrite `bot_username` if the live
credential disagrees with the persisted one (it refuses instead).
Because it validates a live credential and spawns a long-lived process
under it, `daemon start` also requires an *explicit* profile —
`--profile`, `CHANVOY_PROFILE`, or a sourced agent identity — and never
falls back to "whichever daemon is running" or the active marker.

Sharing one primitive is a deliberate structural choice, not a
refactor for tidiness. When the two paths had separate spawn
implementations, only one of them got PER-008D detachment and PER-014
bootstrap; the other could report a successful start and then lose its
daemon at the next process-group teardown.

### Detachment: surviving session boundaries

The daemon is spawned with `libc::setsid()` so it becomes its own
session leader. Operationally:

- The daemon survives the spawning shell's exit — or the end of the
  agent tool invocation that started it.
- `SIGHUP` from the controlling terminal closing does not propagate.
- Operators returning to the same machine in a fresh shell find the
  same daemon still running on its profile socket.

Cross-platform note: `setsid` is uniform across Linux init, systemd-user,
and macOS launchd. This is the contract; tests assert
`getsid(daemon_pid) == daemon_pid` against it, for both background
entry points.

The foreground variant — `chanvoy daemon serve` — is **not** detached,
and the difference is lifetime, not just where stdio points. It stays
in the invoking process's session so operators can `Ctrl-C` it and
follow logs, and it validates identity itself (the legacy direct-start
path) when no bootstrap handoff is in flight. Use `auto-setup` or
`daemon start` for the durable case, `daemon serve` for
foreground-debug.

If a background daemon exits during startup, the starting command fails
with a startup-failure classification naming the stage it died in
(before or after consuming the bootstrap handoff) rather than a bare
"not running" — the two call for different operator actions.

### Restart and recovery

Three restart shapes, all handled idempotently:

| Shape | Detection | Recovery |
|---|---|---|
| Stale socket file (daemon died, file remains) | Bind fails with `EADDRINUSE`, then probe shows no listener | `auto-setup` / `daemon start` remove the stale file and bind fresh — no manual file movement |
| Wedged daemon (alive but unresponsive) | Ping over UDS times out | `auto-setup` / `daemon start` `SIGKILL` it via the pid file and respawn |
| Identity drift (token now authenticates as a different bot) | Periodic `whoami()` re-check returns a different bot id | Daemon stays bound on the socket but refuses network-backed RPCs (`post`, `read`, `check`, `notifications`, `search`, `react`, etc.) with a clear diagnostic. `daemon status` remains queryable. Recovery: re-run `auto-setup` to re-validate end-to-end. |

The drift gate is intentionally one-way: the daemon doesn't try to
"recover" by silently re-binding to a different identity. That would
mis-attribute posts and corrupt cursors. Refuse loudly, escalate to
operator.

---

## Profile model

A profile is the unit of identity. Each profile binds:

| Field | Source | Notes |
|---|---|---|
| Profile name | env-derived `<role>-<scope>` | Canonical convention; `auto-setup` synthesizes this from `LANYTE_AGENT_ROLE` + `LANYTE_AGENT_SCOPE` |
| Bot username | Mattermost `whoami()` at bootstrap | Validated; drift detected on re-check |
| Mattermost URL | `LANYTE_MM_URL` | Per-profile; not shared across profiles on the same machine |
| Token | env, indirected via `CHANVOY_TOKEN_ENV_NAME` (default `LANYTE_MM_TOKEN`) | Never persisted to disk; re-read each daemon start |
| Primary team | `LANYTE_MM_TEAM` if set, else `org-<scope>` | Used as the first try for channel-name resolution |
| Capability class | per-profile flag | Gates admin-only operations like `channel restore` |

### Resolution: which profile gets used?

Six-step resolver when `--profile` is not passed:

1. Explicit `--profile <name>` — wins always.
2. `CHANVOY_PROFILE` env var — explicit override; refuses if the
   named profile doesn't exist on disk.
3. Env-derived exact-name match — `<role>-<scope>` resolves to the
   profile by exact name. Refuses with the available-profile list if
   no exact match exists (does not silently fall through).
4. Single running daemon — if exactly one chanvoy daemon is currently
   running on this machine, that profile is used.
5. `active_profile` marker — single-tenant convenience pointer.
   Subject to stale-marker recovery.
6. Refuse — print available profiles, require explicit `--profile`.

Two carve-outs:

- **Profile-collection management verbs** (`profile list`, `profile
  create`, `profile create-from-env`) and **`auto-setup`** bypass
  this resolver. They operate on the collection or env-derived
  synthesis, not on a single existing target.
- **Side-effecting daemon-lifecycle verbs** (currently just `daemon
  stop`) refuse on rules 4 and 5 — they require an explicit target
  to avoid acting on another operator's daemon on a shared machine.

The full resolver contract lives in
[`agent-chat-conventions.md`](https://github.com/lanytehq/lanyte-crucible/blob/main/docs/specs/agent-chat-conventions.md)
in lanyte-crucible. Operators rarely need to read it.

### Stale-marker recovery

After a profile rename or deletion, the `active_profile` marker may
point at a profile that no longer exists. The resolver detects this
and refuses with `ActiveProfileNotFound` rather than silently falling
through to a different identity. Recovery: re-run `chanvoy auto-setup`
to refresh the marker against your sourced env.

---

## Attention state

Attention state is the daemon's per-channel cursor record: where you
last left off, whether your cursor is fresh, and whether the channel
has new content since.

### The cursor key is qualified

Every cursor is keyed on `<team>/<channel>`, not just `<channel>`. A
bot that's a member of multiple Mattermost teams may see the same
channel name on different teams (e.g., `general` exists on every
team); chanvoy tracks them as independent cursors. Reading
`org-lanytehq/general` does not advance cursors for
`3-leaps-operations/general`.

This matters for:
- Multi-team agents (most production agents are members of more than
  one team).
- Migration from earlier chanvoy versions: pre-qualified-key state
  files are migrated forward automatically; ambiguous historical
  names quarantine into `AttentionState.quarantined` for inspection
  rather than silently binding to one team.

### Three cursor states

For every tracked channel, the cursor is in one of three states:

| State | Meaning | What `chanvoy check` returns |
|---|---|---|
| `live` | Cursor exists and the daemon was able to confirm the anchor post is still resolvable | `new: <count>` based on the cursor's position |
| `no_anchor` | No cursor exists yet for this channel on this profile | `new: 0 anchor=none source=no_anchor`, exit 1 |
| `stale_cursor` | A cursor exists but the anchor post is gone (deleted, archived, etc.) | `new: 0 anchor=none source=stale_cursor`, exit 1 |

Both refusals exit non-zero so loop scripts (`while chanvoy check
<channel>; do ...`) don't process garbage. Recovery for `no_anchor` is
typically a `chanvoy post` or full `chanvoy notifications` to seed.
Recovery for `stale_cursor` is the same — a successful write or full
notifications read re-anchors.

### Pure-read vs cursor-advance taxonomy

Every read-shaped verb is one of three kinds:

| Kind | Verbs | Cursor effect |
|---|---|---|
| Pure read | `read --after`, `read --since`, `read --since-bootstrap`, `read --since-last-mine`, `pinned`, `show`, `thread` (with or without `--latest`), `notifications --unread`, `search`, `attention list`, `attention show` | None |
| Probe (pure-read with cursor consultation) | `check` | Reads cursor, never writes |
| Cursor-advance | `read --advance`, `ack`, `post`, full `notifications` (without `--unread`; with or without `--since`) | Writes cursor on success |

The taxonomy is load-bearing for agent loops: a script can `check` →
`read --since-last-mine` → process → `ack` and trust that the cursor
moves exactly once, at exactly the moment the agent decides "I've
handled everything up to here."

`react` and `unreact` are cursor-neutral. Reacting to a post does not
mark the channel as read; reactions are auth-bound but conversationally
asynchronous, and the cursor model treats them as out-of-band.

---

## Channel-name resolution (γ hybrid)

When a CLI verb takes a channel name like `bravo-team` or
`org-3leaps/leadership`, the daemon resolves it through a four-step
chain:

1. **Explicit `<team>/<channel>` syntax** wins over everything.
2. **Explicit `--team <slug>` flag** is equivalent to (1) but as a
   flag.
3. **Profile's primary team** is tried first when no explicit team is
   given. Common case; no extra API call.
4. **Fallback across other member teams** — if the primary team
   doesn't have the channel, the daemon searches every team the bot
   is a member of (15-minute cache, force-refreshed on no-match
   before failing).

Three distinct refusal shapes when resolution can't pick a single
target:

| Refusal | When | Recovery |
|---|---|---|
| `ChannelNotFoundInAnyTeam` | Channel doesn't exist on any team the bot belongs to | Check spelling; ask supervisor to add the bot to the host team |
| `NotAMemberOfTeam` | Explicit `<team>/<channel>` or `--team` named a team the bot isn't a member of | Choose a team the bot is in, or get added |
| `AmbiguousChannel` | Channel name exists on multiple teams the bot is in | Use `<team>/<channel>` syntax or `--team <slug>` to disambiguate |

Refusals always name the teams searched, so the operator can disambiguate
in one step. They never silently 404.

The resolver is called "γ hybrid" in implementation comments; in
operator language it's just "the cross-team channel resolver."

---

## Storage layout

```
$CONFIG_ROOT/                          (default: ~/.config/lanytehq/chanvoy/
                                        on Linux, ~/Library/Application
                                        Support/lanytehq/chanvoy/ on macOS;
                                        override via CHANVOY_CONFIG_DIR — used
                                        as-is, no lanytehq/chanvoy suffix added)
├── profiles/
│   └── <profile-name>.toml            Profile binding (bot identity, MM url,
│                                       team binding) as TOML
├── state-<profile-name>.json          Per-profile attention state (cursors
│                                       keyed by <team>/<channel>) as JSON,
│                                       one file per profile, at config root
└── active_profile                     Single-line marker file (legacy
                                       convenience pointer)

$RUNTIME_ROOT/                         (default: $XDG_RUNTIME_DIR/chanvoy/
                                        or platform runtime dir / OS temp,
                                        with /chanvoy/ suffix appended;
                                        override via CHANVOY_RUNTIME_DIR — used
                                        as-is, no chanvoy suffix added)
├── <profile-name>.sock                UDS socket — CLI ↔ daemon RPC
├── <profile-name>.pid                 Daemon pid file
└── <profile-name>.bootstrap.json      One-shot bootstrap-state handoff
```

### Permission contract

- Config dirs: `0700`. Runtime dir: `0700`.
- Profile TOML files + state JSON files + bootstrap handoff: `0600`.
- Socket: `0700` parent dir; the socket itself inherits.

This is a contract, not a hint. The `same-Unix-user` trust boundary
is intentional: chanvoy M1 does not attempt to protect one process
from another running as the same user. The permission masks reduce
*accidental* cross-process exposure (other users on the box, world-
readable misconfigured backups, etc.); they don't claim to defeat a
local attacker.

### Path overrides

Two environment variables override the defaults:

- `CHANVOY_CONFIG_DIR` — overrides config root.
- `CHANVOY_RUNTIME_DIR` — overrides runtime dir.

Both are operator-set per shell. Profile data and runtime state
partition by directory. Use cases: parallel test sessions, sandboxed
agents that need a writable runtime path inside their mount, isolated
debug environments.

### Product namespace, not org restriction

The `lanytehq` segment in `~/.config/lanytehq/chanvoy/` is the
**product namespace** — chanvoy is a lanytehq-developed tool that
namespaces its config the way other vendor-developed tools do (e.g.,
`~/.config/google/...` housing Google products for non-Google users).
It is **not** an org restriction. Operators in any org —
lanytehq, enacthq, fulmenhq, third-party adopters — use the same
default path. Profile data inside is partitioned per-profile, and
profile names encode the org via the `<role>-<scope>` convention.

---

## Peer contract relationship (channel 260)

Chanvoy is one of the Lanyte platform's communication peers. The peer
contract — common patterns every peer implements — is specified in
STD-006 (lanyte-crucible). Common patterns:

- Control lifecycle (hello / hello_ack / ping / pong / disconnect)
- Request/response correlation via `request_id` (UUID v4)
- Delegation scoping via `delegation_id` on every operation
- Autonomy gating via `gate_token` on actions with real-world side
  effects (post, react, channel create)
- Typed error envelope (`error_code` enum + `message` + `retryable`)
- Per-peer domain verbs (chat verbs for chanvoy; mail verbs for
  mlvoy; etc.)

Today's chanvoy implements the **local-mode subset**: CLI ↔ daemon
over UDS, daemon ↔ Mattermost over REST/WS, no Lanyte-core peer
session. The channel 260 surface (chanvoy ↔ Lanyte-core orchestrator
over IPC) is the next layer — when wired, it lets the orchestrator
route delegation envelopes through chanvoy as one of several peers.
That work is deferred; the schemas exist in lanyte-crucible.

What this means for changes today: the local-mode CLI and daemon
shapes are stable contracts that the future peer surface will preserve.
A breaking change to CLI argument shape or daemon RPC will need to
preserve schema compatibility for the eventual peer wiring; chanvoy-
core's public types (`ResolvedChannel`, `MigrationOutcome`,
`AttentionState`, etc.) are the surface to keep stable.

---

## Crate layout

```
crates/
├── chanvoy-core      shared domain types, profile model, JSON-RPC envelopes,
│                     Mattermost client, attention-state machine
├── chanvoy-daemon    UDS server, identity validation, WebSocket client,
│                     attention-state owner
├── chanvoy-cli       CLI surface, argument parsing, output formatting
├── chanvoy-ipc       JSON-RPC envelope types (factored out of chanvoy-core
│                     so chanvoy-mcp can use them without pulling in MM client)
└── chanvoy-mcp       MCP 2026-07-28 face (stdio + loopback HTTP) over DaemonClient

src/
└── main.rs           binary entry, wires chanvoy-cli into the binary
```

Public API stability: `chanvoy-core`'s exported types are the
de-facto contract for downstream tools (chanvoy-mcp, future
peer-adapters, integration-test fixtures). Removing or re-typing them
is a contract change.

Adding is not automatically safe, which earlier wording here implied.
Adding a variant to an exhaustive public enum, or a field to a public
struct that callers construct themselves, is **source-breaking**: code
matching or constructing exhaustively stops compiling. It is only safe
when the type is designed for evolution — `#[non_exhaustive]` on an
enum, or a constructor that callers must go through.

`CoreError` is `#[non_exhaustive]` for exactly this reason. Types that
are not should be treated as closed: adding to them needs the same
deliberate version boundary as removing from them.

---

## If you change X, also change Y

A short list of cross-cutting touchpoints contributors hit most often:

| If you change… | Also update… | Why |
|---|---|---|
| A CLI verb's argument shape (positional, flag, default) | `docs/operator-guide.md` (per-section) + this doc's CLI references | Operator guide is the reference; arch doc has examples |
| The resolver's refusal taxonomy (new variant, message change) | `docs/troubleshooting.md` symptom table | Symptom-keyed entries match exact error text |
| Storage layout (new file, renamed file, permission mask) | `docs/architecture.md §Storage layout` + REPOSITORY_SAFETY_PROTOCOLS.md | Storage contract is referenced by audit docs |
| Daemon RPC method (new, renamed, response shape) | `crates/chanvoy-daemon/src/lib.rs::LOCAL_ONLY_METHODS` if appropriate, plus integration tests under `tests/` | Drift gate must classify every RPC; tests exercise dispatch |
| Bootstrap flow (auto-setup or daemon spawn) | `docs/getting-started.md §Bootstrap` and integration tests under `tests/restart_harness.rs`, `tests/per_023_*.rs`, etc. | Getting-started is the agent's first read; tests exercise the spawn path |
| Profile resolution rules | `docs/operator-guide.md §Profile Resolution` + this doc's resolver section + the canonical spec link in `agent-chat-conventions.md` (lanyte-crucible) | Operators read operator-guide; the canonical contract is in crucible |
| Attention-state schema or cursor-advance rules | `docs/architecture.md §Attention state` + `docs/operator-guide.md §Resume And Attention` + the migration logic in `chanvoy-core::migrate_attention_state` | Schema changes are forward-only; migration needs to handle the prior shape |
| Sandbox-handling design | `docs/getting-started.md §Sandboxed agents` + `docs/troubleshooting.md` sandbox entries | Sandboxed agents are a first-class audience; the path-tree there is load-bearing |

When in doubt, run `make pr-final` (the CI-exact merge gate) and
search `docs/` for the symbol you changed.

---

## Further reading

- [`getting-started.md`](./getting-started.md) — agent-first onboarding walkthrough
- [`operator-guide.md`](./operator-guide.md) — full operator reference (long; use as a lookup)
- [`troubleshooting.md`](./troubleshooting.md) — symptom-keyed recovery
- [`migration-runbook.md`](./migration-runbook.md) — for operators replacing `lanyte-chat` with chanvoy
- [`integration-tests.md`](./integration-tests.md) — for contributors adding tests
- [`BACKGROUNDER.md`](../BACKGROUNDER.md) — the "why chanvoy exists" narrative
- [`agent-chat-conventions.md` in lanyte-crucible](https://github.com/lanytehq/lanyte-crucible/blob/main/docs/specs/agent-chat-conventions.md) — canonical profile-naming + resolver spec
