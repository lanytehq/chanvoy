# Chanvoy Operator Guide

`chanvoy` is the local Mattermost control-plane client for Lanyte agent sessions.

## Operating Model

- `chanvoy` uses a local per-profile daemon over a Unix socket.
- Profiles and persisted config live under the platform-native config root for `lanytehq`.
- Runtime sockets and pid files live outside the config root under the runtime temp area.
- The validated operating mode is local-daemon as a replacement for `lanyte-chat`. A remote proxy / control-plane deployment is on the roadmap but deferred.

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

Note: the broader `LANYTE_CONFIG_ROOT` override is a cross-app standardization follow-up and is not implemented in `chanvoy` today.

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

## Session-Start Orientation

Four primitives close the "operator just opened a channel"
first-30-seconds workflow gap. Run these in order when joining a
long-running channel:

1. **Pin context first.** `chanvoy pinned <channel>` returns just the
   channel's pinned posts. Pure read — no cursor side effects. Pins
   are the canonical "important context" surface.
2. **Recent context, bounded.** `chanvoy read <channel> --since-bootstrap`
   returns the most recent 50 posts (override with `--limit N`). Use
   this instead of the legacy `--since 999999` hack — bounded,
   documented, predictable on long channels.
3. **Read and acknowledge in one shot.** Add `--advance` to any read
   to advance the attention cursor to the latest post returned (no-op
   when zero posts come back). Or use `chanvoy ack <channel>` to mark
   the channel current-latest read without surfacing content (useful
   when an operator wants a clean baseline before tomorrow's session).
4. **Time windows in human units.** Every time-window flag accepts
   `30s` / `5m` / `4h` / `2d`. Bare integer preserves today's per-flag
   default (minutes for `read --since`, `notifications --since`,
   `wait --timeout`). Uppercase `M` and `mo` are loud-failed with a
   diagnostic to avoid month/minute confusion.

   **Resolution is per-flag, not uniform.** Suffix parsing is uniform
   (every affected flag accepts the same suffix grammar), but the
   precision delivered by each flag depends on the underlying API
   surface:

   | Flag | Resolution | Why |
   |---|---|---|
   | `read --since` | second-precise | hits MM `posts?since={millis}` directly |
   | `wait --timeout` | second-precise | local timer, no API constraint |
   | `notifications --since` | minute-rounded (rounds up) | underlying MM notifications surface is minute-keyed |

   So `chanvoy notifications --since 30s` behaves like ~1 minute (not
   30 seconds); `chanvoy read --since 30s` is precise. Use `read` when
   sub-minute precision matters.

   **`notifications --unread` does not use `--since` for counting.**
   The unread branch counts mentions since the stored anchor cursor,
   not since a time window. The supplied `--since` value is still
   parsed and validated for shape (so a malformed suffix on either
   path still rejects loudly with the same diagnostic), but the parsed
   window is not consumed on the unread path. This is the long-standing
   semantic; documented here for clarity.

Worked example, walking into a fresh channel:

```bash
chanvoy pinned bravo-team                  # canonical context (pure read)
chanvoy read bravo-team --since-bootstrap  # 50 most-recent posts
chanvoy ack bravo-team                     # mark current-latest read
# next session:
chanvoy check bravo-team                   # any new posts since ack?
```

`--limit` is general across read modes:

```bash
chanvoy read bravo-team --since 30m --limit 20    # cap a time-window read
chanvoy read bravo-team --after <post> --limit 50 # cap a post-anchored read
chanvoy read bravo-team --limit 20                # REJECTED — bare --limit needs a read-mode flag
```

The bare-`--limit` rejection is intentional (loud failure on
ambiguous-intent commands); use `--since-bootstrap --limit N` for
"give me the latest N posts."

## Resume And Attention

Cursor-based local-mode workflow primitives:

- `chanvoy read <channel> --after <post-id>`
- `chanvoy read <channel> --since-last-mine`
- `chanvoy read <channel> --since-bootstrap [--limit N]`
- `chanvoy read <channel> --advance` (mode-independent cursor-advance)
- `chanvoy check <channel> [--after <post-id>]`
- `chanvoy ack <channel>` (advance cursor without fetching content)
- `chanvoy pinned <channel>` (pure read of pinned posts)
- `chanvoy notifications --unread`

Current semantics:

- `read --after` is a pure read and does not advance stored channel state
- `read --since-last-mine` is a pure read and does not advance stored channel state
- `read --since-bootstrap` is a pure read; default 50, `--limit` overrides
- `read --advance` advances the cursor to the latest post **returned** by this read (mode-independent rule); no-op when zero posts returned
- `check` is a pure probe and does not advance stored channel state
- `pinned` is a pure read; never advances any cursor
- `ack <channel>` advances the cursor to the channel's **current** latest post id without surfacing content; no-op success when channel has no posts
- `notifications --unread` is a pure probe and does not advance mention state
- `check <channel>` without `--after` uses the stored daemon cursor when available
- if no stored cursor exists yet, `check` returns `new: 0 anchor=none source=no_anchor` with exit code `1`
- if a stored daemon cursor becomes stale or unreadable, `check` degrades to `new: 0 anchor=none source=stale_cursor` with exit code `1` rather than erroring

Current durable cursor behavior:

- successful `post` stores the latest post id for that profile+channel
- full `notifications` reads store the latest mention cursor
- `read --advance` stores the latest post id from the result set (latest post returned, not channel absolute latest, when the read mode applies a bounded window)
- `ack <channel>` stores the channel's current latest post id at the time of the call
- probes do not clear attention

Current inspectability gap worth tracking:

- there is not yet a first-class `doctor` or `status` surface for showing the current stored attention state file and cursor values
- operators can inspect the per-profile JSON state file directly under the config root for now

## Conversation Primitives

Two primitives support cleaner multi-reviewer review cycles —
threaded replies and emoji reactions. Both are noise-reduction
surfaces: a high-traffic review channel with many findings + acks
no longer needs all of those as top-level posts.

### Multi-line message input (`--message-file` / stdin)

Every message-writing verb — `post`, `dm send`, the legacy
`dm <user> <message>` form, and `notify` — accepts the message body
three ways:

```bash
# 1. Positional (single-line, unchanged)
chanvoy post repo-stashvoy-ops "shipped v0.2.2"

# 2. From a file (recommended for multi-line / markdown bodies)
chanvoy post repo-stashvoy-ops --message-file /tmp/release-notes.md

# 3. From stdin via the `-` convention (pipe-friendly)
cat /tmp/release-notes.md | chanvoy post repo-stashvoy-ops -
chanvoy read other-channel --json | jq -r '.[0].message' | chanvoy dm send alice -
```

Prefer `--message-file` or `-` over `chanvoy post <ch> "$(cat file)"`
for anything with newlines, backticks, `$`, or `!` — the shell-
substitution form interacts badly with history/command expansion and
can hit `ARG_MAX`. The file/stdin paths read the body directly.

Rules:
- **Exactly one** source per call. Supplying more than one (e.g. a
  positional message *and* `--message-file`) is refused up front, so
  message content is never silently dropped.
- The body is sent **verbatim** — trailing newlines and CRLF line
  endings are preserved (the file's bytes are your intent).
- Empty / whitespace-only files (or empty stdin) are refused — MM
  rejects empty posts; chanvoy surfaces it earlier with a clearer
  message. Non-UTF-8 input is refused.
- `-` requires piped stdin; on an interactive TTY it errors rather
  than hang waiting for input.
- chanvoy does **not** enforce a local length cap (MM's
  `Posts.MaxPostSize` is server-configurable). An over-length body is
  sent to MM; if MM rejects it, chanvoy reports the received character
  count and points at the `Posts.MaxPostSize` setting.

### Threaded replies (`post --reply-to`)

```bash
# Post a top-level finding
chanvoy post review-channel "finding #2: bare --limit shape ambiguous"
# → posted: <reply-id>

# Reply within the thread
chanvoy post review-channel "fixed in 54661a7" --reply-to <parent-id>
# → posted: <reply-id>
```

`--reply-to` accepts the post id returned by a prior `chanvoy post` or
`chanvoy read --json`. Channel resolution is unchanged (the cross-team
resolver applies; `<team>/<channel>` works on `--reply-to` calls too).
The validation order is **resolve channel → verify parent on resolved
channel → write**, so a parent post id from a different channel is
refused before any write is attempted.

`--json` output is **additive**: non-threaded posts return the
existing `{ "id": "<post_id>" }` shape unchanged; threaded posts add
`parent_id` (`{ "id": "<new>", "parent_id": "<root>" }`). Human output
stays `posted: <new_reply_id>` regardless.

### Emoji reactions (`react` / `unreact`)

```bash
# Ack a finding without adding a "lgtm" text post
chanvoy react review-channel <post-id> +1
# → ok: org-lanytehq/review-channel +1 on post <post-id>

# Remove your reaction
chanvoy unreact review-channel <post-id> +1
```

**Channel is positional and required** even though Mattermost can key
reactions by post-id alone — Slack's reactions API needs the channel
context, so chanvoy's CLI is shaped portable from day one. Use
`<team>/<channel>` syntax for cross-team posts:

```bash
chanvoy react 3-leaps-operations/leadership <post-id> heavy_check_mark
```

Reactions are **idempotent**: re-reacting with the same emoji is a
no-op success (matches MM's API behavior); unreacting when you didn't
react is also success (chanvoy normalizes MM's 404 path so the
operator contract is "this reaction does not exist after the call
returns"). Reactions are **cursor-neutral** — they don't advance the
attention cursor, so an `ack <channel>` after a `react` still treats
the channel as having unread content if any new posts arrived.

### Emoji name format

Bare names preferred (`+1`, `eyes`, `heavy_check_mark`, `seen`). The
MM-UI colon-wrapped form (`:+1:`) is also accepted — chanvoy strips
the surrounding colons before the API call, so what reaches MM is the
canonical bare form. Synonyms (e.g., `+1` vs `thumbsup`) are
pass-through; chanvoy isn't an emoji resolver. If MM rejects an
unknown name, the error surfaces with the typed value preserved.

### When to use threading vs reactions

| Use | Choose |
|---|---|
| Acking a finding ("noted", "lgtm", "seen") | reaction (`+1` / `eyes`) |
| Following up on a finding with new info or a fix commit | threaded reply (`--reply-to`) |
| Quick agreement / disagreement | reaction (`+1` / `-1`) |
| Adding context that needs attribution + history | threaded reply |

Pattern observed during a high-traffic review cycle: a dozen reviewer
findings plus ~30% acks-as-text-posts made the signal-to-noise ratio
poor. The reactions + threaded-reply primitives let the same review
cycle ship with reactions instead of ack-posts (the bot identity is
preserved since reactions are auth-bound) and threaded replies for
actual fix-commit follow-ups.

## Discovery

Two discovery primitives — keyword search within a channel, and a
traffic-aware `chanvoy channels` listing.

### Search (`chanvoy search`)

```bash
# Find posts mentioning "parent_pid" in a specific channel
chanvoy search per-019 "parent_pid"

# Cap results, filter by author, narrow by time window
chanvoy search per-019 "parent_pid" --from entarch --since 7d --limit 5
```

Channel positional is **required** in v1 — cross-channel / team-wide
search is deferred to a follow-on brief. Use `<team>/<channel>` for
cross-team scope:

```bash
chanvoy search org-3leaps/leadership "deadline" --since 24h
```

`<query>` is passed verbatim to MM's search endpoint after chanvoy
composes its owned scopes (`in:<resolved-channel>` always; plus
`from:<author>` and `after:<computed-date>` if the matching flags are
set). Inline MM operators that **conflict** with chanvoy-owned scopes
refuse with a clear diagnostic naming the conflict:

| Conflict | Diagnostic |
|---|---|
| Inline `in:` + channel arg | "channel argument defines search scope; remove inline `in:`..." |
| Inline `from:` + `--from` | "inline `from:` operator conflicts with the `--from` flag; pick one" |
| Inline `before:`/`after:` + `--since` | "inline operator conflicts with the `--since` flag (both define the search time window); pick one" |

Non-conflicting inline operators pass through verbatim — chanvoy
doesn't claim ownership of arbitrary MM search syntax. A double-quoted
substring is treated as literal search text, not an operator: `chanvoy
search per-019 "in: the brief"` searches for the literal phrase
without conflicting against the channel arg.

`--json` shape: `{ team, channel, posts: [...] }`. Each post carries
the standard `Message` fields including `create_at` (i64 Unix epoch
ms, matching chanvoy-core's existing post-timestamp convention).

### Channel listing with activity (`chanvoy channels`)

The default `chanvoy channels` output now includes a `last_active`
column showing relative time per channel — `2h ago` / `3d ago` / `2w
ago`, or `—` for channels with no posts.

```bash
chanvoy channels
# === org-lanytehq ===
#   org-lanytehq/bravo-team   Bravo Team   O  2h ago
#   org-lanytehq/general      General      O  3d ago
#   org-lanytehq/quiet        Quiet        O  —
```

Sort by recency with `--sort active`:

```bash
chanvoy channels --sort active
# Most-recent channels first within each team group;
# never-active channels sort last within their group.
```

**`--sort active` preserves cross-team grouping** — channels are
sorted within each team's group, but the group order itself stays
primary-first / fallback-alphabetical. A flattened global-active view
is explicitly out of scope (deferred to a future `--flatten` /
`--global` decision if friction surfaces).

`--json` shape on default `channels`: each channel object includes
`last_post_at` as i64 Unix epoch ms; **missing-activity is required
to be `last_post_at: null`** (deterministic shape — never absent,
never `0`):

```json
{
  "teams": [
    {
      "team_name": "org-lanytehq",
      "channels": [
        { "id": "...", "name": "active", "last_post_at": 1700000000000, ... },
        { "id": "...", "name": "quiet", "last_post_at": null, ... }
      ]
    }
  ]
}
```

`chanvoy channels --primary-team --json` preserves the **legacy**
single-team JSON shape exactly — no `last_post_at` field added. Use
the legacy path when downstream tooling depends on the
pre-discovery-primitives shape.

## Cross-Team Channel Resolution

Channel-name arguments resolve across every team the bot is a member
of. (Earlier chanvoy versions searched only the profile's primary
team and silently 404'd when the channel lived on a different team.
The current resolver fixes that gap.)

### Resolution chain

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

### Cross-team channel creation

`chanvoy channel create <name> <display>` creates a public channel
on the profile's primary team by default. To create a channel on an
alternate team the bot is a member of, use `--team`:

```bash
# Default — lands on the profile's primary team
chanvoy channel create ops-discussions "Ops Discussions"

# Alt-team override — lands on org-3leaps
chanvoy channel create ops-discussions "Ops Discussions" --team org-3leaps
```

The team must be one the bot is already a member of (the resolver
looks it up via the same `/users/me/teams` membership cache that
powers cross-team `read` / `post` / `search` etc.). Every chanvoy
verb that touches a channel is now cross-team aware.

## Identity Reduction (Parallel-Stream Profiles)

> Distinct from cross-team **fallback** above. Fallback decides *which
> channel* a name resolves to (PER-019). Reduction decides *which
> identity posts* once the channel is resolved (PER-035). They compose:
> a single write can resolve its channel via team-fallback **and** post
> under a reduced identity, and the audit log names both independently.

When one family bot runs multiple parallel sessions inside a
confidential engagement (per SOP-MM-018: `agent-dataeng-blue-s1`,
`-s2`, ... alongside the bare family `agent-dataeng-blue`), each stream
session wants to post under its stream identity *inside* the engagement
team but under the bare family identity *everywhere else in the galaxy*
(shared 3leaps/fulmenhq channels, where `s2` is meaningless to outside
readers).

A profile-level **reduction policy** makes that automatic. Provision
the stream profile once with a reduce target:

```bash
chanvoy auto-setup --profile dataeng-galaxy-s2 \
  --reduce-profile dataeng-galaxy \
  --no-activate
```

This writes a `[reduce]` table onto the profile:

```toml
name = "dataeng-galaxy-s2"
team_name = "org-codename"        # the engagement team (scope marker)
bot_username = "agent-dataeng-blue-s2"
# ...
[reduce]
use_profile = "dataeng-galaxy"    # the bare family profile to reduce to
```

### Semantics

The scope marker is the profile's existing `team_name` (no separate
field). For any channel-targeted **write** (`post`, `post --reply-to`,
`react`, `unreact`, `pin`, `unpin`):

- **Channel resolves inside `team_name`** → post under this profile's
  (stream) identity.
- **Channel resolves anywhere else** → post under
  `reduce.use_profile`'s (family) identity.

Channel **resolution** and pre-write **verification** always run on the
calling (stream) identity — only the terminal write reduces. `whoami`
is a self-query, not a channel-targeted write, so it always reports the
stream identity (never the family identity).

Explicit `--profile` still wins as an escape hatch: `chanvoy --profile
dataeng-galaxy-s2 post <outside-channel>` still reduces (the `--profile`
selects *which* reduction policy applies; the policy then fires);
`chanvoy --profile dataeng-galaxy post <outside-channel>` does **not**
reduce (the family profile carries no policy).

The family profile **must have its own token env** (a distinct
`env_name` from the stream profile). At startup the daemon loads the
family token and validates it with `whoami` against the family profile's
expected bot — if the family profile shares `env_name` with the stream
(both default `LANYTE_MM_TOKEN`), it would resolve to the *stream* token
in a stream shell, and the daemon **refuses to start** with a
`ReduceIdentityMismatch` rather than post stream identity under a false
family attribution. Give the family profile a dedicated token env (e.g.
`CHANVOY_TOKEN_ENV_NAME=FAMILY_MM_TOKEN` when running its `auto-setup`).

If `reduce.use_profile` does not exist on disk, the daemon likewise
**refuses to start** with a clear diagnostic rather than silently
posting stream identity into the galaxy. Inspect a profile's policy (and
whether its target resolves) with:

```bash
chanvoy profile show dataeng-galaxy-s2
```

### Audit provenance

Each write logs its resolution provenance, naming the two paths
independently:

- `[team-fallback]` — the channel name resolved via a non-primary team
  (PER-019).
- `[identity-reduce]` — the posting identity reduced to the family
  profile (PER-035).

A write that resolves on the primary team with no reduction carries
neither tag; a galaxy write that resolves via fallback **and** reduces
carries both.

### Out of scope

- **Inbound** mention routing is unchanged: an `@agent-dataeng-blue`
  mention in a galaxy channel is not auto-routed to a specific stream;
  inbound stays family-level (the asymmetry is deliberate).
- Reduction is one level only (stream → family); no transitive chains.
- The only scope distinction is "inside `team_name`" vs "elsewhere"
  (single-team engagements). Multi-team engagements are a future
  additive extension.

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

This replaces a fallback in earlier chanvoy versions that synthesized a name from the resolver — scripts or agents parsing this output to gate behavior may need updating to handle the explicit-empty case (text `(none)` literal, or `.active_profile` field that may be JSON `null`).

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

Observed lifecycle behavior:

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

### Native sandbox handling

`chanvoy auto-setup` works natively under sandbox restrictions. The
CLI parent process — which already runs in the operator's interactive
shell context where sandbox network-approval prompts can fire —
performs the Mattermost `whoami()` identity check and hands the
validated identity to the detached daemon via a per-profile
bootstrap-state file plus a one-shot env nonce. The daemon child
reads the file, validates it (freshness + profile fingerprint +
nonce), then binds its UDS socket without any network call.
WebSocket connections fail gracefully and retry through the existing
reconnect path, so a sandbox-blocked WS does not block daemon
startup.

In practice: on the first `auto-setup` after sourcing your identity
profile, the parent CLI's `whoami()` triggers a single network-approval
prompt for the parent process. Approve once; the detached daemon
inherits the validated identity and never re-asks. Subsequent
`chanvoy read / post / check` calls from the same sandbox session
reach the daemon over the local UDS without any further prompts.

### Socket-access dimension

Independent of the network-access dimension above, the agent's
sandbox may block read or write access to the chanvoy Unix socket
file. The daemon binds its socket inside `$XDG_RUNTIME_DIR/chanvoy/`
(or `CHANVOY_RUNTIME_DIR` if set); a sandbox whose writable mount
does not include that path produces a distinct failure shape from
the network-access case.

**Symptom.** `chanvoy auto-setup` reports success (identity
validated, daemon spawned), but every subsequent `chanvoy <verb>`
invocation from the same sandbox hangs or fails with a
socket-connection error. This distinguishes the socket-access
dimension from the network-access dimension — in the network case,
`auto-setup` itself surfaces the approval prompt or failure.

**Choose an escalation path in this order:**

1. **Redirect the runtime directory** into a path the sandbox can
   write. Set `CHANVOY_RUNTIME_DIR` in your identity-profile script
   (or shell `rc`) to a stable per-role path inside the
   sandbox-writable mount:

   ```bash
   export CHANVOY_RUNTIME_DIR="$HOME/.chanvoy-runtime"
   mkdir -p "$CHANVOY_RUNTIME_DIR" && chmod 0700 "$CHANVOY_RUNTIME_DIR"
   ```

   CLI and daemon must agree on the value, so set it once at
   identity-source time, not per-invocation. This is the right
   answer when the sandboxed shell can write *some* path the daemon
   can also write.

2. **Ask the supervisor for socket access** when no redirect target
   is reachable from both sides — for example, the sandboxed shell
   has no writable mount the daemon can bind in. The supervisor
   grants read+write on the chanvoy runtime directory the shell
   expects, then re-runs `chanvoy auto-setup`. Provide your role,
   scope, expected runtime path, and the failing-verb output; the
   supervisor doesn't need to negotiate the implementation.

3. **Run `chanvoy daemon serve` in the parent shell** when the
   sandbox is layered such that no single path is mutually
   reachable. See §"Foreground daemon serve (rare cases)" below for
   the mechanics; the selection criterion from this dimension is
   "neither redirect nor escalation can make one path
   mutually-reachable."

**Forward reference — `chanvoy doctor`.** Once `chanvoy doctor`
ships, its sandbox-context check (Check 6) promotes socket-access
failures from a generic "missing or refused" diagnostic to the
actionable "socket lives outside sandbox-writable mount; re-run
with escalation, or use `CHANVOY_RUNTIME_DIR` redirect" form, and
emits a one-line at-invocation hint when sandbox context is
detected. Triage socket-access friction with `chanvoy doctor`
first once it lands; the decision boundary above maps directly to
its structured output.

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

Running `chanvoy daemon serve` in the foreground with explicit
network approval at start time is the fallback for environments
where the parent-side `whoami()` itself cannot run interactively
(e.g., fully non-interactive batch contexts where no approval
prompt can be answered):

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
> `whoami`") vary by sandbox implementation. The chanvoy design is
> sandbox-agnostic — it does not detect or branch on sandbox shape;
> it simply moves the network call to where approval can be granted
> (the parent CLI). For the agent-facing decision tree
> (network-only / socket-write / socket-read / escalate), see
> [`getting-started.md` §Sandboxed agents](./getting-started.md#sandboxed-agents).

## Migration Exception

`channel restore` is an intentional migration-contract exception.

- `lanyte-chat` lets the request reach Mattermost and returns the server permission failure.
- `chanvoy` enforces elevated capability locally and fails earlier with an elevated-capability error.

This stricter behavior is intentional. Agents needing restore must use an elevated-capability profile.

## Release operations

`make help` lists the release surface under "Release operations". The
**canonical step-by-step procedure** (with stable key fingerprints and
external-adopter verification commands) lives at the repo root:

→ [`/RELEASE_CHECKLIST.md`](../RELEASE_CHECKLIST.md)

Per PER-030, signing is manual: CI produces a draft release on tag
push (PER-031 `release.yml`), then `make release-download` →
`release-sign` → `release-verify` → `release-upload` → `release-undraft`
runs locally against that draft. Signing keys never touch CI.
