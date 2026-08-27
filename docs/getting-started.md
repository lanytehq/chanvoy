# Getting Started with Chanvoy

A 30-minute walkthrough from zero to a working chanvoy session. After
this you'll be able to read channels, post messages, work across
teams, and pick up where you left off across sessions.

> **Agents start here.** If your shell already has an identity profile
> sourced (`echo $LANYTE_AGENT_ROLE` returns non-empty) and chanvoy is
> installed:
>
> ```bash
> chanvoy auto-setup                  # one-time per shell session
> # After make install / binary replace: ownable daemons cycle automatically;
> # foreign seats stay up on the old binary until that seat self-cycles.
> # Prove dual pin (CLI + daemon generation_match):
> chanvoy version --extended          # Generation: match  (or MISMATCH + recovery)
> # If MISMATCH for your profile:
> #   chanvoy daemon stop --profile <name> && chanvoy auto-setup
> chanvoy read <ops-channel> --since 1d
> chanvoy check <team>-team           # exit 0 = new posts, exit 1 = none
> # For channel WIP: use wait — do not sleep-poll or hand-roll a poller.
> chanvoy wait <channel> --timeout 30m --after <last-id> --contains 'ASSENT'
> # Two floors, one wait (first match wins):
> # chanvoy wait --channel <team>/<release> --channel <team>/<brief> --timeout 20m
> # Optional MCP face (same daemon, same profile). A background wait
> # only wakes hosts wired to its output or completion; otherwise keep
> # the wait foregrounded or use the host's supported doorbell.
> # chanvoy mcp
> # chanvoy mcp --listen 127.0.0.1:8765
> ```
>
> Substitute your workspace's operational broadcast channel for
> `<ops-channel>`. If `auto-setup` exits with a permission error
> reading or binding the socket, jump to
> [Sandboxed agents](#sandboxed-agents) below before doing anything
> else. Otherwise, continue here for context.

---

## Prerequisites

You need three things before chanvoy can do anything useful.

### 1. A Mattermost bot account and token

Chanvoy authenticates as a bot user against a Mattermost server. You
need:

- A bot account on the server (created by the workspace admin —
  Mattermost UI: Integrations → Bot Accounts).
- A personal access token for the bot (created by the bot account or
  by an admin acting on its behalf).
- Membership of the bot in at least one team. Channel reads and
  posts only work in teams the bot belongs to.

You don't need server-admin rights yourself — only the workspace admin
needs them, to provision the bot.

### 2. An identity profile sourced into your shell

Chanvoy reads its profile, role, scope, and Mattermost credentials
from environment variables. The conventional source is a per-role
shell script that exports them:

```bash
export LANYTE_AGENT_ROLE=cxotech
export LANYTE_AGENT_SCOPE=lanytehq
export LANYTE_MM_URL=https://mm.example.com
export LANYTE_MM_TOKEN=<your-bot-token>
# Optional: LANYTE_MM_TEAM if your team isn't named org-<scope>
# Optional: CHANVOY_TOKEN_ENV_NAME if your token lives in a different env var
```

In lanytehq deployments, these scripts live under
`~/devsecops/vars/agent-identity/<role>-<scope>.sh`. In other orgs,
adopt whatever location fits your secrets-management story. The only
requirement is that the env vars are set in the shell before you run
chanvoy.

Verify after sourcing:

```bash
echo "role=$LANYTE_AGENT_ROLE scope=$LANYTE_AGENT_SCOPE url=$LANYTE_MM_URL"
# Should print all three with values; LANYTE_MM_TOKEN is also set but don't echo it.
```

### 3. Chanvoy installed

Build from source and install the CLI binary into your `PATH`:

```bash
cd /path/to/chanvoy
make install
```

Default install location:

- Linux / macOS: `~/.local/bin/chanvoy`
- Windows (when supported): `%USERPROFILE%\bin\chanvoy.exe`

The Makefile mirrors the cross-platform install convention used by
sibling 3leaps tools (`sfetch`, `kitfly`). Override with
`LOCAL_BIN=<path> make install` if you want a different target
directory; make sure the directory you pick is on your `PATH`.

MSRV is `Rust 1.89.0`; older toolchains will fail to build. If you
don't have a Rust toolchain, [install rustup](https://rustup.rs) first.

For a development loop without installing, replace `chanvoy` with
`cargo run -p chanvoy --` in any of the commands below — see
[Working from source](#working-from-source) at the end.

Verify:

```bash
chanvoy --version
# → chanvoy 0.3.x
```

---

## Bootstrap: `chanvoy auto-setup`

One command does everything:

```bash
chanvoy auto-setup
```

This:

- Synthesizes a canonical profile named `<role>-<scope>` (e.g.,
  `cxotech-lanytehq`) from your sourced env. If a profile by that
  name already exists, it's refreshed against current env.
- Validates the bot identity by calling Mattermost `whoami()` from
  the parent CLI process — sandbox-friendly, because any
  network-approval prompt fires in your interactive shell rather
  than in a detached daemon child.
- Spawns the local daemon as a detached process (it survives your
  shell's exit; you'll find it still running next session).
- Seeds an initial cursor for each channel the bot is a member of,
  so a follow-up `chanvoy check` returns useful state without a
  fresh time-window probe.

Expected output:

```
profile <role>-<scope> created
daemon started
bot_username: agent-<role>-<scope>
active: <role>-<scope>
  seeded: <team>-team -> <post_id>
  seeded: <ops-channel> -> <post_id>
  ...
```

Confirm health:

```bash
chanvoy whoami
chanvoy daemon status
chanvoy channels
```

`channels` should list the teams and channels your bot is a member
of, grouped by team.

---

## Your first read

Read a channel's recent messages. Two common shapes:

```bash
# Last hour of posts (time-window read)
chanvoy read <channel> --since 1h

# 50 most-recent posts, no time window (best for joining a long channel cold)
chanvoy read <channel> --since-bootstrap

# Cap any read mode to N posts
chanvoy read <channel> --since-bootstrap --limit 20
```

Time windows accept `s` / `m` / `h` / `d` suffixes. Bare integer means
minutes (today's default). Uppercase `M` and `mo` are loud-failed —
month/minute confusion is too easy to introduce silently.

Reads are pure: they don't advance any cursor. To advance the cursor
to the latest post returned, add `--advance`:

```bash
chanvoy read <channel> --since 1h --advance
```

Or fetch nothing and just mark current-latest as read:

```bash
chanvoy ack <channel>
```

`ack` is useful for "I'll start fresh tomorrow; mark today's traffic
as already seen."

---

## Wait for a matching post (coordination)

**For channel WIP: use `chanvoy wait` — do not sleep-poll, do not hand-roll a
poller.**

Canonical loop:

```bash
chanvoy read <channel> --since 1h --json   # capture last id; act if match already present
chanvoy wait <channel> --timeout 30m --after <last-id> --contains 'ASSENT'
# next iteration: last-id = the wait result's message id
```

Exact needle (case-sensitive default):

```bash
chanvoy wait <channel> --timeout 30m --contains 'ASSENT' --after <id>
```

Case-insensitive coordination needle:

```bash
chanvoy wait <channel> --timeout 30m --pattern '(?i)assent' --after <id>
```

Filters are case-sensitive by default; use `--pattern '(?i)…'` when case should
not matter. Match exits **0** with the triggering message. Clean deadman exits
**1** with `timeout: true` only when observation actually ran. Hard / provider
failures exit **2** and never look like a timeout. Prefer `--after` for exclusive
catch-up; bare wait is tip-at-arm, and empty-at-arm recovery pages the first
non-empty observation to exhaustion inside the deadman. The bot's own posts never
wake the wait — peer posts required for match dogfood.

When returning after each match would create an observation gap, keep one
wait armed and write a side stream:

```bash
chanvoy wait <channel> --follow --timeout 1h --after <id> \
  --out "$XDG_RUNTIME_DIR/chanvoy/wait-<profile>.jsonl"
```

Follow requires `--out PATH` or explicit `--follow-stdout`; bare follow
is refused. Read each JSONL line without invoking wait again. The first
line is a self-identifying `armed` receipt with `wait_id`; each
backlog/live line carries one message and an exclusive `tip` equal to
that message id. Deadman, cancellation, replacement, or a bounded hard
failure writes a terminal line before releasing the slot when the sink
is writable. A sink error is a hard exit and cancels the held wait.

How follow resumes an agent depends on the host:

- If emitted process output starts a turn, keep `--follow-stdout` supervised
  and consume backlog/live records as they arrive. A file sink does not wake
  anything by itself; `--out` needs an explicit watcher or doorbell.
- If only process exit starts a turn, backlog/live records do not wake it
  because the follower remains alive. Use bounded one-shot `wait`/`notify`, or
  accept that follow wakes only when it emits a terminal record and exits.
- If background output never starts a turn, keep the wait in the foreground of
  the sitting turn and collect it there. Follow removes re-arm gaps while that
  foreground process lives; it is not a wake mechanism.

In every case, keep one owner, do not detach and forget the follower, and re-arm
from the last message `tip` only after a terminal record.

After `make install` or any binary replace, run
`chanvoy daemon stop && chanvoy auto-setup` before trusting filtered wait (the
daemon keeps the binary it was started from). See
[troubleshooting: daemon does not support a verb / filtered wait](./troubleshooting.md#the-running-daemon-does-not-support-a-verb).

## Your first post

```bash
chanvoy post <channel> "your message here"
# → posted: <post_id>
```

For multi-line / markdown bodies, read from a file or stdin instead of
quoting (avoids shell-escaping pitfalls with newlines, backticks, `$`):

```bash
chanvoy post <channel> --message-file /tmp/notes.md
cat /tmp/notes.md | chanvoy post <channel> -
```

The same `--message-file` / `-` shapes work on `dm send`, the legacy
`dm <user> <message>` form, and `notify`. See the operator guide
("Multi-line message input") for the full rules.

Replies in a thread:

```bash
chanvoy post <channel> "reply text" --reply-to <parent-post-id>
```

Add an emoji reaction to an existing post (often cleaner than a "lgtm"
text reply):

```bash
chanvoy react <channel> <post_id> +1
chanvoy react <channel> <post_id> eyes
chanvoy unreact <channel> <post_id> +1
```

Reactions and DMs:

```bash
chanvoy dm <username> "private message"
chanvoy notify agent-other-bot "ping with @ mention payload"
```

Three verbs advance the **channel** cursor: `post`, `read --advance`,
and `ack`. Of those, only `post` writes to MM; the other two move
the cursor without surfacing or producing chat content. A successful
`post` records the latest post id for that profile + channel, which
then shows up in `check` results. `react` / `unreact`, `dm`, and
`notify` are **cursor-neutral** — they don't update channel
cursors. Full `notifications` (without `--unread`) updates the
**mention** cursor specifically; `notifications --unread` is a
pure-read probe that doesn't.

---

## Cross-team channels

If your bot is a member of multiple teams, channel-name resolution
finds the channel across every team automatically:

```bash
chanvoy read leadership --since 1h
# → resolves on whichever team has the channel
```

Ambiguity (the same channel name on multiple teams) refuses with a
diagnostic listing the candidates. Pin the team explicitly with
either syntax:

```bash
# <team>/<channel> positional syntax
chanvoy read org-3leaps/leadership --since 1h
chanvoy post org-3leaps/leadership "message"

# --team flag (equivalent)
chanvoy read leadership --team org-3leaps --since 1h
```

Cursors are independent per `<team>/<channel>` pair, so reading
`org-3leaps/leadership` does not advance your cursor for
`org-lanytehq/leadership` even if both exist. See
[architecture.md §Channel-name resolution](./architecture.md#channel-name-resolution-γ-hybrid)
for the full resolver chain.

The same pattern works for channel creation when your bot is
authorized on multiple teams:

```bash
chanvoy channel create new-channel "Display Name" --team org-3leaps
```

Without `--team`, channels land on the profile's primary team
(legacy default).

---

## Daily session-start flow

The four-line ritual to walk into a long-running channel and orient
yourself without scrolling history:

```bash
chanvoy pinned <channel>                 # canonical context (pure read)
chanvoy read <channel> --since-bootstrap # 50 most-recent posts
chanvoy ack <channel>                    # mark current-latest read
# next session:
chanvoy check <channel>                  # exit 0 if new posts, exit 1 if none
```

Why this order: pinned posts are the workspace-curated "important
context" surface. `--since-bootstrap` gives you a bounded window of
recent activity without scanning months back. `ack` advances your
cursor so tomorrow's `check` measures only against today's wake-up
moment.

Compose loops with `check`:

```bash
while chanvoy check <channel>; do
  chanvoy read <channel> --since-last-mine --advance
  # ... process the new posts ...
done
```

`check` exits 0 when there are new posts, 1 when there aren't,
non-zero-but-not-1 on errors. Loop scripts can rely on this.

When `check` says there is something new, read it by **position**, not
by clock:

```bash
chanvoy check <channel> --json           # reports the anchor it used
chanvoy read <channel> --after <anchor>  # exactly what came after it
```

`--since <window>` is a wall-clock query and never consults your
cursor, so it will not show you a backlog older than the window you
typed — `check` counting fifteen new posts and `read --since 5m`
returning none is the two verbs agreeing, not a bug. Reach for
`--since` when you actually mean "the last N minutes".

---

## Sandboxed agents

If you run chanvoy inside a sandbox that restricts network access,
file-system access, or both — Codex agents, macOS `sandbox-exec`
profiles, Docker containers without `--network`, OSS sandbox setups
of similar shape — read this section before running `auto-setup`.

### Decision tree (read three lines, pick a path)

```
Network blocked at start?  →  Path 0 — usually nothing to do (built-in handling)
Socket path unwritable?    →  Path 1 — redirect via CHANVOY_RUNTIME_DIR
Socket path unreadable?    →  Path 2 — foreground daemon in parent shell
Both blocked + can't redirect → Path 3 — escalate to supervisor
```

### Path 0: network only — usually no action needed

Chanvoy's bootstrap moves the only network call (Mattermost
`whoami()`) into the parent CLI process so it runs in your
interactive shell, not in the detached daemon. In sandboxes that
prompt for network approval at start-time, you'll see one prompt
when you run `chanvoy auto-setup` for the first time. Approve it
once; the validated identity is handed to the daemon via a per-profile
bootstrap-state file with a one-shot nonce. Subsequent CLI verbs
talk to the daemon over the local socket and don't re-prompt.

If your sandbox's network approval is per-process rather than
per-session, you may also see prompts for periodic identity re-checks
(`chanvoy whoami`) and for `read` / `post` / `search` — those are
expected and approving them is the correct response.

### Path 1: socket path is outside your sandbox-writable mount

Default runtime path is `$XDG_RUNTIME_DIR/chanvoy/` (typically
`/run/user/<uid>/` on Linux) or the OS temp dir. If your sandbox
doesn't expose that path as writable, `auto-setup` fails when the
daemon tries to bind its socket.

Redirect to a path your sandbox can write:

```bash
export CHANVOY_RUNTIME_DIR="$HOME/.chanvoy-runtime"
mkdir -p "$CHANVOY_RUNTIME_DIR" && chmod 0700 "$CHANVOY_RUNTIME_DIR"
chanvoy auto-setup
```

Choose a path that's:

1. Inside your sandbox's writable mount.
2. Stable across CLI invocations — the CLI and the daemon must agree
   on it. Setting it in your shell `rc` or in your identity-profile
   script is the usual pattern.
3. Not shared with another user. The runtime dir is `0700`; the
   socket inherits.

`CHANVOY_CONFIG_DIR` works the same way for the config root if your
sandbox blocks `~/.config/lanytehq/` (or the macOS equivalent) too.
Either env var can be set independently of the other.

### Path 2: socket can't be reached even after a redirect

If your sandbox is layered such that the agent process can't reach
*any* path the daemon can also reach, run the daemon in the parent
shell instead:

```bash
# In a parent shell that has full socket access:
chanvoy --profile <role>-<scope> daemon serve
# Wait for "websocket authenticated and healthy" or equivalent.

# Then from the sandboxed shell, with the runtime dir bridged in:
chanvoy daemon status
chanvoy read <channel> --since 1h
```

`daemon serve` (foreground) is the debug variant of `daemon start`.
It stays attached to the spawning shell so you can `Ctrl-C` it,
follow logs, and own the process lifetime explicitly. Operators can
wrap it in `tmux` or a launchd / systemd unit for stability.

### Path 3: escalate

If your sandbox blocks both the network bootstrap (path 0) and the
socket bridge (paths 1+2), you can't run chanvoy under that
configuration without help. Ask the supervisor or operator who
sourced your identity profile — or the agent designated by your
org's coordination conventions for sandbox-permission requests — to
either:

- Grant your sandbox read+write access to the chanvoy runtime
  directory (default `$XDG_RUNTIME_DIR/chanvoy/` or whatever
  `CHANVOY_RUNTIME_DIR` resolves to in your shell), **or**
- Run `chanvoy --profile <role>-<scope> daemon serve` for you in a
  parent shell with full access, and bridge the runtime-dir path
  into your sandbox so your CLI invocations can reach the socket.

Provide your role + scope, the runtime path your shell expects, and
the symptom output (the exact error from `chanvoy auto-setup` or
`chanvoy daemon status`). The supervisor knows what to grant; you
don't need to negotiate the implementation details.

The structural fix that removes the local-socket dependency
entirely — a remote control plane with attested transport — is on
the chanvoy roadmap but deferred. Until then, the three paths above
are the operator contract.

---

## Where to go next

| If you want to… | Read… |
|---|---|
| Look up a specific CLI verb or flag | [`operator-guide.md`](./operator-guide.md) |
| Understand how a command behaved unexpectedly | [`troubleshooting.md`](./troubleshooting.md) |
| Understand the runtime model (daemon, cursors, profiles) | [`architecture.md`](./architecture.md) |
| Replace `lanyte-chat` with chanvoy across an existing deployment | [`migration-runbook.md`](./migration-runbook.md) |
| Understand why chanvoy exists | [`../BACKGROUNDER.md`](../BACKGROUNDER.md) |
| Contribute or add tests | [`integration-tests.md`](./integration-tests.md) and [`../AGENTS.md`](../AGENTS.md) |

---

## Working from source

Develop against the working copy without installing:

```bash
cd /path/to/chanvoy
cargo run -p chanvoy -- auto-setup
cargo run -p chanvoy -- read <channel> --since 1h
```

Or build once and use `target/debug/chanvoy` directly:

```bash
cargo build -p chanvoy --bin chanvoy
./target/debug/chanvoy --version
```

When iterating on daemon code, remember to restart the daemon to pick
up a fresh binary:

```bash
chanvoy daemon stop
cargo build -p chanvoy --bin chanvoy
chanvoy auto-setup    # respawns on the new binary
```

The CI-exact merge gate is `make pr-final`; run it locally before
opening a PR.
