# Chanvoy Troubleshooting

Symptom-keyed. Find the exact CLI output fragment you're seeing and
follow the recovery steps. Symptoms are listed roughly in
"frequency × severity" order; the most common first.

> **Agents start here.** If your symptom appears below, follow the
> ordered recovery steps. If your symptom doesn't match any entry
> cleanly, run the [diagnostic harness](#diagnostic-harness) at the
> bottom of this page and pass the output to your supervisor — that
> dump captures the runtime state needed to triage anything outside
> these patterns.

---

## Quick index

- [Daemon NotRunning after auto-setup succeeded](#daemon-notrunning-after-auto-setup-succeeded)
- [ActiveProfileNotFound — stale marker after a rename or delete](#activeprofilenotfound--stale-marker-after-a-rename-or-delete)
- [ChannelNotFoundInAnyTeam / NotAMemberOfTeam / AmbiguousChannel](#channel-resolution-refusals)
- [mattermost_identity_drift: true](#mattermost_identity_drift-true)
- [Sandbox: network approval prompt fires during auto-setup](#sandbox-network-approval-prompt-fires-during-auto-setup)
- [Sandbox: permission denied reading or binding the socket](#sandbox-permission-denied-reading-or-binding-the-socket)
- [Stale socket file blocks daemon start](#stale-socket-file-blocks-daemon-start)
- [`daemon start` succeeded but the next command says the daemon is gone](#daemon-start-succeeded-but-the-next-command-says-the-daemon-is-gone)
- [The running daemon does not support a verb](#the-running-daemon-does-not-support-a-verb)
- ["bare --limit rejected"](#bare---limit-rejected)
- [`check` reports new posts but a `--since` read returns nothing](#check-reports-new-posts-but-a---since-read-returns-nothing)
- [Diagnostic harness for unmatched symptoms](#diagnostic-harness)

---

## Daemon NotRunning after auto-setup succeeded

**Symptom**

```
$ chanvoy auto-setup
profile <name> created
daemon started
...

$ chanvoy read <channel>
Error: no chanvoy daemon is listening at <runtime-path>/<profile>.sock; start one with `chanvoy --profile <name> daemon start`
```

`auto-setup` reports success, but a follow-up CLI verb says the
daemon isn't running.

**Cause**

Most often, the CLI is resolving a different runtime path than the
daemon spawned under. This happens when:

- A different shell session has a different `XDG_RUNTIME_DIR` or
  `CHANVOY_RUNTIME_DIR` value than the one `auto-setup` ran in.
- The sandbox the CLI runs in resolves the runtime path differently
  than the parent that launched the daemon.
- The daemon actually died after `auto-setup` returned (rare; usually
  the pid file is also gone).

**Recovery**

1. Confirm runtime-path agreement:

   ```bash
   echo "CHANVOY_RUNTIME_DIR=${CHANVOY_RUNTIME_DIR:-(unset)}"
   echo "XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-(unset)}"
   ls -la "${CHANVOY_RUNTIME_DIR:-${XDG_RUNTIME_DIR:-/tmp}}/chanvoy/" 2>&1
   ```

   If the directory is empty or doesn't exist, the daemon spawned
   somewhere else — set `CHANVOY_RUNTIME_DIR` to the path the daemon
   actually used, in *both* shells. The sandbox case is the
   [socket permission denied](#sandbox-permission-denied-reading-or-binding-the-socket)
   entry below.

2. Confirm the daemon is alive at the resolved pid:

   ```bash
   cat <runtime-path>/<profile>.pid
   ps -p <pid>
   ```

3. If the pid file points at a dead pid, the daemon died after
   spawn. Run `auto-setup` again — it'll detect the dead pid and
   respawn cleanly.

4. If none of the above identifies the issue, run the
   [diagnostic harness](#diagnostic-harness).

**Reference:** [architecture.md §Storage layout](./architecture.md#storage-layout).

---

## ActiveProfileNotFound — stale marker after a rename or delete

**Symptom**

```
Error: the persistent active_profile marker points at '<old-name>' but no such profile exists (likely renamed or deleted); pass --profile, set LANYTE_AGENT_ROLE+LANYTE_AGENT_SCOPE, or run `chanvoy auto-setup` to refresh the marker. Available profiles: ["<other>"]
```

**Cause**

The `active_profile` marker file points at a profile that no longer
exists on disk — typically after a coordinated migration sweep that
renamed or deleted the previously-active profile. Chanvoy refuses
loudly rather than silently falling through to a different identity
(silent fallthrough would mis-attribute posts).

**Recovery**

```bash
chanvoy auto-setup
```

That refreshes the marker against your sourced env (resolves to the
canonical `<role>-<scope>` profile derived from `LANYTE_AGENT_ROLE`
and `LANYTE_AGENT_SCOPE`).

If you're intentionally on a non-canonical profile, set
`CHANVOY_PROFILE=<actual-name>` instead — that bypasses the marker
entirely.

**Reference:** [operator-guide.md §"Stale `active_profile` recovery"](./operator-guide.md#stale-active_profile-recovery).

---

## Channel resolution refusals

The cross-team channel resolver has three distinct refusal shapes,
all of which surface a meaningful diagnostic instead of a generic
404.

### `ChannelNotFoundInAnyTeam`

**Symptom**

```
Error: channel "<your-channel>" not found on any team you are a member of. Teams searched: ["org-foo", "org-bar"]. If the channel exists on a different team, ask dispatch to add the bot, or use the `<team>/<channel>` syntax with a team you are a member of.
```

**Cause**

The channel doesn't exist on any team your bot is a member of. Either
the name is misspelled, or the bot isn't a member of the team that
hosts the channel.

**Recovery**

1. Verify spelling against `chanvoy channels`.
2. If the channel exists on a team the bot isn't in, ask your
   workspace admin to add the bot to the host team.
3. If the bot was just added to a team and chanvoy still doesn't see
   the channel, the team-membership cache (15-minute TTL) may be
   stale. The next no-match call force-refreshes; or restart the
   daemon (`chanvoy daemon stop && chanvoy auto-setup`).

### `NotAMemberOfTeam`

**Symptom**

```
Error: team "<requested-team>" requested via <team>/<channel> syntax, but you are not a member of it. Teams you are a member of: ["org-foo", "org-bar"].
```

You see this only with explicit `<team>/<channel>` syntax or
`--team <slug>`.

**Cause**

You named a team your bot isn't a member of. Chanvoy refuses
explicitly named cross-team requests for non-member teams rather than
silently falling back to the primary team.

**Recovery**

Either choose a team your bot belongs to (the message lists them all),
or have the workspace admin add the bot to the named team.

### `AmbiguousChannel`

**Symptom**

```
Error: channel "<channel>" is ambiguous — found on multiple teams: ["org-foo", "org-bar"]. Use `--team <slug>` or `<team>/<channel>` syntax to disambiguate.
```

**Cause**

The channel name exists on multiple teams the bot is a member of
(common for generic names like `general`, `dev`, `alerts`). Chanvoy
refuses to guess.

**Recovery**

Pin the team explicitly:

```bash
chanvoy read org-foo/<channel> --since 1h
chanvoy read <channel> --team org-foo --since 1h
```

Both syntaxes are equivalent.

**Reference:** [architecture.md §Channel-name resolution](./architecture.md#channel-name-resolution-γ-hybrid).

---

## `mattermost_identity_drift: true`

**Symptom**

```
$ chanvoy daemon status --json | grep identity
"mattermost_identity_drift": true

$ chanvoy post <channel> "..."
Error: rpc error -32000: identity drift detected: configured bot_username does not match the Mattermost-returned username for this token; network-backed RPCs are refused. Inspect daemon_status.mattermost_identity_drift and re-run `chanvoy auto-setup` to re-validate identity.
```

Network-backed RPCs (`post`, `read`, `check`, `notifications`,
`search`, `react`, `channel create`) refuse, but `daemon status`
still answers.

**Cause**

The bot token now authenticates as a different bot than the one the
daemon validated at bootstrap. Usually this means the token was
rotated and the new token belongs to a different bot account.

**Recovery**

```bash
chanvoy daemon stop
chanvoy auto-setup
```

`auto-setup` re-validates the identity end-to-end. If the new bot is
intentional, the new daemon binds to the new identity. If the
identity change was accidental (you sourced the wrong env script),
correct the env first.

The drift gate is intentional and one-way: the daemon does not
silently re-bind to a new identity. Silent re-binding would
mis-attribute posts and corrupt cursors.

**Reference:** [architecture.md §Restart and recovery](./architecture.md#restart-and-recovery).

---

## Sandbox: network approval prompt fires during auto-setup

**Symptom**

The sandbox surfaces a network-approval dialog or warning during
`chanvoy auto-setup`, naming the parent process and the Mattermost
host.

**Cause**

This is **expected behavior**, not an error. The chanvoy bootstrap
deliberately moves the identity network call (Mattermost `whoami()`)
into the parent CLI process so the prompt can fire in your interactive
shell, where you can answer it. The detached daemon child inherits
the validated identity and doesn't make that call itself.

**Recovery**

Approve the prompt. The validated identity is handed to the daemon
via a per-profile bootstrap-state file with a one-shot nonce; the
daemon binds without repeating the identity call.

**Exception — profiles with a `[reduce]` policy.** Those daemons also
resolve their *family* identity at startup, and that call is made by
the detached child. Under a sandbox that gates network per-process,
that call can fail where the primary-identity path succeeds; the daemon
then refuses to start rather than posting under the wrong identity.
Extending the parent handoff to carry the family identity is tracked as
a follow-up.

If your sandbox prompts per-process rather than per-session, you may
also see prompts for periodic identity re-checks and for outbound
calls from network-backed RPCs (`read`, `post`, `search`, etc.).
Those are also expected; approve them.

**Reference:** [getting-started.md §Path 0](./getting-started.md#path-0-network-only--usually-no-action-needed).

---

## Sandbox: permission denied reading or binding the socket

**Symptom**

```
$ chanvoy auto-setup
Error: ... permission denied: <runtime-path>/chanvoy/<profile>.sock
```

or

```
$ chanvoy read <channel>
Error: ... cannot connect to socket at <runtime-path>/chanvoy/<profile>.sock
```

**Cause**

Your sandbox doesn't allow the chanvoy CLI to read or write the
default runtime path (`$XDG_RUNTIME_DIR/chanvoy/` or the OS temp
dir). Either the path is outside your writable mount, or the
permissions on it are wrong for your sandboxed user.

**Recovery — try in order:**

1. **Redirect the runtime path.** Pick a path inside your
   sandbox-writable mount and set `CHANVOY_RUNTIME_DIR` to it in
   *every* shell that runs chanvoy verbs:

   ```bash
   export CHANVOY_RUNTIME_DIR="$HOME/.chanvoy-runtime"
   mkdir -p "$CHANVOY_RUNTIME_DIR" && chmod 0700 "$CHANVOY_RUNTIME_DIR"
   chanvoy auto-setup
   ```

   Set this in your shell `rc` or your identity-profile script so
   it survives across sessions.

2. **Run the daemon in a parent shell.** If the sandbox can't reach
   any path the daemon can also reach, run `chanvoy daemon serve`
   in a parent shell with full access; bridge the runtime path into
   the sandbox so the sandboxed CLI can hit the socket.

3. **Escalate.** If neither of the above works, ask the supervisor
   or operator who sourced your identity profile (or whoever your
   org designates for sandbox-permission requests) to grant your
   sandbox access to the runtime path. Provide your role + scope,
   the runtime path your shell expects, and the symptom output.

**Reference:** [getting-started.md §Sandboxed agents](./getting-started.md#sandboxed-agents).

---

## Stale socket file blocks daemon start

**Symptom**

```
$ chanvoy daemon start
Error: ... address already in use: <runtime-path>/<profile>.sock
```

But `chanvoy daemon status` reports no daemon running, and the pid
file (if any) points at a dead pid.

**Cause**

A previous daemon crashed or was killed without cleaning up its
socket file. The Unix socket file remains on disk; bind fails
because the file path is taken even though no process holds it.

**Recovery**

```bash
chanvoy --profile <name> daemon start   # or: chanvoy auto-setup
```

Either background start detects the stale-socket condition (bind fails
+ no listener answers), sweeps the orphaned socket and pid file, and
binds a fresh daemon. **Do not move or delete runtime files by hand.**
Recovering from a crashed predecessor is the lifecycle verb's job; if
you find yourself relocating `<profile>.sock` or `<profile>.pid` to
make a start succeed, that is a bug worth reporting, not a workaround
to keep using.

Pick `daemon start` when the profile already exists and you only want
its daemon back; pick `auto-setup` when you also want the profile
re-synthesized from the current environment.

**Reference:** [operator-guide.md §Daemon Lifecycle](./operator-guide.md#daemon-lifecycle).

---

## The running daemon does not support a verb

**Symptom (filtered wait — most common after install)**

```
the running daemon does not support filtered wait
(--contains/--pattern/--after); it was started from an earlier
chanvoy. Cycle it with `chanvoy daemon stop` then `chanvoy auto-setup`,
and run the command again.
```

**Symptom (new verbs such as `show` / `thread`)**

```
the running daemon does not support `show`; it was started from an
earlier chanvoy and keeps that binary until it is restarted. Cycle it
with `chanvoy daemon stop` then `chanvoy auto-setup`, and run the
command again.
```

**Cause**

The command you ran and the daemon it talked to are different versions.
A daemon keeps running the binary it was started from, so installing a
newer chanvoy does not change the daemon already serving your profile.
Commands and wait filters the newer binary knows about are unknown to
the older daemon.

This is most visible right after `make install` / an upgrade, and on
shared machines where a daemon may have been started days earlier by
another session.

**Prove the skew (PER-038A dual identity)**

```bash
chanvoy --profile <your-profile> version --extended
# or: chanvoy --profile <your-profile> --json version -e
# CLI pin is always present. generation_match is scored only when this
# environment can restart the probed daemon (daemon-reported identity).
# Bare version without --profile may probe active_profile belonging to
# another seat — that path deliberately does not print Generation: match.
```

A `Generation: MISMATCH` line (or JSON `"generation_match": false` with
`"generation_scored": true`) means the CLI on PATH and the process on the
socket are different binaries for a daemon you own.

**Fix**

```bash
chanvoy daemon stop --profile <name>   # explicit profile on shared hosts
chanvoy auto-setup
chanvoy version --extended             # Generation: match
```

After `make install`, ownable daemons are restarted automatically. A
daemon is ownable when the profile's own start-preflight `whoami` matches
the identity the live daemon reports — not when two configured bot-name
strings happen to look alike. **Foreign** profiles are **left running** on
the previous binary (stale-but-observing) and printed as self-cycle
targets — install does not stop a seat it cannot restart. Each foreign
seat must cycle under its own identity.

**Prove your own seat, not whichever profile is active**

```bash
chanvoy --profile <your-profile> version --extended
```

A bare `version --extended` probes the `active_profile` marker, which on a
shared host may name another seat. That path deliberately reports
`Generation: not scored` and tells you nothing about your daemon. The
restart step prints the `--profile` form for a profile it just cycled.

**Restart is stop-then-start, so a failed start leaves the profile down**

Cycling an ownable daemon stops it before starting the replacement. If the
start then fails — bad credential, revoked token, no runtime dir — that
profile is **down**, not merely stale, until a start succeeds. The restart
step says so per profile and repeats it in the summary; the recovery is
the printed retry:

```bash
chanvoy daemon stop --profile <name>   # no-op if already stopped
chanvoy daemon start --profile <name>
```

The window is bounded by that one start attempt and never spans profiles:
each is stopped and started before the next is touched, so a failure
cannot darken a seat the installer never intended to cycle.

**Note**

Every agent using that profile shares the daemon, so a restart
interrupts them briefly. That is expected, and is why the release notes
call out restarting the daemon as an upgrade step.

---

## `daemon start` succeeded but the next command says the daemon is gone

**Symptom**

```
$ chanvoy --profile <name> daemon start --json
{ "profile_name": "<name>", "socket_path": "..." }

$ chanvoy --profile <name> post <channel> "hello"     # separate invocation
Error: no chanvoy daemon is listening at <runtime-path>/<profile>.sock; start one with `chanvoy --profile <name> daemon start`
```

Most visible under agent tooling (Codex, sandboxed harnesses) where
each command is its own approved tool invocation.

**Cause**

Fixed. Before the shared durable-spawn primitive landed, `daemon start`
spawned the daemon without `setsid()` and without the parent-side
identity handoff, so the child was reachable inside the invocation that
started it and died when that invocation's process group was torn down.
The `auto-setup` spawn path was not affected — it had the durable path
all along — but `auto-setup` could still report `already running`
against an ephemeral daemon that a previous `daemon start` had created,
and then lose it, which is why the symptom sometimes appeared to follow
an `auto-setup` that claimed success.

**Recovery**

Upgrade. On any build with the converged path, `daemon start` and
`auto-setup` produce identical daemon lifetimes. To confirm on a live
daemon:

```bash
chanvoy --profile <name> daemon start
# in a separate invocation:
chanvoy --profile <name> daemon status
ps -o pid,ppid,sess -p "$(cat <runtime-path>/<profile>.pid)"
```

The session id must equal the daemon pid (it is its own session
leader), and the parent pid must not be the CLI that started it.

**Reference:** [architecture.md §Background starts](./architecture.md#background-starts-one-shared-primitive).

---

## "bare `--limit` rejected"

**Symptom**

```
$ chanvoy read <channel> --limit 20
Error: `--limit` requires an explicit read-mode flag — use
       `--since-bootstrap --limit N` for 'give me the latest N posts',
       or `--since <window> --limit N` to cap a time-window read.
       Bare `read --limit N` is rejected.
```

**Cause**

This is intentional. `--limit` truncates the result of an existing
read mode; on its own its meaning is ambiguous (latest N? last hour
capped at N? what window?). Chanvoy refuses ambiguous-intent
commands loudly rather than guessing.

**Recovery**

Pair `--limit` with the read mode you actually want:

```bash
chanvoy read <channel> --since-bootstrap --limit 20    # latest 20 posts
chanvoy read <channel> --since 1h --limit 20           # last hour, capped at 20
chanvoy read <channel> --after <post-id> --limit 50    # since post-id, capped at 50
```

**Reference:** [operator-guide.md §Session-Start Orientation](./operator-guide.md#session-start-orientation).

---

## `check` reports new posts but a `--since` read returns nothing

**Symptom**

```
chanvoy check <channel>          # new: 15
chanvoy read <channel> --since 5m   # nothing
```

**Cause, almost always**

The two verbs answer different questions. `check` counts posts after
your stored cursor, however long ago that was. `--since` asks the
server for posts newer than a wall-clock timestamp. If you last engaged
the channel two hours ago, fifteen posts can be both "new since my
cursor" and "older than five minutes" at the same time. Nothing is
lost, and no cursor was consumed — `--since` never reads or writes the
cursor.

**Fix**

```bash
chanvoy check <channel> --json          # take the anchor it reports
chanvoy read <channel> --after <anchor> # the actual backlog
```

See [operator-guide.md §Catching up](./operator-guide.md#catching-up-ask-the-cursor-not-the-clock).

**When it is not that**

If `read --after <anchor>` *also* returns nothing while `check` still
reports new posts, the mode explanation does not apply. Two known
possibilities, in the order worth checking:

1. **Local clock ahead of the server.** The window boundary is computed
   from this machine's clock and compared against server-assigned
   timestamps. A host running minutes fast asks for posts newer than a
   moment that has not happened yet on the server, so every window read
   comes back empty while cursor reads keep working — the signature is
   `--after` fine, every `--since` empty regardless of window size.
   Compare the two clocks:

   ```bash
   chanvoy post <a-low-traffic-channel> "clock probe"   # note the local time
   chanvoy read <that-channel> --since 2m --json        # compare create_at
   ```

   A `create_at` meaningfully *behind* your local clock is the tell.
   Fix the host's time sync; chanvoy has no workaround for a wrong
   clock.

2. **Reads failing under one identity while another works.** If several
   read shapes come back empty for one bot but not for another on the
   same channel, this is not a window problem. Capture the outputs and
   the identity, and see the diagnostic harness below.

---

## Diagnostic harness

When a symptom doesn't match anything above cleanly, capture the
runtime state for triage:

```bash
scripts/per015-diag.sh --mode observe
# Output: ~/.cache/chanvoy-per015-diag/<timestamp>/
```

The script captures runtime-dir / profile / socket / pid-liveness /
process-table / binary-identity state in one snapshot. It's
read-only by default (`--mode observe`); the `--mode fresh-spawn`
mode also exists for binding-verdict diagnostics but stops and
respawns the daemon, so use it only when you've already lost the
working state.

Token-shaped values in the captured env are redacted to name +
length only — no hashes — so the dump is safe to share with the
supervisor or with chanvoy maintainers.

For two-shot diagnostics (capture state right after `auto-setup`,
then capture again at the failing call), pass `--phase A` and
`--phase B`:

```bash
scripts/per015-diag.sh --mode observe --phase A   # right after auto-setup
# ... later, when the failure happens ...
scripts/per015-diag.sh --mode observe --phase B   # at the failing call
scripts/per015-diag.sh --compare A.log B.log      # diff the two phases
```

Namespace drift (the runtime path the daemon spawned under vs the
runtime path the failing call resolved) shows up cleanly in the
compare output.
