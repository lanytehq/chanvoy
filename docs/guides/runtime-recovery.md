# Recovering a daemon or an abandoned wait

Use an explicit profile for recovery. A healthy Mattermost connection, matching
bot credentials and native ownership of a process are separate observations.
None alone proves that stopping or replacing a daemon is safe.

## Choose the operation

| Intent | Operation | What success establishes |
| --- | --- | --- |
| Inspect the responding daemon | `chanvoy --profile <name> daemon status --json` | A current daemon response and its reported health/build information. |
| Observe native runtime & sandbox capabilities | `chanvoy --profile <name> doctor --json` | Read-only `native_runtime` (`socket_access`, `process_inventory`, `listener_table`) with `qualification: false`. |
| Compare credentials with the daemon's bot | `chanvoy --profile <name> daemon ownable --json` | Matching remote bot identity; not native stop authority. |
| Ensure an existing profile is running | `chanvoy --profile <name> daemon start --json` | A usable daemon, which may be the existing instance. |
| Stop the selected daemon | `chanvoy --profile <name> daemon stop --json` | Confirmed termination and guarded cleanup, or an explicit refusal. |
| Restart the selected daemon | `chanvoy --profile <name> daemon restart --json` | For v1-managed daemons: confirmed retirement/exit (`Busy` refusal if active work exists), guarded socket reap, `Exited` tombstone, and fresh successor readiness. For pre-v0.3.4 daemons without a v1 record: verified legacy stop followed by fresh v1 successor start. |
| Recover an abandoned inbox waiter | `wait --inbox --replace-wait <wait-id>` with the saved cursor | Replacement after the old wait's cleanup is confirmed. |

Use `daemon restart` when cycling a running daemon to a fresh epoch or updated
binary. When the predecessor is a v1-managed daemon, `daemon restart` retires it
over `runtime_retire_v1` (refusing with `Busy` if active wait, stream, or RPC
obligations exist — v1 retirement never force-kills active waits), confirms
whole-process exit and remaining-listener absence, reaps the socket under the
namespace lock, publishes an `Exited` tombstone, and starts a fresh successor
epoch. When replacing a pre-v0.3.4 daemon that has no v1 runtime record on disk,
`daemon restart` uses the verified legacy stop fallback, which does not refuse
with `Busy` and terminates active waits after its shutdown grace window — finish
or close active waits before running the first restart after upgrading from
v0.3.3. `daemon start` preserves a responsive daemon, so running `daemon start`
after a failed stop is not a successful restart. Do not interpret a
`daemon start` receipt as evidence that the previous process exited.

## Recover an inbox wait without replacing the daemon

If a foreground client is lost, preserve the last delivered `next_inbox_cursor`.
A subsequent inbox arm may report `wait_already_active` and the existing wait ID.
Use that exact ID and the saved cursor:

```sh
chanvoy --profile <name> wait --inbox --after <saved-inbox-cursor> \
  --replace-wait <existing-wait-id> --timeout 20m --json
```

An idle inbox receiver notices replacement without needing a new message.
Replacement still waits for confirmed cleanup, including previously selected
delivery and cursor work. `wait_replace_unconfirmed` means the old ownership
cannot yet be released. `wait_conflict_changed` means the captured owner changed;
inspect the new conflict rather than reusing an obsolete wait ID blindly.

Direct-message and inbox waits share an ownership class. Opening a direct wait
to a particular person is not a way around an existing inbox owner. A reply in
another channel does not automatically cancel that owner's wait.

## Understand a stop refusal

`process identity unavailable` names the failed inspection stage when available.
Possible prerequisites include native process birth, account owner, executable,
arguments and start time. `permission-denied` records an observed denial;
`unavailable` does not establish its cause. Metadata can also disappear during
process exit, so missing metadata alone is not proof of death.

A restricted agent can authenticate with Mattermost and communicate over the
daemon's local socket while lacking permission to inspect that process. Run
native inspection in an operator-approved environment with the required access.
Do not relax file permissions, change the profile's identity or remove runtime
files to turn an inspection refusal into apparent success.

Matching CLI and daemon build metadata does not establish idleness. Finish or
close the owning seat's active waits, streams and requests before a planned
service interruption. Preserve saved cursors and pending delivery information.

## Legacy or otherwise unconfirmed recovery

The steps below are for a separately authorized human operator. They do not
grant an agent permission to signal a process or cycle other seats' daemons.

1. Notify the owning seat and identify the explicit profile. Capture status and
   build information while the daemon responds. Record which native facts are
   proved and which are unavailable; an unreachable socket is not a dead daemon.
2. Have the seat finish or close its owned work. An owner's statement of idleness
   is useful coordination, but does not replace process or runtime evidence.
3. Independently inspect the current candidate's account, native birth,
   executable, arguments and socket ownership. A PID file is a hint: the number
   may have been reused. Compare the candidate again immediately before any
   authorized action. If identity cannot be established, retain the state.
4. Prefer the explicit-profile stop operation when it can prove the candidate.
   If software refuses, any signal outside Chanvoy is a separate human decision
   based on fresh native identity evidence. Never signal a process merely because
   its number appears in an old PID file.
5. Confirm whole-process death and that no other process retains the listener.
   Socket-path disappearance, a disconnected client, a timeout or a free lock
   alone is insufficient. An inherited listener can outlive its original creator.
6. Clean only artifacts still belonging to the captured, confirmed-dead instance.
   Recheck identities before cleanup. Preserve any replaced file, new epoch,
   foreign occupant or remaining listener; never unlink a live socket to make
   the next bind succeed.
7. Start the explicit profile as a separate checked operation. Verify the
   responding daemon's build and readiness, then restore waits from the saved
   cursors. Report stop success and successor readiness separately when only one
   of them is established.

Changing a CLI binary does not upgrade an already-running daemon. Plan migration
per profile, including long-lived and temporarily unreachable seats. An install
or a version match is not evidence of a completed restart.

## Runtime-control observations

Native managed lifecycle (`runtime_observation_v1` / `runtime_retire_v1`) binds
daemon startup, `daemon stop`, and `daemon restart` to the native runtime
authority (`~/.chanvoy/lifecycle/records/<namespace-hex>.json`), control-socket
epoch, process birth/owner/executable/argument identity, and kernel Unix domain
socket listener table. For v1-managed daemons, when active wait, follow-stream,
or RPC obligations are present, managed retirement refuses with `Busy` and
retains the running daemon rather than interrupting active waits. When a
pre-v0.3.4 daemon without a v1 runtime record is targeted, `daemon stop` and
`daemon restart` fall back to the verified legacy stop path (only when no v1
runtime record exists on disk); that legacy fallback does not refuse with `Busy`
and can force-signal the predecessor after its shutdown grace window, so finish
active waits before migrating a pre-v0.3.4 daemon.

`chanvoy doctor` includes a read-only `native_runtime` diagnostic block
(`qualification: false`, `socket_access`, `process_inventory`, `listener_table`,
and optional sandbox `guidance`). Those probes classify `supported`, `denied`,
`incomplete_or_changing`, or `unavailable` (and `reachable`, `absent`, `denied`,
or `unreachable` for `socket_access`) so sandboxed agents can distinguish
filesystem/sysctl permission denials from ordinary daemon absence without
mutating state or claiming lifecycle qualification.

Native lifecycle operation requires a qualified native-host process and socket
view, the same account and supported namespaces, and compatible participating
CLI launches. Shared runtime roots across hosts or containers are unsupported.
Process discovery recognizes the supported launch arguments; it does not provide
an identity guarantee against a same-user program that deliberately rewrites
those arguments. Remaining-listener inspection is a separate kernel-level gate
and includes references held by otherwise unrelated processes.

The native discovery draft distinguishes `same-account process relevance
unavailable` (the environment prevented a complete discovery observation) from
`candidate daemon identity unavailable` (a possible daemon or recorded target
could not be fully identified). An unrelated process needs complete, rechecked
birth, account, namespace and argument evidence for exclusion; a candidate also
requires its executable identity. Neither refusal permits cleanup or startup.
