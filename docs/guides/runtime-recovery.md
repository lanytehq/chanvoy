# Recovering a daemon or an abandoned wait

Use an explicit profile for recovery. A healthy Mattermost connection, matching
bot credentials and native ownership of a process are separate observations.
None alone proves that stopping or replacing a daemon is safe.

## Choose the operation

| Intent | Operation | What success establishes |
| --- | --- | --- |
| Inspect the responding daemon | `chanvoy --profile <name> daemon status --json` | A current daemon response and its reported health/build information. |
| Compare credentials with the daemon's bot | `chanvoy --profile <name> daemon ownable --json` | Matching remote bot identity; not native stop authority. |
| Ensure an existing profile is running | `chanvoy --profile <name> daemon start --json` | A usable daemon, which may be the existing instance. |
| Stop the selected daemon | `chanvoy --profile <name> daemon stop --json` | Confirmed termination and guarded cleanup, or an explicit refusal. |
| Recover an abandoned inbox waiter | `wait --inbox --replace-wait <wait-id>` with the saved cursor | Replacement after the old wait's cleanup is confirmed. |

The current release has no `daemon restart` command. A healthy daemon can still
be explicitly stopped. Start preserves a responsive daemon, so start after a
failed stop is not a successful restart. Do not interpret a successful start
receipt as evidence that the previous process exited.

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

The current daemon answers the strict runtime-control observation and retirement
methods, but native retirement is inactive. Retirement refuses with
`native_evidence_unavailable`; an observation can report application admission
and counts without proving native process death, lease freedom or listener
absence. This guide's presence does not activate that lifecycle protocol.

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
