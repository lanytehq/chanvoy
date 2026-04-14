# Chanvoy Migration Runbook

This runbook covers local replacement of `lanyte-chat` with `chanvoy` for agent sessions.

Current validated platform scope for this runbook:

- Linux
- macOS

Windows local-daemon support is not yet implemented.

## Preconditions

- Lanyte identity env sourced
- `LANYTE_MM_URL`, `LANYTE_MM_TOKEN`, `LANYTE_MM_TEAM` available
- `chanvoy` built locally

## One-Time Setup

1. Create or refresh the profile from the current shell:

```bash
chanvoy profile create-from-env --activate
```

2. Start the daemon:

```bash
chanvoy daemon start
```

3. Verify health:

```bash
chanvoy daemon status
chanvoy whoami
```

4. Verify the configured team is usable before relying on the profile for normal work:

```bash
chanvoy channels
```

`profile create-from-env` now validates both the token and the configured team before it persists a profile, so a missing or mistyped `LANYTE_MM_TEAM` should fail during bootstrap rather than later during channel operations.

## Daily Flow

Use plain `chanvoy` commands after identity is sourced and the daemon is running:

```bash
chanvoy read per-007 --since 60
chanvoy post per-007 "status update"
chanvoy notifications
chanvoy wait per-007 --timeout 10
```

For cursor-based resume and cheap attention checks after PER-008:

```bash
chanvoy read per-008 --after <post-id>
chanvoy read per-008 --since-last-mine
chanvoy check per-008
chanvoy notifications --unread
```

Important semantics:

- `read --after`, `read --since-last-mine`, `check`, and `notifications --unread` are observe-only
- `check` with no stored cursor returns `no_anchor` instead of silently falling back to a time window
- `check` with a stale daemon-owned cursor degrades to `stale_cursor` instead of hard-failing
- durable channel cursors are currently established by successful `post`
- durable mention cursors are currently established by full `notifications` reads

## Cutover Checklist

1. Update evergreen docs and operator references from `lanyte-chat` to `chanvoy`.
2. Post the planned cutover notice in `ops-updates`.
3. Bootstrap the target shared-machine profile with `chanvoy profile create-from-env --activate`.
4. Start and verify the daemon:

```bash
chanvoy daemon start
chanvoy daemon status
```

5. Run the smoke gate:

```bash
CHANVOY_PROFILE=<profile> scripts/per007-smoke.sh per-007
```

6. Switch routine agent operations to `chanvoy`.
7. Post cutover confirmation in `ops-updates`, including any migration exceptions still in force.

## Post-Cutover Cleanup

1. Sweep the repo and adjacent operator docs for lingering workflow references:

```bash
rg "lanyte-chat" .
```

2. Keep explicit migration notes and historical references that are still useful.
3. Remove stale day-to-day workflow references that should now point at `chanvoy`.

## Rollback

If `chanvoy` is unavailable during a session:

1. Capture the failing command, stdout/stderr, and exit code.
2. Stop or restart the daemon if appropriate.
3. Fall back temporarily to `lanyte-chat` for the affected operation.
4. Post the discrepancy in the relevant PER channel before continuing.

## Rollback Window

- Keep `lanyte-chat` available as a fallback for one week after cutover.
- Treat any regression that blocks normal agent operations during that week as rollback-eligible.
- At the end of the week, do a final doc/grep sweep and remove remaining non-historical `lanyte-chat` workflow references.

## Known PER-007 Notes

- `channel restore` requires an elevated-capability profile in `chanvoy`.
- Runtime sockets live outside config storage.
- Rebuilds require daemon restart to pick up new behavior.
