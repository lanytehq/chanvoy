# Release Notes

**Content policy**: This file contains the most recent 3 releases (reverse chronological). Older releases are archived in `docs/releases/vX.Y.Z.md`.

## v0.3.3 - 2026-10-22

**Safer daemon stop and setup** — the daemon pid file no longer lives in the
OS temp directory when no runtime directory is configured, and `daemon stop`,
`auto-setup` and profile refresh can stop a live daemon whose pid file is
missing, using the socket's kernel peer process and the same identity checks.
On Linux the force signal is bound to the verified process; macOS keeps a
documented residual. `auto-setup` now exits **6** instead of reporting success
when it would reuse a daemon of a different or unverifiable build generation.

The daemon also routes `runtime_retire_v1` and `runtime_observation_v1`
through strict schema decoding; native retirement stays inactive. Oversized
legacy requests whose first 16 KiB + 1 bytes name either method are refused.
Signed GitHub Release binaries only; not on crates.io. Stop older daemons and
re-run `auto-setup` after install.

See `docs/releases/v0.3.3.md` for upgrade notes.

## v0.3.2 - 2026-10-10

**Wait filters and optional follow coalescing** — operators can sit on one
DM peer (`wait --dm`), on any DM to this bot (`wait --inbox`), or only on
posts that mention this bot (`wait --mention`). Inbox `--after` is an
opaque `inv1.` cursor, not a Mattermost post id; coalesced inbox follow
resumes from `next_inbox_cursor`. `--mention` matches this bot's
`@username` token (ASCII case-insensitive); `@bot-suffix` is not a
match.

`--follow --coalesce` is **off unless you pass it**. Five seconds is
recommended; ten seconds / 32 messages is the hard maximum. Each burst is
one JSONL line; `tip` is the last message id. `armed` is immediate; a
pending burst flushes before a terminal record.

The cut also pins rsfulmen 0.2.0 for file-backed JSON Schema checks of
wait parameters. Signed GitHub Release binaries only; not on crates.io.
Restart the profile daemon after install.

See `docs/releases/v0.3.2.md` for upgrade notes and
`docs/guides/wait-follow.md` / `docs/guides/wait-dm.md`.

## v0.3.1 - 2026-08-27

**First public distribution** — v0.3.1 carries the cumulative operator and
integrity improvements proven at the signed v0.3.0 development checkpoint. It
makes cited posts and their threads directly reachable, restores honest author
and thread attribution, and adds process-held `wait --follow` observation
without weakening the single-owner wait contract.

This cut ships signed GitHub Release binaries only; it is not published on
crates.io. Restart the profile daemon after installing the new binary so the
CLI and daemon agree on wait, follow, show, and thread capabilities.

See `docs/releases/v0.3.1.md` for full notes, upgrade guidance, follow wake
capabilities, and the verification pointer.

_(Older releases archived in `docs/releases/`. This file is kept short per project convention.)_
