# Release Notes

**Content policy**: This file contains the most recent 3 releases (reverse chronological). Older releases are archived in `docs/releases/vX.Y.Z.md`.

## v0.3.2 - 2026-09-12

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

## v0.3.0 - 2026-08-27

**Signed development checkpoint, not distributed** — the immutable v0.3.0 tag
was retained after the release procedure was exercised, but no GitHub Release
or crates.io package was published. Its cumulative changes are distributed in
v0.3.1.

**Post rehydration, thread orientation, and author honesty — with a deliberate source-compatibility boundary** — a cited post is now reachable. Several verbs already took a post id, but none of them would show you the post; the one read verb that accepted an id was the resume flag, which excludes the post it names. Two verbs close that, and two long-standing integrity bugs in reading are fixed alongside them.

- **`chanvoy show <channel> <post-id>`** — reopen one cited post. The post is bound to the named channel and refused before any content is returned if it lives elsewhere. `--json` emits one object.
- **`chanvoy thread <channel> <root-or-post-id> [--latest]`** — read a whole conversation. Accepts the root's id or any reply's, so a citation from the middle of a thread works without finding the root first. `--json` emits an array in both modes, including with `--latest` (a one-element array), so a flag never changes the output type. Both verbs are pure reads and never touch the attention cursor.
- **Citable human output** — default `read` rows carry `id=<post-id>`, plus `root=<root-id>` wherever a thread root is known, so a post id can be handed straight to `show`, `thread`, or `post --reply-to` without re-running with `--json`. Every message on the read and push paths now reports its thread root; `--json` gains an additive `root_id` field.
- **Author names restored** — posts carry only a user id and the code read an author-name field the server does not send, so every message read as `unknown`. Names now resolve from the user id through a shared cache; an unresolvable author is reported as the literal user id rather than a placeholder that reads like a person.
- **Threads come back** — thread reads filtered on that same absent field and discarded every post, reporting success with nothing in it. A root plus N replies now returns N+1 messages. A genuinely empty thread response is an error, not a plausible-looking empty result.
- **Channel-bound thread reads over the agent IPC surface** — the thread was previously fetched on the post id alone and stamped with whatever channel the caller claimed. The anchor is now checked first (a mismatch issues no thread request at all), every post in the response is checked, and a truncated read reports `has_more` instead of being indistinguishable from a complete one.
- **Durable `daemon start`** — it now detaches into its own session with the parent-side identity handoff, so the daemon outlives the command that started it; a start reported as failed no longer leaves a daemon running.
- **Process-held wait streams** — `chanvoy wait <channel> --follow` keeps one
  single-channel wait armed and writes self-identifying JSONL to an explicit
  secure file or stdout sink. It preserves ordered backlog/live messages,
  replacement lineage, clean deadline, failure, and cancellation terminals;
  `Ctrl-C` writes `canceled` and exits 130. Cycle an older daemon before use.
- **Operator-legible errors** — errors print their message instead of an internal debug shape, and a daemon older than the verb you just used names the verb and the two commands that fix it (`chanvoy daemon stop`, then `chanvoy auto-setup`).
- **Compatibility**: source-breaking for Rust code building against `chanvoy-core` — `CoreError` is now `#[non_exhaustive]` and gained two variants, and `MattermostClient::read_thread` is deprecated and always refuses (use `read_thread_in_channel`). No on-disk or state migration; exit codes unchanged; a binary distribution needs no source rebuild. Cycle the daemon before using the new verbs, and review strict parsers of human output or stderr — default `read` rows and error text both changed. Messages gain an additive `root_id` in JSON.

See `docs/releases/v0.3.0.md` for full notes.

_(Older releases archived in `docs/releases/`. This file is kept short per project convention.)_
