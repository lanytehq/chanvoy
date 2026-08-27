# Release Notes

**Content policy**: This file contains the most recent 3 releases (reverse chronological). Older releases are archived in `docs/releases/vX.Y.Z.md`.

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

## v0.2.1 (May 2026)

**Session-start ergonomics, conversation shape, discovery, and onboarding** — a new agent walking into a long-running channel can run the four-line ritual without scrolling history; multi-reviewer review cycles get cleaner via threaded replies + reactions; channel discovery (search + traffic-aware listing) lands; every chanvoy verb that touches a channel is now cross-team aware. Plus a major onboarding doc surface expansion.

- **PER-023** — `chanvoy pinned`, `read --since-bootstrap`, general `--limit`, time-unit suffixes (`30s`/`5m`/`4h`/`2d`), `read --advance`, `chanvoy ack`. Four-line session-start ritual works end-to-end.
- **PER-024** — `chanvoy post --reply-to` for threaded replies; `chanvoy react`/`unreact <channel> <post> <emoji>` for cursor-neutral acks. Channel positional + required for multi-provider portability. Idempotent on duplicate-react and missing-unreact.
- **PER-025** — `chanvoy search <channel> <query>` with operator-conflict refusal (`in:`/`from:`/`before:`/`after:` against chanvoy-owned scope, quoted-region-aware). `chanvoy channels --sort active` adds `last_active` column; preserves PER-019 grouping (no flatten); `last_post_at: null` deterministic shape on missing-activity; `--primary-team --json` legacy preservation.
- **Cross-team `channel create --team <slug>`** — closes the last cross-team admin-verb gap. Membership-checked: refuses `NotAMemberOfTeam` if the bot isn't a member of the requested team.
- **PER-026** — agent-first `docs/getting-started.md`, symptom-keyed `docs/troubleshooting.md`, runtime-model `docs/architecture.md`. README + safety protocols rewritten chanvoy-specific.
- **Bugfix**: `chanvoy pinned` URL `pinned_posts` → `pinned` (canonical MM v4 endpoint). Caught by prodmktg dogfooding during PER-026; wiremock-vs-real-API drift class flagged for v0.2.2 structural follow-up.
- **Build**: `make release-prep` umbrella (goneat-driven license + vulnerability + SBOM generation). `rustls-webpki` 0.103.11 → 0.103.13 clears three RUSTSEC advisories.

See `docs/releases/v0.2.1.md` for full notes.

_(Older releases archived in `docs/releases/`. This file is kept short per project convention.)_
