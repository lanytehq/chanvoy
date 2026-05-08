# Release Notes

**Content policy**: This file contains the most recent 3 releases (reverse chronological). Older releases are archived in `docs/releases/vX.Y.Z.md`.

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

## v0.2.0 (May 2026)

**Local-mode polish bundle** — sandbox-aware daemon startup, cross-team channel resolution, and a forensic harness for daemon-startup failures. The first chanvoy release with the Mattermost-adoption-ready surface.

- **PER-014** — `chanvoy auto-setup` works end-to-end under sandbox restrictions (Codex agents, macOS sandboxd, Docker without `--network`). Identity validation moved into the parent CLI; daemon child receives validated identity via per-profile bootstrap-state file.
- **PER-019** — Channel-name resolution finds channels across every team the bot is a member of. Closes the silent-404 cross-team posting gap. Cursors are independent per `<team>/<channel>` pair. Pre-PER-019 records migrate automatically; ambiguous historical names quarantine.
- **PER-015 Phase 1** — `scripts/per015-diag.sh` forensic harness for daemon-startup failures. PER-015 itself scope-collapsed to done.

See `docs/releases/v0.2.0.md` for full notes.

_(Older releases archived in `docs/releases/`. This file is kept short per project convention.)_
