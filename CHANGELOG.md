# Changelog

All notable changes to chanvoy are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **PER-019 attention list human renderer (devrev PR #17 follow-up,
  2026-04-30).** The JSON-side fix at `3156a0a` added `quarantined`
  to `AttentionListResult`, but `render_attention_list_text` ignored
  it — operators using the default text mode for `chanvoy attention
  list` couldn't see quarantined records without reaching for
  `--json`. Renderer now emits a `quarantined (N record(s)):`
  section with `LEGACY_NAME / AMBIGUOUS_TEAMS / QUARANTINED_AT`
  columns plus a hint at the `--team` / `<team>/<channel>`
  disambiguation syntax.
- **PER-019 review fixes (devrev PR #17, 2026-04-28).** Four behavior-
  coverage gaps closed:
  - `chanvoy check --team` and `chanvoy wait --team` now thread the
    operator's override through to channel resolution. Previously the
    handlers parsed `team` but called the resolver with `None`,
    silently routing duplicate-name channels to the primary team.
  - `chanvoy post --team` cursor recording binds to the same team the
    side effect landed on. Previously the post API call honored
    `--team` but `record_channel_cursor` re-resolved with `None`,
    breaking cursor isolation for duplicate-name channels.
  - `read --since-last-mine` threads `--team` through
    `latest_authored_post_id` so the `posts/search` endpoint targets
    the right team's id.
  - `<team>/#<channel>` syntax now strips the leading `#` from the
    channel segment, matching the existing whole-arg `#` trim
    operators rely on when pasting channel names from the Mattermost
    UI.
  - `wait_push_backed` matches inbound WS events by resolved
    `channel_id`, not `channel_name`. Same-named channels on different
    teams have distinct ids, so the previous name-based predicate
    could let an event from the wrong team wake a wait. The matcher
    is extracted into a small testable predicate
    (`inbound_event_wakes_wait`) with a regression test that fires
    two events sharing the same name but different ids and verifies
    only the matching id wakes the wait.
- **PER-019 review fixes (entarch + secrev PR #17, 2026-04-29).**
  Three follow-on gaps from the qualified-key migration's
  propagation:
  - `compute_seed_outcomes` skip-check now honors qualified keys.
    The helper enumerates primary-team channels; it qualifies each
    enumerated name against the primary team before checking the
    existing-cursors set. Pre-fix, post-migration cursor sets
    contained qualified keys but the helper compared against bare
    names — already-cursored primary-team channels were no longer
    skipped at enumeration, risking spurious `Failed` outcomes on
    transient HEAD fetches and degraded `auto-setup` readiness
    (entarch P2 + secrev residual). Bare-name fallback is retained.
  - `attention_list` qualifies `monitored_channels` entries against
    the primary team before unioning with the qualified
    `attention.channels` keys. Pre-fix, a tracked channel with a
    persisted cursor under `org-lanytehq/bravo-team` could emit two
    rows: a bare `bravo-team` no_anchor and the qualified cursor
    row. Post-fix, the union deduplicates because both forms hash
    to the same qualified key (secrev finding #1).
  - `AttentionListResult` gains a `quarantined: Vec<QuarantinedCursor>`
    field surfaced in `attention list` output. Quarantined records
    were invisible to operators pre-fix; now they're listed with
    their original bare name + the ambiguous teams they resolved
    to + the preserved cursor state (secrev finding #2).
  Plus one new regression test
  (`compute_seed_outcomes_skips_qualified_key_after_per019_migration`)
  that fails on the pre-fix bare-name comparison.
- **PER-019 attention-surface contract restoration (secrev PR #17,
  2026-04-29).** My earlier fix routed `attention show` through
  `qualified_attention_key`, which calls `resolve_channel` and so
  makes a Mattermost API call. That violated the PER-008B
  strict-read-only contract on the `attention` prefix (no network
  calls allowed; status surface stays available during outages).
  Restored the contract: `attention show` now uses a new pure-string
  helper `local_attention_key` whose heuristic mirrors
  `attention_list` — explicit `<team>/<channel>` or `--team` wins;
  bare name defaults to the primary team. Trade-off: a bare name
  typed against a non-primary cursor returns `NoAnchor`; operators
  disambiguate via `--team` or `<team>/<channel>` for cross-team
  inspection. Regression test `secrev_pr17_attention_show_local_key_no_network`
  pins the heuristic across all four input shapes.

## [0.1.3] - 2026-04-28

### Added

- **PER-019: cross-team channel resolution.** `chanvoy post / read /
  check / notifications / wait / attention show` resolve channel names
  across every team the bot is a member of, not just the profile's
  primary team. Previously a channel on a non-primary team would
  silently 404 — exactly the failure mode SOP-MM-015 cross-org standing
  channels expose. The new γ hybrid resolver tries the primary team
  first (no perf change for the common case), then falls back across
  member teams. Explicit `<team>/<channel>` syntax and `--team <slug>`
  flag are per-invocation overrides.
- **Distinct error diagnostics**: `ChannelNotFoundInAnyTeam`,
  `NotAMemberOfTeam`, `AmbiguousChannel` — never a generic 404. Each
  diagnostic names the next-step flag/syntax to use.
- **`chanvoy channels` cross-team output** with `--team <slug>` filter,
  `--primary-team` legacy single-team view, and `--json` structured
  per-team output. Default output groups channels by team with the
  qualified `<team>/<channel>` form on each line for direct copy-paste
  into other verbs.
- **Bot team-membership cache** with 15-minute TTL plus self-healing
  refresh on no-match (newly-added team memberships surface without
  rerunning `auto-setup`).
- **Cursor isolation across teams**: `AttentionState` is now keyed by
  qualified `<team>/<channel>` pair so same-named channels on different
  teams maintain independent cursors. Pre-PER-019 records migrate at
  daemon `start()`; ambiguous historical names are quarantined rather
  than silently bound to a single team (per devrev's pin).
- Operator-guide §"Cross-Team Channel Resolution" — documents the
  resolution chain, error shapes, cursor-isolation guarantee, and the
  new `chanvoy channels` flags with worked examples.

### Changed

- `MattermostClient::read_channel`, `read_channel_after`,
  `read_channel_since_last_mine`, `post_message` now take an optional
  `team: Option<&str>` parameter. Internal — daemon handlers thread
  the operator's `--team` flag through.
- `ChannelCursorState` gains `channel_id`, `team_id`, `team_name`,
  `channel_name` denormalized metadata fields. Pre-PER-019 records
  remain readable via `#[serde(default)]`.
- `latest_authored_post_id` now resolves the channel via the cross-team
  resolver before searching, so `read --since-last-mine` against a
  non-primary-team channel uses the correct team's `posts/search`
  endpoint (per secrev's pin).
- `chanvoy_core::ResolvedChannel`, `ResolutionSource`, `TeamInfo`,
  `TeamChannels`, `MigrationOutcome`, `QuarantinedCursor`,
  `attention_key_for`, `migrate_attention_state` are new public API.

## [0.1.2] - 2026-04-27

### Added

- **PER-014 review fixes (devrev + entarch PR #16, 2026-04-27/28).**
  Drift gate now refuses `subscribe` RPCs and suppresses event
  forwarding to existing subscribers when the identity-drift bit is
  set — closes the gap where network-sourced WebSocket events could
  continue flowing for the wrong authenticated bot. `unsubscribe` /
  `daemon_status` / `profile_status` / `attention` / `shutdown` remain
  answerable for diagnostic and cleanup. **IPC peer surface honors the
  same drift gate**: network-backed IPC requests (channel list / read /
  post / channel get / subscribe) refuse with the new
  `ChatErrorCode::IdentityDrift`, and IPC subscription event forwarding
  is suppressed under drift. Missing bootstrap-state file behavior
  split: if `CHANVOY_BOOTSTRAP_NONCE` is set but the file is absent,
  daemon refuses with `CoreError::BootstrapHandoffFailed` (failed
  auto-setup handoff — likely runtime-dir drift, sandbox /tmp cleanup,
  or a consume race); if the env nonce is absent, daemon falls back to
  legacy `whoami()` (manual `daemon serve` path). Resolution logic
  factored into `chanvoy_core::resolve_startup_identity` for unit
  testability. **Post-spawn readiness now uses a local-only RPC
  (`profile_status`) instead of `daemon_status`**: under sandbox
  restrictions where REST is stalled rather than denied, the
  Mattermost-probing `daemon_status` could exceed the post-spawn ping
  timeout and cause `auto-setup` to report `Daemon(NotRunning)` even
  though the socket was bound — exactly the failure mode PER-014 is
  trying to eliminate. The operator-facing `chanvoy daemon status` keeps
  the network probe. **Pre-spawn check uses a separate network-aware
  helper** (`ping_full` → `daemon_status`) so a bound-but-unhealthy
  existing daemon (rotated token, identity drift) gets stopped and
  respawned with the parent's freshly validated credential — preserves
  the previous stale-daemon-respawn semantics that local-only ping
  alone would have masked.

## [0.1.2] - 2026-04-27

### Added

- **PER-014: sandbox-aware daemon startup via pre-detach identity bootstrap.**
  `chanvoy auto-setup` now succeeds in sandboxed agent contexts (Codex,
  macOS sandboxd, Docker without `--network`, OSS sandbox setups) without
  manual operator intervention beyond the one-time network-approval
  prompt that fires for the parent CLI's own `whoami()` call. The
  detached daemon inherits the validated identity from the parent via a
  per-profile `<runtime_dir>/<profile>.bootstrap.json` file (mode 0600
  in 0700 dir; identity-only, no token) plus a one-shot `CHANVOY_BOOTSTRAP_NONCE`
  env var for anti-replay. The daemon validates freshness (60s),
  profile fingerprint (SHA-256 over canonical non-secret fields), nonce
  match, and username match before binding the UDS — no network call
  pre-bind. WS first-connect failures are absorbed by PER-010's existing
  reconnect path.
- **Drift floor (`daemon_status.mattermost_identity_drift`).** Bind-first /
  probe-after / surface-in-status: a one-shot post-bind `whoami()` probe
  runs asynchronously (and refreshes on every `daemon_status` call); on
  identity mismatch (Mattermost-returned username ≠ configured
  `bot_username`), `daemon_status.mattermost_identity_drift = true` and
  network-backed RPCs (post / read / check / notifications / etc.)
  refuse with a clear diagnostic. The local socket stays bound so
  operators can query `daemon_status` to learn what's wrong.
- Operator-guide: §"Sandboxed Agent Contexts" rewritten — primary path
  is now native `auto-setup`; foreground `daemon serve` retained as the
  rare-case fallback for fully non-interactive batch contexts.

### Changed

- `validate_and_finalize_profile` returns `(Profile, Identity)` instead
  of `Profile` so the validated `Identity.id` (Mattermost user_id) flows
  pre-detach into `ensure_daemon_running`. Internal CLI helper; no
  public-API change.
- `ensure_daemon_running` now writes the bootstrap-state file and sets
  the env nonce immediately before spawning the detached daemon. Site
  discipline by structural placement: only the daemon-spawn path can
  emit a bootstrap file, by construction; profile-create paths cannot.

## [0.1.1] - 2026-04-26

### Added

- Operator-guide rewrite: primary bootstrap path is now `chanvoy auto-setup`;
  manual `profile create-from-env` + `daemon start` retained as a
  debugging/custom-scenario fallback.
- Operator-guide: dedicated "Profile Resolution" section reflecting the
  post-PER-012 6-step contract, with a stale-marker recovery instruction
  for the `ActiveProfileNotFound` case.
- Operator-guide: new "Profile and Team Naming Convention" section
  documenting `<role>-<scope>` and `org-<scope>` as the portability contract.
- Operator-guide: new "Using Chanvoy in Another Org" walkthrough for
  non-lanytehq adopters, plus a "Product namespace (not org restriction)"
  clarification on the default config root.
- Operator-guide: new "Sandboxed Agent Contexts" section documenting the
  foreground `daemon serve` workaround for environments where `daemon start`
  cannot bootstrap (PER-014 tracks the underlying design fix).
- Operator-guide: documentation for `CHANVOY_CONFIG_DIR` and
  `CHANVOY_RUNTIME_DIR` env overrides, with a concrete worked example.
- `Makefile`: `version-patch`, `version-minor`, `version-major`, `version-set`,
  `version-sync`, and `version-check` targets. Bump targets update both
  `VERSION` (repo-root SSOT) and `Cargo.toml` across the workspace atomically.
  `version-check` is wired into `pr-final` and `prepush` so version drift
  cannot land in main.

### Changed

- **`chanvoy profile active`** now reports the marker state truthfully
  when no marker is set: `(none)` in text mode, or
  `{"active_profile": null}` in JSON mode (a JSON object with a `null`
  field, not bare `null`). This replaces a prior synthetic-name fallback
  that returned the resolver's guess. Scripts or agents that parse this
  output to gate behavior may need to handle the explicit-empty case
  (text `(none)` literal, or `.active_profile` JSON field that may be
  `null`).
- **Default profile resolution** now requires an exact `<role>-<scope>`
  name match against env (`LANYTE_AGENT_ROLE` + `LANYTE_AGENT_SCOPE`).
  Sibling profiles sharing role+scope no longer prevent the canonical match.
  When env identifies a profile that does not exist, the resolver refuses
  with a clear error and the available-profile list rather than silently
  falling through to a different identity. New canonical-name profiles
  materialize via `chanvoy auto-setup`.
- **`daemon stop`** now refuses on fallback resolution. Pass `--profile`,
  `CHANVOY_PROFILE`, or source an identity script with
  `LANYTE_AGENT_ROLE` + `LANYTE_AGENT_SCOPE` for destructive verbs.
  Stale `active_profile` markers no longer route a destructive command
  to another operator's daemon.
- **`profile create --team-name`** no longer hardcodes `org-lanytehq`.
  Defaults to `org-<scope>` derived from the positional `<scope>`
  argument; explicit `--team-name` flag still overrides.
- **Profile-collection management verbs** (`profile list`, `profile create`,
  `profile create-from-env`) now bypass default resolution entirely. Fresh
  bootstrap on an empty config dir works as expected — the resolver no
  longer blocks the verbs that exist to populate the collection.
- **Migration runbook + README** corrected to reference `chanvoy auto-setup`
  as the primary bootstrap path.

### Fixed

- HTTP user-agent string is now derived from `CARGO_PKG_VERSION` rather
  than hardcoded, so it stays in sync with the workspace version
  automatically across future releases.

## [0.1.0] - Initial version

Initial chanvoy release. Local Mattermost control-plane client for Lanyte
agent sessions: Rust daemon over UDS, CLI + MCP surfaces, named profiles,
WebSocket push events, attention/cursor primitives, daemon detachment for
session-survival, hash-chained reconnect-health surface. Pre-this-changelog
shipping history is captured in git log and the per-task briefs under
`lanyte-productbook-internal/content/projmgmt/peers/`.

[Unreleased]: https://github.com/lanytehq/chanvoy/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/lanytehq/chanvoy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lanytehq/chanvoy/releases/tag/v0.1.0
