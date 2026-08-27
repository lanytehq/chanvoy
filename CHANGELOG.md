# Changelog

All notable changes to chanvoy are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - unreleased

### Migration

- **The error type is now open to extension, and this release is the boundary
  where that lands.** `CoreError` carries `#[non_exhaustive]`, so code matching
  on it must include a catch-all arm (`_ => { ... }`). Two error cases were also
  added, for a thread request that comes back with no posts and for the removed
  unbound thread read.

  Rust treats adding a variant to an exhaustive public enum as a breaking change
  even though nothing was removed, so this is a deliberate version boundary
  rather than a patch. Marking the type non-exhaustive now means later
  **variants** will not be — this is an operational taxonomy that will keep
  growing.

- **Upgrading, for everyone else.** There is no on-disk or state migration for
  profiles or attention state, and a binary distribution needs no source rebuild
  for ordinary operators. Things worth checking: a daemon keeps the binary it
  was started from and must be cycled before new verbs and the enhanced wait
  path work; default `read` rows and error text on stderr both changed; messages
  gain a `root_id` field in JSON (additive); **`wait` always returns at most one
  message** (including unfiltered REST/lag paths that previously could return a
  multi-post window); and wait hard failures exit **2** with structured JSON
  (`error_class`, `retryable`) rather than looking like a clean timeout.

### Added

- **`chanvoy wait` waitprims hold (local A2).** Fan-in
  `wait_channels_v1` is one `run_first_match` registration set
  (tie = registration order). Grok-bot / no-listener uses one
  `run_poll_cycle` per wake; `read --after` still returns the
  complete provider page (including self-authored posts); cursors
  stay uncommitted until a request-owned `poll_cycle_ack` after a
  successful RPC write. Poll bounds follow the fetched page so a
  large `read --after` is not truncated. Fan-in tie losers replay from
  daemon-owned retention; the member-cancel watcher aborts on drop.
  Fan-in acquires the key set before baseline observation. Poll cursors
  use the tool-owned bounded reader and a 0600 non-following temp file.
  Selected fan-in matches stay in replay until the RPC write succeeds.
  The winning fan-in arm stays owned until that write completes.
  `read --advance` and poll cursors apply only after a successful RPC
  write; a combined poll+attention commit uses one recoverable txn;
  poll persist fsyncs the parent directory after rename. Combined
  `read --after --advance` recovers a pending redo before building the
  next attention candidate so a later channel cannot roll back a
  recovered cursor. Ordinary attention writers persist a cloned
  candidate before replacing in-memory state. Acknowledged attention
  state uses the same 0600 no-follow temp, file sync, rename, and
  parent-directory sync as poll cursors. `restore_ready` is `Result` and fail-closed.
  A1 single-channel `wait_channel_v3` remains. Waitprims is pinned
  to git tag `v0.1.2`.

- **`chanvoy wait` waitprims hold (local A1).** Single-channel
  `wait_channel_v3` is driven by `waitprims-async::run_first_match`
  over the existing daemon stream. MCP `wait` single and CLI
  `chanvoy wait <channel>` keep the same RPC bodies. Waitprims is
  pinned to git tag `v0.1.2`.

- **`chanvoy mcp`.** MCP 2026-07-28 access face on the existing profile
  daemon (`stdio` by default; `--listen 127.0.0.1:<port>` for loopback
  Streamable HTTP). Tools: `whoami`, `read_channel`, `show`, `thread`,
  `wait`, `post`. Same `DaemonClient` as the CLI; no second Mattermost
  client and no new daemon RPCs. Blocking `wait` does not wake Grok
  Bot. `wait.mode=single` uses `wait_channel_v3`; `fan_in` uses
  `wait_channels_v1` (no v2 fallback).

- **`chanvoy wait` single-waiter ownership.** One active wait per
  canonical channel on **this profile daemon**. A second wait without
  `--replace-wait <id>` exits **2** with `wait_already_active` (never
  `timeout:true`). `--replace-wait` is compare-and-replace only. New CLI
  uses `wait_channel_v3` and does not fall back to v2. This is not a
  host-wide lock.
- **`chanvoy wait` multi-channel fan-in.** Repeat `--channel team/channel`
  (2–8 arms) to block on the first eligible peer post under one shared
  deadline. Per-arm exclusive baselines use `--after-channel
  team/channel=post-id`. Match JSON is `{mode:"fan_in", channels, matched_channel,
  messages:[one]}`; clean deadman is exit **1** with `timeout:true`;
  capability/input/provider failures are exit **2** and never set
  `timeout:true`. A daemon that does not implement `wait_channels_v1` is a
  hard failure — cycle it after install. Single-channel `wait <channel>`
  is unchanged.
- **`chanvoy doctor [channel]`** — cursor-neutral read-visibility self-diagnostic
  (identity, dual CLI/daemon generation, HTTP `Date` clock skew, optional channel
  resolve). Human + `--json`; exit **0** / **1** (soft) / **2** (hard). Never
  mutates attention state. Suspected-ahead guidance points at
  `check --json` → `read --after <anchor>`.
- **`chanvoy show <channel> <post-id>`** fetches a single post. The channel is
  required and the post is bound to it: a post that lives in another channel is
  refused before any of its content is returned, so a post id on its own is not
  authority to read a post. `--json` emits one object.
- **`chanvoy thread <channel> <post-id>`** reads a whole thread — the root post
  plus every reply. The id may be the root's or any reply's; both read the same
  thread, so a citation taken from the middle of a conversation works without
  the operator having to find the root first. `--latest` narrows the result to
  the most recent message. `--json` emits an array in both cases, including
  with `--latest`, so a flag never changes the shape of the output.
- **`chanvoy wait` content filter, exclusive baseline, and deadman contract.**
  For channel WIP, use `chanvoy wait` — do not sleep-poll or hand-roll a poller.
  New flags: `--contains` (case-sensitive body substring), `--pattern` (Rust
  regex over body; use `(?i)…` when case should not matter), and `--after
  <post-id>` (exclusive baseline). Match exits **0** with one message payload
  (`{channel, messages:[one]}`); clean deadman exits **1** with
  `timeout: true` only when observation actually ran; hard/config and
  provider-exhausted (including stalled provider calls and an unhealthy push
  path with no successful observe) exit **2** and never set `timeout: true`.
  Prefer `--after` for catch-up; empty-at-arm recovery pages the first non-empty
  observation to exhaustion inside the deadman. Filters refuse empty values and
  oversize/invalid patterns before the wait arms. The daemon method
  `wait_channel_v2` is the capability gate (cycle the daemon after install).
- **Install ↔ daemon cycle honesty (PER-038A).** Shared-host `make install`
  restarts only **ownable** daemons. Ownership is daemon-reported:
  `chanvoy daemon ownable --profile` (start-preflight whoami matches live
  `status.mattermost_username`) — not ambient env vs TOML bot string equality
  (org-spanning seat naming conventions diverge). Foreign profiles are **left
  running** on the previous binary with an explicit self-cycle list.
  `chanvoy version --extended` / `--json -e` reports dual identity: CLI host
  pin (top-level, back-compat) plus best-effort `daemon` pin, `daemon_profile`,
  and `generation_match` only when the probed daemon is restart-ownable in this
  environment (`generation_scored`); foreign/`active_profile` probes never
  emit a bare Generation match. `daemon status` includes additive `binary`
  (daemon process pin) and human `binary_commit`.
- **Release fingerprint contract (decernor 0.1.4).**
  `scripts/insert-expected-fingerprints.sh` writes
  `keys/expected-fingerprints.txt` from explicit public files.
  GPG is the unique `--gpg-role primary` record. Minisign is
  `minisign-public-blob-sha256-v1` (64 lowercase hex), not the
  20-hex key-id prefix. Both lines or neither. Requires
  `decernor` 0.1.4 or later.

### Changed

- **`scripts/verify-public-keys.sh` recomputes decernor records**
  instead of `gpg --show-keys` / minisign blob prefix. The
  checked-in contract comments describe 64-hex minisign blob SHA-256
  and 40-hex GPG primary. The checked-in production values are
  generated together from the exported public files and verified by
  independent recomputation.

- **The `CoreError` type gained additional variants, which will stop some code
  from compiling.** This affects Rust code that depends on the `chanvoy-core`
  library. Variants in this release include empty-thread and unbound-thread
  removals from the rehydrate cut, plus wait filter-invalid and
  provider-degraded cases from the wait cut. It requires no on-disk or state
  migration. (Human output and stderr text also changed — see the entries
  below.) If your code has a `match` on a `CoreError` that lists every variant
  by name and has no catch-all arm, that `match` no longer covers every case and
  the compiler will reject it. The fix is to add a catch-all arm —
  `_ => { ... }` — which also keeps the `match` compiling the next time a
  variant is added.

  The unbound thread read, `read_thread`, is still exported and still compiles,
  now marked deprecated and always refusing. Keeping that name available does
  not by itself make this release a drop-in recompile: the added variants are a
  separate break, and both need handling before code that matches exhaustively
  on `CoreError` will build again.

- **Human-readable reads now show the post id and the thread it belongs to.**
  `chanvoy read` printed a timestamp, an author, and a body, with no id
  anywhere — so an operator who had not asked for `--json` could not cite a
  post to `show`, `thread`, or `post --reply-to`. Every row now carries
  `id=<post-id>` and `root=<root-id>`, including top-level posts, where the
  root repeats the post's own id. Both crumbs are on every row so a row can be
  read without knowing that a post with no root is its own root. The crumbs are
  human output only; they are not how the same information appears in `--json`,
  where it is a field on the message object (see the thread-orientation entry
  below).

- **A daemon older than the verb you just used now says so.** Installing a new
  chanvoy leaves any already-running daemon on its previous binary until it is
  restarted, so a fresh CLI can call `show` or `thread` against a daemon that
  has never heard of them. That surfaced as a JSON-RPC "method not found"
  quoting an internal method name — neither of which is the verb the operator
  typed, and neither of which says what to do. It now names the verb and the
  two commands that resolve it: `chanvoy daemon stop`, then
  `chanvoy auto-setup`. Other daemon failures are unaffected.

- **Messages now carry the thread they belong to.** Every message on the read
  and push paths reports a thread root: its own id when the message is
  top-level, the thread's root id when it is a reply. This is needed to reply
  at all — the server rejects a reply aimed at another reply — so previously a
  caller citing a post had no way to tell which ids were valid reply targets.
  In `--json`, the output type is unchanged — a read still returns what it
  returned before — and every existing field on a message keeps its name and
  meaning. Each message object gains one field, `root_id`. A consumer that
  reads fields by name is unaffected; one that requires an exact set of keys
  will see the new one.

- **Errors now print their message instead of their internal shape.** The CLI
  returned errors from `main`, which makes Rust print the `Debug`
  representation — so operators saw
  `Error: Daemon(NotRunning("/var/.../profile.sock"))` and
  `Error: Resolver(DestructiveRequiresExplicit { available: [...] })` while the
  actual diagnostic messages went unread. Worse, the type name was doing the
  talking: a `daemon start` refused for want of an explicit profile appeared to
  call starting a daemon "destructive". Errors are now rendered as their
  message, and the process still exits non-zero as before. Scripts that match
  on stderr text will need updating; exit codes are unchanged.
- **"daemon not running" message is accurate and actionable.** It named a
  profile while actually carrying a socket path. It now reads
  ``no chanvoy daemon is listening at <path>; start one with `chanvoy --profile
  <name> daemon start` ``. This is the message quoted in most reports of a
  daemon that vanished between commands.

### Fixed

- **A thread read over the agent IPC surface is now bound to the channel it
  names.** The request carries a channel id and a thread root id, but only the
  root id was used: the thread was fetched on the strength of the post id
  alone, and every post in the response was then stamped with whatever channel
  the caller had claimed. A caller holding any post id could read that thread
  by naming any channel. The anchor post is now checked against the named
  channel first, and a mismatch issues no thread request at all. Every post in
  the thread response is checked too, not only the anchor: the credential the
  bot reads with reaches more channels than the caller named, so a response
  mixing the two is refused whole rather than returned in part. Nothing
  downstream can re-check it — a post's channel is dropped on the way into a
  message, and every result is stamped with the channel that was asked for.
- **A truncated thread over the agent IPC surface now says it is truncated.**
  The read limit was applied to the result while `has_more` was left unset, so
  a thread cut short was indistinguishable from a complete one and a caller
  reasoning about the conversation had no way to learn it had seen only the
  front of it. `has_more` now reports whether anything was withheld.
- **Message authors are real names again, instead of "unknown".** Posts carry
  only a user id, and the code read an author-name field the server does not
  send on a post, so every message in every listing was attributed to
  `unknown`. Names are now resolved from the user id through a shared cache.
  When a name genuinely cannot be resolved the author is reported as the
  literal user id — something an operator can look up — rather than a
  placeholder that reads like a person's name.
- **Reading a thread no longer returns an empty thread.** Thread reads filtered
  posts through that same absent author-name field, so every post was discarded
  and the read reported success with nothing in it. A thread with a root and N
  replies now returns N+1 messages. A thread response that genuinely contains
  no posts is now reported as an error rather than as a plausible-looking empty
  result, since the causes are permanent — a deleted post, an unreadable
  channel, or an id that is not a post.
- **`daemon start` now starts a daemon that outlives the command that
  started it.** It previously spawned the daemon without detaching it
  into its own session and without the parent-side identity handoff, so
  the daemon could answer inside the invocation that started it and then
  be gone by the next one — the command reported success and the
  following `post` or `read` failed with `NotRunning`. Most visible
  under agent tooling, where every command is a separate invocation.
  The `auto-setup` **spawn path** was not affected; both background
  starts now use the same durable-spawn path. (`auto-setup` could still
  report an ephemeral daemon as `already running` and then lose it,
  because the daemon it reused had been started by the defective path.)
- **A background start that is reported as failed no longer leaves a
  daemon running.** If the daemon had not answered its socket by the end
  of the startup budget, the command returned an error while that child
  kept starting — so the next start found nothing to clean up and
  spawned a second daemon for the same profile. A failed start is now
  terminal: the child is terminated and reaped, and its startup residue
  swept, before the error is returned. This covers a daemon that died on
  its own *after* binding its socket and writing its pid file — that
  residue used to survive and make the next start look like a crashed-
  predecessor recovery. In the rare case where termination cannot be
  confirmed, the command says so and names the pid instead of claiming a
  clean sweep, and leaves the runtime files alone rather than deleting
  them under a process that may still be alive.
- **A profile this environment cannot speak for now says so in one line,
  instead of quoting the server's error payload.** Probing another seat's
  profile — through `daemon ownable`, or through `version --extended`
  reporting why a generation verdict was withheld — echoed the provider's
  rejection body verbatim, including its internal error id and a
  per-request correlation id. None of that told an operator what to do,
  and it buried the fact that mattered: this environment does not hold
  that profile's identity. The failure is now classified and the status
  kept, with the body left out of both the human line and the JSON
  `reason`. The wording follows the status rather than assuming a
  refusal: `401`/`403` name the credential, `429` names throttling, and
  any other server-side failure says the identity check failed at the
  server — so a provider outage never sends an operator off to re-source
  an identity that was fine.

### Changed

- **`daemon start` validates the daemon's own identity in the calling
  process** — token, bot identity, and team access — before spawning, and
  refuses when the live credential authenticates as a different bot than
  the profile records. Note this covers the daemon's *primary* identity:
  a profile with a `[reduce]` policy still resolves its family identity
  in the daemon at startup, so reduce-configured profiles under a
  network-gated sandbox are not yet fully covered.
- **`daemon start` requires an explicit profile** (`--profile`,
  `CHANVOY_PROFILE`, or a sourced agent identity), matching
  `daemon stop`. It no longer falls back to the `active_profile` marker
  or to a single running daemon, so it cannot start a daemon under an
  identity you did not name.
- **`daemon start` reuses a running daemon only when it is healthy.**
  The pre-start check is network-aware: a daemon holding a revoked or
  drifted credential is replaced rather than reported as `already
  running`.
- **`daemon serve` documented as the foreground diagnostic surface.**
  Behavior is unchanged (attached, `Ctrl-C`-able); the docs and
  `--help` text no longer imply it differs from `daemon start` only in
  where stdio points.
- **The post-install daemon cycle now hands back a check that actually
  proves the upgrade.** It printed a bare `chanvoy version --extended`,
  which probes whichever profile the shared `active_profile` marker
  names — on a multi-seat host, frequently someone else's, which reports
  `Generation: not scored` and says nothing about the operator's own
  daemon. The hint now names a profile the step just restarted (falling
  back to `--profile <your-profile>`) and states why the bare form is not
  a proof. A profile whose restart stops but fails to start again is also
  now called out as **down** rather than left to read as merely stale,
  both per profile and in the summary line.

### Documentation

- **Catching up on a channel is documented as a cursor operation.**
  `check` counts posts after the stored cursor; `read --since` queries the
  server by wall-clock timestamp and never consults the cursor. The
  operator guide and the agent onboarding doc now give the
  `check --json` → `read --after <anchor>` loop, and troubleshooting
  covers the case where `check` reports new posts and a `--since` read
  returns none. The residual branches on what `read --after` did. It
  returns the backlog while an expected window is empty — where the
  test is which side of the emitted boundary the posts sit on: recent
  posts falling *outside* a locally computed window point at a clock
  running ahead (windows shorter than the skew are empty, wider ones
  are not), while a post at or after the emitted boundary that is still
  missing is a request or provider question, since that boundary came
  from the same clock. Or `--after` is empty too, which no clock
  explains, being anchored to a post id rather than a time.
- **Test coverage: the `?since=` query a time-window read emits.** The
  existing time-window tests match on path only, so they assert what the
  daemon does with a response and not what it asked for. Added a
  request-side regression that reads the recorded query and pins the
  millisecond boundary to the moment of the call. No behavior change.

- **CLI `--help` cleanup.** Stripped internal brief-ID references
  (e.g., "PER-023 primitive 1," "PER-019 γ hybrid resolver,"
  "PER-008B") from clap doc-comments and from the bare-`--limit`
  rejection diagnostic. Public `chanvoy --help` and per-subcommand
  `--help` now use feature-named terminology that maps directly to
  the user-facing docs (e.g., "the pinned-posts read," "the
  cross-team channel resolver," "the attention-state inspection
  commands"). Source-only `//` comments retain brief references
  where they help maintainers; only output-facing strings were in
  scope. Closes the follow-on flagged in PER-026's out-of-scope
  carve-out. No behavior change, no CLI surface change, no API
  change.
- **Public-readiness pass.** README opens with a standard badge row
  (CI / license / MSRV / version) above the lead paragraph. New
  `SECURITY.md` at repo root documents the vulnerability-reporting
  path, supported versions, signing-key verification posture
  (cross-referencing `RELEASE_CHECKLIST.md`), the chanvoy-specific
  security-issue class enumeration (token leaks; permission-mask
  regressions; sandbox-bypass class; identity-attribution bugs;
  bootstrap-handoff corruption), upstream-dependency posture, and
  disclosure policy. New `CONTRIBUTING.md` at repo root covers the
  toolchain, repo layout, branching, commit-attribution convention
  (supervised-commit shape — human or agent), test discipline,
  reviewer routing, code of conduct, and security-reporting pointer.
  Content audit closed several Lanyte-internal-context leakage
  sites in `AGENTS.md` and `docs/getting-started.md`;
  `docs/migration-runbook.md` gained a top-of-file annotation
  framing it as a Lanyte-internal artifact (the `lanyte-chat` →
  `chanvoy` migration).
- **Cargo metadata polish.** `[workspace.package]` populated with
  `description`, `repository`, `homepage`, `keywords`,
  `categories`, `authors`, `readme`. Root `chanvoy` package and
  all five sub-crates inherit via `<key>.workspace = true`; each
  sub-crate adds a per-crate `description` override. `publish =
  false` preserved on the root package — chanvoy distributes via
  signed-binary GitHub Releases, not crates.io. Every workspace
  member now reports non-empty metadata under `cargo metadata
  --no-deps`.

No CLI behavior changes. No CLI surface changes. No Rust code
changes outside `Cargo.toml` files.

## [0.2.1] - 2026-05-08

### Release highlights

The session-start ergonomics push. v0.2.1 bundles **PER-023**
(session-start primitives — pinned, bootstrap, time-unit suffixes,
read-and-ack), **PER-024** (conversation-shape — threaded replies +
emoji reactions), **PER-025** (discovery — channel-scoped search +
traffic-aware channels listing), the cross-team-aware
`channel create --team` admin-verb, and one v0.2.1-blocker bugfix
(the canonical MM `pinned` endpoint). PER-026 ships the major
onboarding doc surface expansion alongside.

Net operator impact:

- A new agent walking into a long-running channel can run the
  four-line ritual without scrolling history:
  `chanvoy pinned <ch>` → `chanvoy read <ch> --since-bootstrap` →
  `chanvoy ack <ch>` → next session `chanvoy check <ch>`.
- Time-window flags (`read --since`, `notifications --since`,
  `wait --timeout`, `search --since`) accept `s`/`m`/`h`/`d`
  suffixes. Bare integer preserves today's per-flag default
  (minutes). Uppercase `M` and `mo` are loud-failed to avoid
  month/minute confusion. Resolution is per-flag and not uniform:
  `read --since` and `wait --timeout` are second-precise;
  `notifications --since` rounds up to the nearest minute (MM's
  surface is minute-keyed); `search --since` narrows via the MM-
  native `after:<YYYY-MM-DD>` operator (date granularity, not
  sub-day precision). The suffix grammar is uniform; downstream
  precision is what the underlying API supports.
- Multi-reviewer review cycles get cleaner: `chanvoy post
  --reply-to <post>` for threaded follow-ups; `chanvoy react
  <ch> <post> +1` / `unreact` for ack-without-text-noise.
- Channel discovery: `chanvoy search <ch> <query>` for keyword
  search; `chanvoy channels --sort active` surfaces recency
  signal within each team group.
- Every chanvoy verb that touches a channel is now cross-team
  aware. `channel create --team <slug>` closes the last
  cross-team gap on the admin-verb side.
- Onboarding docs: a new `docs/getting-started.md` agent-first
  path, `docs/troubleshooting.md` symptom-keyed recovery,
  `docs/architecture.md` runtime model. README + safety protocols
  rewritten chanvoy-specific.

### Build & release-prep tooling

- **Goneat (3leaps DX) integration.** New release-cycle gate via
  `make release-prep` runs `pr-final` + license compliance +
  vulnerability scan + CycloneDX SBOM generation. Individual
  targets — `make sbom`, `make security-scan`, `make license-check`
  — are available for dev-loop use. Goneat presence is required;
  the targets fail with a clear install hint
  (`sfetch --repo fulmenhq/goneat`) when goneat isn't on `PATH`.
  Defensible failure mode for a release gate — the alternative
  would let a misconfigured environment ship un-scanned releases.
- **`.goneat/dependencies.yaml`** policy file — explicit
  permissive-license allow-list (MIT / Apache-2.0 / BSD /
  ISC / Unicode-3.0 / Zlib / CC0-1.0 / CDLA-Permissive-2.0);
  hard-refuse on copyleft (GPL-family / AGPL / MPL-2.0).
  Symmetric `deny.toml` for cargo-deny.
- **`deny.toml`** (new) — cargo-deny config carrying the same
  permissive license posture; one per-crate exception for
  `option-ext` (transitive MPL-2.0 utility, single-file copyleft
  not project-wide); one advisory `ignore` for RUSTSEC-2025-0134
  (rustls-pemfile unmaintained advisory; resolved by reqwest 0.13.x
  bump scheduled for v0.2.2).
- **SBOM artifact** generated by `make sbom` /
  `make release-prep` under `sbom/chanvoy-vX.Y.Z.cdx.json`
  (CycloneDX JSON via Syft, 294 packages). The `sbom/` directory
  is gitignored — SBOMs are dev-loop artifacts (per seclusor /
  ipcprims convention). Release-cycle SBOMs may be attached to
  the corresponding GitHub release as a downloadable asset by
  the operator cutting the tag.
- **rustls-webpki bump 0.103.11 → 0.103.13** clears three CVE-shaped
  RUSTSEC advisories (RUSTSEC-2026-0098 / 0099 / 0104; CRL parsing
  + name-constraint handling). No code change required —
  transitive bump via `cargo update`.

### License

- **LICENSE summary trademark notice aligned to the lanytehq/lanyte
  canonical form.** Adopts the registered-trademark (®) and
  trademarked (™) marks, names the Florida LLC, and tightens the
  derivative-works wording. Split-licensing structure (dual MIT-or-
  Apache-2.0 for code + CC0-1.0 for non-code assets) preserved as
  chanvoy's intentional more-generous posture for the new
  documentation surface (which is large enough to warrant explicit
  CC0 dedication).

### Added — PER-023 (session-start ergonomics)

- **`chanvoy pinned <channel>`** — fetch the channel's pinned
  posts via MM `GET /api/v4/channels/{id}/pinned`. Pure read; no
  cursor side effects. Resolves via the γ hybrid resolver
  (cross-team: `<team>/<channel>` syntax + `--team` flag).
- **`chanvoy read --since-bootstrap`** — bounded most-recent-N
  posts (default N=50; override with `--limit N`). Replaces the
  legacy `--since 999999` hack with a documented, bounded
  pattern.
- **`chanvoy read --limit N`** general flag — composes with any
  read mode (`--since`, `--after`, `--since-last-mine`,
  `--since-bootstrap`). Hard cap on the existing read-mode result
  set; PER-023 explicitly does NOT add full-window pagination
  semantics. Bare `read --limit N` (no read-mode flag) is rejected
  with a diagnostic suggesting `--since-bootstrap --limit N`.
- **Time-unit suffix parsing** on `read --since`,
  `notifications --since`, `wait --timeout`. Accepts `30s`/`5m`/
  `4h`/`2d`. Uppercase `M` and `mo` rejected with diagnostic
  (month/minute ambiguity).
- **`chanvoy read --advance`** — advances the attention cursor to
  the latest post **returned** by this read (mode-independent
  rule). No-op when zero posts returned.
- **`chanvoy ack <channel>`** — advances the attention cursor to
  the channel's current latest post id without surfacing content.
  No-op success on empty channels.

### Added — PER-024 (conversation shape)

- **`chanvoy post --reply-to <post_id>`** — threaded replies via
  MM `root_id`. Validation order: resolve channel → verify parent
  exists on resolved channel → write. `PostReceipt` JSON shape
  is **additive**: non-threaded posts return `{ "id": "..." }`
  unchanged; threaded posts add `parent_id` field.
- **`chanvoy react <channel> <post_id> <emoji>`** /
  **`chanvoy unreact <channel> <post_id> <emoji>`** — emoji
  reactions under the bot identity. Channel positional + required
  for multi-provider portability (Slack reactions API needs the
  channel-id tuple). **Idempotent**: re-react on existing emoji
  is no-op success; unreact-when-not-reacted is no-op success
  (404 normalized at the chanvoy-core layer). **Cursor-neutral**:
  reactions never advance attention cursors. Colon-wrapped emoji
  form (`:+1:`) accepted with stripping; canonical bare names
  (`+1`, `eyes`, `heavy_check_mark`) preferred.

### Added — PER-025 (discovery)

- **`chanvoy search <channel> <query>`** — channel-scoped search
  via MM `POST /api/v4/teams/{id}/posts/search`. Channel
  positional + required; cross-channel / team-wide search
  deferred from v1. Inline operator conflicts refused: `in:` vs
  channel arg, `from:` vs `--from` flag, `before:`/`after:` vs
  `--since` flag. Non-conflicting inline operators pass through
  verbatim. Quoted-region-aware: `"in: the brief"` is searchable
  text, not an operator conflict.
- **`chanvoy channels` enriched output** — adds `last_active`
  column to default human render (relative time / `—` for
  missing-activity). New `--sort active` flag sorts within each
  PER-019 team group (preserves grouping; does NOT flatten
  globally). Default `channels --json` adds `last_post_at` as i64
  Unix epoch ms; missing-activity is **required** to be
  `last_post_at: null` (deterministic shape; never absent).
  `--primary-team --json` preserves the legacy single-team JSON
  field set exactly (no `last_post_at` added).

### Added — cross-team `channel create`

- **`chanvoy channel create <name> <display> --team <slug>`** —
  closes the last cross-team admin-verb gap. Default behavior
  (no `--team`) preserved: legacy primary-team landing. The
  `--team` override resolves through the bot's `/users/me/teams`
  membership cache (with one self-healing force-refresh on
  no-match) before posting; refuses with `NotAMemberOfTeam`
  diagnostic if the bot is not a member of the requested team.

### Fixed

- **`chanvoy pinned` endpoint URL.** PER-023 originally shipped
  with `/channels/{id}/pinned_posts`; the canonical Mattermost v4
  endpoint is `/channels/{id}/pinned` (no `_posts` suffix). Live
  request returned 404 with MM's "missing team_id or user_id?"
  diagnostic. Wiremock test mocked the same wrong URL so it
  didn't catch the live divergence (wiremock-vs-real-API drift
  class — flagged for a v0.2.2 structural follow-up). Prodmktg
  dogfooding flagged this 2026-05-07; one-line fix at
  `crates/chanvoy-core/src/lib.rs:2927` plus three wiremock
  matcher updates.

### Documentation

- **Onboarding doc surface expansion (PER-026).** Major rewrite of
  the user-facing docs: new `README.md` reflecting current state
  (M1+M2+M3 shipped, cross-team resolution, sandbox-aware startup,
  full v0.2.1 surface) with a categorized command index pointing at
  the operator guide for reference detail. New
  `docs/getting-started.md` — agent-first 30-minute onboarding with
  an "Agents start here" pointer, a sandboxed-agent decision tree
  covering network-side, socket-write, socket-read, and supervisor-
  escalation paths. New `docs/troubleshooting.md` — symptom-keyed
  recovery for the eight most common failure modes (Daemon
  NotRunning after auto-setup, ActiveProfileNotFound, the
  cross-team resolution refusal trio, identity drift, sandbox
  network prompt, sandbox socket-permission denied, stale socket,
  bare `--limit` rejection) with a forward link to
  `scripts/per015-diag.sh` for unmatched cases. New
  `docs/architecture.md` — runtime model (daemon lifecycle,
  profile→bot binding, attention-state cursor isolation per
  `<team>/<channel>`, cursor-advance taxonomy, peer contract
  pointer) for contributors and bootstrap-curious agents. Includes
  an "If you change X, also change Y" cross-reference table.
- **`REPOSITORY_SAFETY_PROTOCOLS.md` rewritten chanvoy-specific.**
  Previously contained content from a different repository.
  Coverage: never-commit list (tokens, profile state, attention
  snapshots, live MM URLs, diag dumps), permission contract (0700
  dirs, 0600 files), trust-boundary framing (same-Unix-user is
  intentional and not a defect), downstream contract surfaces
  (public types in `chanvoy-core`, daemon RPC names, CLI argument
  shape, profile capability classes, cursor-advance taxonomy,
  bootstrap-state file format), sandbox-permission asks (chanvoy
  doesn't negotiate; supervisor decides), required reviews, and
  security reporting. Public-readable.
- **`docs/operator-guide.md` reconciliation.** Dropped the "(chanvoy
  v0.2.1)" parenthetical labels from four section headers
  (Session-Start Orientation, Conversation Primitives, Discovery,
  Cross-team channel creation) — by the time readers fetch the
  merged docs, v0.2.1 is the released version and the doc-and-code-
  ship-together convention applies.
- **Internal brief-ID leakage scrubbed from user-facing prose.**
  References to internal brief identifiers (PER-008B, PER-019 γ
  hybrid, PER-023/024/025 primitive numbers, etc.) replaced with
  feature-named anchors in `README.md`,
  `docs/operator-guide.md`, and `docs/migration-runbook.md`.
  Incidental brief mentions inside existing sections (e.g.,
  context for an example) acceptable but not the primary anchor
  for any user-visible concept.
- **`AGENTS.md` pointer.** Added a new-to-chanvoy pointer to
  `docs/getting-started.md` at the top of the agent guide.

### Build

- **Cross-platform install convention.** `make install` now picks
  the install location per the convention used by sibling 3leaps
  tools (`sfetch`, `kitfly`): `$HOME/.local/bin/chanvoy` on
  Linux/macOS, `$USERPROFILE\bin\chanvoy.exe` on Windows. `LOCAL_BIN`
  override remains. Previously the Makefile defaulted only to
  `$HOME/.local/bin` and required manual override on Windows; the
  Windows path now resolves automatically. (No effect on existing
  Linux/macOS installs.)

### Notes

To upgrade from v0.2.0: `make install`, then `chanvoy daemon stop`
and `chanvoy auto-setup` to cycle any running daemon onto the new
binary. Profile and attention-state files from v0.2.0 load forward
unchanged. New public chanvoy-core API: `parse_time_window`,
`TimeWindowDefaultUnit`, `TIME_WINDOW_SUFFIX_HELP`,
`PinnedChannelParams`, `AckChannelParams`, `AckResult`, `ReactParams`,
`UnreactParams`, `ReactionResult`, `SearchParams`, `SearchResult`,
`ChanvoyScopes`, `LegacyChannel`, `Channel.to_legacy()`,
`normalize_emoji_name`, `check_search_operator_conflicts`,
`seconds_ago_millis`. `Channel` gains additive `last_post_at:
Option<i64>` field; `PostReceipt` gains additive `parent_id:
Option<String>` field. `CreateChannelParams` gains additive
`team: Option<String>` field. `ReadChannelParams`,
`NotificationsParams`, `WaitChannelParams` gain
seconds-resolution fields preferred over the legacy minutes
fields by the daemon (back-compat for in-flight CLI/daemon
upgrades preserved).

Out-of-scope follow-ups (deferred to v0.2.2 or later):
- Wiremock-vs-real-API URL drift class — release-cycle smoke
  pass against live MM (or recorded fixture) that exercises
  URL-shape contracts end-to-end. Caught the `pinned` endpoint
  bug at v0.2.1 dogfooding; structural fix prevents the next.
- `reqwest` 0.12 → 0.13 bump — drops the unmaintained
  `rustls-pemfile` transitive dep (RUSTSEC-2025-0134, currently
  ignored in `deny.toml`).
- Cross-channel / team-wide search shape (`chanvoy search
  <query> --team <slug>` without channel arg) — deferred from
  PER-025 v1 per cross-reviewer alignment.
- `read --json` reaction metadata + human-mode reaction summary
  on `chanvoy read` — deferred from PER-024 v1.

## [0.2.0] - 2026-05-02

### Release highlights

The local-mode polish push is functionally complete. v0.2.0 bundles
**PER-014** (sandbox-aware daemon startup), **PER-019** (cross-team
channel resolution), and **PER-015 Phase 1** (diagnostic harness for
the daemon-startup / namespace-drift failure family).

Net operator impact:
- `chanvoy auto-setup` now works end-to-end under sandbox restrictions
  (Codex agents, macOS sandboxd, Docker without `--network`, OSS
  sandbox setups). The detached daemon no longer needs network at
  startup — identity validation moved into the parent CLI.
- Channel-name resolution finds channels across every team the bot
  is a member of, not just the profile's primary team. Closes the
  silent-404 cross-team posting gap. Explicit `<team>/<channel>` and
  `--team <slug>` overrides are available; ambiguity refuses with
  clear disambiguation guidance.
- Cursor isolation: same-named channels on different teams maintain
  independent state. Pre-PER-019 records migrate automatically;
  ambiguous historical names quarantine rather than silently bind.
- Diagnostic harness `scripts/per015-diag.sh` ships as a forensic
  tool for any future regression in the daemon-startup family.
- PER-015 itself was investigated and **scope-collapsed to done** —
  no Phase 2 implementation needed; the post-PER-014/PER-019
  baseline resolves the originally-observed failure.

No breaking CLI surface change vs 0.1.x. Profile/state files from
0.1.x load forward via `#[serde(default)]` migration. New public
chanvoy-core API: `ResolvedChannel`, `ResolutionSource`, `TeamInfo`,
`TeamChannels`, `MigrationOutcome`, `QuarantinedCursor`,
`attention_key_for`, `migrate_attention_state`, plus new error
variants `ChannelNotFoundInAnyTeam`, `NotAMemberOfTeam`,
`AmbiguousChannel`, `BootstrapHandoffFailed`.

To upgrade: `make install`, then `chanvoy daemon stop` and
`chanvoy auto-setup` to cycle any running daemon onto the new binary.

### Added

- **PER-015 Phase 1 review fixes (devrev PR #18, 2026-05-01).** Four
  follow-on fixes from devrev's review of the harness:
  - Fresh-spawn mode now re-runs every probe (status / paths / pid /
    binary / ps) after `auto-setup` so the verdict reflects the
    daemon the spawn actually created. Pre-fix, the verdict computed
    from the pre-teardown snapshot — wrong direction for the binding
    diagnostic. Probe block factored into a `run_probes` function
    that takes a section label so the same code drives observe-mode
    and both pre/post-spawn passes in fresh-spawn mode.
  - Process-detail capture now includes `sess` (session id) so
    reviewers can verify the PER-008D `setsid` contract held in the
    observed environment (a daemon whose SESS == PID is its own
    session leader; SESS != PID indicates the new-session step
    didn't take effect).
  - New `--compare A.log B.log` cross-phase mode emits
    `runtime_or_profile_mismatch` (or `same_namespace_across_phases`)
    by diffing the two logs' resolved_profile / runtime_dir /
    socket_path / pid_path fields. Pre-fix, the per-phase verdict
    taxonomy advertised that classification but no code path emitted
    it — operators would have had to eyeball the diff manually.
  - Fresh-spawn teardown logs the target's full identity (profile +
    socket + pid + binary) before stop, scoped strictly to the
    resolved profile.
  - **PR #18 second-pass (devrev re-review):** post-spawn missing pid
    file now classifies as `pid_dead_or_missing_after_spawn`, not
    `insufficient_visibility`. New `FRESH_SPAWN_EXECUTED` flag tracks
    whether the spawn actually ran; verdict treats post-spawn
    `PID_ALIVE != true` as the lifecycle verdict regardless of whether
    the pid file is missing OR the pid is dead. Reason field
    distinguishes the two sub-cases. Pre-fix, missing-pid-file post
    auto-setup fell through to `insufficient_visibility` — exactly
    the failure shape the binding diagnostic needs to surface
    cleanly.
- **PER-015 Phase 1: `scripts/per015-diag.sh` diagnostic harness.** Investigation
  tool for the "auto-setup succeeds but later `chanvoy read` fails with
  `Daemon(NotRunning)`" failure mode. Captures runtime-dir / profile /
  socket / pid-liveness / process-table / binary-identity state at one
  invocation; designed for two-shot use (`phase=A` after auto-setup,
  `phase=B` at the failing call) so namespace drift is diff-able.
  Two modes: `--mode observe` (default; no teardown — safe) and
  `--mode fresh-spawn` (binding-verdict mode: scoped `daemon stop` →
  `auto-setup` → re-probe). Emits a stable `VERDICT=` field per the
  six-state taxonomy entarch + secrev pinned. Output written to
  `~/.cache/chanvoy-per015-diag/<timestamp>/` mode 0700, file mode
  0600. Env captures redact `TOKEN|SECRET|KEY|PASSWORD|AUTH|COOKIE|SESSION`
  patterns to name + length only — no hashes (per secrev: avoid
  reusable fingerprint). Investigation-tool only; no chanvoy CLI
  surface change, no version bump, no rust code touched.

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

[Unreleased]: https://github.com/lanytehq/chanvoy/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/lanytehq/chanvoy/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/lanytehq/chanvoy/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/lanytehq/chanvoy/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/lanytehq/chanvoy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lanytehq/chanvoy/releases/tag/v0.1.0
