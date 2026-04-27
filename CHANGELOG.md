# Changelog

All notable changes to chanvoy are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
