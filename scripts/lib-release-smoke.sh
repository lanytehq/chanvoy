#!/usr/bin/env bash
# PER-032 Tier-B helper library — pure-function shell helpers extracted
# so `tests/release_smoke_post_id_parse.rs` can source them in
# isolation. Sourcing this file must have ZERO side effects: no env
# mutation, no pre-flight checks, no I/O. Define helpers only.

# extract_post_id <PostReceipt JSON>
#
# Extracts the post id from a `chanvoy --json post` response.
# Canonical PostReceipt shape (chanvoy-core::PostReceipt) is
# `{"id": "<post_id>" [, "parent_id": "..."]}` — the JSON key is `id`,
# NOT `post_id`. Caught during devrev review of PR #27 on 2026-05-12.
#
# Defensive: tries `id` first (canonical PER-024 shape), falls back to
# `post_id` so a future rename in either direction doesn't silently
# break the smoke harness. Returns empty string on no match.
extract_post_id() {
  local json="$1"
  local id
  id="$(printf '%s' "${json}" | sed -nE 's/.*"id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -n1)"
  if [[ -z "${id}" ]]; then
    id="$(printf '%s' "${json}" | sed -nE 's/.*"post_id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -n1)"
  fi
  printf '%s' "${id}"
}

# scrub_stream
#
# Filters stdin → stdout, replacing live identifiers with placeholder
# tokens so the scrubbed output is safe to include in release notes or
# committed/shared artifacts. PER-032 AC #10 (devrev review pin #3) +
# `REPOSITORY_SAFETY_PROTOCOLS.md` (no live Mattermost server URLs in
# committed artifacts; secrev review of PR #27 on 2026-05-13).
#
# Replacements applied:
# - 26-char lowercase-alphanumeric MM IDs        → <mm-id>
# - `${LANYTE_MM_URL}` (if set)                  → <mm-url>
# - `${SMOKE_CHANNEL}` (if set)                  → <smoke-channel>
# - `${SMOKE_TEAM}`    (if set)                  → <smoke-team>
# - `${SMOKE_BOT_USERNAME}` (if set)             → <smoke-bot>
#
# Optional env vars are skipped via `${VAR:+ -e ...}` parameter
# expansion so an unset variable does not emit a malformed `s||<x>|g`
# regex (which would silently no-op or, worse, match every empty
# string position depending on sed implementation).
#
# Pure-function shape: reads stdin, writes stdout. Easy to test by
# piping a known-shaped log through it and asserting the output.
scrub_stream() {
  sed -E \
      -e 's/[a-z0-9]{26}/<mm-id>/g' \
      ${LANYTE_MM_URL:+ -e "s|${LANYTE_MM_URL}|<mm-url>|g"} \
      ${SMOKE_CHANNEL:+ -e "s|${SMOKE_CHANNEL}|<smoke-channel>|g"} \
      ${SMOKE_TEAM:+ -e "s|${SMOKE_TEAM}|<smoke-team>|g"} \
      ${SMOKE_BOT_USERNAME:+ -e "s|${SMOKE_BOT_USERNAME}|<smoke-bot>|g"}
}
# Derive the disposable Mattermost channel slug from VERSION. Mattermost
# channel names accept lowercase ASCII letters, digits, hyphens, and
# underscores, with a 64-byte maximum. Dots from semantic versions are
# normalized to hyphens; any other unsupported character fails closed.
derive_smoke_channel() {
  local version="${1:-}"
  local run_suffix="${2:-}"
  local version_slug
  local channel

  version_slug="${version//./-}"
  channel="chanvoy-smoke-v${version_slug}-${run_suffix}"

  if [[ -z "${run_suffix}" || ! "${channel}" =~ ^[a-z0-9_-]+$ ]] || (( ${#channel} > 64 )); then
    return 1
  fi

  printf '%s\n' "${channel}"
}

# The archive CLI currently operates on the profile's primary team and has
# no cross-team override. Refuse a smoke team that cleanup cannot archive.
validate_smoke_team() {
  local selected_team="${1:-}"
  local identity_team="${2:-}"

  [[ -n "${selected_team}" && -n "${identity_team}" && "${selected_team}" == "${identity_team}" ]]
}
