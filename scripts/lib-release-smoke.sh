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
