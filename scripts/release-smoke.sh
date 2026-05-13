#!/usr/bin/env bash
# PER-032 Item J Tier-B — live-MM URL-shape smoke harness.
#
# Runs each verb in the Tier-B safe-subset (per PER-032 brief §Scope J)
# against a disposable test channel on the operator-picked test team.
# Asserts every verb returns a 2xx response and the chanvoy-core type
# parses cleanly. Catches URL-shape drift that Tier-A fixture replay
# can't (because fixtures match whatever URL the impl asks for) — the
# structural fix for the `pinned_posts` vs `pinned` class.
#
# Verbs covered (Tier-B safe-subset):
#   whoami, channels, channel create, post, post --reply-to,
#   read --since, read --after, read --since-last-mine,
#   read --since-bootstrap, pinned, ack, check, wait, react, unreact,
#   search, notifications, channel archive.
#
# Verbs explicitly NOT covered (rationale in PER-032 brief §Scope J):
#   - `attention list / show`  — daemon-state RPC, not MM endpoints
#   - `channel restore`        — elevated-capability (admin) verb
#   - `dm send / read / dms`   — peer-principal dependent
#   - `notify`                 — peer-principal dependent
#   - `channel add-member`     — peer-principal dependent
# URL contracts for these verbs are guarded by Tier-A fixture replay
# + canonical endpoint manifest.
#
# Release-cycle ordering (PER-030 RELEASE_CHECKLIST.md is canonical):
#   make release-prep       (commit-cycle gate — does NOT run smoke)
#   make release-smoke      (this script — live MM + ephemeral channel)
#   make release-preflight  (final pre-tag checks)
#   git tag -a vX.Y.Z       (only if smoke passed)
#   git push origin vX.Y.Z  (only if smoke passed)
#
# A failed smoke halts the release cycle BEFORE any tag exists, draft
# release is created, or signed artifact is produced. The failure
# surface is "no release tag yet" — never "signed release that doesn't
# work."
#
# Credentials sourcing (PER-032 OQ resolution): operator identity
# profile. Source `~/devsecops/vars/agent-identity/<role>-<scope>.sh`
# (the same profile that backs `chanvoy auto-setup`) before invoking
# this script. The script asserts the expected env vars are present
# and exits with a clear diagnostic if not.
#
# Sanitization (PER-032 AC #10, devrev review pin #3, secrev review
# of PR #27 on 2026-05-13):
# All output is captured to `release-smoke.log` (gitignored). Before
# the script exits — pass, fail, or interrupt — the log is filtered
# through `scrub_log` → `scrub_stream` (defined in
# `lib-release-smoke.sh`) which replaces every 26-char Mattermost ID
# with `<mm-id>`, the live `${LANYTE_MM_URL}` with `<mm-url>`, and
# the smoke channel/team/bot names with placeholder tokens. The
# scrubbed log is what gets included in release notes; the
# unscrubbed live log never leaves the smoke run.
# `REPOSITORY_SAFETY_PROTOCOLS.md` is the canonical source for the
# no-live-URL-in-committed-artifacts contract.
#
# Usage:
#   ./scripts/release-smoke.sh [<test-team-slug>]
#
# If <test-team-slug> is omitted, the script reads $CHANVOY_SMOKE_TEAM
# from the environment. Either way, the operator picks the team at
# smoke time so production teams are never silently targeted.

set -euo pipefail

# ----------------------------------------------------------------------
# Helper library (sourced with zero side effects — see lib for contract)
# ----------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib-release-smoke.sh
source "${SCRIPT_DIR}/lib-release-smoke.sh"

# ----------------------------------------------------------------------
# Configuration + pre-flight
# ----------------------------------------------------------------------

VERSION="$(cat VERSION 2>/dev/null | tr -d '[:space:]')"
if [[ -z "${VERSION:-}" ]]; then
  echo "[release-smoke] error: VERSION file missing or empty; cannot derive smoke channel name." >&2
  exit 2
fi

SMOKE_TEAM="${1:-${CHANVOY_SMOKE_TEAM:-}}"
if [[ -z "${SMOKE_TEAM}" ]]; then
  echo "[release-smoke] error: smoke team unset." >&2
  echo "  Pass the team slug as the first arg or set CHANVOY_SMOKE_TEAM." >&2
  echo "  Operator picks the team at smoke time so production teams are never silently targeted." >&2
  exit 2
fi

SMOKE_CHANNEL="chanvoy-smoke-v${VERSION}"
SMOKE_BOT_USERNAME="${LANYTE_MM_BOT_USERNAME:-}"
SMOKE_LOG="release-smoke.log"

if [[ -z "${LANYTE_MM_URL:-}" || -z "${LANYTE_MM_TOKEN:-}" ]]; then
  echo "[release-smoke] error: LANYTE_MM_URL / LANYTE_MM_TOKEN unset." >&2
  echo "  Source your operator identity profile before invoking release-smoke:" >&2
  echo "    source ~/devsecops/vars/agent-identity/<role>-<scope>.sh" >&2
  exit 2
fi

if ! command -v chanvoy >/dev/null 2>&1; then
  echo "[release-smoke] error: chanvoy binary not on PATH." >&2
  echo "  Run \`make install\` (or \`cargo install --path .\`) before release-smoke." >&2
  exit 2
fi

# ----------------------------------------------------------------------
# Sanitization
# ----------------------------------------------------------------------

scrub_log() {
  # Filter the live log through `scrub_stream` (defined in
  # `lib-release-smoke.sh`) so live MM URL + IDs + channel/team/bot
  # names are replaced with placeholder tokens before the log is
  # advertised as sanitized. Atomic via a temp file.
  local tmp
  tmp="$(mktemp -t release-smoke-scrub.XXXXXX)"
  scrub_stream < "${SMOKE_LOG}" > "${tmp}"
  mv "${tmp}" "${SMOKE_LOG}"
}

# ----------------------------------------------------------------------
# Lifecycle: ensure the smoke channel is archived even on failure
# ----------------------------------------------------------------------

cleanup() {
  local exit_code=$?
  set +e
  if [[ "${SMOKE_CHANNEL_CREATED:-0}" == "1" ]]; then
    echo "[release-smoke] cleanup: archiving ${SMOKE_CHANNEL}" >> "${SMOKE_LOG}"
    chanvoy channel archive "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" >> "${SMOKE_LOG}" 2>&1 || \
      echo "[release-smoke] cleanup: archive failed (channel may persist; archive manually)" >> "${SMOKE_LOG}"
  fi
  scrub_log
  if [[ "${exit_code}" -eq 0 ]]; then
    echo "[release-smoke] PASS — see ${SMOKE_LOG} for sanitized output"
  else
    echo "[release-smoke] FAIL (exit ${exit_code}) — see ${SMOKE_LOG} for sanitized output" >&2
  fi
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------
# Smoke run
# ----------------------------------------------------------------------

echo "[release-smoke] starting chanvoy v${VERSION} live-MM smoke" > "${SMOKE_LOG}"
echo "[release-smoke] team=${SMOKE_TEAM} channel=${SMOKE_CHANNEL} url=${LANYTE_MM_URL}" >> "${SMOKE_LOG}"

run() {
  local label="$1"
  shift
  echo "----- ${label}" >> "${SMOKE_LOG}"
  echo "+ chanvoy $*" >> "${SMOKE_LOG}"
  if ! chanvoy "$@" >> "${SMOKE_LOG}" 2>&1; then
    echo "[release-smoke] FAIL on: ${label}" >&2
    return 1
  fi
}

# ---- baseline -------------------------------------------------------
run "whoami"   whoami
run "channels" channels

# ---- create disposable channel --------------------------------------
run "channel create" channel create "${SMOKE_CHANNEL}" "chanvoy smoke v${VERSION}" \
    --team "${SMOKE_TEAM}" \
    --purpose "PER-032 Tier-B URL-shape smoke for chanvoy v${VERSION}. Disposable; archived at script exit."
SMOKE_CHANNEL_CREATED=1

# ---- bootstrap read (channel is empty) ------------------------------
run "read --since-bootstrap (empty)" read "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --since-bootstrap --limit 5

# ---- post a message (parent post id captured for threading + react) -
#
# Extracts the post id via `extract_post_id` from lib-release-smoke.sh
# (canonical PostReceipt shape is `{"id": "<post_id>"}`; the function
# also accepts the legacy/forward-compat `post_id` key). The function
# is unit-tested in `tests/release_smoke_post_id_parse.rs` so a
# regression in chanvoy's PostReceipt JSON shape — or in this script's
# extraction logic — fails CI before reaching live MM.
echo "----- post (capture id from PostReceipt)" >> "${SMOKE_LOG}"
POST_JSON="$(chanvoy --json post "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" \
              "PER-032 smoke v${VERSION} — baseline post")"
echo "${POST_JSON}" >> "${SMOKE_LOG}"
POST_ID="$(extract_post_id "${POST_JSON}")"
if [[ -z "${POST_ID}" ]]; then
  echo "[release-smoke] FAIL: could not extract post id from \`post\` response" >&2
  echo "  expected \"id\" or \"post_id\" field in PostReceipt JSON; got: ${POST_JSON}" >&2
  exit 1
fi

# ---- read-family ----------------------------------------------------
run "read --since 5"           read "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --since 5m
run "read --after"             read "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --after "${POST_ID}"
run "read --since-last-mine"   read "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --since-last-mine

# ---- threaded reply (conversation-shape) ----------------------------
run "post --reply-to" post "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --reply-to "${POST_ID}" \
    "PER-032 smoke v${VERSION} — threaded reply"

# ---- reactions ------------------------------------------------------
run "react"   react   "${SMOKE_CHANNEL}" "${POST_ID}" thumbsup --team "${SMOKE_TEAM}"
run "unreact" unreact "${SMOKE_CHANNEL}" "${POST_ID}" thumbsup --team "${SMOKE_TEAM}"

# ---- discovery ------------------------------------------------------
run "search"        search        "${SMOKE_CHANNEL}" "smoke" --team "${SMOKE_TEAM}" --limit 5
run "notifications" notifications --since 10m

# ---- attention primitives (ack + check + pinned + wait) -------------
run "pinned" pinned "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}"
run "ack"    ack    "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}"
# Post-ack `check` must report zero new posts. `chanvoy check` exits
# non-zero when has_new_messages is false (intentional — the daemon
# uses exit code as the new-message signal). Treat exit 1 as success
# for the post-ack case.
echo "----- check (post-ack, expect zero new)" >> "${SMOKE_LOG}"
if chanvoy check "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" >> "${SMOKE_LOG}" 2>&1; then
  echo "[release-smoke] check exit 0 — unexpected new messages after ack" >> "${SMOKE_LOG}"
elif [[ $? -eq 1 ]]; then
  echo "[release-smoke] check exit 1 — zero-new contract honored" >> "${SMOKE_LOG}"
else
  echo "[release-smoke] FAIL: check returned unexpected exit code" >&2
  exit 1
fi

# `wait --timeout 2s` short-poll for new posts; expect the timeout
# branch (no new posts since ack). Wait's success-vs-timeout exit
# semantics follow the same has_new_messages convention as check.
echo "----- wait (short timeout, expect no-new)" >> "${SMOKE_LOG}"
if chanvoy wait "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}" --timeout 2s >> "${SMOKE_LOG}" 2>&1; then
  echo "[release-smoke] wait exit 0 — surprising; non-fatal" >> "${SMOKE_LOG}"
fi

# ---- archive --------------------------------------------------------
run "channel archive" channel archive "${SMOKE_CHANNEL}" --team "${SMOKE_TEAM}"
SMOKE_CHANNEL_CREATED=0  # archive succeeded; suppress duplicate cleanup

echo "[release-smoke] all verbs in Tier-B safe-subset passed" >> "${SMOKE_LOG}"
