#!/usr/bin/env bash
#
# scripts/per015-diag.sh — PER-015 Phase 1 namespace + lifecycle diagnostic harness.
#
# Captures the runtime/profile/socket/pid/process state of a chanvoy
# daemon at one invocation, optionally tearing the daemon down first
# (fresh-spawn mode), and emits a single classified verdict at the
# bottom of the log. Two such logs from the same operator session
# (`phase=A` after auto-setup, `phase=B` at the failing call) are
# diff-able to determine whether `Daemon(NotRunning)` is caused by
# runtime/profile namespace drift or by actual process death.
#
# Usage:
#   ./scripts/per015-diag.sh phase=A
#   ./scripts/per015-diag.sh phase=B
#   ./scripts/per015-diag.sh --mode fresh-spawn phase=A
#   ./scripts/per015-diag.sh --allow-binary-skew --mode fresh-spawn phase=A
#
# The default mode is `observe` (no teardown, captures current state).
# Use `--mode fresh-spawn` for the binding PER-015 verdict path: the
# script logs the target's existing identity, then runs
# `chanvoy daemon stop` against just that profile (never wildcard kill),
# then `chanvoy auto-setup`, then captures the post-spawn state.
#
# Output: ~/.cache/chanvoy-per015-diag/<timestamp>/per015-diag-<phase>.log
# Directory mode 0700, file mode 0600. Safe to paste into #per-015 once
# eyeballed. Env vars matching TOKEN|SECRET|KEY|PASSWORD|AUTH|COOKIE|SESSION
# are redacted to name + length only — no hashes (avoid reusable
# fingerprint per secrev).

set -euo pipefail

# -- defaults ---------------------------------------------------------------

MODE="observe"
PHASE=""
PROFILE_OVERRIDE=""
ALLOW_BINARY_SKEW="false"
OUT_BASE="${HOME}/.cache/chanvoy-per015-diag"

# Patterns whose env-var values get redacted in the captured log.
# Match against the var NAME (case-insensitive) — value is replaced with
# `<redacted len=NN>`. Names themselves are still emitted so reviewers
# can see what was present.
REDACT_PATTERN='TOKEN|SECRET|KEY|PASSWORD|AUTH|COOKIE|SESSION'

# -- arg parsing ------------------------------------------------------------

usage() {
	cat <<EOF
Usage: $0 [--mode observe|fresh-spawn] [--allow-binary-skew] [--profile <name>] [--out-dir <dir>] phase=A|B

  --mode observe       Capture current state without teardown (default; safe).
  --mode fresh-spawn   Stop+respawn the resolved profile's daemon, then capture.
                       Required for the binding PER-015 verdict.
  --allow-binary-skew  In fresh-spawn mode, proceed even when the caller binary
                       and daemon binary differ (otherwise emits the
                       binary_skew_inconclusive verdict). Use only when you
                       have explicitly verified both binaries are at the
                       intended version.
  --profile <name>     Override profile resolution (defaults to chanvoy's
                       resolver chain — same as bare \`chanvoy daemon status\`).
  --out-dir <dir>      Override the per-run output directory (default
                       ~/.cache/chanvoy-per015-diag/<timestamp>/).
  phase=A|B            Required. A = post-auto-setup pass; B = at-failing-call
                       pass. Both phases write a log; diff them.

Verdicts emitted:
  same_namespace_pid_alive_socket_reachable
  same_namespace_pid_alive_socket_unreachable
  runtime_or_profile_mismatch
  pid_dead_or_missing_after_spawn
  binary_skew_inconclusive
  insufficient_visibility
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--mode)
			MODE="$2"; shift 2;;
		--mode=*)
			MODE="${1#--mode=}"; shift;;
		--allow-binary-skew)
			ALLOW_BINARY_SKEW="true"; shift;;
		--profile)
			PROFILE_OVERRIDE="$2"; shift 2;;
		--profile=*)
			PROFILE_OVERRIDE="${1#--profile=}"; shift;;
		--out-dir)
			OUT_BASE="$2"; shift 2;;
		--out-dir=*)
			OUT_BASE="${1#--out-dir=}"; shift;;
		phase=*)
			PHASE="${1#phase=}"; shift;;
		-h|--help)
			usage; exit 0;;
		*)
			printf 'unknown arg: %s\n' "$1" >&2; usage; exit 2;;
	esac
done

if [[ "$PHASE" != "A" && "$PHASE" != "B" ]]; then
	printf 'phase=A or phase=B is required\n' >&2
	usage
	exit 2
fi
if [[ "$MODE" != "observe" && "$MODE" != "fresh-spawn" ]]; then
	printf 'unknown mode: %s\n' "$MODE" >&2
	exit 2
fi

# -- output dir + log file (secure perms) -----------------------------------

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_BASE}/${TS}"
mkdir -p "$OUT_DIR"
chmod 700 "$OUT_DIR" || true
LOG="${OUT_DIR}/per015-diag-${PHASE}.log"
: > "$LOG"
chmod 600 "$LOG" || true

emit() { printf '%s\n' "$*" >> "$LOG"; }
emit_kv() { emit "$1=$2"; }
section() { emit ""; emit "## $1"; }

# -- helpers ----------------------------------------------------------------

# Try a command, capture exit + first-line of output. Avoids set -e
# aborting on probes whose failure is itself the data.
probe() {
	local _label="$1"; shift
	local _out _rc
	if _out="$( "$@" 2>&1 )"; then
		_rc=0
	else
		_rc=$?
	fi
	emit_kv "${_label}_rc" "$_rc"
	# Truncate very-long output (e.g. ps -ef) to keep logs paste-friendly.
	emit "${_label}_out<<EOF"
	printf '%s\n' "$_out" | head -n 80 >> "$LOG"
	emit "EOF"
}

# Compute SHA-256 of a binary if shasum/sha256sum is available.
sha256_of() {
	local path="$1"
	if [[ ! -f "$path" ]]; then printf 'absent\n'; return; fi
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$path" 2>/dev/null | awk '{print $1}'
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$path" 2>/dev/null | awk '{print $1}'
	else
		printf 'no_sha_tool\n'
	fi
}

# mtime of a file as ISO8601 (best-effort across macOS/Linux).
mtime_of() {
	local path="$1"
	if [[ ! -e "$path" ]]; then printf 'absent\n'; return; fi
	if stat -c '%y' "$path" >/dev/null 2>&1; then
		stat -c '%y' "$path" 2>/dev/null | awk '{print $1"T"$2}'
	elif stat -f '%Sm' "$path" >/dev/null 2>&1; then
		stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%S' "$path" 2>/dev/null
	else
		printf 'no_stat_tool\n'
	fi
}

# Find the binary that produced /proc/self/exe (Linux) or via lsof on macOS.
find_caller_binary() {
	if [[ -L /proc/self/exe ]]; then
		readlink -f /proc/self/exe 2>/dev/null || true
	elif command -v lsof >/dev/null 2>&1; then
		lsof -p "$$" 2>/dev/null | awk '$4=="txt" {print $NF; exit}'
	fi
}

# -- header -----------------------------------------------------------------

emit "# PER-015 Phase 1 diagnostic — $(basename "$0")"
emit_kv "phase" "$PHASE"
emit_kv "mode" "$MODE"
emit_kv "allow_binary_skew" "$ALLOW_BINARY_SKEW"
emit_kv "captured_at_utc" "$TS"
emit_kv "log_path" "$LOG"
emit_kv "host" "$(hostname 2>/dev/null || echo unknown)"
emit_kv "uname" "$(uname -a 2>/dev/null || echo unknown)"
emit_kv "pwd" "$PWD"

# -- section: env (redacted) ------------------------------------------------

section "env_relevant"
# Capture CHANVOY_*, XDG_*, LANYTE_* — names + values, with redaction by
# pattern. Other env vars are not captured (privacy + signal-to-noise).
while IFS='=' read -r name value; do
	[[ -z "$name" ]] && continue
	if printf '%s' "$name" | grep -Eqi "$REDACT_PATTERN"; then
		emit_kv "$name" "<redacted len=${#value}>"
	else
		emit_kv "$name" "$value"
	fi
done < <(env | grep -E '^(CHANVOY_|XDG_|LANYTE_|HOME|TMPDIR|TMP|USER|SHELL)=' || true)

# -- section: caller binary identity ----------------------------------------

section "caller_binary"
CALLER_BIN="$(find_caller_binary || echo unknown)"
emit_kv "caller_self" "$CALLER_BIN"
WHICH_CHANVOY="$(command -v chanvoy 2>/dev/null || echo not_on_path)"
emit_kv "which_chanvoy" "$WHICH_CHANVOY"
if [[ -x "$WHICH_CHANVOY" ]]; then
	emit_kv "which_chanvoy_version" "$("$WHICH_CHANVOY" --version 2>/dev/null || echo unknown)"
	emit_kv "which_chanvoy_mtime" "$(mtime_of "$WHICH_CHANVOY")"
	emit_kv "which_chanvoy_sha256" "$(sha256_of "$WHICH_CHANVOY")"
fi

# Pick the chanvoy we'll use for status/profile probes — prefer the
# operator's PATH binary (consistent with how they actually use chanvoy).
CHANVOY="$WHICH_CHANVOY"
if [[ "$CHANVOY" == "not_on_path" || ! -x "$CHANVOY" ]]; then
	emit_kv "chanvoy_resolution" "missing"
	emit ""
	emit "VERDICT=insufficient_visibility"
	emit_kv "verdict_reason" "chanvoy not on PATH; cannot probe daemon state"
	exit 0
fi

# -- section: profile resolution --------------------------------------------

section "profile_resolution"
PROFILE_ARGS=()
[[ -n "$PROFILE_OVERRIDE" ]] && PROFILE_ARGS+=(--profile "$PROFILE_OVERRIDE")

# Helper: extract a top-level string field from JSON via python (best)
# or a regex fallback. Returns empty string on miss.
extract_json_string() {
	local field="$1" json="$2"
	if command -v python3 >/dev/null 2>&1; then
		printf '%s' "$json" | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    print(d.get('$field') or '')
except Exception:
    pass" 2>/dev/null || true
		return
	fi
	printf '%s' "$json" | grep -oE "\"${field}\"[ ]*:[ ]*\"[^\"]+\"" | head -n1 | sed -E 's/.*"([^"]+)"$/\1/' || true
}

# Two independent sources of profile name, captured separately:
#
#   1. `daemon status` → profile_name field. This is the profile the
#      *running daemon* believes it's serving. Authoritative for the
#      path-inspection sections below — the socket/pid the daemon
#      actually uses are derived from this.
#
#   2. `profile active` → the persisted active-profile marker. May
#      disagree with #1 when env-derived resolution selects a different
#      profile than the marker (PER-012 resolver chain). Surfacing the
#      disagreement is itself a valuable data point for PER-015.
#
# Run the network-aware status probe first so the path-inspection that
# follows uses the daemon's actual profile.

DAEMON_STATUS_EARLY="$("$CHANVOY" "${PROFILE_ARGS[@]}" --json daemon status 2>&1 || true)"
DAEMON_PROFILE="$(extract_json_string profile_name "$DAEMON_STATUS_EARLY")"

PROFILE_JSON="$("$CHANVOY" "${PROFILE_ARGS[@]}" --json profile active 2>&1 || true)"
emit "profile_active_json<<EOF"
printf '%s\n' "$PROFILE_JSON" >> "$LOG"
emit "EOF"
ACTIVE_MARKER_PROFILE="$(extract_json_string active_profile "$PROFILE_JSON")"
[[ -z "$ACTIVE_MARKER_PROFILE" ]] && ACTIVE_MARKER_PROFILE="$(extract_json_string profile "$PROFILE_JSON")"

emit_kv "daemon_status_profile" "${DAEMON_PROFILE:-unresolved}"
emit_kv "active_marker_profile" "${ACTIVE_MARKER_PROFILE:-unresolved}"

# Choose the daemon-reported profile as the authoritative source for
# path computation. If `daemon status` failed (no daemon running, or
# unreachable), fall back to the marker; the verdict logic will catch
# the resulting socket/pid-absent state.
RESOLVED_PROFILE="${DAEMON_PROFILE:-$ACTIVE_MARKER_PROFILE}"
emit_kv "resolved_profile" "${RESOLVED_PROFILE:-unresolved}"
emit_kv "resolved_profile_source" "$([[ -n "$DAEMON_PROFILE" ]] && echo daemon_status || echo active_marker)"

# Flag profile-name disagreement explicitly. Operators should resolve
# this before treating the verdict as binding.
if [[ -n "$DAEMON_PROFILE" && -n "$ACTIVE_MARKER_PROFILE" \
		&& "$DAEMON_PROFILE" != "$ACTIVE_MARKER_PROFILE" ]]; then
	emit_kv "profile_resolution_disagreement" "true"
	emit_kv "profile_resolution_disagreement_note" \
		"daemon_status reports '$DAEMON_PROFILE'; active marker says '$ACTIVE_MARKER_PROFILE'; using daemon_status for path inspection"
else
	emit_kv "profile_resolution_disagreement" "false"
fi

# Compute the runtime-dir chanvoy would use. Mirrors
# default_runtime_dir() in chanvoy-core.
if [[ -n "${CHANVOY_RUNTIME_DIR:-}" ]]; then
	RUNTIME_DIR="$CHANVOY_RUNTIME_DIR"
elif [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
	RUNTIME_DIR="${XDG_RUNTIME_DIR}/chanvoy"
elif [[ -n "${TMPDIR:-}" ]]; then
	RUNTIME_DIR="${TMPDIR%/}/chanvoy"
else
	RUNTIME_DIR="/tmp/chanvoy"
fi
emit_kv "computed_runtime_dir" "$RUNTIME_DIR"

if [[ -n "$RESOLVED_PROFILE" ]]; then
	SOCK_PATH="${RUNTIME_DIR}/${RESOLVED_PROFILE}.sock"
	PID_PATH="${RUNTIME_DIR}/${RESOLVED_PROFILE}.pid"
else
	SOCK_PATH=""
	PID_PATH=""
fi
emit_kv "computed_socket_path" "${SOCK_PATH:-unknown}"
emit_kv "computed_pid_path" "${PID_PATH:-unknown}"

# -- section: path inspection -----------------------------------------------

section "path_inspection"
inspect_path() {
	local label="$1" path="$2"
	if [[ -z "$path" ]]; then
		emit_kv "${label}_present" "unknown_no_path"
		return
	fi
	if [[ -e "$path" ]]; then
		emit_kv "${label}_present" "true"
		emit_kv "${label}_mtime" "$(mtime_of "$path")"
		probe "${label}_lsla" ls -la "$path"
	else
		emit_kv "${label}_present" "false"
	fi
}
inspect_path "socket" "$SOCK_PATH"
inspect_path "pid_file" "$PID_PATH"

# -- section: pid liveness + process detail ---------------------------------

section "pid_liveness"
PID_VALUE=""
if [[ -n "$PID_PATH" && -f "$PID_PATH" ]]; then
	PID_VALUE="$(cat "$PID_PATH" 2>/dev/null || true)"
fi
emit_kv "pid_value" "${PID_VALUE:-unknown}"

PID_ALIVE="unknown"
if [[ -n "$PID_VALUE" ]]; then
	if kill -0 "$PID_VALUE" 2>/dev/null; then
		PID_ALIVE="true"
	else
		PID_ALIVE="false"
	fi
fi
emit_kv "pid_alive" "$PID_ALIVE"

# Process detail. The `ps` form is portable across macOS + Linux.
PS_AVAILABLE="true"
if [[ -n "$PID_VALUE" ]]; then
	if ps -p "$PID_VALUE" >/dev/null 2>&1; then
		probe "ps_target" ps -o pid,ppid,pgid,stat,comm -p "$PID_VALUE"
	else
		PS_AVAILABLE="false"
		emit_kv "ps_target_failure" "ps -p $PID_VALUE failed (sandbox restriction or pid gone)"
	fi
fi
emit_kv "ps_available" "$PS_AVAILABLE"

# Linux-only: /proc detail.
if [[ -n "$PID_VALUE" && -d "/proc/${PID_VALUE}" ]]; then
	probe "proc_status" cat "/proc/${PID_VALUE}/status"
	probe "proc_cmdline" tr '\0' ' ' < "/proc/${PID_VALUE}/cmdline"
fi

# System-wide ps (best-effort; some sandboxes deny). Used to detect
# whether ANY chanvoy daemon process exists, even outside our resolved
# profile's pid file. Fold into binary-skew detection downstream.
section "system_ps"
PS_EF_OUTPUT="$(ps -ef 2>&1 | grep -E '\bchanvoy\b' | grep -v "$$\|grep" || true)"
if [[ -n "$PS_EF_OUTPUT" ]]; then
	emit "ps_chanvoy<<EOF"
	printf '%s\n' "$PS_EF_OUTPUT" | head -n 20 >> "$LOG"
	emit "EOF"
else
	emit_kv "ps_chanvoy" "no_chanvoy_processes_visible"
fi

# -- section: daemon binary identity (from ps if pid alive) -----------------

section "daemon_binary"
DAEMON_BIN_PATH=""
if [[ "$PID_ALIVE" == "true" ]]; then
	DAEMON_BIN_PATH="$(ps -o args= -p "$PID_VALUE" 2>/dev/null | awk '{print $1}' || true)"
fi
emit_kv "daemon_binary_path" "${DAEMON_BIN_PATH:-unknown}"
if [[ -x "$DAEMON_BIN_PATH" ]]; then
	emit_kv "daemon_binary_version" "$("$DAEMON_BIN_PATH" --version 2>/dev/null || echo unknown)"
	emit_kv "daemon_binary_mtime" "$(mtime_of "$DAEMON_BIN_PATH")"
	emit_kv "daemon_binary_sha256" "$(sha256_of "$DAEMON_BIN_PATH")"
fi

BINARY_SKEW="false"
if [[ -n "$DAEMON_BIN_PATH" && -x "$DAEMON_BIN_PATH" && -x "$WHICH_CHANVOY" ]]; then
	if [[ "$(sha256_of "$DAEMON_BIN_PATH")" != "$(sha256_of "$WHICH_CHANVOY")" ]]; then
		BINARY_SKEW="true"
	fi
fi
emit_kv "binary_skew" "$BINARY_SKEW"

# -- section: daemon status (full output for review) -----------------------

# The status JSON was captured early (above) so its profile_name could
# drive path inspection. Re-emit the full payload here for the
# operator-readable section + compute the socket-reachable signal.
section "daemon_status_probe"
emit "daemon_status_out<<EOF"
printf '%s\n' "$DAEMON_STATUS_EARLY" | head -n 40 >> "$LOG"
emit "EOF"

SOCK_REACHABLE="unknown"
if printf '%s' "$DAEMON_STATUS_EARLY" | grep -q '"socket_path"'; then
	SOCK_REACHABLE="true"
elif printf '%s' "$DAEMON_STATUS_EARLY" | grep -qi "Daemon(NotRunning"; then
	SOCK_REACHABLE="false"
fi
emit_kv "socket_reachable" "$SOCK_REACHABLE"

# -- section: optional fresh-spawn teardown + respawn ----------------------

if [[ "$MODE" == "fresh-spawn" && "$PHASE" == "A" ]]; then
	section "fresh_spawn_teardown"
	# Pre-teardown identity already captured above. Scope strictly to the
	# resolved profile — never wildcard kill.
	if [[ "$BINARY_SKEW" == "true" && "$ALLOW_BINARY_SKEW" != "true" ]]; then
		emit_kv "fresh_spawn_skipped" "binary_skew_detected_and_no_allow_flag"
	elif [[ -z "$RESOLVED_PROFILE" ]]; then
		emit_kv "fresh_spawn_skipped" "profile_unresolved"
	else
		emit_kv "fresh_spawn_target_profile" "$RESOLVED_PROFILE"
		emit_kv "fresh_spawn_target_pid" "${PID_VALUE:-none}"
		probe "stop_daemon" "$CHANVOY" "${PROFILE_ARGS[@]}" daemon stop
		probe "auto_setup" "$CHANVOY" "${PROFILE_ARGS[@]}" auto-setup
		# Re-probe pid + socket after spawn.
		section "post_spawn_state"
		if [[ -f "$PID_PATH" ]]; then
			NEW_PID="$(cat "$PID_PATH" 2>/dev/null || true)"
			emit_kv "post_spawn_pid" "${NEW_PID:-unknown}"
			if [[ -n "$NEW_PID" ]] && kill -0 "$NEW_PID" 2>/dev/null; then
				emit_kv "post_spawn_pid_alive" "true"
				PID_VALUE="$NEW_PID"
				PID_ALIVE="true"
			else
				emit_kv "post_spawn_pid_alive" "false"
			fi
		else
			emit_kv "post_spawn_pid_file_present" "false"
		fi
	fi
fi

# -- verdict ----------------------------------------------------------------

emit ""
emit "## verdict"

VERDICT=""

if [[ "$BINARY_SKEW" == "true" && "$ALLOW_BINARY_SKEW" != "true" ]]; then
	VERDICT="binary_skew_inconclusive"
	emit_kv "verdict_reason" "caller and daemon binaries differ; rerun with matching binaries or --allow-binary-skew before driving Phase 2 design"
elif [[ -z "$RESOLVED_PROFILE" ]]; then
	VERDICT="insufficient_visibility"
	emit_kv "verdict_reason" "profile resolution failed; cannot compute socket/pid paths"
elif [[ "$PID_ALIVE" == "true" && "$SOCK_REACHABLE" == "true" ]]; then
	VERDICT="same_namespace_pid_alive_socket_reachable"
	emit_kv "verdict_reason" "daemon process alive and socket reachable; PER-015 not reproducing in this run"
elif [[ "$PID_ALIVE" == "true" && "$SOCK_REACHABLE" == "false" ]]; then
	VERDICT="same_namespace_pid_alive_socket_unreachable"
	emit_kv "verdict_reason" "daemon process alive but UDS unreachable; investigate socket access/permissions, not lifetime"
elif [[ "$PID_ALIVE" == "false" ]]; then
	VERDICT="pid_dead_or_missing_after_spawn"
	emit_kv "verdict_reason" "pid file present but process not alive (or pid file missing); actual lifecycle/detach failure candidate"
elif [[ "$PS_AVAILABLE" == "false" ]]; then
	VERDICT="insufficient_visibility"
	emit_kv "verdict_reason" "ps blocked by sandbox; cannot determine pid liveness"
else
	VERDICT="insufficient_visibility"
	emit_kv "verdict_reason" "diagnostic signals incomplete; review the log sections above"
fi

# Cross-phase verdicts (runtime_or_profile_mismatch) require diffing two
# logs. The harness emits per-phase verdicts here; the operator (or a
# follow-up wrapper) compares phase=A vs phase=B logs to detect the
# namespace-drift case explicitly:
#   diff -u <(grep -E '^(resolved_profile|computed_runtime_dir|computed_socket_path|computed_pid_path)=' phaseA.log) ...
# A non-empty diff on those four fields IS the runtime_or_profile_mismatch
# verdict. This script doesn't emit that one because it's two-log-scoped
# by definition.

emit ""
emit "VERDICT=$VERDICT"

# -- echo summary to stderr so the operator sees the verdict immediately ---
{
	printf 'PER-015 phase=%s mode=%s verdict=%s\n' "$PHASE" "$MODE" "$VERDICT"
	printf 'log: %s\n' "$LOG"
	printf 'redacted env capture; safe to paste back to channel after eyeballing\n'
} >&2
