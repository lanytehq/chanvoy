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
COMPARE_LOG_A=""
COMPARE_LOG_B=""

# Patterns whose env-var values get redacted in the captured log.
# Match against the var NAME (case-insensitive) — value is replaced with
# `<redacted len=NN>`. Names themselves are still emitted so reviewers
# can see what was present.
REDACT_PATTERN='TOKEN|SECRET|KEY|PASSWORD|AUTH|COOKIE|SESSION'

# -- arg parsing ------------------------------------------------------------

usage() {
	cat <<EOF
Usage: $0 [--mode observe|fresh-spawn] [--allow-binary-skew] [--profile <name>] [--out-dir <dir>] phase=A|B
       $0 --compare <phaseA.log> <phaseB.log>

  --mode observe       Capture current state without teardown (default; safe).
  --mode fresh-spawn   Stop+respawn the resolved profile's daemon, then capture.
                       Required for the binding PER-015 verdict. Probes are
                       re-run after spawn so the verdict reflects the
                       freshly-spawned daemon, not the torn-down one.
  --allow-binary-skew  In fresh-spawn mode, proceed even when the caller binary
                       and daemon binary differ (otherwise emits the
                       binary_skew_inconclusive verdict). Use only when you
                       have explicitly verified both binaries are at the
                       intended version.
  --profile <name>     Override profile resolution (defaults to chanvoy's
                       resolver chain — same as bare \`chanvoy daemon status\`).
  --out-dir <dir>      Override the per-run output directory (default
                       ~/.cache/chanvoy-per015-diag/<timestamp>/).
  --compare A B        Read two logs (e.g. phase=A and phase=B from the same
                       session), diff their resolved_profile / runtime_dir /
                       socket_path / pid_path fields, and emit a cross-phase
                       verdict. No new log is written; verdict goes to stdout.
  phase=A|B            Required for capture mode. A = post-auto-setup pass;
                       B = at-failing-call pass.

Per-phase verdicts (emitted in capture mode, recorded as VERDICT=<name>):
  same_namespace_pid_alive_socket_reachable
  same_namespace_pid_alive_socket_unreachable
  pid_dead_or_missing_after_spawn
  binary_skew_inconclusive
  insufficient_visibility

Cross-phase verdicts (emitted by --compare):
  runtime_or_profile_mismatch
  same_namespace_across_phases
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
		--compare)
			# devrev PR #18 finding #4: --compare A.log B.log emits
			# the cross-phase `runtime_or_profile_mismatch` verdict
			# without making operators eyeball the diff manually.
			COMPARE_LOG_A="$2"; COMPARE_LOG_B="$3"; shift 3;;
		phase=*)
			PHASE="${1#phase=}"; shift;;
		-h|--help)
			usage; exit 0;;
		*)
			printf 'unknown arg: %s\n' "$1" >&2; usage; exit 2;;
	esac
done

# -- compare mode (no log file written; verdict goes to stdout) -------------

if [[ -n "$COMPARE_LOG_A" || -n "$COMPARE_LOG_B" ]]; then
	if [[ ! -f "$COMPARE_LOG_A" || ! -f "$COMPARE_LOG_B" ]]; then
		printf '--compare requires two readable log paths\n' >&2
		exit 2
	fi
	compare_field() {
		local field="$1"
		# Pull the LAST occurrence of "field=" from each log so we read the
		# post-spawn value when the log contains both pre- and post-spawn
		# probe sections.
		local va vb
		va="$(grep -E "^${field}=" "$COMPARE_LOG_A" | tail -n1 | sed -E "s/^${field}=//")"
		vb="$(grep -E "^${field}=" "$COMPARE_LOG_B" | tail -n1 | sed -E "s/^${field}=//")"
		printf '%s' "$va|$vb"
	}
	declare -a MISMATCHES=()
	for f in resolved_profile computed_runtime_dir computed_socket_path computed_pid_path; do
		IFS='|' read -r va vb <<< "$(compare_field "$f")"
		if [[ "$va" != "$vb" ]]; then
			MISMATCHES+=("${f}: A=${va} B=${vb}")
		fi
	done
	printf 'PER-015 compare\n'
	printf '  log_A: %s\n' "$COMPARE_LOG_A"
	printf '  log_B: %s\n' "$COMPARE_LOG_B"
	if [[ "${#MISMATCHES[@]}" -gt 0 ]]; then
		printf 'VERDICT=runtime_or_profile_mismatch\n'
		printf 'mismatch_count=%d\n' "${#MISMATCHES[@]}"
		for m in "${MISMATCHES[@]}"; do printf '  %s\n' "$m"; done
		exit 0
	else
		printf 'VERDICT=same_namespace_across_phases\n'
		printf 'note: A/B agree on resolved_profile + runtime_dir + socket_path + pid_path; if either log emitted a per-phase failure verdict, treat that as the binding diagnosis (re-run --mode fresh-spawn for the dead-pid case).\n'
		exit 0
	fi
fi

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

# -- section: probe block (function so it can run twice in fresh-spawn) ----

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

# `run_probes <section_label>` captures the full state snapshot to the
# log under the given section label and assigns the script-global
# variables (RESOLVED_PROFILE, PID_VALUE, PID_ALIVE, SOCK_REACHABLE,
# BINARY_SKEW, PS_AVAILABLE, etc.). In observe mode this runs once; in
# fresh-spawn mode this runs twice (`pre_spawn` then `post_spawn`),
# and the verdict logic uses the post-spawn assignments.
#
# devrev PR #18 finding #1: fresh-spawn must base the verdict on the
# daemon created by the spawn, not the daemon that was running before
# teardown. Wrapping the probe in a function makes the second call
# trivial; previously the script captured early and never re-probed
# beyond a small post_spawn_state section that the verdict ignored.
run_probes() {
	local section_label="$1"

	section "${section_label}_profile_resolution"

	local daemon_status_raw daemon_profile profile_json marker_profile
	daemon_status_raw="$("$CHANVOY" "${PROFILE_ARGS[@]}" --json daemon status 2>&1 || true)"
	daemon_profile="$(extract_json_string profile_name "$daemon_status_raw")"
	profile_json="$("$CHANVOY" "${PROFILE_ARGS[@]}" --json profile active 2>&1 || true)"
	emit "profile_active_json<<EOF"
	printf '%s\n' "$profile_json" >> "$LOG"
	emit "EOF"
	marker_profile="$(extract_json_string active_profile "$profile_json")"
	[[ -z "$marker_profile" ]] && marker_profile="$(extract_json_string profile "$profile_json")"
	emit_kv "daemon_status_profile" "${daemon_profile:-unresolved}"
	emit_kv "active_marker_profile" "${marker_profile:-unresolved}"

	RESOLVED_PROFILE="${daemon_profile:-$marker_profile}"
	emit_kv "resolved_profile" "${RESOLVED_PROFILE:-unresolved}"
	emit_kv "resolved_profile_source" "$([[ -n "$daemon_profile" ]] && echo daemon_status || echo active_marker)"
	if [[ -n "$daemon_profile" && -n "$marker_profile" \
			&& "$daemon_profile" != "$marker_profile" ]]; then
		emit_kv "profile_resolution_disagreement" "true"
		emit_kv "profile_resolution_disagreement_note" \
			"daemon_status reports '$daemon_profile'; active marker says '$marker_profile'; using daemon_status for path inspection"
	else
		emit_kv "profile_resolution_disagreement" "false"
	fi

	# Mirrors default_runtime_dir() in chanvoy-core.
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

	section "${section_label}_path_inspection"
	inspect_path "socket" "$SOCK_PATH"
	inspect_path "pid_file" "$PID_PATH"

	section "${section_label}_pid_liveness"
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

	# devrev PR #18 finding #3: capture session id alongside pid/ppid/pgid
	# so reviewers can verify the PER-008D `setsid` contract held in the
	# observed environment (a daemon whose SESS == PID is its own session
	# leader; a daemon where SESS != PID inherited the parent's session
	# and would die on parent exit). `sess` is portable across macOS and
	# modern Linux; older BSDs may not support it, in which case the
	# probe records the failure but doesn't abort.
	PS_AVAILABLE="true"
	if [[ -n "$PID_VALUE" ]]; then
		if ps -p "$PID_VALUE" >/dev/null 2>&1; then
			probe "ps_target" ps -o pid,ppid,pgid,sess,stat,comm -p "$PID_VALUE"
		else
			PS_AVAILABLE="false"
			emit_kv "ps_target_failure" "ps -p $PID_VALUE failed (sandbox restriction or pid gone)"
		fi
	fi
	emit_kv "ps_available" "$PS_AVAILABLE"

	if [[ -n "$PID_VALUE" && -d "/proc/${PID_VALUE}" ]]; then
		probe "proc_status" cat "/proc/${PID_VALUE}/status"
		probe "proc_cmdline" tr '\0' ' ' < "/proc/${PID_VALUE}/cmdline"
	fi

	section "${section_label}_system_ps"
	# pgrep would also work but ps -ef + grep is intentionally portable to
	# environments where pgrep may be sandbox-restricted.
	local ps_ef_output
	# shellcheck disable=SC2009
	ps_ef_output="$(ps -ef 2>&1 | grep -E '\bchanvoy\b' | grep -v "$$\|grep" || true)"
	if [[ -n "$ps_ef_output" ]]; then
		emit "ps_chanvoy<<EOF"
		printf '%s\n' "$ps_ef_output" | head -n 20 >> "$LOG"
		emit "EOF"
	else
		emit_kv "ps_chanvoy" "no_chanvoy_processes_visible"
	fi

	section "${section_label}_daemon_binary"
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

	section "${section_label}_daemon_status_probe"
	emit "daemon_status_out<<EOF"
	printf '%s\n' "$daemon_status_raw" | head -n 40 >> "$LOG"
	emit "EOF"

	SOCK_REACHABLE="unknown"
	if printf '%s' "$daemon_status_raw" | grep -q '"socket_path"'; then
		SOCK_REACHABLE="true"
	elif printf '%s' "$daemon_status_raw" | grep -qi "Daemon(NotRunning"; then
		SOCK_REACHABLE="false"
	fi
	emit_kv "socket_reachable" "$SOCK_REACHABLE"
}

# -- run probes (once for observe; twice for fresh-spawn) ------------------

if [[ "$MODE" == "fresh-spawn" && "$PHASE" == "A" ]]; then
	# devrev PR #18 finding #1: capture pre-spawn state for diagnostic
	# record only, then perform scoped teardown + auto-setup, then
	# RE-RUN every probe so the verdict reflects the daemon the
	# fresh-spawn actually created. The pre-spawn snapshot is preserved
	# in the log under the `pre_spawn_*` section labels for diff value.
	run_probes "pre_spawn"

	section "fresh_spawn_teardown"
	if [[ "$BINARY_SKEW" == "true" && "$ALLOW_BINARY_SKEW" != "true" ]]; then
		emit_kv "fresh_spawn_skipped" "binary_skew_detected_and_no_allow_flag"
		emit_kv "fresh_spawn_executed" "false"
	elif [[ -z "$RESOLVED_PROFILE" ]]; then
		emit_kv "fresh_spawn_skipped" "profile_unresolved"
		emit_kv "fresh_spawn_executed" "false"
	else
		# entarch PR #18 pin: log target metadata BEFORE teardown,
		# scope strictly to the resolved profile — never pkill chanvoy.
		emit_kv "fresh_spawn_target_profile" "$RESOLVED_PROFILE"
		emit_kv "fresh_spawn_target_pid" "${PID_VALUE:-none}"
		emit_kv "fresh_spawn_target_socket" "$SOCK_PATH"
		emit_kv "fresh_spawn_target_binary" "$DAEMON_BIN_PATH"
		probe "stop_daemon" "$CHANVOY" "${PROFILE_ARGS[@]}" daemon stop
		probe "auto_setup" "$CHANVOY" "${PROFILE_ARGS[@]}" auto-setup
		emit_kv "fresh_spawn_executed" "true"

		# devrev PR #18 finding #1: re-run the full probe block so
		# verdict-driving variables (PID_VALUE / PID_ALIVE /
		# SOCK_REACHABLE / BINARY_SKEW / DAEMON_BIN_PATH /
		# RESOLVED_PROFILE) reflect the post-spawn daemon, not the
		# torn-down one. The verdict logic below uses these final
		# assignments.
		run_probes "post_spawn"
	fi
else
	run_probes "snapshot"
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

# The cross-phase `runtime_or_profile_mismatch` verdict is emitted by
# `--compare A.log B.log` (handled at the top of the script before this
# capture block runs). Per-phase verdicts above are still binding for
# the single-log dead-vs-unreachable verdicts.

emit ""
emit "VERDICT=$VERDICT"

# -- echo summary to stderr so the operator sees the verdict immediately ---
{
	printf 'PER-015 phase=%s mode=%s verdict=%s\n' "$PHASE" "$MODE" "$VERDICT"
	printf 'log: %s\n' "$LOG"
	printf 'redacted env capture; safe to paste back to channel after eyeballing\n'
} >&2
