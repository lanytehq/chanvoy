#!/usr/bin/env bash

set -euo pipefail

CHANNEL="${1:-per-007}"
EXPECTED_USERNAME="${EXPECTED_MM_USERNAME:-agent-bravo-devlead}"
PROFILE_ARG=()
if [[ -n "${CHANVOY_PROFILE:-}" ]]; then
	PROFILE_ARG=(--profile "$CHANVOY_PROFILE")
fi

probe_id="per007-smoke-$(uuidgen | tr 'A-Z' 'a-z')"
notify_probe="${probe_id}-notify"
dm_probe="${probe_id}-dm"
bad_team="per007-bad-team-${probe_id}"

printf '== build ==\n'
cargo build >/dev/null

run() {
	target/debug/chanvoy "${PROFILE_ARG[@]}" "$@"
}

assert_contains() {
	local haystack="$1"
	local needle="$2"
	if [[ "$haystack" != *"$needle"* ]]; then
		printf 'assertion failed: expected output to contain: %s\n' "$needle" >&2
		exit 1
	fi
}

assert_regex() {
	local haystack="$1"
	local pattern="$2"
	if ! printf '%s' "$haystack" | grep -Eq "$pattern"; then
		printf 'assertion failed: expected output to match regex: %s\n' "$pattern" >&2
		exit 1
	fi
}

assert_command_success() {
	local output
	output="$(run "$@")"
	printf '%s\n' "$output"
}

assert_command_failure_contains() {
	local expected="$1"
	shift
	set +e
	local output
	output="$("$@" 2>&1)"
	local rc=$?
	set -e
	printf '%s\n' "$output"
	if [[ $rc -eq 0 ]]; then
		printf 'assertion failed: command unexpectedly succeeded\n' >&2
		exit 1
	fi
	assert_contains "$output" "$expected"
}

printf '== daemon start ==\n'
daemon_start_output="$(assert_command_success daemon start)"
printf '%s\n' "$daemon_start_output"
if [[ "$daemon_start_output" != *"daemon already running"* && "$daemon_start_output" != *"daemon listening"* ]]; then
	printf 'assertion failed: unexpected daemon start output\n' >&2
	exit 1
fi

printf '\n== daemon status ==\n'
daemon_status_output="$(assert_command_success daemon status)"
printf '%s\n' "$daemon_status_output"
assert_contains "$daemon_status_output" 'profile: '
assert_contains "$daemon_status_output" 'socket: '
assert_contains "$daemon_status_output" 'mattermost_username: '
assert_contains "$daemon_status_output" "mattermost_username: $EXPECTED_USERNAME"
assert_contains "$daemon_status_output" 'mattermost_ok: true'

printf '\n== bootstrap bad team ==\n'
assert_command_failure_contains \
	'Unable to find the existing team.' \
	env LANYTE_MM_TEAM="$bad_team" CHANVOY_PROFILE= target/debug/chanvoy profile create-from-env --name "$bad_team"

printf '\n== whoami ==\n'
whoami_output="$(assert_command_success whoami)"
printf '%s\n' "$whoami_output"
assert_contains "$whoami_output" '"username"'
assert_contains "$whoami_output" '"id"'
assert_contains "$whoami_output" '"is_bot"'
assert_contains "$whoami_output" '"email"'

printf '\n== channels ==\n'
channels_output="$(assert_command_success channels)"
printf '%s\n' "$channels_output"
assert_contains "$channels_output" "$CHANNEL"

printf '\n== post ==\n'
post_output="$(assert_command_success post "$CHANNEL" "$probe_id")"
printf '%s\n' "$post_output"
assert_regex "$post_output" '^posted: [a-z0-9]+$'

printf '\n== read ==\n'
read_output="$(assert_command_success read "$CHANNEL" --since 10)"
printf '%s\n' "$read_output"
assert_contains "$read_output" "$probe_id"
assert_regex "$read_output" '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[[^]]+\]'
assert_contains "$read_output" '---'

printf '\n== notify ==\n'
notify_output="$(assert_command_success notify agent-bravo-devlead "$notify_probe")"
printf '%s\n' "$notify_output"
assert_regex "$notify_output" '^notified @agent-bravo-devlead: [a-z0-9]+$'

printf '\n== notifications ==\n'
notifications_output="$(assert_command_success notifications --since 1440)"
printf '%s\n' "$notifications_output"
assert_contains "$notifications_output" "$notify_probe"
assert_contains "$notifications_output" '**[notify]**'
assert_contains "$notifications_output" '---'

printf '\n== wait timeout ==\n'
set +e
wait_output="$(run wait "$CHANNEL" --timeout 0 2>&1)"
wait_rc=$?
set -e
printf '%s\n' "$wait_output"
if [[ $wait_rc -eq 0 ]]; then
	printf 'unexpected wait success\n' >&2
	exit 1
fi
assert_contains "$wait_output" "waiting for new message in #$CHANNEL (timeout: 0m)..."
assert_contains "$wait_output" "timeout: no new messages in #$CHANNEL after 0 minutes"

printf '\n== dm send ==\n'
dm_send_output="$(assert_command_success dm agent-bravo-devlead "$dm_probe")"
printf '%s\n' "$dm_send_output"
assert_regex "$dm_send_output" '^dm sent: [a-z0-9]+ \(to @agent-bravo-devlead\)$'

printf '\n== dms ==\n'
dms_output="$(assert_command_success dms)"
printf '%s\n' "$dms_output"
assert_contains "$dms_output" "Use 'chanvoy dm read <username>' to read a conversation."

printf '\n== dm read ==\n'
dm_read_output="$(assert_command_success dm read agent-bravo-devlead --since 10)"
printf '%s\n' "$dm_read_output"
assert_contains "$dm_read_output" "$dm_probe"
assert_contains "$dm_read_output" '---'

printf '\n== daemon stop/start ==\n'
daemon_stop_output="$(assert_command_success daemon stop)"
printf '%s\n' "$daemon_stop_output"
assert_contains "$daemon_stop_output" 'stopped daemon for profile '

daemon_restart_output="$(assert_command_success daemon start)"
printf '%s\n' "$daemon_restart_output"
assert_contains "$daemon_restart_output" 'daemon listening for profile '

daemon_status_after_restart="$(assert_command_success daemon status)"
printf '%s\n' "$daemon_status_after_restart"
assert_contains "$daemon_status_after_restart" "mattermost_username: $EXPECTED_USERNAME"
assert_contains "$daemon_status_after_restart" 'mattermost_ok: true'

printf '\nsmoke probe complete: %s\n' "$probe_id"
