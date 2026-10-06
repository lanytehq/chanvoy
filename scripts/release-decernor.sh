#!/usr/bin/env bash
# Source this file, then resolve the Decernor executable once per invocation.
resolve_release_decernor() {
	local mode="${1:-general}" candidate short extended version
	if [[ "$mode" == ceremony ]]; then
		[[ -n "${CHANVOY_DECERNOR_BIN:-}" ]] || {
			echo 'error: CHANVOY_DECERNOR_BIN is required for the release ceremony' >&2
			return 1
		}
		candidate="$CHANVOY_DECERNOR_BIN"
	else
		candidate="${CHANVOY_DECERNOR_BIN:-${DECERNOR_BIN:-${DECERNOR:-}}}"
		if [[ -z "$candidate" ]]; then
			candidate="$(command -v decernor)" || {
				echo 'error: decernor is unavailable' >&2
				return 1
			}
		fi
	fi
	[[ "$candidate" == /* && -f "$candidate" && ! -L "$candidate" && -x "$candidate" ]] || {
		echo 'error: Decernor binary must be an absolute executable regular file' >&2
		return 1
	}
	short="$("$candidate" version 2>/dev/null)" || {
		echo 'error: Decernor version probe failed' >&2
		return 1
	}
	extended="$("$candidate" version -e 2>/dev/null)" || {
		echo 'error: Decernor extended identity probe failed' >&2
		return 1
	}
	[[ "$short" =~ ^decernor\ ([0-9]+\.[0-9]+\.[0-9]+)$ ]] || {
		echo 'error: Decernor identity/version mismatch' >&2
		return 1
	}
	version="${BASH_REMATCH[1]}"
	[[ "$extended" == *$'\n'* && "$extended" == "Version:"* ]] || {
		echo 'error: invalid Decernor extended version' >&2
		return 1
	}
	python3 - "$version" "$extended" <<'PY' || return 1
import re
import sys
version, extended = sys.argv[1:]
lines = extended.splitlines()
if len(lines) != 6 or not re.fullmatch(r'Version:\s+' + re.escape(version), lines[0]) or any(
    not re.fullmatch(label + r':\s+\S.*', line)
    for label, line in zip(('Commit', 'Build Date', 'Go Version', 'Gofulmen', 'Crucible'), lines[1:])
):
    raise SystemExit('error: invalid Decernor extended identity')
if tuple(map(int, version.split('.'))) < (0, 1, 8):
    raise SystemExit('error: Decernor >= 0.1.8 required')
PY
	# shellcheck disable=SC2034 # Consumed by scripts sourcing this helper.
	RELEASE_DECERNOR_BIN="$candidate"
}
