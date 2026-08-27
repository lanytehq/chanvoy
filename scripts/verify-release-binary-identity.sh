#!/usr/bin/env bash
# Execute a downloaded host binary and verify its embedded release identity.

set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: verify-release-binary-identity.sh <release-tag> <release-dir> [test-binary]

Production callers omit test-binary so the canonical host artifact is selected
from uname. The explicit path exists only for isolated verifier tests.
EOF
}

fail() {
	echo "error: $*" >&2
	exit 1
}

[ "$#" -ge 2 ] && [ "$#" -le 3 ] || {
	usage
	exit 2
}

tag="$1"
release_dir="$2"
binary="${3:-}"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" ||
	fail "not inside a git repository"
cd "$repo_root"

[ -f VERSION ] || fail "VERSION file not found"
version="$(tr -d '[:space:]' <VERSION)"
[ "$tag" = "v${version}" ] ||
	fail "release tag ${tag} does not match VERSION ${version}"

tagged_commit="$(git rev-parse "${tag}^{}" 2>/dev/null)" ||
	fail "could not resolve annotated tag ${tag}"
expected_commit="$(printf '%s' "$tagged_commit" | cut -c1-7)"

if [ -z "$binary" ]; then
	case "$(uname -s):$(uname -m)" in
	Darwin:arm64) platform="macos-aarch64" ;;
	Linux:x86_64) platform="linux-x86_64" ;;
	Linux:aarch64 | Linux:arm64) platform="linux-aarch64" ;;
	*) fail "no release artifact mapping for host $(uname -s)/$(uname -m)" ;;
	esac
	binary="${release_dir}/chanvoy-${tag}-${platform}"
fi

[ -f "$binary" ] || fail "downloaded host binary not found: ${binary}"
chmod u+x "$binary"

set +e
identity="$("$binary" version --extended 2>&1)"
identity_status=$?
set -e
[ "$identity_status" -eq 0 ] || {
	printf '%s\n' "$identity" >&2
	fail "downloaded host binary identity command failed"
}

reported_version="$(
	printf '%s\n' "$identity" |
		awk '$1 == "chanvoy" && NF == 2 { count += 1; value = $2 } END { if (count == 1) print value }'
)"
reported_commit="$(
	printf '%s\n' "$identity" |
		awk '$1 == "Commit:" && NF == 2 { count += 1; value = $2 } END { if (count == 1) print value }'
)"
reported_dirty="$(
	printf '%s\n' "$identity" |
		awk '$1 == "Dirty:" && NF == 2 { count += 1; value = $2 } END { if (count == 1) print value }'
)"

[ "$reported_version" = "$version" ] ||
	fail "downloaded binary version ${reported_version:-missing} does not match ${version}"
[ "$reported_commit" = "$expected_commit" ] ||
	fail "downloaded binary commit ${reported_commit:-missing} does not match tagged commit ${expected_commit}"
[ "$reported_dirty" = "false" ] ||
	fail "downloaded binary must report Dirty: false (got ${reported_dirty:-missing})"

echo "[ok] downloaded host binary identity matches ${tag} at ${expected_commit} (Dirty: false)"
