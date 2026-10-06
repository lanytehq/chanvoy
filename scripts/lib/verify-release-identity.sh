#!/usr/bin/env bash
# Internal identity parser. Caller must authenticate the binary before entry.

set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: verify-release-identity.sh <authenticated-binary> <version> <commit>
EOF
}

fail() {
	echo "error: $*" >&2
	exit 1
}

[ "$#" -eq 3 ] || {
	usage
	exit 2
}

binary="$1"
version="$2"
tag="v$version"
tagged_commit="$3"
[[ "$tagged_commit" =~ ^[0-9a-f]{40}$ ]] || fail 'exact tagged commit required'
expected_commit="$(printf '%s' "$tagged_commit" | cut -c1-7)"

[ -f "$binary" ] && [ ! -L "$binary" ] || fail "downloaded host binary not found or unsafe: ${binary}"
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
