#!/usr/bin/env bash
# Verify exact, duplicate-free checksum manifests and every signed input.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/release-common.sh"
directory="${1:-dist/release}"
release_tag >/dev/null
[[ -f "$directory/checksums.txt" && ! -L "$directory/checksums.txt" ]] &&
	cmp -s "$directory/SHA256SUMS" "$directory/checksums.txt" || {
	echo 'error: checksums.txt must be byte-identical to SHA256SUMS' >&2; exit 1;
}
expected="$(mktemp "${TMPDIR:-/tmp}/chanvoy-checksum-expected.XXXXXX")"
actual="$(mktemp "${TMPDIR:-/tmp}/chanvoy-checksum-actual.XXXXXX")"
trap 'rm -f "$expected" "$actual"' EXIT
release_signable_assets >"$expected"
LC_ALL=C sort "$expected" -o "$expected"
while IFS= read -r name; do
	[[ -f "$directory/$name" && ! -L "$directory/$name" ]] || {
		echo 'error: missing or unsafe signable asset' >&2
		exit 1
	}
done <"$expected"
for manifest in SHA256SUMS SHA512SUMS; do
	[[ -f "$directory/$manifest" && ! -L "$directory/$manifest" ]] || {
		echo 'error: missing checksum manifest' >&2
		exit 1
	}
	algorithm="${manifest#SHA}"
	algorithm="${algorithm%SUMS}"
	awk -v width="$((algorithm / 4))" '
		NF != 2 { exit 1 }
		length($1) != width || $1 !~ /^[0-9a-f]+$/ { exit 1 }
		$2 !~ /^\*?[A-Za-z0-9][A-Za-z0-9._-]*$/ { exit 1 }
		{ name = $2; sub(/^\*/, "", name); print name }
	' "$directory/$manifest" | LC_ALL=C sort >"$actual" || {
		echo 'error: malformed or path-bearing checksum entry' >&2
		exit 1
	}
	if [[ "$(wc -l <"$actual" | tr -d ' ')" != "$(sort -u "$actual" | wc -l | tr -d ' ')" ]]; then
		echo 'error: duplicate checksum entry' >&2
		exit 1
	fi
	cmp -s "$expected" "$actual" || {
		echo 'error: checksum manifest inventory mismatch' >&2
		exit 1
	}
	(cd "$directory" && shasum -a "$algorithm" -c "$manifest")
done
echo '[ok] exact checksum manifests verified'
