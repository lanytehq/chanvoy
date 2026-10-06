#!/usr/bin/env bash
# Compare signable metadata to inert objects in the receipt-bound tagged commit.
# This is the later-main adaptation of the tagged-notes/anchor staging contract.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
require_release_guard "$directory" >/dev/null
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-staged-data.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
compare() {
	local source="$1" name="$2" mode
	mode="$(git -C "$root" ls-tree "$commit" -- "$source" | awk '{print $1}')"
	[[ "$mode" == 100644 || "$mode" == 100755 ]] || {
		echo 'error: regular tagged release metadata required' >&2
		exit 1
	}
	[[ -f "$directory/$name" && ! -L "$directory/$name" ]] || {
		echo 'error: regular staged release metadata required' >&2
		exit 1
	}
	git -C "$root" cat-file blob "$commit:$source" >"$scratch/data"
	cmp -s "$scratch/data" "$directory/$name" || {
		echo 'error: staged metadata differs from verified tagged commit' >&2
		exit 1
	}
}
compare "docs/releases/$tag.md" "release-notes-$tag.md"
compare keys/expected-fingerprints.txt expected-fingerprints.txt
compare keys/expected-fingerprints.ndjson expected-fingerprints.ndjson
compare LICENSE-MIT LICENSE-MIT
compare LICENSE-APACHE LICENSE-APACHE
echo '[ok] notes, paired anchors and licenses match the verified tagged commit'
