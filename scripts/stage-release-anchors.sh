#!/usr/bin/env bash
# Stage exact tagged notes and both anchors before checksumming; no tagged code.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
require_release_guard "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" base >/dev/null
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-stage-data.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
committed() {
	local source="$1" target="$2" mode
	mode="$(git -C "$root" ls-tree "$commit" -- "$source" | awk '{print $1}')"
	[[ "$mode" == 100644 || "$mode" == 100755 ]] || {
		echo 'error: regular tagged release data required' >&2
		exit 1
	}
	git -C "$root" cat-file blob "$commit:$source" >"$scratch/$target"
	[[ -s "$scratch/$target" ]] || { echo 'error: empty tagged release data' >&2; exit 1; }
}
committed "docs/releases/$tag.md" "release-notes-$tag.md"
committed keys/expected-fingerprints.txt expected-fingerprints.txt
committed keys/expected-fingerprints.ndjson expected-fingerprints.ndjson
committed docs/security/release-signing-keys.asc release-signing-keys.asc
for name in LICENSE-APACHE LICENSE-MIT; do
	committed "$name" "$name"
	cmp -s "$scratch/$name" "$directory/$name" || { echo 'error: staged license differs from tagged license' >&2; exit 1; }
done
bash "$root/scripts/validate-release-anchors.sh" "$scratch/expected-fingerprints.txt" \
	"$scratch/release-signing-keys.asc" "$scratch/expected-fingerprints.ndjson" >/dev/null
for name in "release-notes-$tag.md" expected-fingerprints.txt expected-fingerprints.ndjson; do
	cp "$scratch/$name" "$directory/$name"
done
bash "$root/scripts/validate-release-assets.sh" "$directory" signable >/dev/null
echo '[ok] exact tagged notes and public anchors staged before checksums'
