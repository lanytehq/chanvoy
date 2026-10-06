#!/usr/bin/env bash
# Maintainer-only exact checksum-verified draft creation; CI never creates it.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
CHANVOY_RELEASE_TAG="$(release_tag "${1:-}")"
export CHANVOY_RELEASE_TAG
directory="${2:-dist/release}"
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" checksummed >/dev/null
bash "$root/scripts/verify-checksums.sh" "$directory" >/dev/null
bash "$root/scripts/check-draft-release.sh" before "$directory" >/dev/null
inventory="$(release_checksummed_assets)"
files=()
while IFS= read -r asset; do files+=("$directory/$asset"); done <<<"$inventory"
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
require_release_guard "$directory" >/dev/null
gh release create "$CHANVOY_RELEASE_TAG" --repo "$CHANVOY_REPOSITORY" --verify-tag --draft \
	--target "$commit" --title "$CHANVOY_RELEASE_TAG" \
	--notes-file "$directory/release-notes-$CHANVOY_RELEASE_TAG.md" "${files[@]}" || {
	echo 'error: draft creation uncertain or failed; inspect before retrying' >&2; exit 1;
}
bash "$root/scripts/check-draft-release.sh" after "$directory" >/dev/null
echo '[ok] exact draft created from receipt-bound assets; not published'
