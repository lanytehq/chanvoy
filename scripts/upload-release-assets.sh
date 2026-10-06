#!/usr/bin/env bash
# Upload only the exact missing provenance set, without globs or clobber.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" signed >/dev/null
bash "$root/scripts/verify-checksums.sh" "$directory" >/dev/null
bash "$root/scripts/verify-signatures.sh" "$directory" >/dev/null
assert_github_release_state release_checksummed_assets
inventory="$(release_provenance_assets)"
files=()
while IFS= read -r asset; do
	case "$asset" in SHA256SUMS | SHA512SUMS | checksums.txt) ;; *) files+=("$directory/$asset") ;; esac
done <<<"$inventory"
require_release_guard "$directory" >/dev/null
gh release upload "$tag" "${files[@]}" --repo "$CHANVOY_REPOSITORY" || {
	echo 'error: provenance upload uncertain or partial; inspect before retrying' >&2; exit 1;
}
assert_github_release_state release_signed_assets
echo '[ok] exact signed provenance uploaded; release remains draft'
