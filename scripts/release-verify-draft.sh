#!/usr/bin/env bash
# Fresh download proves the exact remote inventory, metadata and signatures.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
require_release_guard "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" signed >/dev/null
bash "$root/scripts/verify-signatures.sh" "$directory" >/dev/null
assert_github_release_state release_signed_assets
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-fresh-draft.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/assets"
cp "$directory.anchor" "$scratch/assets.anchor"
gh release download "$tag" --repo "$CHANVOY_REPOSITORY" --dir "$scratch/assets" || {
	echo 'error: fresh draft download failed; inspect before retrying' >&2; exit 1;
}
bash "$root/scripts/validate-release-assets.sh" "$scratch/assets" signed >/dev/null
bash "$root/scripts/release-verify-staged-data.sh" "$scratch/assets" >/dev/null
bash "$root/scripts/verify-signatures.sh" "$scratch/assets" >/dev/null
# Compare to the locally approved signed cut as well; otherwise a changed but
# validly signed manifest could silently replace the staged approval artifact.
inventory="$(release_signed_assets)"
while IFS= read -r name; do
	if [[ ! -f "$directory/$name" || -L "$directory/$name" ]] ||
		! cmp -s "$directory/$name" "$scratch/assets/$name"; then
		echo 'error: remote draft differs from locally approved signed cut' >&2; exit 1;
	fi
done <<<"$inventory"
# Execute only the freshly downloaded, authenticated and byte-matched host asset.
bash "$root/scripts/verify-release-binary-identity.sh" "$tag" "$scratch/assets" >/dev/null
require_release_guard "$directory" >/dev/null
assert_github_release_state release_signed_assets
echo '[ok] freshly downloaded exact draft matches approved signed cut'
