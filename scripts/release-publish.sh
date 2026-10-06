#!/usr/bin/env bash
# Separately cued maintainer promotion, guarded on direct entry.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
[[ "${CHANVOY_CONFIRM_PUBLISH:-}" == "$tag" ]] || {
	echo 'error: separate publication cue required: CHANVOY_CONFIRM_PUBLISH must equal tag' >&2; exit 1;
}
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" signed >/dev/null
bash "$root/scripts/verify-signatures.sh" "$directory" >/dev/null
bash "$root/scripts/release-verify-draft.sh" "$directory" >/dev/null
require_release_guard "$directory" >/dev/null
assert_github_release_state release_signed_assets
gh release edit "$tag" --repo "$CHANVOY_REPOSITORY" --draft=false || {
	echo 'error: promotion uncertain or failed; inspect before retrying' >&2; exit 1;
}
echo '[ok] verified draft promoted; inspect published state before any further action'
