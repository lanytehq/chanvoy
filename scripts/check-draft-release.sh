#!/usr/bin/env bash
# Refuse replacement of any existing release; verify exact draft inventory.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
mode="${1:-}"
directory="${2:-dist/release}"
tag="$(release_tag)"
require_release_guard "$directory" >/dev/null
case "$mode" in
before)
	# Paginate to avoid treating an older existing release as absent. Any API
	# failure preserves uncertainty and blocks creation.
	releases="$(gh api --paginate --slurp "repos/$CHANVOY_REPOSITORY/releases?per_page=100")" || {
		echo 'error: release state unavailable; inspect before retrying' >&2; exit 1;
	}
	jq -e 'type == "array" and all(.[]; type == "array" and all(.[]; type == "object" and (.tag_name | type == "string")))' \
		<<<"$releases" >/dev/null || { echo 'error: malformed release state; inspect before retrying' >&2; exit 1; }
	if jq -e --arg tag "$tag" 'any(.[][]; .tag_name == $tag)' <<<"$releases" >/dev/null; then
		echo 'error: release already exists; never replace it' >&2; exit 1;
	fi
	;;
after) assert_github_release_state release_checksummed_assets ;;
signed) assert_github_release_state release_signed_assets ;;
*) echo 'error: expected before, after or signed draft check' >&2; exit 1 ;;
esac
echo '[ok] draft release state verified'
