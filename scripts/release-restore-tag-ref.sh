#!/usr/bin/env bash
# Restore the exact remote annotated ref after checkout peels it on a CI runner.
set -euo pipefail
tag="${CHANVOY_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || { echo 'error: canonical release tag required' >&2; exit 1; }
case "$(git config --get remote.origin.url)" in
	https://github.com/lanytehq/chanvoy | https://github.com/lanytehq/chanvoy.git) ;;
	*) echo 'error: unexpected release origin' >&2; exit 1 ;;
esac
remote="$(git ls-remote --exit-code origin "refs/tags/$tag" | awk '{print $1}')"
[[ "$remote" =~ ^[0-9a-f]{40}$ ]] || { echo 'error: remote release tag absent or invalid' >&2; exit 1; }
if [[ -n "${CHANVOY_EXPECTED_TAG_OBJECT+x}" ]]; then
	[[ "$CHANVOY_EXPECTED_TAG_OBJECT" =~ ^[0-9a-f]{40}$ && "$remote" == "$CHANVOY_EXPECTED_TAG_OBJECT" ]] || {
		echo 'error: remote tag differs from expected object' >&2; exit 1;
	}
fi
git fetch --quiet --no-tags origin "+refs/tags/$tag:refs/tags/$tag"
[[ "$(git rev-parse "refs/tags/$tag")" == "$remote" &&
	"$(git cat-file -t "refs/tags/$tag")" == tag &&
	"$(git rev-parse "refs/tags/$tag^{}")" == "$(git rev-parse HEAD)" ]] || {
	echo 'error: restored tag must equal remote annotated object targeting HEAD' >&2; exit 1;
}
echo '[ok] exact runner-local annotated tag restored'
