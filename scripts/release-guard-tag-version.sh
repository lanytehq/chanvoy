#!/usr/bin/env bash
# Version guard for creation/CI checkout, not post-tag publication from later main.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
version="$(cat VERSION)"
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || { echo 'error: stable VERSION required' >&2; exit 1; }
tag="${CHANVOY_RELEASE_TAG:-${RELEASE_TAG:-v$version}}"
[[ -z "${RELEASE_TAG:-}" || "$RELEASE_TAG" == "$tag" ]] || { echo 'error: release tag aliases disagree' >&2; exit 1; }
[[ "$tag" == "v$version" ]] || { echo 'error: canonical release tag must match VERSION' >&2; exit 1; }
if [[ "${CHANVOY_REQUIRE_TAG:-0}" == 1 ]]; then
	[[ -n "${CHANVOY_RELEASE_TAG:-}" ]] || { echo 'error: strict guard requires explicit release tag' >&2; exit 1; }
	if git symbolic-ref -q HEAD >/dev/null; then echo 'error: strict checkout must be detached' >&2; exit 1; fi
	[[ -z "$(git status --porcelain --untracked-files=all)" ]] || { echo 'error: clean checkout required' >&2; exit 1; }
	case "$(git config --get remote.origin.url)" in
		https://github.com/lanytehq/chanvoy | https://github.com/lanytehq/chanvoy.git | git@github.com:lanytehq/chanvoy.git) ;;
		*) echo 'error: fixed chanvoy GitHub origin required' >&2; exit 1 ;;
	esac
	git fetch --quiet --no-tags origin '+refs/heads/main:refs/remotes/origin/main'
	[[ "$(git cat-file -t "refs/tags/$tag")" == tag ]] || { echo 'error: annotated tag required' >&2; exit 1; }
	remote="$(git ls-remote --exit-code origin "refs/tags/$tag" | awk '{print $1}')"
	[[ "$remote" == "$(git rev-parse "refs/tags/$tag")" &&
		"$(git rev-parse "refs/tags/$tag^{}")" == "$(git rev-parse HEAD)" &&
		"$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] || {
		echo 'error: tag object, detached HEAD and origin/main must match' >&2; exit 1;
	}
fi
echo '[ok] canonical release version guard passed'
