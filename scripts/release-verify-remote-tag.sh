#!/usr/bin/env bash
# shellcheck disable=SC2154 # tag_root is assigned by the sourced common helper.
# Verify just-pushed object at creation-time HEAD, including GitHub status.
set -euo pipefail
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091,SC2154
source "$(dirname "$0")/release-tag-common.sh"
cd "$tag_root"
bash "$tag_root/scripts/release-verify-tag.sh"
local_object="$(git rev-parse "refs/tags/$CHANVOY_RELEASE_TAG")"
remote_object="$(git ls-remote --exit-code origin "refs/tags/$CHANVOY_RELEASE_TAG" | awk '{print $1}')"
[[ "$local_object" == "$remote_object" ]] || { tag_die 'remote tag object differs from local tag'; exit 1; }
tag_ref="$(gh api "repos/lanytehq/chanvoy/git/ref/tags/$CHANVOY_RELEASE_TAG")"
jq -e --arg object "$local_object" '.object.type == "tag" and .object.sha == $object' <<<"$tag_ref" >/dev/null || { tag_die 'GitHub tag ref mismatch'; exit 1; }
CHANVOY_EXPECTED_TAG_OBJECT="$local_object" CHANVOY_EXPECTED_COMMIT="$(git rev-parse HEAD)" \
	bash "$tag_root/scripts/release-verify-published-tag.sh"
