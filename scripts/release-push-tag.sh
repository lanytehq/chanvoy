#!/usr/bin/env bash
# shellcheck disable=SC2154 # tag_root is assigned by the sourced common helper.
# Separately cued remote push, never force or replacement.
set -euo pipefail
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091,SC2154
source "$(dirname "$0")/release-tag-common.sh"
cd "$tag_root"
bash "$tag_root/scripts/release-verify-tag.sh"
bash "$tag_root/scripts/release-inspect-tag-ruleset.sh" "$CHANVOY_RELEASE_TAG"
[[ -z "$(git ls-remote --tags origin "refs/tags/$CHANVOY_RELEASE_TAG")" ]] || { tag_die 'remote tag already exists'; exit 1; }
git push origin "refs/tags/$CHANVOY_RELEASE_TAG" || { tag_die 'tag push uncertain or failed; inspect before retrying'; exit 1; }
bash "$tag_root/scripts/release-verify-remote-tag.sh"
