#!/usr/bin/env bash
# shellcheck disable=SC2154 # tag_root is assigned by the sourced common helper.
# Create a signed tag locally only. Publication is a separate command.
set -euo pipefail
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091,SC2154
source "$(dirname "$0")/release-tag-common.sh"
cd "$tag_root"
tag_version
tag_identity
tag_key_selector
tag_checkout
[[ -z "$(git ls-remote --tags origin "refs/tags/$CHANVOY_RELEASE_TAG")" ]] || { tag_die 'remote tag already exists'; exit 1; }
[[ -z "$(git show-ref --tags "refs/tags/$CHANVOY_RELEASE_TAG" || true)" ]] || { tag_die 'local tag already exists'; exit 1; }
message="$(mktemp "${TMPDIR:-/tmp}/chanvoy-tag-message.XXXXXX")"
trap 'rm -f "$message"' EXIT
tag_expected_message >"$message"
export GIT_COMMITTER_NAME="$CHANVOY_TAGGER_NAME" GIT_COMMITTER_EMAIL="$CHANVOY_TAGGER_EMAIL"
git -c gpg.program=gpg tag -s -a --cleanup=verbatim -u "$CHANVOY_PGP_KEY_ID" \
	-F "$message" "$CHANVOY_RELEASE_TAG" HEAD 2>/dev/null || { tag_die 'local signing failed; inspect before retrying'; exit 1; }
bash "$tag_root/scripts/release-verify-tag.sh"
echo '[ok] signed tag remains local only'
