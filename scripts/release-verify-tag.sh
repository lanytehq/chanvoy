#!/usr/bin/env bash
# shellcheck disable=SC2154 # tag_root is assigned by the sourced common helper.
# Creation-time verification, before separately cued tag push.
set -euo pipefail
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091,SC2154
source "$(dirname "$0")/release-tag-common.sh"
cd "$tag_root"
tag_version
tag_identity
tag_selector_shape
tag_checkout
object="$(git rev-parse "refs/tags/$CHANVOY_RELEASE_TAG")"
expected="$(mktemp "${TMPDIR:-/tmp}/chanvoy-expected-message.XXXXXX")"
trap 'rm -f "$expected"' EXIT
tag_expected_message >"$expected"
tag_verify_object "$object" "$expected"
bash "$tag_root/scripts/verify-pinned-tag.sh"
[[ "$(awk '$1=="gpg" {print $2}' keys/expected-fingerprints.txt)" == "$CHANVOY_GPG_SIGNING_FINGERPRINT" ]] || { tag_die 'authorized primary differs from committed anchor'; exit 1; }
