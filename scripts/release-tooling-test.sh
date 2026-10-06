#!/usr/bin/env bash
# All remote interactions in this suite use disposable repositories or stubs.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
for tool in bash python3 git gpg gpgconf minisign jq shasum; do
	command -v "$tool" >/dev/null || { echo "error: required test tool missing: $tool" >&2; exit 1; }
done
export CHANVOY_DECERNOR_BIN="${CHANVOY_DECERNOR_BIN:-$(command -v decernor)}"
for test in release-decernor-resolver release-workflow-permissions release-assets release-staging \
	release-tag-controls verify-pinned-tag release-verify-published-tag release-negative-controls sign-release-assets; do
	bash "$root/scripts/$test.test.sh"
done
