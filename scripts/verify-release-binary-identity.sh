#!/usr/bin/env bash
# Authenticate the exact staged cut before executing its canonical host binary.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
[[ $# == 2 ]] || { echo 'usage: verify-release-binary-identity.sh <tag> <directory>' >&2; exit 2; }
CHANVOY_RELEASE_TAG="$(release_tag "$1")"
export CHANVOY_RELEASE_TAG
directory="$2"
bash "$root/scripts/verify-signatures.sh" "$directory" >/dev/null
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
case "$(uname -s):$(uname -m)" in
Darwin:arm64) platform=macos-aarch64 ;;
Linux:x86_64) platform=linux-x86_64 ;;
Linux:aarch64 | Linux:arm64) platform=linux-aarch64 ;;
*) echo 'error: host has no native release artifact' >&2; exit 1 ;;
esac
binary="$directory/chanvoy-$CHANVOY_RELEASE_TAG-$platform"
bash "$root/scripts/lib/verify-release-identity.sh" "$binary" "${CHANVOY_RELEASE_TAG#v}" "$commit"
