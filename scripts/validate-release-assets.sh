#!/usr/bin/env bash
# Validate the exact release inventory for one ceremony stage.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/release-common.sh"
directory="${1:-dist/release}"
mode="${2:-base}"
release_tag >/dev/null
case "$mode" in
base) assert_exact_directory_inventory "$directory" release_base_assets ;;
signable) assert_exact_directory_inventory "$directory" release_signable_assets ;;
checksummed) assert_exact_directory_inventory "$directory" release_checksummed_assets ;;
signed-without-keys) assert_exact_directory_inventory "$directory" release_signed_without_keys_assets ;;
signed) assert_exact_directory_inventory "$directory" release_signed_assets ;;
*) echo 'usage: validate-release-assets.sh DIR MODE' >&2; exit 1 ;;
esac
echo "[ok] exact $mode release asset inventory verified"
