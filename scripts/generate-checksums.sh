#!/usr/bin/env bash
# Generate exact SHA256 and SHA512 manifests for one staged release.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/release-common.sh"
directory="${1:-dist/release}"
release_tag >/dev/null
require_release_guard "$directory" >/dev/null
bash "$SCRIPT_DIR/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signable >/dev/null

# Capture the producer status before parsing; Bash 3.2 has no mapfile.
inventory="$(release_signable_assets)"
assets=()
while IFS= read -r value; do assets+=("$value"); done <<<"$inventory"
(
	cd "$directory"
	printf '%s\n' "${assets[@]}" | LC_ALL=C sort | xargs shasum -a 256 >SHA256SUMS
	printf '%s\n' "${assets[@]}" | LC_ALL=C sort | xargs shasum -a 512 >SHA512SUMS
	cp SHA256SUMS checksums.txt
)
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" checksummed >/dev/null
echo '[ok] exact SHA256 and SHA512 manifests generated'
