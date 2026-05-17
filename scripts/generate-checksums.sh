#!/usr/bin/env bash
# Generate SHA-256 checksums.txt over downloaded chanvoy binaries.
# Matches PER-031's GHA aggregate-job behavior (same algorithm, same
# filename) so an operator's local re-checksum independently produces
# byte-identical content to what shipped in the draft release.
#
# Filename / hash-algorithm conventions per PER-030 + PER-031 briefs:
#   - checksums.txt (single SHA-256 manifest)
#   - sorted by filename for deterministic ordering
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: generate-checksums.sh <release-dir>

  release-dir  Directory containing chanvoy-v*-* binaries

Generates:
  <release-dir>/checksums.txt  — SHA-256 hashes (one line per binary,
                                 sorted by filename)

Example:
  scripts/generate-checksums.sh release/v0.2.2
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
fi

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 1
fi

release_dir="$1"
if [ ! -d "$release_dir" ]; then
    echo "error: release dir not found: ${release_dir}" >&2
    exit 1
fi

artifacts=()
while IFS= read -r path; do
    artifacts+=("$path")
done < <(find "$release_dir" -maxdepth 1 -type f -name 'chanvoy-v*-*' \
    ! -name '*.minisig' ! -name '*.asc' | sort)

if [ "${#artifacts[@]}" -eq 0 ]; then
    echo "error: no chanvoy-v*-* binaries found in ${release_dir}" >&2
    exit 1
fi

(
    cd "$release_dir"
    files=()
    for f in "${artifacts[@]}"; do
        files+=("$(basename "$f")")
    done
    shasum -a 256 "${files[@]}" >checksums.txt
)

echo "[ok] wrote ${release_dir}/checksums.txt (${#artifacts[@]} binaries)"
