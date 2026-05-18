#!/usr/bin/env bash
# Download draft-release binaries + checksums from GitHub into a local
# working directory. Consumed by `make release-download` as part of the
# PER-030 manual signing flow.
#
# Adapted from seclusor's download-release-assets.sh (verbatim port
# with chanvoy org/repo substitution).
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: download-release-assets.sh <release-tag> <output-dir>

  release-tag   GitHub release tag (e.g., v0.2.2)
  output-dir    Local directory to download assets into (created if missing)

Requires:
  gh CLI on PATH; authenticated against lanytehq/chanvoy

Example:
  scripts/download-release-assets.sh v0.2.2 release/v0.2.2
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
fi

if [ "$#" -ne 2 ]; then
    usage >&2
    exit 1
fi

tag="$1"
out_dir="$2"

if ! command -v gh >/dev/null 2>&1; then
    echo "error: gh CLI is required" >&2
    exit 1
fi

mkdir -p "$out_dir"

echo "Downloading assets for ${tag} into ${out_dir}..."
gh release download "$tag" --repo lanytehq/chanvoy --dir "$out_dir"
echo "[ok] download complete"
