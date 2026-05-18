#!/usr/bin/env bash
# Attach signed artifacts + public keys to the GitHub draft release
# created by the PER-031 GHA workflow. This is the atomic asset-upload
# step — does NOT flip the draft state. `release-undraft` is the
# separate atomic verb that flips draft → published; the composite
# `release-upload-all` chains both.
#
# Atomic split per cxotech 2026-05-11 PR #55 review: preserves recovery
# composability — re-run a missing-key upload without re-flipping draft
# state, and re-run an undraft without re-attaching assets.
#
# Idempotent: re-running after partial completion uses --clobber to
# re-upload assets safely (GH replaces by filename) and the script
# tolerates a release already-undrafted by another caller.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: upload-release-assets.sh <release-tag> <release-dir>

  release-tag   GitHub release tag (e.g., v0.2.2)
  release-dir   Directory containing signed artifacts + public keys

Required artifact set (all mandatory per devrev + secrev PR #33 review;
upload refuses to run if any is missing):
  - chanvoy-v*-*               one or more binaries
  - chanvoy-v*-*.minisig       per-binary minisign signature for EACH binary
  - checksums.txt              SHA-256 manifest
  - checksums.txt.asc          GPG signature over the manifest
  - chanvoy.pub                minisign public key
  - chanvoy.gpg.asc            GPG public key

Optionally refreshed if present:
  - docs/releases/<tag>.md     release notes (via gh release edit --notes-file)

Requires:
  gh CLI on PATH; authenticated against lanytehq/chanvoy

Example:
  scripts/upload-release-assets.sh v0.2.2 release/v0.2.2
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

release_tag="$1"
release_dir="$2"

if ! command -v gh >/dev/null 2>&1; then
    echo "error: gh CLI is required" >&2
    exit 1
fi

# Completeness check — refuse to upload an incomplete artifact set
# (secrev PR #33 review). Script-level guard is belt-and-suspenders
# alongside the Makefile `release-upload: release-verify` dependency:
# even if called directly bypassing `make`, this script will not
# attach a partial set to a draft release that's about to be undrafted.

binaries=()
while IFS= read -r path; do
    binaries+=("$path")
done < <(find "$release_dir" -maxdepth 1 -type f -name 'chanvoy-v*-*' \
    ! -name '*.minisig' ! -name '*.asc' | sort)

if [ "${#binaries[@]}" -eq 0 ]; then
    echo "error: no chanvoy-v*-* binaries found in ${release_dir}" >&2
    exit 1
fi

missing=()
for binary in "${binaries[@]}"; do
    [ -f "${binary}.minisig" ] || missing+=("${binary}.minisig")
done
[ -f "${release_dir}/checksums.txt" ]      || missing+=("${release_dir}/checksums.txt")
[ -f "${release_dir}/checksums.txt.asc" ]  || missing+=("${release_dir}/checksums.txt.asc")
[ -f "${release_dir}/chanvoy.pub" ]        || missing+=("${release_dir}/chanvoy.pub")
[ -f "${release_dir}/chanvoy.gpg.asc" ]    || missing+=("${release_dir}/chanvoy.gpg.asc")

if [ "${#missing[@]}" -gt 0 ]; then
    echo "error: release dir is missing required artifacts:" >&2
    for m in "${missing[@]}"; do
        echo "  - ${m}" >&2
    done
    echo "" >&2
    echo "       run 'make release-sign' + 'make release-export-keys'" >&2
    echo "       + 'make release-verify' before uploading" >&2
    exit 1
fi

# Collect the full asset set in deterministic order.
assets=()
while IFS= read -r path; do
    assets+=("$path")
done < <(
    find "$release_dir" -maxdepth 1 -type f \
        \( -name 'chanvoy-v*-*' \
        -o -name 'checksums.txt' \
        -o -name 'checksums.txt.asc' \
        -o -name 'chanvoy.pub' \
        -o -name 'chanvoy.gpg.asc' \) |
        sort
)

echo "Uploading ${#assets[@]} assets to ${release_tag}..."
gh release upload "$release_tag" --repo lanytehq/chanvoy --clobber "${assets[@]}"

# Refresh release notes if the canonical source exists. Idempotent.
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
notes_file="${repo_root}/docs/releases/${release_tag}.md"
if [ -f "$notes_file" ]; then
    gh release edit "$release_tag" --repo lanytehq/chanvoy --notes-file "$notes_file"
    echo "[ok] refreshed release notes from ${notes_file}"
fi

echo "[ok] uploaded ${#assets[@]} assets to ${release_tag} (draft state unchanged)"
