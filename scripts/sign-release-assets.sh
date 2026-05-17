#!/usr/bin/env bash
# Produce minisign + GPG signatures over chanvoy release assets.
#
# Signature model (per PER-030 brief verification snippets):
#   - minisign signs each binary individually (one .minisig per binary)
#     → external operators can verify any single download
#   - GPG signs checksums.txt (single .asc over the manifest)
#     → operators can verify the whole asset set via the manifest
#
# Signing keys are NEVER in CI. Per PER-030's manual-signing v0.2.2
# baseline: Dave runs this locally; the keys have passphrases; nothing
# touches GHA runners.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: sign-release-assets.sh <release-tag> <release-dir>

  release-tag   GitHub release tag (e.g., v0.2.2)
  release-dir   Directory containing chanvoy binaries + checksums.txt

Environment:
  CHANVOY_MINISIGN_KEY   Path to minisign secret key (required)
  CHANVOY_PGP_KEY_ID     GPG key ID for checksums.txt signature
                         (optional; if unset, GPG step is skipped)
  CHANVOY_GPG_HOMEDIR    Optional GPG homedir override

Produces:
  <release-dir>/chanvoy-v*-*.minisig   one per binary (minisign)
  <release-dir>/checksums.txt.asc      single (GPG, if PGP_KEY_ID set)

Example:
  CHANVOY_MINISIGN_KEY=~/.minisign/chanvoy.key \
  CHANVOY_PGP_KEY_ID=ABC123... \
    scripts/sign-release-assets.sh v0.2.2 release/v0.2.2
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
minisign_key="${CHANVOY_MINISIGN_KEY:-}"
pgp_key_id="${CHANVOY_PGP_KEY_ID:-}"
gpg_homedir="${CHANVOY_GPG_HOMEDIR:-}"

if [ -z "$minisign_key" ]; then
    echo "error: CHANVOY_MINISIGN_KEY is required" >&2
    exit 1
fi

if ! command -v minisign >/dev/null 2>&1; then
    echo "error: minisign is required" >&2
    exit 1
fi

if [ ! -f "${release_dir}/checksums.txt" ]; then
    echo "error: missing ${release_dir}/checksums.txt" >&2
    echo "       run 'make release-checksums' first" >&2
    exit 1
fi

# minisign each binary individually.
binaries=()
while IFS= read -r path; do
    binaries+=("$path")
done < <(find "$release_dir" -maxdepth 1 -type f -name 'chanvoy-v*-*' \
    ! -name '*.minisig' ! -name '*.asc' | sort)

if [ "${#binaries[@]}" -eq 0 ]; then
    echo "error: no chanvoy-v*-* binaries found in ${release_dir}" >&2
    exit 1
fi

for binary in "${binaries[@]}"; do
    minisign -S -s "$minisign_key" -m "$binary" -x "${binary}.minisig"
done

# GPG sign checksums.txt (manifest-level signature).
if [ -n "$pgp_key_id" ]; then
    if ! command -v gpg >/dev/null 2>&1; then
        echo "error: gpg is required when CHANVOY_PGP_KEY_ID is set" >&2
        exit 1
    fi
    gpg_args=(--batch --yes --armor --local-user "$pgp_key_id")
    if [ -n "$gpg_homedir" ]; then
        gpg_args+=(--homedir "$gpg_homedir")
    fi
    gpg "${gpg_args[@]}" \
        --output "${release_dir}/checksums.txt.asc" \
        --detach-sign "${release_dir}/checksums.txt"
fi

echo "[ok] signed ${#binaries[@]} binaries + checksums.txt manifest for ${release_tag}"
