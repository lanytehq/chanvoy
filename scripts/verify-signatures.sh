#!/usr/bin/env bash
# Verify minisign + GPG signatures on release assets locally before
# upload. Run by `make release-verify-signatures`; composite gate
# `make release-verify` chains this with verify-public-keys.sh.
#
# Verification model:
#   - For each chanvoy-v*-* binary, expect a .minisig and verify
#     against the bundled chanvoy.pub
#   - For checksums.txt, expect a .asc and verify against the GPG
#     keyring (or CHANVOY_GPG_HOMEDIR override)
#
# Fails on any missing signature or mismatch.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: verify-signatures.sh <release-dir>

  release-dir  Directory containing binaries + signatures + chanvoy.pub

Environment:
  CHANVOY_MINISIGN_PUB   Path to minisign public key
                         (default: <release-dir>/chanvoy.pub)
  CHANVOY_GPG_HOMEDIR    Optional GPG homedir override

Example:
  scripts/verify-signatures.sh release/v0.2.2
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
minisign_pub="${CHANVOY_MINISIGN_PUB:-${release_dir}/chanvoy.pub}"
gpg_homedir="${CHANVOY_GPG_HOMEDIR:-}"

if [ ! -f "$minisign_pub" ]; then
    echo "error: minisign public key not found at ${minisign_pub}" >&2
    exit 1
fi

if ! command -v minisign >/dev/null 2>&1; then
    echo "error: minisign is required" >&2
    exit 1
fi

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
    sig="${binary}.minisig"
    if [ ! -f "$sig" ]; then
        echo "error: missing minisign signature ${sig}" >&2
        exit 1
    fi
    minisign -V -p "$minisign_pub" -m "$binary" -x "$sig"
done

asc="${release_dir}/checksums.txt.asc"
if [ -f "$asc" ]; then
    if ! command -v gpg >/dev/null 2>&1; then
        echo "error: GPG signature present but gpg not installed" >&2
        exit 1
    fi
    gpg_args=(--verify "$asc" "${release_dir}/checksums.txt")
    if [ -n "$gpg_homedir" ]; then
        gpg_args=(--homedir "$gpg_homedir" "${gpg_args[@]}")
    fi
    gpg "${gpg_args[@]}"
fi

echo "[ok] signature verification passed (${#binaries[@]} binaries$([ -f "$asc" ] && echo ' + checksums.txt manifest'))"
