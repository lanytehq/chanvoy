#!/usr/bin/env bash
# Write keys/expected-fingerprints.txt from decernor 0.1.4 records.
# Explicit public files only. Both contract lines or neither.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: insert-expected-fingerprints.sh --minisign <pub> --gpg <asc> [--output <path>]

  --minisign  Exported minisign public key file
  --gpg       Exported OpenPGP public key (armored)
  --output    Destination contract file (default: keys/expected-fingerprints.txt)

Environment:
  DECERNOR    Explicit decernor binary (must be 0.1.4 or later)

Writes both lines atomically. On any failure the destination is left
unchanged. Never hand-types hex. Never walks a keyring or vault.
EOF
}

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=lib/fingerprint-contract.sh
source "${script_dir}/lib/fingerprint-contract.sh"

minisign_pub=""
gpg_asc=""
output="${repo_root}/keys/expected-fingerprints.txt"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --minisign)
            minisign_pub="${2:-}"
            shift 2
            ;;
        --gpg)
            gpg_asc="${2:-}"
            shift 2
            ;;
        --output)
            output="${2:-}"
            shift 2
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [ -z "$minisign_pub" ] || [ -z "$gpg_asc" ]; then
    echo "error: --minisign and --gpg are both required" >&2
    usage >&2
    exit 1
fi
if [ ! -f "$minisign_pub" ]; then
    echo "error: minisign public file not found: ${minisign_pub}" >&2
    exit 1
fi
if [ ! -f "$gpg_asc" ]; then
    echo "error: GPG public file not found: ${gpg_asc}" >&2
    exit 1
fi

chanvoy_refuse_private "$minisign_pub"
chanvoy_refuse_private "$gpg_asc"

bin="$(chanvoy_require_decernor)"

gpg_fp="$(chanvoy_gpg_primary_fp "$bin" "$gpg_asc")"
mini_fp="$(chanvoy_minisign_blob_fp "$bin" "$minisign_pub")"

if [ -z "$gpg_fp" ] || [ -z "$mini_fp" ]; then
    echo "error: refused to write a partial fingerprint contract" >&2
    exit 1
fi
case "$mini_fp" in TBD-*|*" "*) echo "error: minisign fingerprint is not a 64-hex contract token" >&2; exit 1 ;; esac
case "$gpg_fp" in TBD-*|*" "*) echo "error: GPG fingerprint is not a 40-hex contract token" >&2; exit 1 ;; esac

dest_dir="$(cd "$(dirname "$output")" && pwd)"
dest_base="$(basename "$output")"
dest="${dest_dir}/${dest_base}"
tmp="${dest}.tmp.$$"

trap 'rm -f "$tmp"' EXIT

cat >"$tmp" <<EOF
# Expected chanvoy release-signing public-key fingerprints.
#
# Load-bearing trust contract: release-verification asserts against
# these values, not "some key file exists." Fill only via
# scripts/insert-expected-fingerprints.sh (decernor 0.1.4+ records).
# Do not hand-type hex.
#
# Format: one \`<algo> <fingerprint>\` line per key.
#   minisign  <64 lowercase hex, minisign-public-blob-sha256-v1>
#   gpg       <40 uppercase hex, OpenPGP primary, --gpg-role primary>
#
# Lines starting with '#' are comments.

minisign  ${mini_fp}
gpg       ${gpg_fp}
EOF

mv "$tmp" "$dest"
trap - EXIT
echo "[ok] wrote ${dest}"
echo "     minisign ${mini_fp}"
echo "     gpg      ${gpg_fp}"
