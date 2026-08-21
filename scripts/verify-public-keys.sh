#!/usr/bin/env bash
# Verify bundled public-key files match the checked-in contract by
# recomputing the same decernor 0.1.4 records the inserter wrote.
#
# Load-bearing trust contract per devrev pin #4 (2026-05-09):
# verification asserts against stable checked-in fingerprints, not
# "some key file exists."
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: verify-public-keys.sh <release-dir>

  release-dir  Directory containing chanvoy.pub and chanvoy.gpg.asc

Environment:
  CHANVOY_EXPECTED_FINGERPRINTS  Path to expected-fingerprints file
                                  (default: keys/expected-fingerprints.txt
                                   relative to repo root)
  DECERNOR                       Explicit decernor binary (must be 0.1.4+)

Checks (all mandatory):
  - chanvoy.pub and chanvoy.gpg.asc are both present
  - Neither file contains private-key markers
  - expected file has both lines, neither TBD
  - recomputed minisign-public-blob-sha256-v1 matches
  - recomputed GPG primary (--gpg-role primary) matches

Example:
  scripts/verify-public-keys.sh release/v0.3.0
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
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=lib/fingerprint-contract.sh
source "${script_dir}/lib/fingerprint-contract.sh"
fingerprints_file="${CHANVOY_EXPECTED_FINGERPRINTS:-${repo_root}/keys/expected-fingerprints.txt}"

if [ ! -f "$fingerprints_file" ]; then
    echo "error: expected-fingerprints file not found at ${fingerprints_file}" >&2
    exit 1
fi

for key in "${release_dir}/chanvoy.pub" "${release_dir}/chanvoy.gpg.asc"; do
    if [ ! -f "$key" ]; then
        echo "error: missing public key file: ${key}" >&2
        echo "       run 'make release-export-keys' with both" >&2
        echo "       CHANVOY_MINISIGN_PUB and CHANVOY_PGP_KEY_ID set" >&2
        exit 1
    fi
done

chanvoy_refuse_private "${release_dir}/chanvoy.pub"
chanvoy_refuse_private "${release_dir}/chanvoy.gpg.asc"

expected_minisign=""
expected_gpg=""
while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
        '#'*|'') continue ;;
    esac
    algo="$(printf '%s\n' "$line" | awk '{print $1}')"
    value="$(printf '%s\n' "$line" | awk '{print $2}')"
    case "$algo" in
        minisign) expected_minisign="$value" ;;
        gpg)      expected_gpg="$value" ;;
    esac
done < "$fingerprints_file"

if [ -z "$expected_minisign" ]; then
    echo "error: no 'minisign' line in ${fingerprints_file}" >&2
    exit 1
fi
if [ -z "$expected_gpg" ]; then
    echo "error: no 'gpg' line in ${fingerprints_file}" >&2
    exit 1
fi
if [ "${expected_minisign#TBD-}" != "$expected_minisign" ] || [ "${expected_gpg#TBD-}" != "$expected_gpg" ]; then
    echo "error: fingerprint contract still contains a TBD placeholder in ${fingerprints_file}" >&2
    echo "       run scripts/insert-expected-fingerprints.sh against exported public files" >&2
    echo "       (decernor 0.1.4+). Do not hand-type hex." >&2
    exit 1
fi

bin="$(chanvoy_require_decernor)"

actual_minisign="$(chanvoy_minisign_blob_fp "$bin" "${release_dir}/chanvoy.pub")"
actual_gpg="$(chanvoy_gpg_primary_fp "$bin" "${release_dir}/chanvoy.gpg.asc")"

if [ "$actual_minisign" != "$expected_minisign" ]; then
    echo "error: minisign fingerprint mismatch" >&2
    echo "  expected: $expected_minisign" >&2
    echo "  actual:   $actual_minisign" >&2
    exit 1
fi
if [ "$actual_gpg" != "$expected_gpg" ]; then
    echo "error: GPG fingerprint mismatch" >&2
    echo "  expected: $expected_gpg" >&2
    echo "  actual:   $actual_gpg" >&2
    exit 1
fi

echo "[ok] public-key fingerprints match expected values"
