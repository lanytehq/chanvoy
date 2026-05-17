#!/usr/bin/env bash
# Verify that bundled public-key files match the checked-in expected
# fingerprints. This is the load-bearing trust contract per devrev pin
# #4 (2026-05-09): verification asserts against stable key fingerprints,
# not "some key file exists."
#
# Also defensively scans the .pub / .asc files for private-key markers
# to catch a "wrong file copied into release dir" footgun.
#
# Expected fingerprints live in keys/expected-fingerprints.txt at the
# repo root. Format: one `<algo> <fingerprint>` line per key.
#   minisign  <minisign-public-key-fingerprint>
#   gpg       <gpg-key-fingerprint-or-id>
#
# Until dispatch's keypair provisioning completes, the fingerprints file
# may contain `TBD-...` placeholders; this script will then fail and
# print a clear next-step hint.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: verify-public-keys.sh <release-dir>

  release-dir  Directory containing chanvoy.pub and (optionally)
               chanvoy.gpg.asc

Environment:
  CHANVOY_EXPECTED_FINGERPRINTS  Path to expected-fingerprints file
                                  (default: keys/expected-fingerprints.txt
                                   relative to repo root)
  CHANVOY_GPG_HOMEDIR             Optional GPG homedir override

Checks:
  - Public key files contain no private-key markers
  - minisign fingerprint matches expected
  - GPG fingerprint matches expected (if chanvoy.gpg.asc present)

Example:
  scripts/verify-public-keys.sh release/v0.2.2
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
fingerprints_file="${CHANVOY_EXPECTED_FINGERPRINTS:-${repo_root}/keys/expected-fingerprints.txt}"
gpg_homedir="${CHANVOY_GPG_HOMEDIR:-}"

if [ ! -f "$fingerprints_file" ]; then
    echo "error: expected-fingerprints file not found at ${fingerprints_file}" >&2
    exit 1
fi

# Defensive: catch "private key copied into release dir" footgun.
scan_for_private_material() {
    local file="$1"
    grep -E "PRIVATE|SECRET|BEGIN PGP PRIVATE KEY" "$file" >/dev/null 2>&1
}

for key in "${release_dir}/chanvoy.pub" "${release_dir}/chanvoy.gpg.asc"; do
    [ -f "$key" ] || continue
    if scan_for_private_material "$key"; then
        echo "error: key file appears to contain private material: ${key}" >&2
        exit 1
    fi
done

# Read expected fingerprints. Lines starting with '#' are comments.
expected_minisign=""
expected_gpg=""
while IFS= read -r line; do
    case "$line" in
        '#'*|'') continue ;;
        minisign\ *) expected_minisign="${line#minisign }" ;;
        gpg\ *)      expected_gpg="${line#gpg }" ;;
    esac
done < "$fingerprints_file"

# minisign fingerprint check.
if [ -f "${release_dir}/chanvoy.pub" ]; then
    if [ -z "$expected_minisign" ]; then
        echo "error: no 'minisign' line in ${fingerprints_file}" >&2
        exit 1
    fi
    if [ "${expected_minisign#TBD-}" != "$expected_minisign" ]; then
        echo "error: minisign fingerprint is a TBD placeholder in ${fingerprints_file}" >&2
        echo "       fill in the real fingerprint after dispatch's keypair provisioning" >&2
        exit 1
    fi
    # minisign public key file format:
    #   untrusted comment: ...
    #   <base64-blob>
    # The second line contains a key identifier (10-hex-char prefix of
    # the SHA-256 of the public key). We extract it for the comparison.
    actual_minisign=$(awk 'NR==2 {print; exit}' "${release_dir}/chanvoy.pub" \
        | base64 -d 2>/dev/null \
        | xxd -p \
        | tr -d '\n' \
        | head -c 20 \
        || true)
    if [ -z "$actual_minisign" ]; then
        echo "error: failed to extract minisign key id from ${release_dir}/chanvoy.pub" >&2
        exit 1
    fi
    if [ "$actual_minisign" != "$expected_minisign" ]; then
        echo "error: minisign fingerprint mismatch" >&2
        echo "  expected: $expected_minisign" >&2
        echo "  actual:   $actual_minisign" >&2
        exit 1
    fi
fi

# GPG fingerprint check.
if [ -f "${release_dir}/chanvoy.gpg.asc" ]; then
    if [ -z "$expected_gpg" ]; then
        echo "error: no 'gpg' line in ${fingerprints_file}" >&2
        exit 1
    fi
    if [ "${expected_gpg#TBD-}" != "$expected_gpg" ]; then
        echo "error: GPG fingerprint is a TBD placeholder in ${fingerprints_file}" >&2
        echo "       fill in the real fingerprint after dispatch's keypair provisioning" >&2
        exit 1
    fi
    if ! command -v gpg >/dev/null 2>&1; then
        echo "error: gpg is required to verify chanvoy.gpg.asc fingerprint" >&2
        exit 1
    fi
    gpg_args=()
    if [ -n "$gpg_homedir" ]; then
        gpg_args+=(--homedir "$gpg_homedir")
    fi
    # `gpg --show-keys` parses a key file without importing.
    actual_gpg=$(gpg "${gpg_args[@]}" --show-keys --with-colons \
        "${release_dir}/chanvoy.gpg.asc" \
        | awk -F: '$1 == "fpr" {print $10; exit}')
    if [ -z "$actual_gpg" ]; then
        echo "error: failed to extract GPG fingerprint from ${release_dir}/chanvoy.gpg.asc" >&2
        exit 1
    fi
    if [ "$actual_gpg" != "$expected_gpg" ]; then
        echo "error: GPG fingerprint mismatch" >&2
        echo "  expected: $expected_gpg" >&2
        echo "  actual:   $actual_gpg" >&2
        exit 1
    fi
fi

echo "[ok] public-key fingerprints match expected values"
