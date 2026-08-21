# Shared helpers for the decernor 0.1.4 fingerprint contract.
# Sourced by insert-expected-fingerprints.sh and verify-public-keys.sh.
# Requires bash, python3, and a decernor >= 0.1.4 on PATH (or $DECERNOR).

CHANVOY_MIN_DECERNOR="${CHANVOY_MIN_DECERNOR:-0.1.4}"

chanvoy_refuse_private() {
    local file="$1"
    if grep -Eqi "PRIVATE|SECRET|BEGIN PGP PRIVATE KEY|minisign secret key" "$file"; then
        echo "error: key file appears to contain private material: ${file}" >&2
        return 1
    fi
}

chanvoy_decernor_bin() {
    if [ -n "${DECERNOR:-}" ]; then
        if [ ! -x "$DECERNOR" ]; then
            echo "error: DECERNOR is not executable: ${DECERNOR}" >&2
            return 1
        fi
        printf '%s\n' "$DECERNOR"
        return 0
    fi
    if command -v decernor >/dev/null 2>&1; then
        command -v decernor
        return 0
    fi
    echo "error: decernor not found on PATH (set DECERNOR= to an explicit binary)" >&2
    return 1
}

# True if $1 is a dotted version >= $2 (numeric components only).
chanvoy_version_ge() {
    python3 - "$1" "$2" <<'PY'
import sys

def parts(s):
    out = []
    for p in s.strip().lstrip("vV").split("."):
        digits = "".join(c for c in p if c.isdigit())
        out.append(int(digits) if digits else 0)
    return out

have, need = parts(sys.argv[1]), parts(sys.argv[2])
n = max(len(have), len(need))
have += [0] * (n - len(have))
need += [0] * (n - len(need))
sys.exit(0 if have >= need else 1)
PY
}

chanvoy_decernor_version() {
    local bin="$1"
    local raw
    raw="$("$bin" version 2>/dev/null || true)"
    python3 - "$raw" <<'PY'
import re, sys
raw = sys.argv[1]
m = re.search(r"(\d+\.\d+\.\d+)", raw)
if not m:
    sys.exit(1)
print(m.group(1))
PY
}

chanvoy_require_decernor() {
    local bin ver
    bin="$(chanvoy_decernor_bin)" || return 1
    ver="$(chanvoy_decernor_version "$bin")" || {
        echo "error: could not parse version from \`$bin version\`" >&2
        return 1
    }
    if ! chanvoy_version_ge "$ver" "$CHANVOY_MIN_DECERNOR"; then
        echo "error: decernor ${ver} is too old; need ${CHANVOY_MIN_DECERNOR} or later" >&2
        echo "       this host must not insert or verify against a pre-0.1.4 contract" >&2
        return 1
    fi
    printf '%s\n' "$bin"
}

# stdout: uppercase 40-hex GPG primary fingerprint
chanvoy_gpg_primary_fp() {
    local bin="$1"
    local asc="$2"
    local json
    json="$("$bin" fingerprint "$asc" --class public --kind gpg --format json --path-mode none --gpg-role primary --fail-on-empty)" || {
        echo "error: decernor fingerprint failed on GPG public file: ${asc}" >&2
        return 1
    }
    python3 - "$json" <<'PY'
import json, sys
raw = sys.argv[1]
try:
    recs = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"error: GPG fingerprint JSON is not valid: {e}", file=sys.stderr)
    sys.exit(1)
if not isinstance(recs, list):
    print("error: GPG fingerprint JSON is not an array", file=sys.stderr)
    sys.exit(1)
if len(recs) != 1:
    print(f"error: expected exactly one GPG primary record, got {len(recs)}", file=sys.stderr)
    sys.exit(1)
r = recs[0]
if r.get("class") != "public" or r.get("kind") != "gpg":
    print("error: GPG record is not kind=gpg class=public", file=sys.stderr)
    sys.exit(1)
if r.get("fingerprint_scheme") != "openpgp-fingerprint-v1" or r.get("key_role") != "primary":
    print("error: GPG record is not openpgp-fingerprint-v1 key_role=primary", file=sys.stderr)
    sys.exit(1)
fp = r.get("fingerprint") or ""
if len(fp) != 40 or any(c not in "0123456789ABCDEF" for c in fp):
    print("error: GPG fingerprint is not uppercase 40-hex", file=sys.stderr)
    sys.exit(1)
print(fp)
PY
}

# stdout: lowercase 64-hex minisign public-blob SHA-256
chanvoy_minisign_blob_fp() {
    local bin="$1"
    local pub="$2"
    local json
    json="$("$bin" fingerprint "$pub" --class public --kind minisign --format json --path-mode none --fail-on-empty)" || {
        echo "error: decernor fingerprint failed on minisign public file: ${pub}" >&2
        return 1
    }
    python3 - "$json" <<'PY'
import json, sys
raw = sys.argv[1]
try:
    recs = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"error: minisign fingerprint JSON is not valid: {e}", file=sys.stderr)
    sys.exit(1)
if not isinstance(recs, list):
    print("error: minisign fingerprint JSON is not an array", file=sys.stderr)
    sys.exit(1)
blobs = [
    r for r in recs
    if r.get("fingerprint_scheme") == "minisign-public-blob-sha256-v1"
    and r.get("class") == "public"
    and r.get("kind") == "minisign"
]
if len(blobs) != 1:
    print(f"error: expected exactly one minisign-public-blob-sha256-v1 public record, got {len(blobs)}", file=sys.stderr)
    sys.exit(1)
fp = blobs[0].get("fingerprint") or ""
if len(fp) != 64 or any(c not in "0123456789abcdef" for c in fp):
    print("error: minisign fingerprint is not lowercase 64-hex", file=sys.stderr)
    sys.exit(1)
print(fp)
PY
}
