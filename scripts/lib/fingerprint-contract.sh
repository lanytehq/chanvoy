# Shared helpers for the decernor 0.1.8 fingerprint contract.
# Sourced by insert-expected-fingerprints.sh and verify-public-keys.sh.
# Requires bash, python3, and a decernor >= 0.1.8 on PATH (or $DECERNOR).

# The standalone legacy TXT parser/inserter shares the ceremony tool floor.
# It is retained for explicit historical exports, not release staging.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/release-decernor.sh"

chanvoy_refuse_private() {
    if grep -Eqi 'PRIVATE|SECRET|BEGIN PGP PRIVATE KEY|minisign secret key' "$1"; then
        echo 'error: key file appears to contain private material' >&2
        return 1
    fi
}

chanvoy_require_decernor() {
    resolve_release_decernor general || return 1
    printf '%s\n' "$RELEASE_DECERNOR_BIN"
}

chanvoy_preflight_decernor() {
    local bin
    bin="$(chanvoy_require_decernor)" || return 1
    "$bin" version
}

# stdout: minisign<TAB>gpg
# Rejects duplicates, unknown algos, extra fields. Values may still be TBD-*.
chanvoy_read_expected_contract() {
    python3 - "$1" <<'PY'
import pathlib, re, sys

path = pathlib.Path(sys.argv[1])
minisign = None
gpg = None
for lineno, raw in enumerate(path.read_text().splitlines(), 1):
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    parts = line.split()
    if len(parts) != 2:
        print(
            f"error: expected contract line {lineno} must be '<algo> <fingerprint>' with no extra fields",
            file=sys.stderr,
        )
        sys.exit(1)
    algo, value = parts
    if algo == "minisign":
        if minisign is not None:
            print("error: duplicate minisign line in expected-fingerprints", file=sys.stderr)
            sys.exit(1)
        minisign = value
    elif algo == "gpg":
        if gpg is not None:
            print("error: duplicate gpg line in expected-fingerprints", file=sys.stderr)
            sys.exit(1)
        gpg = value
    else:
        print(
            f"error: unknown algorithm {algo!r} on expected contract line {lineno}",
            file=sys.stderr,
        )
        sys.exit(1)

if minisign is None:
    print("error: no 'minisign' line in expected-fingerprints", file=sys.stderr)
    sys.exit(1)
if gpg is None:
    print("error: no 'gpg' line in expected-fingerprints", file=sys.stderr)
    sys.exit(1)

def classify(kind, value):
    if value.startswith("TBD-"):
        return "tbd"
    if kind == "minisign" and re.fullmatch(r"[0-9a-f]{64}", value):
        return "ok"
    if kind == "gpg" and re.fullmatch(r"[0-9A-F]{40}", value):
        return "ok"
    print(
        f"error: {kind} fingerprint is not a well-formed contract token",
        file=sys.stderr,
    )
    sys.exit(1)

mini_kind = classify("minisign", minisign)
gpg_kind = classify("gpg", gpg)
if mini_kind == "tbd" or gpg_kind == "tbd":
    print("TBD", minisign, gpg)
    sys.exit(2)

print(f"{minisign}\t{gpg}")
PY
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
