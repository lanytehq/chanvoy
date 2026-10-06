#!/usr/bin/env bash
# Cross-check the Decernor pair and the pinned public primary as inert data.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
txt="${1:-$root/keys/expected-fingerprints.txt}"
pin="${2:-$root/docs/security/release-signing-keys.asc}"
ndjson="${3:-$root/keys/expected-fingerprints.ndjson}"
# shellcheck source=release-decernor.sh
# shellcheck disable=SC1091
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony
temp="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-anchor-check.XXXXXX")"
trap 'gpgconf --homedir "$temp/home" --kill all >/dev/null 2>&1 || true; rm -rf "$temp"' EXIT
mkdir -m 700 "$temp/home"
python3 - "$txt" "$ndjson" "$temp" <<'PY'
import json
import pathlib
import re
import sys
txt, ndjson, scratch = (pathlib.Path(p) for p in sys.argv[1:])
if not txt.is_file() or not ndjson.is_file() or txt.is_symlink() or ndjson.is_symlink():
    raise SystemExit('error: committed fingerprint pair missing')
try:
    lines = txt.read_text().splitlines()
    records = [json.loads(line) for line in ndjson.read_text().splitlines()]
except (ValueError, UnicodeError):
    raise SystemExit('error: invalid public fingerprint encoding') from None
if len(lines) != 2 or len(records) != 2 or any(not isinstance(r, dict) for r in records):
    raise SystemExit('error: expected exactly two fingerprint lines and records')
g, m = records
if g.get('fingerprint_scheme') != 'openpgp-fingerprint-v1' or g.get('key_role') != 'primary':
    raise SystemExit('error: expected a primary GPG record')
if m.get('fingerprint_scheme') != 'minisign-public-blob-sha256-v1':
    raise SystemExit('error: expected a minisign blob record')
if any(r.get('class') != 'public' or r.get('confidence') != 'high' or 'path' in r or 'reason' in r for r in records):
    raise SystemExit('error: expected path-free high-confidence public records')
if not isinstance(g.get('fingerprint'), str) or not re.fullmatch('[0-9A-F]{40}', g['fingerprint']):
    raise SystemExit('error: malformed GPG fingerprint')
if not isinstance(m.get('fingerprint'), str) or not re.fullmatch('[0-9a-f]{64}', m['fingerprint']):
    raise SystemExit('error: malformed minisign fingerprint')
if g.get('key_id') != g['fingerprint'][-16:]:
    raise SystemExit('error: GPG key ID differs from primary fingerprint')
if lines != ['gpg ' + g['fingerprint'], 'minisign ' + m['fingerprint']]:
    raise SystemExit('error: fingerprint text and records differ')
for name, record in zip(('gpg', 'minisign'), records):
    (scratch / (name + '.json')).write_text(json.dumps(record) + '\n')
PY
for kind in gpg minisign; do
	"$RELEASE_DECERNOR_BIN" validate --schema "$root/schemas/release/v0/fingerprint-record.schema.json" \
		--data "$temp/$kind.json" >/dev/null 2>&1 || {
		echo 'error: public fingerprint record failed offline schema validation' >&2
		exit 1
	}
done
[[ -f "$pin" && -s "$pin" && ! -L "$pin" ]] || {
	echo 'error: committed public pin missing' >&2
	exit 1
}
grep -q '^-----BEGIN PGP PUBLIC KEY BLOCK-----$' "$pin" || {
	echo 'error: public pin must be an armored public export' >&2
	exit 1
}
if grep -q 'PRIVATE KEY BLOCK' "$pin"; then
	echo 'error: private material forbidden in public pin' >&2
	exit 1
fi
# Decernor distinguishes no private records (3) from inspection failures.
if "$RELEASE_DECERNOR_BIN" fingerprint "$pin" --kind gpg --class private \
	--fail-on-empty --path-mode none >/dev/null 2>&1; then
	echo 'error: private material forbidden in public pin' >&2
	exit 1
else
	[[ "$?" == 3 ]] || { echo 'error: public-only pin inspection failed' >&2; exit 1; }
fi
GNUPGHOME="$temp/home" gpg --batch --quiet --import "$pin" >/dev/null 2>&1 || {
	echo 'error: public pin import failed' >&2
	exit 1
}
listing="$(GNUPGHOME="$temp/home" gpg --batch --with-colons --fingerprint --list-keys 2>/dev/null)"
[[ "$(awk -F: '$1=="pub" {n++} END {print n+0}' <<<"$listing")" == 1 ]] || {
	echo 'error: pin must contain exactly one public primary' >&2
	exit 1
}
[[ "$(awk -F: '$1=="fpr" {print $10;exit}' <<<"$listing")" == "$(awk '$1=="gpg" {print $2}' "$txt")" ]] || {
	echo 'error: primary pin differs from fingerprint pair' >&2
	exit 1
}
echo '[ok] public pin and Decernor anchors agree'
