#!/usr/bin/env bash
# Maintainer-only derivation from approved public exports, never donor key values.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
[[ $# == 0 || ( $# == 2 && "$1" == --rotate-from ) ]] || {
	echo 'usage: release-insert-anchors.sh [--rotate-from <current-txt-sha256>]' >&2; exit 2;
}
expected="${2:-}"
current="$root/keys/expected-fingerprints.txt"
if [[ -e "$current" || -L "$current" || -e "$root/keys/expected-fingerprints.ndjson" ]]; then
	[[ -f "$current" && ! -L "$current" && "$expected" =~ ^[0-9a-f]{64}$ &&
		"$(shasum -a 256 "$current" | awk '{print $1}')" == "$expected" ]] || {
		echo 'error: existing anchors require explicit --rotate-from matching reviewed TXT SHA256' >&2; exit 1;
	}
elif [[ -n "$expected" ]]; then
	echo 'error: rotation requires existing anchors' >&2; exit 1
fi
bash "$root/scripts/release-validate-pin.sh" >/dev/null
# shellcheck source=release-decernor.sh
# shellcheck disable=SC1091
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pg-anchor.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
pin="$root/docs/security/release-signing-keys.asc"
"$RELEASE_DECERNOR_BIN" fingerprint "$pin" --class public --kind gpg --format ndjson \
	--path-mode none --gpg-role primary >"$scratch/gpg.ndjson" 2>/dev/null
"$RELEASE_DECERNOR_BIN" fingerprint "$CHANVOY_MINISIGN_PUB" --class public --kind minisign \
	--format ndjson --path-mode none >"$scratch/minisign.ndjson" 2>/dev/null
python3 - "$scratch" <<'PY'
import json
import pathlib
import sys
base = pathlib.Path(sys.argv[1])
g = [json.loads(l) for l in (base / 'gpg.ndjson').read_text().splitlines()]
m = [json.loads(l) for l in (base / 'minisign.ndjson').read_text().splitlines()
     if json.loads(l).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
if len(g) != 1 or len(m) != 1 or g[0].get('key_role') != 'primary':
    raise SystemExit('error: exactly one primary GPG and minisign blob record required')
(base / 'expected-fingerprints.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(base / 'expected-fingerprints.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
PY
bash "$root/scripts/validate-release-anchors.sh" "$scratch/expected-fingerprints.txt" "$pin" \
	"$scratch/expected-fingerprints.ndjson" >/dev/null
"$RELEASE_DECERNOR_BIN" fingerprint verify --anchors "$scratch/expected-fingerprints.txt" \
	--anchors-ndjson "$scratch/expected-fingerprints.ndjson" --gpg "$pin" \
	--minisign "$CHANVOY_MINISIGN_PUB" >/dev/null 2>&1
if [[ -n "$expected" ]]; then
	[[ "$(shasum -a 256 "$current" | awk '{print $1}')" == "$expected" ]] || {
		echo 'error: prior anchors changed during derivation' >&2; exit 1;
	}
fi
bash "$root/scripts/install-release-anchors.sh" "$scratch" "$root/keys" \
	"$RELEASE_DECERNOR_BIN" fingerprint verify --anchors "$root/keys/expected-fingerprints.txt" \
	--anchors-ndjson "$root/keys/expected-fingerprints.ndjson" --gpg "$pin" --minisign "$CHANVOY_MINISIGN_PUB" >/dev/null 2>&1
echo '[ok] reviewed public fingerprint pair derived for independent review'
