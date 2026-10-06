#!/usr/bin/env bash
# Inspect only approved existing public exports; never generate a key.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-decernor.sh
# shellcheck disable=SC1091
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony
[[ "${CHANVOY_GPG_SIGNING_FINGERPRINT:-}" =~ ^[0-9A-F]{40}$ &&
	"${CHANVOY_PGP_KEY_ID:-}" =~ ^[0-9A-F]{40}!$ && -n "${CHANVOY_MINISIGN_PUB:-}" ]] || {
	echo 'error: approved primary, exact signing subkey and public minisign input required' >&2; exit 1;
}
[[ $# -le 1 ]] || { echo 'error: expected at most one public pin input' >&2; exit 1; }
pin="${1:-$root/docs/security/release-signing-keys.asc}"
[[ -s "$pin" && -f "$pin" && ! -L "$pin" && -s "$CHANVOY_MINISIGN_PUB" &&
	-f "$CHANVOY_MINISIGN_PUB" && ! -L "$CHANVOY_MINISIGN_PUB" ]] || {
	echo 'error: regular nonempty public exports required' >&2; exit 1;
}
for kind in gpg minisign; do
	if [[ "$kind" == gpg ]]; then public="$pin"; else public="$CHANVOY_MINISIGN_PUB"; fi
	if "$RELEASE_DECERNOR_BIN" fingerprint "$public" --kind "$kind" --class private \
		--fail-on-empty --path-mode none >/dev/null 2>&1; then
		echo 'error: private material forbidden in public export' >&2; exit 1;
	else
		[[ "$?" == 3 ]] || { echo 'error: public-only export inspection failed' >&2; exit 1; }
	fi
done
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pg-pin.XXXXXX")"
trap 'gpgconf --homedir "$scratch" --kill all >/dev/null 2>&1 || true; rm -rf "$scratch"' EXIT
chmod 700 "$scratch"
"$RELEASE_DECERNOR_BIN" fingerprint "$CHANVOY_MINISIGN_PUB" --kind minisign --class public --fail-on-empty \
	--path-mode none --format ndjson >"$scratch/minisign-records" 2>/dev/null || {
	echo 'error: malformed minisign public export' >&2; exit 1;
}
python3 - "$scratch/minisign-records" <<'PY'
import json
import pathlib
import sys
records = [json.loads(l) for l in pathlib.Path(sys.argv[1]).read_text().splitlines()]
blobs = [r for r in records if r.get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
if len(blobs) != 1 or blobs[0].get('class') != 'public' or blobs[0].get('confidence') != 'high':
    raise SystemExit('error: exactly one public minisign blob fingerprint required')
PY
if [[ -d "$root/docs/security" ]]; then
	if "$RELEASE_DECERNOR_BIN" fingerprint "$root/docs/security" --class private --fail-on-empty \
		--path-mode none >/dev/null 2>&1; then
		echo 'error: private material forbidden in public pin directory' >&2; exit 1;
	else
		[[ "$?" == 3 ]] || { echo 'error: public pin directory inspection failed' >&2; exit 1; }
	fi
fi
"$RELEASE_DECERNOR_BIN" fingerprint "$pin" --kind gpg --class public --fail-on-empty \
	--path-mode none --format ndjson >"$scratch/records" 2>/dev/null
python3 - "$scratch/records" "$CHANVOY_GPG_SIGNING_FINGERPRINT" "$CHANVOY_PGP_KEY_ID" <<'PY'
import json
import pathlib
import sys
records = [json.loads(l) for l in pathlib.Path(sys.argv[1]).read_text().splitlines()]
primary, selector = sys.argv[2:]
if len(records) != 2 or any(r.get('kind') != 'gpg' or r.get('class') != 'public' for r in records):
    raise SystemExit('error: exactly one public primary and signing subkey required')
if {r.get('key_role'): r.get('fingerprint') for r in records} != {'primary': primary, 'subkey': selector[:-1]}:
    raise SystemExit('error: public pin differs from approved primary or signing subkey')
PY
listing="$(gpg --homedir "$scratch" --batch --show-keys --with-colons --with-subkey-fingerprint "$pin" 2>/dev/null)"
signer="$(awk -F: '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}' <<<"$listing")"
[[ "$signer!" == "$CHANVOY_PGP_KEY_ID" ]] || { echo 'error: selected public subkey must have signing capability' >&2; exit 1; }
if awk -F: '$1=="pub" || $1=="sub" {if ($2 ~ /[erd]/) bad=1} END {exit !bad}' <<<"$listing"; then
	echo 'error: expired, revoked or disabled pinned key forbidden' >&2; exit 1;
fi
echo '[ok] approved existing public primary and signing subkey validated'
