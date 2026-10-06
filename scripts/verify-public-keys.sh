#!/usr/bin/env bash
# Verify public-only release exports against independently trusted tagged anchors.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-decernor.sh
# shellcheck disable=SC1091
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony
directory="${1:-dist/release}"
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
minisign_public="$directory/chanvoy.pub"
pgp_public="$directory/chanvoy.gpg.asc"
for public in "$minisign_public" "$pgp_public"; do
	[[ -f "$public" && -s "$public" && ! -L "$public" ]] || {
		echo 'error: exported public key is missing or unsafe' >&2
		exit 1
	}
done
grep -q '^untrusted comment:' "$minisign_public" || { echo 'error: malformed minisign public export' >&2; exit 1; }
grep -q '^-----BEGIN PGP PUBLIC KEY BLOCK-----$' "$pgp_public" || { echo 'error: malformed GPG public export' >&2; exit 1; }
if grep -qi 'secret' "$minisign_public" || grep -q 'PRIVATE KEY BLOCK' "$pgp_public"; then
	echo 'error: private material forbidden in public exports' >&2
	exit 1
fi
for kind in gpg minisign; do
	if [[ "$kind" == gpg ]]; then public="$pgp_public"; else public="$minisign_public"; fi
	if "$RELEASE_DECERNOR_BIN" fingerprint "$public" --kind "$kind" --class private \
		--fail-on-empty --path-mode none >/dev/null 2>&1; then
		echo 'error: private material forbidden in public exports' >&2
		exit 1
	else
		[[ "$?" == 3 ]] || { echo 'error: public-only inspection failed' >&2; exit 1; }
	fi
done
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
# The public export must be exactly the reviewed pin, not an enlarged keyring
# export that quietly grants an additional signing subkey.
pin="$(mktemp "${TMPDIR:-/tmp}/chanvoy-public-pin.XXXXXX")"
trap 'rm -f "$pin"' EXIT
git -C "$root" cat-file blob "$commit:docs/security/release-signing-keys.asc" >"$pin"
cmp -s "$pin" "$pgp_public" || { echo 'error: public GPG export differs from reviewed tagged pin' >&2; exit 1; }
"$RELEASE_DECERNOR_BIN" fingerprint verify \
	--anchors "$directory/expected-fingerprints.txt" --anchors-ndjson "$directory/expected-fingerprints.ndjson" \
	--gpg "$pgp_public" --minisign "$minisign_public" >/dev/null 2>&1 || {
	echo 'error: public exports differ from trusted tagged fingerprint pair' >&2
	exit 1
}
echo '[ok] public-only exports match the independently trusted tagged anchors'
