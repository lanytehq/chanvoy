#!/usr/bin/env bash
# Export public verification data only and prove all four signatures verify.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
directory="${1:-dist/release}"
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" signed-without-keys >/dev/null
[[ -n "${CHANVOY_MINISIGN_PUB:-}" && -f "$CHANVOY_MINISIGN_PUB" &&
	-s "$CHANVOY_MINISIGN_PUB" && ! -L "$CHANVOY_MINISIGN_PUB" ]] || {
	echo 'error: approved minisign public export required' >&2; exit 1;
}
grep -q '^untrusted comment:' "$CHANVOY_MINISIGN_PUB" || { echo 'error: malformed minisign public export' >&2; exit 1; }
if grep -qi 'secret' "$CHANVOY_MINISIGN_PUB"; then echo 'error: secret material forbidden in public export' >&2; exit 1; fi
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
cp "$CHANVOY_MINISIGN_PUB" "$directory/chanvoy.pub"
# Re-export the exact reviewed public pin, not additional keys from a keyring.
git -C "$root" cat-file blob "$commit:docs/security/release-signing-keys.asc" >"$directory/chanvoy.gpg.asc"
chmod 0644 "$directory/chanvoy.pub" "$directory/chanvoy.gpg.asc"
bash "$root/scripts/verify-public-keys.sh" "$directory" >/dev/null
bash "$root/scripts/verify-signatures.sh" "$directory" >/dev/null
echo '[ok] exact public verification exports proven'
