#!/usr/bin/env bash
# Maintainer-only: sign both exact checksum manifests in both required formats.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
require_release_guard "$directory" >/dev/null
bash "$root/scripts/release-verify-staged-data.sh" "$directory" >/dev/null
bash "$root/scripts/validate-release-assets.sh" "$directory" checksummed >/dev/null
bash "$root/scripts/verify-checksums.sh" "$directory" >/dev/null
require_complete_pgp_config
[[ -n "${CHANVOY_MINISIGN_KEY:-}" && -n "${CHANVOY_MINISIGN_PUB:-}" ]] || {
	echo 'error: approved external minisign secret and public inputs required' >&2
	exit 1
}
python3 - "$root" "$CHANVOY_MINISIGN_KEY" "$CHANVOY_MINISIGN_PUB" "$CHANVOY_GPG_HOMEDIR" <<'PY'
import pathlib
import sys
root, secret, public, home = (pathlib.Path(p) for p in sys.argv[1:])
for path in (secret, public, home):
    if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
        raise SystemExit('error: explicit absolute nonsymlink signing inputs required')
if not secret.is_file() or not public.is_file() or not home.is_dir():
    raise SystemExit('error: configured signing inputs unavailable')
if any(p.resolve().is_relative_to(root) for p in (secret, home)):
    raise SystemExit('error: private signing inputs must be outside the repository')
PY
listing="$(gpg --homedir "$CHANVOY_GPG_HOMEDIR" --batch --with-colons --fingerprint \
	--with-subkey-fingerprint --list-keys "${CHANVOY_PGP_KEY_ID%!}" 2>/dev/null)" || {
	echo 'error: selected signing subkey unavailable' >&2; exit 1;
}
primary="$(awk -F: '$1=="pub" {p=1;next} p && $1=="fpr" {print $10;exit}' <<<"$listing")"
subkey="$(awk -F: -v selected="${CHANVOY_PGP_KEY_ID%!}" \
	'$1=="sub" {s=1;cap=$12;valid=$2;next} s && $1=="fpr" {if ($10==selected && cap ~ /s/ && valid !~ /[erd]/) print $10; s=0}' <<<"$listing")"
[[ "$primary" == "$CHANVOY_GPG_SIGNING_FINGERPRINT" && "$subkey!" == "$CHANVOY_PGP_KEY_ID" ]] || {
	echo 'error: selected live signing subkey must belong to approved primary' >&2; exit 1;
}
# Public inputs must match the tagged trust root before any signature is made.
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-sign-preflight.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
commit="$(awk -F= '$1=="commit" {print $2}' "$directory.anchor")"
git -C "$root" cat-file blob "$commit:docs/security/release-signing-keys.asc" >"$scratch/pin.asc"
# shellcheck source=release-decernor.sh
# shellcheck disable=SC1091
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony
"$RELEASE_DECERNOR_BIN" fingerprint verify --anchors "$directory/expected-fingerprints.txt" \
	--anchors-ndjson "$directory/expected-fingerprints.ndjson" --gpg "$scratch/pin.asc" \
	--minisign "$CHANVOY_MINISIGN_PUB" >/dev/null 2>&1 || { echo 'error: signing public inputs differ from tagged anchors' >&2; exit 1; }
permitted="$(gpg --homedir "$scratch" --batch --with-colons --with-subkey-fingerprint --show-keys "$scratch/pin.asc" 2>/dev/null |
	awk -F: '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}')"
[[ "$permitted!" == "$CHANVOY_PGP_KEY_ID" ]] || { echo 'error: signing selector differs from reviewed pin' >&2; exit 1; }
require_release_guard "$directory" >/dev/null
for manifest in SHA256SUMS SHA512SUMS; do
	printf '[info] Signing %s in both formats; enter a passphrase if prompted.\n' "$manifest"
	minisign -S -s "$CHANVOY_MINISIGN_KEY" -m "$directory/$manifest" \
		-t "chanvoy $tag" -x "$directory/$manifest.minisig" >/dev/null 2>&1 || {
		echo 'error: minisign signing failed; inspect partial local outputs before retrying' >&2; exit 1;
	}
	gpg --homedir "$CHANVOY_GPG_HOMEDIR" --batch --armor --detach-sign \
		--local-user "$CHANVOY_PGP_KEY_ID" --output "$directory/$manifest.asc" \
		"$directory/$manifest" >/dev/null 2>&1 || {
		echo 'error: GPG signing failed; inspect partial local outputs before retrying' >&2; exit 1;
	}
done
cp "$directory/SHA256SUMS.asc" "$directory/checksums.txt.asc"
inventory="$(release_binary_signatures)"
while IFS= read -r signature; do
	minisign -S -s "$CHANVOY_MINISIGN_KEY" -m "$directory/${signature%.minisig}" \
		-t "chanvoy $tag" -x "$directory/$signature" >/dev/null 2>&1 || {
		echo 'error: binary signing failed; inspect partial local outputs before retrying' >&2; exit 1;
	}
done <<<"$inventory"
bash "$root/scripts/validate-release-assets.sh" "$directory" signed-without-keys >/dev/null
echo '[ok] both checksum manifests signed in both formats'
