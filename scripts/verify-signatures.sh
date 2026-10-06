#!/usr/bin/env bash
# Verify both detached formats on both manifests in an isolated keyring.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
directory="${1:-dist/release}"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
bash "$root/scripts/validate-release-assets.sh" "$directory" signed >/dev/null
bash "$root/scripts/verify-checksums.sh" "$directory" >/dev/null
bash "$root/scripts/verify-public-keys.sh" "$directory" >/dev/null
temporary_gpg="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-signature-check.XXXXXX")"
trap 'gpgconf --homedir "$temporary_gpg" --kill all >/dev/null 2>&1 || true; rm -rf "$temporary_gpg"' EXIT
chmod 700 "$temporary_gpg"
gpg --homedir "$temporary_gpg" --batch --import "$directory/chanvoy.gpg.asc" >/dev/null 2>&1 || {
	echo 'error: public signing pin import failed' >&2
	exit 1
}
primary="$(awk '$1=="gpg" {print $2}' "$directory/expected-fingerprints.txt")"
signer="$(gpg --homedir "$temporary_gpg" --batch --with-colons --with-subkey-fingerprint --list-keys 2>/dev/null |
	awk -F: '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}')"
[[ "$signer" =~ ^[0-9A-F]{40}$ && "$primary" =~ ^[0-9A-F]{40}$ && "$signer" != "$primary" ]] || {
	echo 'error: reviewed pin must hold one signing subkey' >&2
	exit 1
}
for manifest in SHA256SUMS SHA512SUMS; do
	minisign -Vm "$directory/$manifest" -p "$directory/chanvoy.pub" \
		-x "$directory/$manifest.minisig" >/dev/null 2>&1 || {
		echo 'error: required minisign manifest signature invalid' >&2
		exit 1
	}
	gpg --homedir "$temporary_gpg" --batch --status-fd 1 \
		--verify "$directory/$manifest.asc" "$directory/$manifest" >"$temporary_gpg/status" 2>/dev/null || {
		echo 'error: required GPG manifest signature invalid' >&2
		exit 1
	}
	awk -v primary="$primary" -v permitted="$signer" '
      $1=="[GNUPG:]" && $2=="VALIDSIG" {valid++; subkey=$3; signer_primary=$NF}
      $1=="[GNUPG:]" && $2=="GOODSIG" {good++}
      $1=="[GNUPG:]" && $2 ~ /^(EXPKEYSIG|EXPSIG|REVKEYSIG|KEYREVOKED|BADSIG|ERRSIG)$/ {bad++}
      END {if (bad || good!=1 || valid!=1 || signer_primary!=primary || subkey!=permitted) exit 1}
    ' "$temporary_gpg/status" || { echo 'error: manifest signer, expiry or revocation check failed' >&2; exit 1; }
done
cmp -s "$directory/checksums.txt.asc" "$directory/SHA256SUMS.asc" || {
	echo 'error: legacy checksum signature differs from manifest signature' >&2; exit 1;
}
inventory="$(release_binary_signatures)"
while IFS= read -r signature; do
	minisign -Vm "$directory/${signature%.minisig}" -p "$directory/chanvoy.pub" \
		-x "$directory/$signature" >/dev/null 2>&1 || {
		echo 'error: required per-binary minisign signature invalid' >&2; exit 1;
	}
done <<<"$inventory"
echo '[ok] all four required manifest signatures verified'
