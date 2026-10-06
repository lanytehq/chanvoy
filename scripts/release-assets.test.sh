#!/usr/bin/env bash
# Exact binary inventory and checksum regression corpus, synthetic assets only.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$SCRIPT_DIR/.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-release-assets.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
export CHANVOY_RELEASE_TAG=v0.1.0
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/release-common.sh"
expect_fail() {
	if "$@" >"$fixture/output" 2>&1; then
		echo 'error: expected inventory/checksum rejection' >&2
		exit 1
	fi
}
directory="$fixture/assets"
mkdir "$directory"
# Independent oracle: do not derive the fixture from the producer under test.
base=(LICENSE-APACHE LICENSE-MIT sbom-0.1.0.cdx.json
	chanvoy-v0.1.0-linux-x86_64 chanvoy-v0.1.0-linux-aarch64 chanvoy-v0.1.0-macos-aarch64)
signable=("${base[@]}" release-notes-v0.1.0.md expected-fingerprints.txt expected-fingerprints.ndjson)
provenance=(SHA256SUMS SHA512SUMS SHA256SUMS.minisig SHA512SUMS.minisig
	SHA256SUMS.asc SHA512SUMS.asc checksums.txt checksums.txt.asc chanvoy.pub chanvoy.gpg.asc
	chanvoy-v0.1.0-linux-x86_64.minisig chanvoy-v0.1.0-linux-aarch64.minisig chanvoy-v0.1.0-macos-aarch64.minisig)
for asset in "${base[@]}"; do printf 'synthetic\n' >"$directory/$asset"; done
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base >/dev/null
expect_fail env CHANVOY_RELEASE_TAG=v0.1.1 bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base
printf 'extra\n' >"$directory/foreign.txt"
expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base
rm "$directory/foreign.txt"
for asset in "${base[@]}"; do
	mv "$directory/$asset" "$fixture/saved"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base
	ln -s "$fixture/saved" "$directory/$asset"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base
	rm "$directory/$asset"
	mkdir "$directory/$asset"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" base
	rmdir "$directory/$asset"
	mv "$fixture/saved" "$directory/$asset"
done
for asset in release-notes-v0.1.0.md expected-fingerprints.txt expected-fingerprints.ndjson; do
	printf 'synthetic\n' >"$directory/$asset"
done
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signable >/dev/null
for asset in release-notes-v0.1.0.md expected-fingerprints.txt expected-fingerprints.ndjson; do
	mv "$directory/$asset" "$fixture/saved"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signable
	mv "$fixture/saved" "$directory/$asset"
done
(
	cd "$directory"
	printf '%s\n' "${signable[@]}" | LC_ALL=C sort | xargs shasum -a 256 >SHA256SUMS
	printf '%s\n' "${signable[@]}" | LC_ALL=C sort | xargs shasum -a 512 >SHA512SUMS
	cp SHA256SUMS checksums.txt
)
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" checksummed >/dev/null
bash "$SCRIPT_DIR/verify-checksums.sh" "$directory" >/dev/null
for asset in "${signable[@]}"; do
	cp "$directory/$asset" "$fixture/saved"
	printf 'changed\n' >>"$directory/$asset"
	expect_fail bash "$SCRIPT_DIR/verify-checksums.sh" "$directory"
	mv "$fixture/saved" "$directory/$asset"
done
for manifest in SHA256SUMS SHA512SUMS; do
	cp "$directory/$manifest" "$fixture/manifest"
	head -n 1 "$fixture/manifest" >>"$directory/$manifest"
	expect_fail bash "$SCRIPT_DIR/verify-checksums.sh" "$directory"
	sed '1d' "$fixture/manifest" >"$directory/$manifest"
	expect_fail bash "$SCRIPT_DIR/verify-checksums.sh" "$directory"
	sed '1s#  #  nested/#' "$fixture/manifest" >"$directory/$manifest"
	expect_fail bash "$SCRIPT_DIR/verify-checksums.sh" "$directory"
	sed '1s/^[^ ]*/bad/' "$fixture/manifest" >"$directory/$manifest"
	expect_fail bash "$SCRIPT_DIR/verify-checksums.sh" "$directory"
	cp "$fixture/manifest" "$directory/$manifest"
done
for asset in SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc checksums.txt.asc \
	chanvoy-v0.1.0-linux-x86_64.minisig chanvoy-v0.1.0-linux-aarch64.minisig chanvoy-v0.1.0-macos-aarch64.minisig; do
	printf 'synthetic signature\n' >"$directory/$asset"
done
unset CHANVOY_PGP_KEY_ID CHANVOY_GPG_HOMEDIR CHANVOY_GPG_SIGNING_FINGERPRINT
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signed-without-keys >/dev/null
for asset in SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc; do
	mv "$directory/$asset" "$fixture/saved"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signed-without-keys
	mv "$fixture/saved" "$directory/$asset"
done
for asset in chanvoy.pub chanvoy.gpg.asc; do
	printf 'synthetic public\n' >"$directory/$asset"
done
bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signed >/dev/null
bash "$SCRIPT_DIR/verify-checksums.sh" "$directory" >/dev/null
for asset in "${provenance[@]}"; do
	mv "$directory/$asset" "$fixture/saved"
	expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signed
	mv "$fixture/saved" "$directory/$asset"
done
# A staging receipt inside the upload set must always be rejected.
printf 'receipt\n' >"$directory/staging.anchor"
expect_fail bash "$SCRIPT_DIR/validate-release-assets.sh" "$directory" signed
rm "$directory/staging.anchor"
# Inventory remains cut-specific when the current checkout VERSION differs.
mkdir -p "$fixture/trusted/scripts" "$fixture/trusted/config/release"
cp "$SCRIPT_DIR/release-common.sh" "$fixture/trusted/scripts/"
cp "$root/config/release/binary-platforms.txt" "$fixture/trusted/config/release/"
printf '9.9.9\n' >"$fixture/trusted/VERSION"
bash -c 'source "$1"; [[ "$(release_version)" == 0.1.0 ]]; assert_exact_directory_inventory "$2" release_signed_assets' \
	_ "$fixture/trusted/scripts/release-common.sh" "$directory"
echo '[ok] exact release inventories, mandatory four signatures and cut-specific checksums'
