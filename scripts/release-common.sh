#!/usr/bin/env bash
# Shared release asset and repository invariants.
set -euo pipefail

CHANVOY_REPOSITORY="lanytehq/chanvoy"

release_repo_root() {
	cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P
}

release_version() {
	local version
	# Post-tag inventory is keyed by the canonical cut, not advancing main.
	if [[ -n "${CHANVOY_RELEASE_TAG:-}" ]]; then
		version="${CHANVOY_RELEASE_TAG#v}"
		[[ "$CHANVOY_RELEASE_TAG" == "v$version" ]] || return 1
	else
		version="$(cat "$(release_repo_root)/VERSION")"
	fi
	if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
		echo 'error: one stable semantic version is required' >&2
		return 1
	fi
	printf '%s\n' "$version"
}

release_tag() {
	local tag="${1:-${CHANVOY_RELEASE_TAG:-}}"
	if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
		echo 'error: canonical CHANVOY_RELEASE_TAG is required' >&2
		return 1
	fi
	if [[ -n "${CHANVOY_RELEASE_TAG:-}" && "$tag" != "$CHANVOY_RELEASE_TAG" ]]; then
		echo 'error: conflicting release tag inputs' >&2
		return 1
	fi
	if [[ -n "${RELEASE_TAG:-}" && "$tag" != "$RELEASE_TAG" ]]; then
		echo 'error: release tag aliases disagree' >&2
		return 1
	fi
	printf '%s\n' "$tag"
}

require_release_guard() {
	# Direct mutation entrypoints must validate the complete staged receipt.
	# The verifier runs from this trusted checkout, never the tagged commit.
	"$(release_repo_root)/scripts/release-verify-staged.sh" "${1:-dist/release}"
}

release_platform_file() {
	printf '%s\n' "$(release_repo_root)/config/release/binary-platforms.txt"
}

release_binary_asset_name() {
	local version="$1" platform="$2" binary="$3"
	[[ "$binary" == chanvoy ]] || return 1
	printf 'chanvoy-v%s-%s\n' "$version" "$platform"
}

release_base_assets() {
	local version platform _target binary
	version="$(release_version)" || return 1
	printf '%s\n' LICENSE-APACHE LICENSE-MIT "sbom-${version}.cdx.json"
	while read -r platform _target binary; do
		[[ -n "$platform" ]] || continue
		release_binary_asset_name "$version" "$platform" "$binary"
	done <"$(release_platform_file)"
}

release_signable_assets() {
	local tag
	tag="$(release_tag)" || return 1
	release_base_assets || return 1
	printf 'release-notes-%s.md\n' "$tag"
	printf '%s\n' expected-fingerprints.txt expected-fingerprints.ndjson
}

release_provenance_assets() {
	# Both signature formats are mandatory, independent of signing environment.
	printf '%s\n' SHA256SUMS SHA256SUMS.minisig SHA256SUMS.asc \
		SHA512SUMS SHA512SUMS.minisig SHA512SUMS.asc \
		checksums.txt checksums.txt.asc chanvoy.pub chanvoy.gpg.asc
	release_binary_signatures
}

release_binary_signatures() {
	local version platform _target binary
	version="$(release_version)" || return 1
	while read -r platform _target binary; do
		[[ -n "$platform" ]] || continue
		printf '%s.minisig\n' "$(release_binary_asset_name "$version" "$platform" "$binary")"
	done <"$(release_platform_file)"
}

release_checksummed_assets() {
	release_signable_assets || return 1
	printf '%s\n' SHA256SUMS SHA512SUMS checksums.txt
}

release_signed_without_keys_assets() {
	release_checksummed_assets || return 1
	printf '%s\n' SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc checksums.txt.asc
	release_binary_signatures
}

release_signed_assets() {
	release_signable_assets || return 1
	release_provenance_assets
}

require_complete_pgp_config() {
	if [[ ! "${CHANVOY_PGP_KEY_ID:-}" =~ ^[0-9A-F]{40}!$ ||
		! "${CHANVOY_GPG_SIGNING_FINGERPRINT:-}" =~ ^[0-9A-F]{40}$ ||
		-z "${CHANVOY_GPG_HOMEDIR:-}" ]]; then
		echo 'error: approved primary, exact signing subkey and external GPG home required' >&2
		return 1
	fi
}

assert_exact_directory_inventory() (
	local directory="$1" producer="$2" expected_file actual_file
	if [[ ! -d "$directory" || -L "$directory" ]]; then
		echo 'error: release directory is absent or unsafe' >&2
		return 1
	fi
	expected_file="$(mktemp "${TMPDIR:-/tmp}/chanvoy-expected.XXXXXX")"
	actual_file="$(mktemp "${TMPDIR:-/tmp}/chanvoy-actual.XXXXXX")"
	trap 'rm -f "$expected_file" "$actual_file"' EXIT
	# Capture before sorting so producer failures cannot disappear in a pipeline.
	"$producer" >"$expected_file" || return 1
	LC_ALL=C sort "$expected_file" -o "$expected_file"
	find "$directory" -mindepth 1 -maxdepth 1 -print |
		while IFS= read -r entry; do basename "$entry"; done |
		LC_ALL=C sort >"$actual_file"
	if ! cmp -s "$expected_file" "$actual_file"; then
		echo 'error: release directory inventory mismatch' >&2
		diff -u "$expected_file" "$actual_file" >&2 || true
		return 1
	fi
	while IFS= read -r name; do
		if [[ ! -f "$directory/$name" || -L "$directory/$name" ]]; then
			echo 'error: release asset is not a regular file' >&2
			return 1
		fi
	done <"$expected_file"
)

assert_github_release_state() (
	local expected_assets_producer="$1" tag root tag_commit
	tag="$(release_tag)" || return 1
	root="$(release_repo_root)"
	tag_commit="$(git -C "$root" rev-parse "refs/tags/${tag}^{}")" || return 1
	if [[ -n "${GH_REPO+x}" ]]; then
		echo 'error: GH_REPO must be unset; repository authority is fixed' >&2
		return 1
	fi
	for command_name in gh jq; do
		command -v "$command_name" >/dev/null 2>&1 || {
			echo 'error: required release command is unavailable' >&2
			return 1
		}
	done
	if [[ "$(gh repo view "$CHANVOY_REPOSITORY" --json nameWithOwner --jq .nameWithOwner)" != "$CHANVOY_REPOSITORY" ]]; then
		echo 'error: GitHub repository identity mismatch' >&2
		return 1
	fi
	local state expected actual
	state="$(mktemp "${TMPDIR:-/tmp}/chanvoy-release-state.XXXXXX")"
	expected="$(mktemp "${TMPDIR:-/tmp}/chanvoy-remote-expected.XXXXXX")"
	actual="$(mktemp "${TMPDIR:-/tmp}/chanvoy-remote-actual.XXXXXX")"
	trap 'rm -f "$state" "$expected" "$actual"' EXIT
	gh release view "$tag" --repo "$CHANVOY_REPOSITORY" \
		--json tagName,targetCommitish,isDraft,assets >"$state" || return 1
	if [[ "$(jq -r .tagName "$state")" != "$tag" ||
		"$(jq -r .targetCommitish "$state")" != "$tag_commit" ||
		"$(jq -r .isDraft "$state")" != true ]]; then
		echo 'error: GitHub release tag, target, or draft state mismatch' >&2
		return 1
	fi
	"$expected_assets_producer" >"$expected" || return 1
	LC_ALL=C sort "$expected" -o "$expected"
	jq -r '.assets[].name' "$state" | LC_ALL=C sort >"$actual"
	if ! cmp -s "$expected" "$actual"; then
		echo 'error: remote draft asset inventory mismatch' >&2
		return 1
	fi
)
