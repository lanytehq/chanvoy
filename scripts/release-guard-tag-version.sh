#!/usr/bin/env bash
# Fail-closed guards for the local signed-tag release ceremony.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${script_dir}/lib/fingerprint-contract.sh"

usage() {
	cat >&2 <<'EOF'
Usage: release-guard-tag-version.sh <pre-create|post-create|pre-push>

Environment:
  CHANVOY_RELEASE_TAG  Ceremony tag override (must equal v$(cat VERSION))
  RELEASE_TAG          Later release-target override; must agree when both are set
  CHANVOY_PGP_KEY_ID   Required for post-create and pre-push signature checks
  CHANVOY_GPG_HOMEDIR  Isolated release keyring for signature checks
EOF
}

fail() {
	echo "error: $*" >&2
	exit 1
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" ||
	fail "not inside a git repository"
cd "$repo_root"

mode="${1:-}"
case "$mode" in
pre-create | post-create | pre-push) ;;
*)
	usage
	exit 2
	;;
esac

[ -f VERSION ] || fail "VERSION file not found"
version="$(tr -d '[:space:]' <VERSION)"
[ -n "$version" ] || fail "VERSION is empty"
expected_tag="v${version}"

chanvoy_tag="${CHANVOY_RELEASE_TAG:-}"
release_tag="${RELEASE_TAG:-}"
if [ -n "$chanvoy_tag" ] && [ -n "$release_tag" ] && [ "$chanvoy_tag" != "$release_tag" ]; then
	fail "CHANVOY_RELEASE_TAG (${chanvoy_tag}) and RELEASE_TAG (${release_tag}) disagree"
fi
tag="${chanvoy_tag:-${release_tag:-$expected_tag}}"
[ "$tag" = "$expected_tag" ] ||
	fail "release tag ${tag} does not match VERSION ${version} (expected ${expected_tag})"

branch="$(git branch --show-current)"
[ "$branch" = "main" ] || fail "release tagging requires main (current branch: ${branch:-detached})"
[ -z "$(git status --porcelain=v1)" ] || fail "release tagging requires a clean working tree"

head="$(git rev-parse HEAD)"

remote_main="$(
	git ls-remote --exit-code --heads origin refs/heads/main 2>/dev/null |
		awk '$2 == "refs/heads/main" { count += 1; sha = $1 } END { if (count == 1) print sha }'
)" || fail "could not query origin for refs/heads/main"
[ -n "$remote_main" ] ||
	fail "origin did not return exactly one refs/heads/main"
[ "$head" = "$remote_main" ] ||
	fail "HEAD (${head}) is not the exact live origin main commit (${remote_main})"

remote_tag_state() {
	local output status
	set +e
	output="$(git ls-remote --exit-code --tags origin "refs/tags/${tag}" 2>/dev/null)"
	status=$?
	set -e
	case "$status" in
	0)
		[ -n "$output" ] || fail "origin tag query succeeded without a ref for ${tag}"
		return 0
		;;
	2) return 1 ;;
	*) fail "could not query origin for ${tag}" ;;
	esac
}

require_origin_absent() {
	if remote_tag_state; then
		fail "tag ${tag} already exists on origin"
	fi
}

require_exact_signed_tag() {
	[ "$(git cat-file -t "refs/tags/${tag}" 2>/dev/null || true)" = "tag" ] ||
		fail "${tag} is missing or is not an annotated tag"

	peeled="$(git rev-list -n 1 "$tag")"
	[ "$peeled" = "$head" ] ||
		fail "${tag} peels to ${peeled}, not HEAD ${head}"

	pgp_key_id="${CHANVOY_PGP_KEY_ID:-}"
	gpg_homedir="${CHANVOY_GPG_HOMEDIR:-}"
	[ -n "$pgp_key_id" ] || fail "CHANVOY_PGP_KEY_ID is required"
	[ -n "$gpg_homedir" ] || fail "CHANVOY_GPG_HOMEDIR is required"
	[ -d "$gpg_homedir" ] || fail "CHANVOY_GPG_HOMEDIR is not a directory"

	contract_file="keys/expected-fingerprints.txt"
	[ -f "$contract_file" ] || fail "release fingerprint contract not found at ${contract_file}"
	set +e
	contract_values="$(chanvoy_read_expected_contract "$contract_file")"
	contract_status=$?
	set -e
	[ "$contract_status" -eq 0 ] ||
		fail "release fingerprint contract is invalid or incomplete"
	IFS=$'\t' read -r contract_minisign contract_fingerprint <<<"$contract_values"
	[ -n "$contract_minisign" ] && [ -n "$contract_fingerprint" ] ||
		fail "release fingerprint contract parser returned an incomplete record"

	selected_fingerprint="$(
		gpg --homedir "$gpg_homedir" --batch --with-colons --fingerprint "$pgp_key_id" 2>/dev/null |
			awk -F: '$1 == "pub" { seen_pub = 1; next } seen_pub && $1 == "fpr" { print $10; exit }'
	)"
	[ -n "$selected_fingerprint" ] ||
		fail "could not resolve the pinned primary fingerprint for CHANVOY_PGP_KEY_ID"
	[ "$selected_fingerprint" = "$contract_fingerprint" ] ||
		fail "selected GPG primary fingerprint ${selected_fingerprint} does not match release contract ${contract_fingerprint}"

	set +e
	verify_output="$(GNUPGHOME="$gpg_homedir" git verify-tag --raw "$tag" 2>&1)"
	verify_status=$?
	set -e
	[ "$verify_status" -eq 0 ] || {
		echo "$verify_output" >&2
		fail "signature verification failed for ${tag}"
	}
	printf '%s\n' "$verify_output" |
		awk -v expected="$contract_fingerprint" '
            $1 == "[GNUPG:]" && $2 == "VALIDSIG" &&
            ($3 == expected || $NF == expected) { found = 1 }
            END { exit(found ? 0 : 1) }
        ' ||
		fail "${tag} was not signed by the contracted primary fingerprint ${contract_fingerprint}"
}

case "$mode" in
pre-create)
	if git show-ref --verify --quiet "refs/tags/${tag}"; then
		fail "tag ${tag} already exists locally"
	fi
	if [ -n "$(git tag --points-at HEAD)" ]; then
		fail "HEAD already has an exact tag; release-tag requires an untagged release commit"
	fi
	require_origin_absent
	echo "[ok] release tag pre-create guard passed (${tag} at ${head})"
	;;
post-create)
	require_exact_signed_tag
	echo "[ok] release tag post-create guard passed (${tag} at ${head})"
	;;
pre-push)
	require_exact_signed_tag
	require_origin_absent
	echo "[ok] release tag pre-push guard passed (${tag} at ${head})"
	;;
esac
