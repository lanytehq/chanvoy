#!/usr/bin/env bash
# Verify published tag identity from the trusted checkout, independent of HEAD.
set -euo pipefail
die() { echo "error: $*" >&2; exit 1; }
tag="${CHANVOY_RELEASE_TAG:-}"
approved="${CHANVOY_GPG_SIGNING_FINGERPRINT:-}"
[[ "$approved" =~ ^[0-9A-F]{40}$ ]] || die 'independently approved primary required (40 uppercase hex)'
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'canonical CHANVOY_RELEASE_TAG required'
[[ -z "${RELEASE_TAG:-}" || "$RELEASE_TAG" == "$tag" ]] || die 'release tag aliases disagree'
[[ -z "${GH_REPO+x}" ]] || die 'GH_REPO must be unset; repository authority is fixed'
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
case "$(git config --get remote.origin.url)" in
	https://github.com/lanytehq/chanvoy | https://github.com/lanytehq/chanvoy.git | \
		git@github.com:lanytehq/chanvoy | git@github.com:lanytehq/chanvoy.git) ;;
	git@*:lanytehq/chanvoy | git@*:lanytehq/chanvoy.git)
		origin="$(git config --get remote.origin.url)"
		host="${origin#git@}"; host="${host%%:*}"
		[[ "$(ssh -G "$host" 2>/dev/null | awk '$1=="hostname" {print $2;exit}')" == github.com ]] ||
			die 'origin SSH alias must resolve to github.com' ;;
	*) die 'origin must be lanytehq/chanvoy on GitHub' ;;
esac
remote_object="$(git ls-remote --exit-code origin "refs/tags/$tag" | awk '{print $1}')" || die 'remote release tag is absent'
[[ "$remote_object" =~ ^[0-9a-f]{40}$ ]] || die 'remote tag object is invalid'
expected_object="${CHANVOY_EXPECTED_TAG_OBJECT:-}"
expected_commit="${CHANVOY_EXPECTED_COMMIT:-}"
if [[ -n "$expected_object" || -n "$expected_commit" ]]; then
	[[ "$expected_object" =~ ^[0-9a-f]{40}$ && "$expected_commit" =~ ^[0-9a-f]{40}$ ]] || die 'expected tag object and commit must both be 40-hex'
	[[ "$remote_object" == "$expected_object" ]] || die 'remote tag object differs from the verified anchor'
fi
git fetch --quiet --no-tags origin "+refs/tags/$tag:refs/tags/$tag"
[[ "$(git rev-parse "refs/tags/$tag")" == "$remote_object" ]] || die 'local tag differs from remote tag object'
[[ "$(git cat-file -t "$remote_object")" == tag ]] || die 'annotated tag required'
commit="$(git rev-parse "refs/tags/$tag^{}")"
[[ -z "$expected_commit" || "$commit" == "$expected_commit" ]] || die 'tagged commit differs from the verified anchor'
pinned="$(git cat-file blob "$commit:keys/expected-fingerprints.txt" 2>/dev/null | awk '$1=="gpg" {print $2}')" || die 'tagged commit has no fingerprint anchors'
[[ "$pinned" == "$approved" ]] || die 'pin in the tagged commit is not the approved primary'
CHANVOY_RELEASE_TAG="$tag" bash "$root/scripts/verify-pinned-tag.sh" --published >/dev/null || die 'published tag signature does not verify against committed pin'
tag_json="$(gh api "repos/lanytehq/chanvoy/git/tags/$remote_object")" || die 'GitHub tag object unavailable'
jq -e --arg tag "$tag" --arg commit "$commit" \
	'.tag == $tag and .object.type == "commit" and .object.sha == $commit and .verification.verified == true and .verification.reason == "valid"' \
	<<<"$tag_json" >/dev/null || die 'GitHub does not report the published tag verified'
echo "[ok] published $tag object $remote_object verified against committed pin and GitHub"
if [[ -n "${CHANVOY_ANCHOR_OUT:-}" ]]; then
	[[ ! -L "$CHANVOY_ANCHOR_OUT" ]] || die 'unsafe anchor output'
	printf 'tag=%s\nobject=%s\ncommit=%s\n' "$tag" "$remote_object" "$commit" >"$CHANVOY_ANCHOR_OUT"
fi
