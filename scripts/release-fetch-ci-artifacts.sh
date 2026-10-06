#!/usr/bin/env bash
# Maintainer-side exact artifact download from the verified release workflow.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
CHANVOY_RELEASE_TAG="$(release_tag "${1:-}")"
export CHANVOY_RELEASE_TAG
dest="${2:-dist/release}"
[[ -z "${GH_REPO+x}" ]] || { echo 'error: GH_REPO must be unset' >&2; exit 1; }
python3 - "$dest" <<'PY'
import pathlib
import sys
p = pathlib.Path(sys.argv[1]).absolute()
if any(a.is_symlink() for a in (p, *p.parents)) or p.exists() and not p.is_dir():
    raise SystemExit('error: unsafe artifact staging directory')
receipt = pathlib.Path(str(p) + '.anchor')
if receipt.exists() or receipt.is_symlink():
    raise SystemExit('error: staging receipt exists; inspect before restaging')
if p.exists() and any(p.iterdir()):
    raise SystemExit('error: staging directory is not empty')
PY
anchor="$(mktemp "${TMPDIR:-/tmp}/chanvoy-ci-anchor.XXXXXX")"
trap 'rm -f "$anchor"' EXIT
CHANVOY_ANCHOR_OUT="$anchor" bash "$root/scripts/release-verify-published-tag.sh" >/dev/null
commit="$(awk -F= '$1=="commit" {print $2}' "$anchor")"
object="$(awk -F= '$1=="object" {print $2}' "$anchor")"
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$object" =~ ^[0-9a-f]{40}$ ]] || { echo 'error: verified tag anchor missing' >&2; exit 1; }
runs="$(gh run list --repo "$CHANVOY_REPOSITORY" --workflow release.yml --branch "$CHANVOY_RELEASE_TAG" \
	--json databaseId,headSha,conclusion,event)" || { echo 'error: release run listing failed' >&2; exit 1; }
matching="$(jq -c --arg commit "$commit" '[.[] | select(.headSha == $commit and .conclusion == "success" and .event == "push") | .databaseId]' <<<"$runs")"
[[ "$(jq 'length' <<<"$matching")" == 1 ]] || { echo 'error: expected exactly one successful push release run at tagged commit' >&2; exit 1; }
run="$(jq -r '.[0]' <<<"$matching")"
[[ "$run" =~ ^[1-9][0-9]*$ ]] || { echo 'error: invalid release run identity' >&2; exit 1; }
mkdir -p "$dest"
gh run download "$run" --repo "$CHANVOY_REPOSITORY" --name "release-packages-$CHANVOY_RELEASE_TAG" --dir "$dest" || {
	echo 'error: artifact download failed; inspect partial staging before retrying' >&2; exit 1;
}
bash "$root/scripts/validate-release-assets.sh" "$dest" base >/dev/null
printf 'tag=%s\nobject=%s\ncommit=%s\nrun=%s\n' "$CHANVOY_RELEASE_TAG" "$object" "$commit" "$run" >"$dest.anchor"
# Recheck the complete run and remote tag after downloading, before downstream use.
require_release_guard "$dest" >/dev/null
echo '[ok] exact CI base inventory staged with external complete receipt'
