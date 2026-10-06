#!/usr/bin/env bash
# Complete receipt continuity: tag, object, commit and successful build run.
# Adapted from the CI fetch/draft binding; receipt stays outside the upload set.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=release-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-common.sh"
directory="${1:-dist/release}"
tag="$(release_tag)"
[[ -z "${GH_REPO+x}" ]] || { echo 'error: GH_REPO must be unset' >&2; exit 1; }
fields="$(python3 - "$directory" "$tag" <<'PY'
import pathlib
import re
import sys
directory = pathlib.Path(sys.argv[1]).absolute()
receipt = pathlib.Path(str(directory) + '.anchor')
for path in (directory, receipt):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SystemExit('error: unsafe staging path')
if not directory.is_dir() or not receipt.is_file():
    raise SystemExit('error: staging directory and external receipt required')
try:
    lines = receipt.read_text().splitlines()
except (UnicodeError, OSError):
    raise SystemExit('error: unreadable staging receipt') from None
if len(lines) != 4:
    raise SystemExit('error: receipt requires exactly tag, object, commit and run')
data = {}
for line in lines:
    key, sep, value = line.partition('=')
    if not sep or key in data:
        raise SystemExit('error: malformed or duplicate receipt field')
    data[key] = value
if set(data) != {'tag', 'object', 'commit', 'run'} or data['tag'] != sys.argv[2]:
    raise SystemExit('error: wrong receipt tag or field set')
if any(not re.fullmatch('[0-9a-f]{40}', data[k]) for k in ('object', 'commit')):
    raise SystemExit('error: malformed staged object or commit')
if not re.fullmatch('[1-9][0-9]*', data['run']):
    raise SystemExit('error: malformed staged workflow run')
print(data['object'], data['commit'], data['run'])
PY
)"
read -r object commit run <<<"$fields"
CHANVOY_EXPECTED_TAG_OBJECT="$object" CHANVOY_EXPECTED_COMMIT="$commit" \
	bash "$root/scripts/release-verify-published-tag.sh" >/dev/null || {
	echo 'error: published tag no longer matches staging receipt' >&2
	exit 1
}
run_json="$(gh api "repos/$CHANVOY_REPOSITORY/actions/runs/$run")" || {
	echo 'error: staged workflow run unavailable; inspect before retrying' >&2
	exit 1
}
jq -e --arg run "$run" --arg tag "$tag" --arg commit "$commit" --arg repo "$CHANVOY_REPOSITORY" \
	'(.id | tostring) == $run and .head_sha == $commit and .head_branch == $tag and
    .event == "push" and .status == "completed" and .conclusion == "success" and
    .path == ".github/workflows/release.yml" and .repository.full_name == $repo' \
	<<<"$run_json" >/dev/null || {
	echo 'error: staged run does not match release workflow, repository, tag and commit' >&2
	exit 1
}
echo '[ok] complete staged receipt matches published tag and successful release run'
