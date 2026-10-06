#!/usr/bin/env bash
# Final pre-tag public metadata and maintainer readiness checks.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
bash scripts/release-tag-operator.sh preflight
bash scripts/release-guard-github-release.sh "$CHANVOY_RELEASE_TAG"
notes="docs/releases/$CHANVOY_RELEASE_TAG.md"
[[ -s "$notes" ]] || { echo 'error: release notes missing' >&2; exit 1; }
if grep -Eiq '^\*\*Release Date\*\*:[[:space:]]*unreleased[[:space:]]*$' "$notes" ||
	grep -Eiq "^## \\[${CHANVOY_RELEASE_TAG#v}\\] - unreleased[[:space:]]*$" CHANGELOG.md; then
	echo 'error: finalize the actual release date before tagging' >&2; exit 1
fi
for file in LICENSE LICENSE-MIT LICENSE-APACHE; do
	[[ -s "$file" ]] || { echo 'error: public license missing' >&2; exit 1; }
done
echo '[ok] preflight passed; live smoke must also pass before the separate tag cue'
