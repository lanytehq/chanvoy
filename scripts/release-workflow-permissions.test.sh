#!/usr/bin/env bash
# Tag-defined workflows must never receive repository write/signing authority.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
python3 - "$root/.github/workflows/release.yml" <<'PY'
import re
import sys
text = open(sys.argv[1], encoding='utf-8').read()
errors = []
for match in re.finditer(r'^\s*([a-z-]+):\s*write\b', text, re.MULTILINE):
    errors.append(f'write permission granted: {match.group(1)}')
if re.search(r'^\s*permissions:\s*write-all\b', text, re.MULTILINE):
    errors.append('write-all permissions granted')
for banned in ('action-gh-release', 'gh release ', 'releases/', 'secrets.',
               'release-create-draft', 'release-publish', 'release-push-tag',
               'release-sign', 'git push', 'CHANVOY_MINISIGN_KEY', 'CHANVOY_GPG_HOMEDIR'):
    if banned in text:
        errors.append(f'publication/signing construct present: {banned!r}')
if not re.search(r'^permissions:\s*\n\s+contents:\s*read\b', text, re.MULTILINE):
    errors.append('workflow permissions must be contents: read')
for construct in ('persist-credentials: false', 'release-restore-tag-ref.sh',
                  'verify-pinned-tag.sh', 'release-packages-', 'validate-release-assets.sh',
                  'EXPECTED_TARGET', 'cyclonedx-json', 'needs.validate.outputs.commit'):
    if construct not in text:
        errors.append(f'required release invariant absent: {construct}')
if errors:
    raise SystemExit('error: ' + '; '.join(errors))
PY
echo '[ok] release workflow is artifact-only, read-only and contains no signing input'
