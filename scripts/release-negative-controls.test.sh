#!/usr/bin/env bash
# Regression for forbidden workflow authority and accidental package publication.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
python3 - "$root" <<'PY'
import pathlib
import re
import sys
root = pathlib.Path(sys.argv[1])
for path in (root / '.github/workflows').glob('*.yml'):
    text = path.read_text()
    forbidden = (r'cargo\s+(publish|login|yank|owner)\b', r'CARGO_REGISTR(?:Y|IES_[A-Z0-9_]+)_TOKEN',
                 r'secrets\.', r'CHANVOY_(MINISIGN_KEY|PGP_KEY_ID|GPG_HOMEDIR)',
                 r'^\s*[a-z-]+:\s*write\b', r'^\s*permissions:\s*write-all\b')
    if any(re.search(p, text, re.MULTILINE) for p in forbidden):
        raise SystemExit('error: forbidden workflow publication/signing authority')
ci = (root / '.github/workflows/check.yml').read_text()
if 'release_tooling:' not in ci or 'make release-tooling-test' not in ci:
    raise SystemExit('error: pull requests must exercise synthetic release-tooling gates')
workflow = (root / '.github/workflows/release.yml').read_text()
if 'needs: [validate, build, sbom]' not in workflow:
    raise SystemExit('error: artifact packaging must require exact signature/native/SBOM qualification')
for name in ('release-create-draft.sh', 'upload-release-assets.sh', 'release-publish.sh'):
    text = (root / 'scripts' / name).read_text()
    if '--repo "$CHANVOY_REPOSITORY"' not in text or 'require_release_guard "$directory"' not in text:
        raise SystemExit('error: direct publication entrypoint lacks fixed repository/receipt guard')
    if '--clobber' in text or re.search(r'gh release [^\n]*\*', text):
        raise SystemExit('error: broad/replacing release upload forbidden')
publish = (root / 'scripts/release-publish.sh').read_text()
if 'CHANVOY_CONFIRM_PUBLISH' not in publish or 'release-verify-draft.sh' not in publish:
    raise SystemExit('error: separate promotion cue and fresh-download proof required')
PY
echo '[ok] no workflow signing/publication credentials; direct publication guards remain'
