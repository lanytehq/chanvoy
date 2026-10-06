#!/usr/bin/env bash
# Install the complete public pair with rollback on failure or interruption.
set -euo pipefail
staging="${1:?staging directory required}"
dest="${2:?destination required}"
shift 2
python3 - "$staging" "$dest" <<'PY'
import pathlib
import sys
staging, dest = map(pathlib.Path, sys.argv[1:])
if any(p.is_symlink() for p in (dest, *dest.parents)):
    raise SystemExit('error: unsafe anchor destination')
for name in ('expected-fingerprints.txt', 'expected-fingerprints.ndjson'):
    source, target = staging / name, dest / name
    if not source.is_file() or source.is_symlink() or not source.stat().st_size:
        raise SystemExit('error: complete regular anchor pair required')
    if target.is_symlink() or target.exists() and not target.is_file():
        raise SystemExit('error: unsafe existing anchor')
    tmp = dest / (name + '.new')
    if tmp.exists() or tmp.is_symlink():
        raise SystemExit('error: ambiguous prior anchor installation; inspect before retrying')
PY
mkdir -p "$dest"
backup="$(mktemp -d "${TMPDIR:-/tmp}/pg-anchor-backup.XXXXXX")"
for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
	if [[ -f "$dest/$name" ]]; then cp "$dest/$name" "$backup/$name"; fi
done
rollback() {
	trap - EXIT INT TERM HUP
	for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
		rm -f "$dest/$name.new"
		if [[ -f "$backup/$name" ]]; then mv -f "$backup/$name" "$dest/$name"; else rm -f "$dest/$name"; fi
	done
	rm -rf "$backup"
}
trap rollback EXIT
trap 'rollback; exit 130' INT
trap 'rollback; exit 143' TERM
trap 'rollback; exit 129' HUP
for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
	(set -C; cat "$staging/$name" >"$dest/$name.new")
done
mv -f "$dest/expected-fingerprints.ndjson.new" "$dest/expected-fingerprints.ndjson"
[[ "${CHANVOY_TEST_FAIL_ANCHOR_INSTALL:-0}" == 0 ]] || { echo 'error: interrupted pair installation' >&2; exit 1; }
mv -f "$dest/expected-fingerprints.txt.new" "$dest/expected-fingerprints.txt"
if [[ $# -gt 0 ]]; then "$@"; fi
trap - EXIT INT TERM HUP
rm -rf "$backup"
