#!/usr/bin/env bash
# Emit exact tag message bytes excluding the armored signature.
set -euo pipefail
object="${1:?tag object required}"
[[ "$(git cat-file -t "$object" 2>/dev/null)" == tag ]] || { echo 'error: annotated tag required' >&2; exit 1; }
git cat-file tag "$object" | python3 -c '
import sys
raw = sys.stdin.buffer.read()
if b"\n\n" not in raw:
    raise SystemExit("error: malformed tag object")
body = raw.split(b"\n\n", 1)[1]
marker = b"-----BEGIN PGP SIGNATURE-----\n"
if body.startswith(marker):
    body = b""
else:
    offset = body.find(b"\n" + marker)
    if offset >= 0:
        body = body[:offset + 1]
sys.stdout.buffer.write(body)
'
