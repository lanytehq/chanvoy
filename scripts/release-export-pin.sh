#!/usr/bin/env bash
# Maintainer-only export of the public portion of an existing approved key.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
[[ $# == 0 || ( $# == 2 && "$1" == --replace-from ) ]] || {
	echo 'usage: release-export-pin.sh [--replace-from <current-pin-sha256>]' >&2; exit 2;
}
expected="${2:-}"
[[ "${CHANVOY_GPG_SIGNING_FINGERPRINT:-}" =~ ^[0-9A-F]{40}$ &&
	"${CHANVOY_PGP_KEY_ID:-}" =~ ^[0-9A-F]{40}!$ && -n "${CHANVOY_GPG_HOMEDIR:-}" &&
	-n "${CHANVOY_MINISIGN_PUB:-}" ]] || { echo 'error: approved existing signing inputs required' >&2; exit 1; }
python3 - "$root" "$CHANVOY_GPG_HOMEDIR" "$HOME/.gnupg" <<'PY'
import pathlib
import sys
root, home, default = map(pathlib.Path, sys.argv[1:])
if not home.is_absolute() or not home.is_dir() or any(a.is_symlink() for a in (home, *home.parents)):
    raise SystemExit('error: explicit external nonsymlink GPG home required')
if home.resolve() == default.resolve() or home.resolve().is_relative_to(root):
    raise SystemExit('error: default or repository-local GPG home not approved for export')
dest = root / 'docs/security'
if any(a.is_symlink() for a in (dest, *dest.parents)):
    raise SystemExit('error: unsafe public pin destination')
PY
pin="$root/docs/security/release-signing-keys.asc"
if [[ -e "$pin" || -L "$pin" ]]; then
	[[ -f "$pin" && ! -L "$pin" && "$expected" =~ ^[0-9a-f]{64}$ &&
		"$(shasum -a 256 "$pin" | awk '{print $1}')" == "$expected" ]] || {
		echo 'error: public pin exists; replacement requires reviewed --replace-from SHA256' >&2; exit 1;
	}
elif [[ -n "$expected" ]]; then
	echo 'error: replacement requires an existing pin' >&2; exit 1
fi
scratch="$(mktemp "${TMPDIR:-/tmp}/pg-export.XXXXXX")"
trap 'rm -f "$scratch"' EXIT
gpg --homedir "$CHANVOY_GPG_HOMEDIR" --batch --armor --export "$CHANVOY_PGP_KEY_ID" >"$scratch" 2>/dev/null || {
	echo 'error: public export failed' >&2; exit 1;
}
# Inspect the public-only export and companion minisign input before creating
# the destination, including the independently selected primary/subkey.
bash "$root/scripts/release-validate-pin.sh" "$scratch" >/dev/null
mkdir -p "$root/docs/security"
if [[ -n "$expected" ]]; then
	[[ "$(shasum -a 256 "$pin" | awk '{print $1}')" == "$expected" ]] || {
		echo 'error: prior pin changed during export' >&2; exit 1;
	}
	# Same-directory rename publishes only the already validated complete export.
	(set -C; cat "$scratch" >"$pin.new")
	mv "$pin.new" "$pin"
	exit 0
fi
set -C
cat "$scratch" >"$pin" || {
	echo 'error: public export failed; inspect newly created pin before retrying' >&2; exit 1;
}
bash "$root/scripts/release-validate-pin.sh"
